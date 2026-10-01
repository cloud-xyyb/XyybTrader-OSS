#include "state/AccountStateStore.h"

#include "XtFixedPoint.h"
#include "state/AccountState.h"
#include "state/AccountReducer.h"
#include "xt_account/api/AccountSnapshot.h"
#include "state/MarkPriceTable.h"
#include "xt_account/api/AccountMarking.h"
#include "xt_config/catalog/XtCommodityCatalog.h"
#include "xt_util/time/XtSteadyClock.h"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace xt_account_store_drops {
std::atomic<uint64_t>& cross_thread_write() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

std::atomic<uint64_t>& snapshot_publish_skipped() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

std::atomic<uint64_t>& publish_post_failed() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

std::atomic<uint64_t>& position_slot_overflow() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}
}  // namespace xt_account_store_drops

namespace xt_account {

namespace {

using PositionPool = xt_util::sync::XtSnapshotPool<PositionSnapshot, kMaxPositionSlots * 4>;
using FullPool = xt_util::sync::XtSnapshotPool<FullSnapshot, 64>;

/// @brief 批次发布抑制 RAII；异常路径也保证复位（apply 链可抛）。
///
/// 析构硬置 false 而不恢复旧值：抑制闩只服务 on_position_snapshot 的单层批次，
/// 嵌套会让「批尾统一发布一次」失去唯一收敛点，构造期直接拒绝。
struct PublishSuppressScope {
    explicit PublishSuppressScope(bool& flag) noexcept : flag_(flag) {
        assert(!flag_ && "PublishSuppressScope must not nest");
        flag_ = true;
    }
    ~PublishSuppressScope() { flag_ = false; }
    PublishSuppressScope(const PublishSuppressScope&) = delete;
    PublishSuppressScope& operator=(const PublishSuppressScope&) = delete;

    bool& flag_;
};

// SnapshotPool 可能因 retired list 未扫描而暂时耗尽；失败时主动 scan 一次再重试。
// 调用方仍需处理 nullptr。
template <typename Pool>
auto acquire_after_reclaim_scan(Pool& pool, xt_util::sync::HazardDomain& domain) noexcept
    -> decltype(pool.acquire()) {
    auto* fresh = pool.acquire();
    if (!fresh) {
        domain.try_scan();
        fresh = pool.acquire();
    }
    return fresh;
}

// HazardDomain retire 回调；ctx 指向 PositionSnapshot pool。
void release_position_snapshot(void* ctx, const void* ptr) noexcept {
    static_cast<PositionPool*>(ctx)->release(const_cast<PositionSnapshot*>(
        static_cast<const PositionSnapshot*>(ptr)));
}

// HazardDomain retire 回调；ctx 指向 FullSnapshot pool。
void release_full_snapshot(void* ctx, const void* ptr) noexcept {
    static_cast<FullPool*>(ctx)->release(const_cast<FullSnapshot*>(
        static_cast<const FullSnapshot*>(ptr)));
}

// 快照池取不到帧/slot：整次发布跳过（读者保持上一自洽帧），计数 + log-once
// （持续耗尽时靠 counter 观测，不按事件刷屏）。
void note_publish_skipped(const char* what) noexcept {
    xt_account_store_drops::snapshot_publish_skipped().fetch_add(1, std::memory_order_relaxed);
    static std::atomic<bool> warned{false};
    if (!warned.exchange(true)) {
        std::fprintf(stderr,
                     "AccountStateStore %s snapshot pool exhausted; publish skipped "
                     "(further skips counted, not logged)\n",
                     what);
    }
}

// 按世代的 slot 顺序复制 code 列表；by_slot 是取 code 的唯一途径（读者不触 deque）。
std::vector<std::string> copy_generation_codes(const SymbolGeneration& gen) {
    std::vector<std::string> out;
    out.reserve(gen.n);
    for (uint32_t i = 0; i < gen.n; ++i) {
        out.emplace_back(*gen.by_slot[i]);
    }
    return out;
}

// 只读审计 helper：比较账户冻结总额与订单冻结 slot 累计值，不修正状态。
void audit_frozen_direct(int64_t frozen_cash_fp,
                         const std::unordered_map<uint64_t, AccountState::FrozenOrderSlot>& slots,
                         account_audit::XtFrozenAuditReport& out) {
    out.ok = true;
    out.reason.clear();
    out.frozen_cash = xt_fp::from_fp(frozen_cash_fp);

    int64_t total_fp = 0;
    for (const auto& kv : slots) {
        total_fp += kv.second.frozen;
    }
    out.frozen_cash_by_orders = xt_fp::from_fp(total_fp);

    if (frozen_cash_fp < 0) {
        out.ok = false;
        out.reason = "frozen_cash negative";
        return;
    }
    if (total_fp == frozen_cash_fp) return;

    out.ok = false;
    std::ostringstream oss;
    oss << "frozen_cash mismatch runtime=" << out.frozen_cash
        << " tracked=" << out.frozen_cash_by_orders
        << " slot_count=" << slots.size()
        << " slots=[";
    constexpr std::size_t kMaxDumpSlots = 64;
    std::size_t dumped = 0;
    for (const auto& kv : slots) {
        if (dumped >= kMaxDumpSlots) break;
        if (dumped > 0) oss << ",";
        oss << "{oid=" << kv.first
            << " frozen=" << xt_fp::from_fp(kv.second.frozen)
            << " filled=" << xt_fp::from_fp(kv.second.filled)
            << " per_unit=" << xt_fp::from_fp(kv.second.per_unit)
            << "}";
        ++dumped;
    }
    if (slots.size() > kMaxDumpSlots) {
        oss << ",... omitted=" << (slots.size() - kMaxDumpSlots);
    }
    oss << "]";
    out.reason = oss.str();
}

}  // namespace

// 事件落账顺序固定：before-apply 只做 symbol 收编/resolve；reducer 修改 AccountState
// 之后 after-apply 按持仓终态派生 cost hint。build_*_snapshot_ 读取的是同一事件落地
// 后的账户状态和 mark 状态。

AccountStateStore::AccountStateStore(const AccountState::Config& cfg)
    : account_(cfg), mark_cache_() {}

void AccountStateStore::apply(const XtAccountEvent& ev) {
    guard_worker_thread_();
    on_event_before_apply(ev);
    apply_event(account_, ev);
    on_event_after_apply(ev);

    // 首个 FullSnapshot 发布前，读侧仍从 writer state 构建；之后才走 partial publish。
    if (current_.load(std::memory_order_acquire)) {
        publish_partial_(ev);
        hp_domain_.try_scan_if_due();
    }
}

void AccountStateStore::on_position_snapshot(
    const std::vector<XtPositionSnapshot>& positions,
    bool is_recovery,
    const XtCommodityCatalog& catalog) {
    guard_worker_thread_();
    // slot admission 先于 reset/重建序列：批内每条 SetPosition 都要在 apply 前拿到
    // slot，逐条追加会造 N 个世代；整批一次锁内追加只造一个世代，读侧也只看到一次
    // 成员面跃迁。未冻结（回测）时 slot 语义未启用，惰性 universe 承担收编。
    if (symbols_frozen_.load(std::memory_order_acquire)) {
        admit_recovery_symbols_(positions);
    }
    {
        // 批次内抑制中间帧发布：reset 帧是「冻结清零 + 持仓全清」的自相矛盾中间态，
        // 逐 symbol 的 set_position 帧是「只重建了一部分」的半成品。风控评估跑在提交
        // 线程（panel manual / force-exit / closeout / watchdog）而非 worker，能落进这个
        // 窗口读到零冻结 → available_cash 高估 → margin 判定 fail-open。
        // 抑制期间读者持续看到上一批次的自洽帧，批尾统一发布一次。
        PublishSuppressScope suppress(suppress_partial_publish_);
        if (is_recovery) {
            // 持仓 recovery 只替换 pos/order frozen；资金由 SetCash/account truth 单独锚定。
            apply(XtAccountEvent::reset());
        }
        for (const auto& p : positions) {
            if (p.code.empty()) continue;
            const XtCommodityInfo* cinfo = catalog.find(p.code.c_str());
            const double vol_scale =
                (cinfo && std::isfinite(cinfo->vol_scale) && cinfo->vol_scale > 0.0)
                    ? cinfo->vol_scale : 1.0;
            const int64_t long_qty_fp   = xt_fp::to_fp(p.long_qty);
            const int64_t short_qty_fp  = xt_fp::to_fp(p.short_qty);
            const int64_t long_avg_fp   = xt_fp::to_fp(p.long_avg  / vol_scale);
            const int64_t short_avg_fp  = xt_fp::to_fp(p.short_avg / vol_scale);
            // 成本价 fallback 不在这里派生：apply 之后由 derive_cost_hint_ 按持仓终态
            // 统一重算（单腿 → 该腿均价、双腿锁仓 / 空仓 → 显式 0）。均价入账前已按
            // vol_scale 归一化，派生出的 hint 与 mark price 因此同量纲。
            // broker frozen / 今昨仓随 SetPosition 入账；非 CTP/回测字段通常为 0。
            apply(XtAccountEvent::set_position(
                p.code.c_str(),
                long_qty_fp,  long_avg_fp,
                short_qty_fp, short_avg_fp,
                /*last_price=*/0,
                xt_fp::to_fp(p.long_today), xt_fp::to_fp(p.short_today),
                xt_fp::to_fp(p.long_frozen), xt_fp::to_fp(p.short_frozen),
                static_cast<int64_t>(p.broker_frozen_last_update_ms)));
        }
    }
    if (current_.load(std::memory_order_acquire)) {
        // 批次结束后全量发布一次，让 account/position 回到最终快照状态。
        (void)publish_all_snapshots_();
        hp_domain_.try_scan_if_due();
    }
    if (is_recovery) {
        // 首次 recovery 完成后 closeable 字段才可信；完成前风控 fail-closed。
        // 置信标记必须晚于本批持仓帧发布：snapshot_position 把该账户级 atomic 覆盖进
        // 每个返回值，先置 true 会让读者在尚未重建持仓的上一帧上看到 closeable 可信。
        set_closeable_known(true);
    }
}

void AccountStateStore::bind_worker_thread() noexcept {
    bind_worker_thread(std::this_thread::get_id());
}

void AccountStateStore::bind_worker_thread(std::thread::id tid) noexcept {
    // 先写 tid 再 release 发布 bound；reader（guard）acquire load bound 后读 tid。
    // 重绑（bootstrap main → loop-start worker）依赖 quiescent 契约，见 header @pre。
    worker_tid_ = tid;
    worker_quiescent_.store(false, std::memory_order_release);  // 新写者期：复位 shutdown 闩
    worker_bound_.store(true, std::memory_order_release);
}

void AccountStateStore::on_session_begin_hook(int64_t equity_fp) noexcept {
    // 保存日内亏损风控的起始 equity；风控直接读该值，不等下一次 snapshot 发布。
    // 如果 session_begin 重复到达，后一次覆盖前一次。
    starting_balance_fp_.store(equity_fp, std::memory_order_release);
}

int64_t AccountStateStore::starting_balance_fp() const noexcept {
    return starting_balance_fp_.load(std::memory_order_acquire);
}

void AccountStateStore::set_closeable_known(bool known) noexcept {
    closeable_known_.store(known, std::memory_order_release);
}

void AccountStateStore::unbind_worker_thread() noexcept {
    // 测试 fixture 可能在 main 线程创建 store、worker 线程首次 apply；
    // 清空绑定后下一次 guard_worker_thread_() 按 first-touch 重新绑定。
    worker_bound_.store(false, std::memory_order_release);
    write_first_touch_tid_.store(std::thread::id{}, std::memory_order_release);
    worker_quiescent_.store(false, std::memory_order_release);
}

void AccountStateStore::enter_quiescent() noexcept {
    worker_quiescent_.store(true, std::memory_order_release);
}

bool AccountStateStore::insert_symbol_sorted_(std::string_view code) {
    const auto it = std::lower_bound(symbols_sorted_.begin(), symbols_sorted_.end(), code,
                                     [](const std::string& lhs, std::string_view rhs) {
                                         return std::string_view(lhs) < rhs;
                                     });
    if (it != symbols_sorted_.end() && std::string_view(*it) == code) {
        return false;
    }
    symbols_sorted_.emplace(it, code);
    return true;
}

void AccountStateStore::register_symbol_(std::string_view code) {
    // 冻结后再注册意味着 bootstrap 时序错了：静默 return 会让该 symbol 对读侧
    // 永久不可见（无 slot、snapshot_position 恒空），必须在启动期就炸出来。
    if (symbols_frozen_.load(std::memory_order_acquire)) {
        throw std::logic_error(
            "AccountStateStore::register_symbol_: symbol set already frozen, code=" +
            std::string(code));
    }
    if (code.empty()) {
        return;
    }
    // 容量检查先于插入：插入后再抛会把超限的 code 留在集合里，
    // caller 若吞掉异常就带着污染集合继续 bootstrap。
    if (symbols_sorted_.size() >= kMaxDeclaredSymbols && !is_declared_(code)) {
        throw std::invalid_argument(
            "AccountStateStore::register_symbol_: declared symbol count would exceed "
            "kMaxDeclaredSymbols " +
            std::to_string(kMaxDeclaredSymbols) + ", code=" + std::string(code));
    }
    insert_symbol_sorted_(code);
    // 同步预热 mark slot，避免首 tick 在热路径插入 registry。
    // intern 表满 ⇒ 该 symbol 永远没有 mark id，估值恒 0 且不可修复：bootstrap 期拒起。
    if (!mark_cache_.pre_warm_one(code)) {
        throw std::invalid_argument(
            "AccountStateStore::register_symbol_: instrument intern table exhausted at " +
            std::to_string(xt_core::kMaxInstruments) + " codes, code=" + std::string(code));
    }
}

void AccountStateStore::freeze_symbols_() {
    // 1) 封口 declared 集合：之后未知 code 不再进入该集合，也不再经事件入口 intern。
    // 先置 frozen 再取 gen-0 帧：gen-0 取不到帧时抛出，store 停在「已封口但无可读帧」
    // 且无法重试。caller 全是 bootstrap composition root，throw 即拒起并销毁 store，
    // 接受该单向态而不为此拆两段状态机。
    if (symbols_frozen_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    mark_cache_.freeze();
    // 截断会让超出的 symbol 对读侧永久不可见（money-path 静默退化）；宁可拒起。
    // register_symbol_ 已在插入前拦截超限，这里是纵深防御（含绕过该注册入口的路径）。
    if (symbols_sorted_.size() > kMaxDeclaredSymbols) {
        throw std::invalid_argument(
            "AccountStateStore::freeze_symbols_: declared symbol count " +
            std::to_string(symbols_sorted_.size()) + " exceeds kMaxDeclaredSymbols " +
            std::to_string(kMaxDeclaredSymbols));
    }

    // 2) 启用 slot 语义：发布 gen-0 空世代。declared symbol 不预占 slot——PositionUniverse
    //    的成员只来自三个创建源，declared 集合规模（≤7168）与 slot 容量（1024）是两条
    //    独立的容量线。未 touched 的 declared symbol 读侧返回零仓，与零值 slot 值级等价。
    symbol_table_.initialize();
    admit_pre_freeze_positions_();

    // 3) 申请 gen-0 FullSnapshot；current_ 从 nullptr 切到可读快照。
    auto* fresh = acquire_after_reclaim_scan(full_pool_, hp_domain_);
    if (!fresh) {
        // fresh pool 上取不到首帧属结构性不可达；真发生则 store 永久停在
        // pre-publish 态（所有读侧退化为直读 writer state），必须拒起。
        throw std::logic_error(
            "AccountStateStore::freeze_symbols_: full snapshot pool exhausted at gen-0");
    }
    fresh->account = build_account_snapshot_();
    // 世代 immutable 且进程内常驻，发布帧存指针即可（不逐帧拷贝）。
    fresh->universe = symbol_table_.active();
    fresh->reset_slots();

    // 4) 为 gen-0 已有的 slot（= 冻结前落账的持仓）建帧内 position slot。
    //    典型 live 时序下 gen-0 为空，本循环零轮。
    const std::size_t n =
        std::min(fresh->universe ? std::size_t{fresh->universe->n} : std::size_t{0},
                 kMaxPositionSlots);
    for (std::size_t i = 0; i < n; ++i) {
        if (auto* pos = acquire_after_reclaim_scan(position_pool_, hp_domain_)) {
            *pos = build_position_snapshot_(*fresh->universe->by_slot[i]);
            fresh->position_slots[i].store(pos, std::memory_order_release);
        } else {
            // fresh pool 上结构性不可达；防御：留 null slot 会让该 symbol 读空快照，
            // 计数 + 落后闩让首个事件的全量重建修复。
            publish_dirty_ = true;
            note_publish_skipped("position");
        }
    }

    // 5) 发布首包；之后 snapshot_* 走 hazard-protected immutable snapshot。
    const auto* old = current_.exchange(fresh, std::memory_order_acq_rel);
    note_published_();  // 首包也是一次成功发布：frame_age 自 bootstrap 起就有基准
    if (old) {
        hp_domain_.retire_raw(old, &full_pool_, release_full_snapshot);
    }
}

bool AccountStateStore::symbols_frozen() const noexcept {
    return symbols_frozen_.load(std::memory_order_acquire);
}

bool AccountStateStore::futures_mode() const noexcept {
    // ctor 后不可变的记账模式标志；直读无需 worker 线程约束。
    return account_.futures_mode();
}

bool AccountStateStore::is_registered_symbol(std::string_view code) const noexcept {
    if (code.empty()) {
        return false;
    }
    // 冻结前 declared 集合仍可变（bootstrap 注册 / 回测惰性收编），跨线程读它是 race；
    // 该阶段没有读侧查询语义，一律 false。
    if (!symbols_frozen_.load(std::memory_order_acquire)) {
        return false;
    }
    return is_in_universe_(code);
}

bool AccountStateStore::is_declared_symbol(std::string_view code) const noexcept {
    if (code.empty()) {
        return false;
    }
    // 与 is_registered_symbol 同一冻结前语义：declared 集合冻结前可变，跨线程读它是 race。
    if (!symbols_frozen_.load(std::memory_order_acquire)) {
        return false;
    }
    return is_declared_(code);
}

bool AccountStateStore::has_position_slot(std::string_view code) const noexcept {
    if (code.empty()) {
        return false;
    }
    // 冻结前 slot 语义未启用（SymbolTable 未 initialize），find_slot 恒不命中；显式短路
    // 保持与 is_registered_symbol / is_declared_symbol 同一「冻结前无读侧语义」口径。
    if (!symbols_frozen_.load(std::memory_order_acquire)) {
        return false;
    }
    return symbol_table_.find_slot(code) != kInvalidSymbolIndex;
}

AccountSnapshot AccountStateStore::snapshot_account() const noexcept {
    auto guard = hp_domain_.protect(current_);
    return guard ? guard->account : build_account_snapshot_();
}

int64_t AccountStateStore::realized_pnl() const noexcept {
    // 用于 writer 线程做 apply 前后 read-delta；跨线程读请用 snapshot_account()。
    return account_.realized_pnl();
}

PositionSnapshot AccountStateStore::snapshot_position(std::string_view code) const noexcept {
    PositionSnapshot out;
    for (;;) {
        auto guard = hp_domain_.protect(current_);
        if (!guard) {
            out = build_position_snapshot_(code);
            break;
        }
        const auto idx = guard->find_symbol_idx(code);
        if (idx == kInvalidSymbolIndex) {
            out = symbols_frozen_.load(std::memory_order_acquire)
                      ? PositionSnapshot{}
                      : build_position_snapshot_(code);
            break;
        }
        // guard 只 pin FullSnapshot 容器；slot protect 后必须确认该容器仍是 current_，
        // 否则旧容器里的 slot 可能已随新发布帧 retire。
        auto pguard = hp_domain_.protect(guard->position_slots[idx]);
        if (current_.load(std::memory_order_acquire) != guard.get()) {
            continue;
        }
        out = pguard ? *pguard : PositionSnapshot{};
        break;
    }
    // closeable_known 是账户级 live atomic，覆盖进每个 PositionSnapshot；
    // set_closeable_known(false) 需要立即影响风控，不等待下一次事件发布。
    out.closeable_known = closeable_known_.load(std::memory_order_acquire);
    return out;
}

AccountStateStore::AccountPositionFrame
AccountStateStore::snapshot_account_and_position(std::string_view code) const noexcept {
    AccountPositionFrame out;
    for (;;) {
        auto guard = hp_domain_.protect(current_);
        if (!guard) {
            out.account = build_account_snapshot_();
            out.position = build_position_snapshot_(code);
            break;
        }
        const auto idx = guard->find_symbol_idx(code);
        if (idx == kInvalidSymbolIndex) {
            if (symbols_frozen_.load(std::memory_order_acquire)) {
                // 无 slot 的 code 在本帧无下标；返回当帧 account + 空 position。
                out.account = guard->account;
                out.position = PositionSnapshot{};
            } else {
                out.account = build_account_snapshot_();
                out.position = build_position_snapshot_(code);
            }
            break;
        }
        // guard 只 pin FullSnapshot 容器；slot protect 后必须确认该容器仍是 current_，
        // 否则旧容器里的 slot 可能已随新发布帧 retire。account 取自同一 guard，
        // 与 position slot 同属这一发布边界。
        auto pguard = hp_domain_.protect(guard->position_slots[idx]);
        if (current_.load(std::memory_order_acquire) != guard.get()) {
            continue;
        }
        out.account = guard->account;
        out.position = pguard ? *pguard : PositionSnapshot{};
        break;
    }
    // closeable_known 是账户级 live atomic，覆盖进 PositionSnapshot；
    // set_closeable_known(false) 需要立即影响风控，不等待下一次事件发布。
    out.position.closeable_known = closeable_known_.load(std::memory_order_acquire);
    return out;
}

std::vector<std::string> AccountStateStore::snapshot_tracked_codes() const {
    auto guard = hp_domain_.protect(current_);
    // 已发布帧按其自身世代的 by_slot 复制；首包前（BT / bootstrap 单线程）直读 writer 侧集合。
    if (guard && guard->universe) {
        return copy_generation_codes(*guard->universe);
    }
    return symbols_sorted_;
}

std::vector<std::string> AccountStateStore::snapshot_position_universe() const {
    const auto* gen = symbol_table_.active();
    return gen ? copy_generation_codes(*gen) : std::vector<std::string>{};
}

std::size_t AccountStateStore::position_slots_used() const noexcept {
    return symbol_table_.size();
}

bool AccountStateStore::ensure_slot(std::string_view code) {
    if (code.empty()) {
        return false;
    }
    // 无锁快路径：活世代已有 slot（稳态下 declared 与既有持仓都走这一支）。
    if (symbol_table_.find_slot(code) != kInvalidSymbolIndex) {
        return true;
    }
    if (!symbols_frozen_.load(std::memory_order_acquire)) {
        // slot 语义未启用（回测形态）：没有帧也没有 slot 表，收编由惰性 universe 承担。
        return false;
    }
    // 不变量 3：先完成 mark registry 注册再建 slot。反序会造出「有 slot 无 mark id」的
    // 持有者——它的估值恒 0 且永远修不回来（frozen 后 MarketTick 不再 intern）。
    if (!mark_cache_.intern_for_slot(code)) {
        return false;
    }
    return symbol_table_.ensure_slot(code) != kInvalidSymbolIndex;
}

int64_t AccountStateStore::venue_mark_price(std::string_view code) const noexcept {
    if (code.empty()) return 0;
    // MarkPriceTable 接口需要 null-terminated code；截断长度与帧内快照构建口径一致。
    char code_buf[128] = {};
    const auto len = std::min(code.size(), sizeof(code_buf) - 1);
    std::copy_n(code.data(), len, code_buf);
    return mark_cache_.venue_mark_price(code_buf);
}

std::size_t AccountStateStore::quarantined_symbol_count() const noexcept {
    return quarantined_count_.load(std::memory_order_acquire);
}

uint64_t AccountStateStore::quarantined_overflow_count() const noexcept {
    return quarantined_overflow_.load(std::memory_order_relaxed);
}

std::vector<std::string> AccountStateStore::snapshot_quarantined_codes() const {
    std::lock_guard<std::mutex> lk(quarantine_mu_);
    return quarantined_codes_;
}

void AccountStateStore::frozen_audit_report(account_audit::XtFrozenAuditReport& out) const {
    audit_frozen_direct(account_.frozen_cash(), account_.order_slots(), out);
}

void AccountStateStore::frozen_audit_from_snapshot(account_audit::XtFrozenAuditReport& out) const {
    out = account_audit::XtFrozenAuditReport{};
    // 只读 published snapshot 的同帧自洽对；禁止回落直读 writer state（那正是本 API
    // 要堆掉的跨线程 race 后门）。首包发布前（configure_symbols 前）无可读帧 → 诚实报未发布。
    auto guard = hp_domain_.protect(current_);
    if (!guard) {
        out.reason = "no published snapshot (pre-freeze)";
        return;
    }
    const AccountSnapshot& a = guard->account;
    out.frozen_cash = xt_fp::from_fp(a.frozen_cash);
    out.frozen_cash_by_orders = xt_fp::from_fp(a.frozen_by_orders);
    if (a.frozen_cash < 0) {
        out.ok = false;
        out.reason = "frozen_cash negative";
        return;
    }
    if (a.frozen_cash == a.frozen_by_orders) return;
    // 1Hz 在线路径只带聚合差；per-slot dump（事故诊断细粒度）保留在停机直读版
    // frozen_audit_report（post-join 无写者后安全）。
    out.ok = false;
    std::ostringstream oss;
    oss << "frozen_cash mismatch runtime=" << out.frozen_cash
        << " tracked=" << out.frozen_cash_by_orders
        << " (publish-boundary aggregate; per-slot dump only in on-stop audit)";
    out.reason = oss.str();
}

void AccountStateStore::guard_worker_thread_() const noexcept {
    const std::thread::id self = std::this_thread::get_id();
    // quiescent：worker 已 join，无并发写者 → 任意线程 apply 合法（shutdown 收尾 drain
    // order_book 溢出侧道时 marshalled gateway 注册闭包在此 apply，不误记跨线程写）。
    if (worker_quiescent_.load(std::memory_order_acquire)) return;
    if (worker_bound_.load(std::memory_order_acquire)) {
        if (self == worker_tid_) return;
    } else {
        // 未显式 bind（BT/单测）：test-then-CAS first-touch —— 稳态只付 load+cmp，
        // 仅首个写入线程 CAS 一次；测试可 unbind_worker_thread() 重开 first-touch。
        std::thread::id seen = write_first_touch_tid_.load(std::memory_order_acquire);
        if (seen == std::thread::id{}) {
            if (write_first_touch_tid_.compare_exchange_strong(
                    seen, self, std::memory_order_acq_rel, std::memory_order_acquire)) {
                return;
            }
            // CAS 失败：seen 已被竞争胜者覆盖，落到下方同线程比较。
        }
        if (seen == self) return;
    }
    on_cross_thread_write_();
}

void AccountStateStore::on_cross_thread_write_() const noexcept {
    // 跨线程写 reducer/hazard publish 是 data race；fail-open（照常入账）——
    // 写路径丢事件 = 丢 fill/冻结记账，比一次 torn write 更糟。计数 + log-once
    // 让 race 可观测。prod 正常时序应恒为 0：off-worker gateway submit 经 order_book marshal
    // 整块序列化到 worker（apply 在 worker 线程），shutdown 收尾走 enter_quiescent
    // 放行——两路都不再跨线程直写（stop 窗口 drain_pending_queues 桥在主线程执行残留
    // 闭包属预期计数，prod 时序下不可达、测试可见）。持续增长才表示有新的
    // off-worker apply 未走 marshal。
    xt_account_store_drops::cross_thread_write().fetch_add(1, std::memory_order_relaxed);
    static std::atomic<bool> warned{false};
    if (!warned.exchange(true)) {
        std::fprintf(stderr,
                     "[AccountStore][error] cross-thread write detected (fail-open) — "
                     "apply()/on_position_snapshot() must run on the bound worker "
                     "thread; event still applied, check caller thread model\n");
    }
}

void AccountStateStore::publish_partial_(const XtAccountEvent& ev) noexcept {
    if (ev.type == XtAccountEvent::Type::MarketTick) {
        // MarketTick 零发布：行情只落 MarkPriceTable 的原子 mark（已在
        // on_event_before_apply 完成），不构帧、不做 head exchange。帧内 mark 与
        // equity 的刷新改由「持仓/资金事件的即时发布 + 控制线程 1s 兜底请求」承担。
        // 位置在落后闩之前：闩置位期间 tick 同样不得发布（升级全量重建的自愈由
        // 兜底请求与后续事件承担，tick 不参与），零发布语义无例外。
        return;
    }
    if (suppress_partial_publish_) {
        // on_position_snapshot 批次内：中间帧不对读侧发布，批尾 publish_all_snapshots_ 统一收敛。
        return;
    }
    if (publish_dirty_) {
        // 此前有 position-affecting 发布因 pool 耗尽被跳过：已发布帧落后于 writer
        // state，任何增量发布都会把落后的 position slot 复制进新帧（跨发布
        // mixed-vintage）。升级为全量原子重建，成功后闩清除、恢复增量发布。
        (void)publish_all_snapshots_();
        return;
    }
    using T = XtAccountEvent::Type;
    switch (ev.type) {
    case T::Invalid:
    case T::MarketTick:  // 零发布，已在函数入口返回
        break;
    case T::TradeEx:
    case T::SetPosition:
        publish_event_snapshot_(ev.code ? std::string_view(ev.code) : std::string_view{});
        break;
    case T::Reset:
    case T::SessionBegin:
        (void)publish_all_snapshots_();
        break;
    case T::OrderAccepted:
    case T::OrderExecUpdate:
    case T::SetCash:
    case T::DailySettlement:
    case T::CashTransfer:
        publish_account_snapshot_();
        break;
    }
}

bool AccountStateStore::publish_all_snapshots_() noexcept {
    const auto* old = current_.load(std::memory_order_acquire);
    if (!old) {
        return false;
    }
    auto* fresh = acquire_after_reclaim_scan(full_pool_, hp_domain_);
    if (!fresh) {
        publish_dirty_ = true;
        note_publish_skipped("full");
        return false;
    }
    fresh->account = build_account_snapshot_();
    // 采纳活世代：全量路径逐 slot 从 writer state 重建，新旧世代都安全。
    fresh->universe = symbol_table_.active();
    fresh->reset_slots();
    const std::size_t n =
        std::min(fresh->universe ? std::size_t{fresh->universe->n} : std::size_t{0},
                 kMaxPositionSlots);
    // 全部 position 先在未发布帧上重建；任一 slot 取不到就整帧放弃（读者保持上一
    // 自洽帧），绝不发布「account 已新、position 半新半旧」的帧。
    for (std::size_t i = 0; i < n; ++i) {
        auto* pos = acquire_after_reclaim_scan(position_pool_, hp_domain_);
        if (!pos) {
            publish_dirty_ = true;
            note_publish_skipped("position");
            for (std::size_t j = 0; j < i; ++j) {
                position_pool_.release(const_cast<PositionSnapshot*>(
                    fresh->position_slots[j].load(std::memory_order_relaxed)));
            }
            full_pool_.release(fresh);
            return false;
        }
        *pos = build_position_snapshot_(*fresh->universe->by_slot[i]);
        fresh->position_slots[i].store(pos, std::memory_order_relaxed);
    }
    const auto* retired = current_.exchange(fresh, std::memory_order_acq_rel);
    note_published_();
    if (retired) {
        // 旧帧的 slot 全部被替换：逐个 retire（仍 pin 旧帧的读者会在 current
        // re-confirm 失败后重试，不会消费这些指针），再 retire 旧帧本体。
        // 扫描面取旧帧自己的世代长度：新世代可能更长，越界 slot 恒 null 不必扫。
        const std::size_t retired_n =
            std::min(retired->universe ? std::size_t{retired->universe->n} : std::size_t{0},
                     kMaxPositionSlots);
        for (std::size_t i = 0; i < retired_n; ++i) {
            if (const auto* p = retired->position_slots[i].load(std::memory_order_acquire)) {
                hp_domain_.retire_raw(p, &position_pool_, release_position_snapshot);
            }
        }
        hp_domain_.retire_raw(retired, &full_pool_, release_full_snapshot);
    }
    // 全量重建成功 = 帧与 writer state 重新收敛，落后闩清除。
    publish_dirty_ = false;
    return true;
}

void AccountStateStore::publish_account_snapshot_() noexcept {
    const auto* old = current_.load(std::memory_order_acquire);
    if (!old) {
        return;
    }
    auto* fresh = acquire_after_reclaim_scan(full_pool_, hp_domain_);
    if (!fresh) {
        note_publish_skipped("full");
        return;
    }
    fresh->account = build_account_snapshot_();
    // 采纳活世代：slot 下标跨世代稳定 ⇒ 旧帧 [0, old_n) 按下标逐位复制，
    // [old_n, new_n) 留 null。新 slot 的 code 此刻必然零仓（本路径只改资金域），
    // null slot 与零值 slot 读侧值级等价。
    fresh->universe = symbol_table_.active();
    fresh->reset_slots();
    const std::size_t copy_n = std::min(
        {old->universe ? std::size_t{old->universe->n} : std::size_t{0},
         fresh->universe ? std::size_t{fresh->universe->n} : std::size_t{0}, kMaxPositionSlots});
    for (std::size_t i = 0; i < copy_n; ++i) {
        fresh->position_slots[i].store(old->position_slots[i].load(std::memory_order_acquire),
                                       std::memory_order_relaxed);
    }
    const auto* retired = current_.exchange(fresh, std::memory_order_acq_rel);
    note_published_();
    if (retired) {
        hp_domain_.retire_raw(retired, &full_pool_, release_full_snapshot);
    }
}

void AccountStateStore::publish_event_snapshot_(std::string_view code) noexcept {
    const auto* old = current_.load(std::memory_order_acquire);
    if (!old) {
        return;
    }
    auto* fresh = acquire_after_reclaim_scan(full_pool_, hp_domain_);
    if (!fresh) {
        publish_dirty_ = true;
        note_publish_skipped("full");
        return;
    }
    fresh->account = build_account_snapshot_();
    // 采纳活世代：本事件可能刚为 code 追加了新 slot（ensure_slot 在 apply 之前）。
    // 照搬旧帧世代会让新 slot 永不进帧、该 code 的仓位对读侧永久不可见。
    fresh->universe = symbol_table_.active();
    fresh->reset_slots();
    const std::size_t copy_n = std::min(
        {old->universe ? std::size_t{old->universe->n} : std::size_t{0},
         fresh->universe ? std::size_t{fresh->universe->n} : std::size_t{0}, kMaxPositionSlots});
    for (std::size_t i = 0; i < copy_n; ++i) {
        fresh->position_slots[i].store(old->position_slots[i].load(std::memory_order_acquire),
                                       std::memory_order_relaxed);
    }
    // 受影响 symbol 的 position 在未发布帧上就地重建后才做唯一一次 head exchange：
    // 读者要么看整帧旧、要么整帧新，不存在「新 account + 旧 position」的中间可见态。
    // 该 slot 若是本事件新追加的（下标 ≥ 旧帧世代长度），复制循环在那里留的是 null，
    // exchange 返回 nullptr 无需 retire——就地重建正是新 slot 首次进帧的唯一途径。
    const PositionSnapshot* replaced = nullptr;
    if (!code.empty()) {
        const auto idx = fresh->find_symbol_idx(code);
        if (idx != kInvalidSymbolIndex) {
            auto* pos = acquire_after_reclaim_scan(position_pool_, hp_domain_);
            if (!pos) {
                // 整次发布跳过：读者保持上一自洽帧（coherent-but-stale），
                // pool 失败不得把撕裂发布出去；落后闩让后续发布升级全量重建。
                publish_dirty_ = true;
                note_publish_skipped("position");
                full_pool_.release(fresh);
                return;
            }
            *pos = build_position_snapshot_(code);
            replaced = fresh->position_slots[idx].exchange(pos, std::memory_order_relaxed);
        }
    }
    const auto* retired = current_.exchange(fresh, std::memory_order_acq_rel);
    note_published_();
    if (retired) {
        hp_domain_.retire_raw(retired, &full_pool_, release_full_snapshot);
    }
    if (replaced) {
        // 被替换的旧 position 只可能被仍 pin 旧帧的读者持有；它们在 current
        // re-confirm 失败后重试，不会消费该指针，head exchange 后 retire 安全。
        hp_domain_.retire_raw(replaced, &position_pool_, release_position_snapshot);
    }
}

void AccountStateStore::note_published_() noexcept {
    // 先写时刻再推进序号（release）：看到新 seq 的观测方必然看到对应的发布时刻。
    last_publish_steady_ms_.store(XtSteadyClock::steady_now_ms(), std::memory_order_relaxed);
    published_seq_.fetch_add(1, std::memory_order_release);
}

uint64_t AccountStateStore::published_seq() const noexcept {
    return published_seq_.load(std::memory_order_acquire);
}

int64_t AccountStateStore::last_publish_steady_ms() const noexcept {
    return last_publish_steady_ms_.load(std::memory_order_relaxed);
}

uint64_t AccountStateStore::request_publish() const noexcept {
    uint64_t cur = requested_ticket_.load(std::memory_order_acquire);
    for (;;) {
        if (cur > acked_ticket_.load(std::memory_order_acquire)) {
            // 已有在途请求：合并到它，新鲜度仍然满足。worker 是唯一写者：在途闭包
            // 从开始执行到 ack 这段窗口里它卡在闭包内，不会 apply 任何事件；因此本次
            // 登记之前已 apply 的事件必然早于闭包的重建，闭包发布的帧不会漏掉它们。
            return cur;
        }
        if (requested_ticket_.compare_exchange_weak(cur, cur + 1,
                                                    std::memory_order_acq_rel,
                                                    std::memory_order_acquire)) {
            return cur + 1;
        }
    }
}

bool AccountStateStore::is_publish_acked(uint64_t ticket) const noexcept {
    if (ticket == 0) return false;
    return acked_ticket_.load(std::memory_order_acquire) >= ticket;
}

uint64_t AccountStateStore::publish_request_attempts() const noexcept {
    return publish_attempts_.load(std::memory_order_acquire);
}

void AccountStateStore::run_publish_request() noexcept {
    guard_worker_thread_();
    const uint64_t serving = requested_ticket_.load(std::memory_order_acquire);
    if (serving == 0) return;  // 无在册请求
    if (serving <= acked_ticket_.load(std::memory_order_acquire)) {
        // 在册请求已被先前闭包完成：多余投递不做冗余全量重建（合并的另一半）。
        return;
    }
    bool published;
    if (!current_.load(std::memory_order_acquire)) {
        // 首包发布前：读侧直读 writer state（bootstrap 单线程期），没有帧需要刷新，
        // 新鲜度天然满足。live 下 configure_symbols 早于控制线程起步，结构性不可达。
        published = true;
    } else {
        published = publish_all_snapshots_();
    }
    if (!published) {
        // pool 耗尽跳过发布：**不 ack**，但推进跳过计数。计数只在尝试**完成且未 ack**
        // 时变化（闭包排队/执行中的窗口内不变），投递方据此区分「在途」与「已执行但
        // 被跳过」：前者不重投（单在途），后者必须重投否则请求永不完成。
        publish_attempts_.fetch_add(1, std::memory_order_release);
        return;
    }
    // ack 只由「携带该 ticket 执行、并自己成功 head exchange」的闭包推进；单调高水位。
    uint64_t prev = acked_ticket_.load(std::memory_order_relaxed);
    while (prev < serving &&
           !acked_ticket_.compare_exchange_weak(prev, serving,
                                                std::memory_order_release,
                                                std::memory_order_relaxed)) {
    }
    hp_domain_.try_scan_if_due();
}

AccountSnapshot AccountStateStore::build_account_snapshot_() const noexcept {
    AccountSnapshot out;
    out.cash = account_.cash();
    out.frozen_cash = account_.frozen_cash();
    out.available_cash = account_.available_cash();
    int64_t total_unrealized = 0;
    int64_t total_market_value = 0;
    // 迭代源 = reducer 持仓 map 本体（pos_ key，live/BT 同一路径，spec D6）：
    // 非零持仓必在 pos_（只有 reset 整体擦除，无逐项 erase），多出的零仓项贡献恒 0。
    // quarantine 两支在此分流：slot 满支有 mark id（ensure_slot 先 intern 后建 slot），
    // 其市值/浮盈如实计入 equity；**intern 满支无 id → 跳过**——venue mark 与 cost hint
    // 都挂在 id 上，无 id 即无价可估，按 mark=0 聚合会把非零仓算成全额名义浮亏
    // （幽灵 drawdown 直通风控闩）；跳过 = 维持旧聚合对该支的 0 贡献，该仓已
    // quarantine 记名 + critical，不是静默面。BT 惰性路径 pos_ 项恒有 id（同事件
    // 注册，intern 满则 note_symbol_if_open_ 已 throw），本分支不改 BT 行为。
    // unordered_map 迭代序不确定：int64 求和可交换，结果与序无关。
    account_.for_each_position_code([&](const std::string& code) {
        if (code.empty()) return;
        if (!mark_cache_.has_mark_id(code.c_str())) return;
        const auto mark_input = make_mark_input(code.c_str());
        total_unrealized += xt_account::position_unrealized(mark_input);
        total_market_value += xt_account::position_market_value(mark_input);
    });
    out.unrealized_pnl = total_unrealized;
    out.equity = xt_account::equity(
        out.cash, total_unrealized, total_market_value, account_.futures_mode());
    out.realized_pnl = account_.realized_pnl();
    out.fees = account_.fees();
    out.market_value = total_market_value;
    out.frozen_by_orders = account_.frozen_by_orders();
    return out;
}

PositionSnapshot
AccountStateStore::build_position_snapshot_(std::string_view code) const noexcept {
    char code_buf[128] = {};
    const auto len = std::min(code.size(), sizeof(code_buf) - 1);
    std::copy_n(code.data(), len, code_buf);
    const char* c = code_buf;
    // Snapshot 构建需要同一 code 的多项持仓字段；先聚合读出，避免重复查持仓表。
    const auto r = account_.position_readout(c);
    PositionSnapshot out;
    out.qty = r.qty;
    out.long_qty = r.long_qty;
    out.short_qty = r.short_qty;
    out.long_today_qty = r.long_today_qty;
    out.short_today_qty = r.short_today_qty;
    out.avg_price = r.avg_price;
    // 帧内 mark 是估值口径（venue 优先、次成本价）；下单定价另走 venue_mark_price()。
    out.mark_price = mark_cache_.effective_mark(c);
    out.unrealized_pnl = xt_account::position_unrealized(make_mark_input(c, r));
    out.available_sell_today_qty = r.available_sell_today_qty;
    out.available_sell_total_qty = r.available_sell_total_qty;
    out.available_cover_today_qty = r.available_cover_today_qty;
    out.available_cover_total_qty = r.available_cover_total_qty;
    // broker 侧冻结量与本地订单冻结量由风控取 max；broker 缺失时为 0。
    out.broker_long_frozen_fp  = r.broker_long_frozen_fp;
    out.broker_short_frozen_fp = r.broker_short_frozen_fp;
    // 0 表示 broker 未提供时间戳，risk path 会按 local-only 口径处理。
    out.broker_frozen_last_update_ms = r.broker_frozen_last_update_ms;
    out.has_today_split = account_.has_today_split();
    out.closeable_known = closeable_known_.load(std::memory_order_acquire);
    return out;
}

xt_account::XtPositionMarkInput AccountStateStore::make_mark_input(const char* code) const {
    return make_mark_input(code, account_.position_readout(code));
}

xt_account::XtPositionMarkInput AccountStateStore::make_mark_input(
    const char* code, const AccountState::PositionReadout& readout) const {
    xt_account::XtPositionMarkInput mark_input;
    mark_input.long_qty = readout.long_qty;
    mark_input.long_avg_price = readout.long_avg_price;
    mark_input.short_qty = readout.short_qty;
    mark_input.short_avg_price = readout.short_avg_price;
    mark_input.mark_price = mark_cache_.effective_mark(code);
    // 期货 unrealized PnL 与 realized PnL 同用 catalog vol_scale；缺 catalog、
    // 非期货或无效 vol_scale 保持默认 1.0，等价于股票/现货口径。
    if (account_.futures_mode()) {
        if (const XtCommodityCatalog* cat = account_.commodity_catalog()) {
            const XtCommodityInfo* info = cat->find(code);
            if (info && info->vol_scale > 0.0)
                mark_input.vol_scale_fp = xt_fp::to_fp(info->vol_scale);
        }
    }
    return mark_input;
}

void AccountStateStore::on_event_before_apply(const XtAccountEvent& ev) {
    switch (ev.type) {
    case XtAccountEvent::Type::Reset:
        // 持仓域被整体擦除重建 ⇒ 旧持仓的成本价随之失效，留着会让 venue 缺失的 code
        // 按陈旧成本价估值。venue mark 与持仓无关，必须留存（清掉会让重建后的持仓在
        // 下一笔 MarketTick 之前按 mark=0 估值）。
        mark_cache_.clear_cost_hints();
        return;
    case XtAccountEvent::Type::MarketTick:
        note_symbol_if_open_(ev.code,
                             mark_cache_.on_market_tick(ev.code, ev.payload.tick.price));
        return;
    case XtAccountEvent::Type::SetPosition:
        if (symbols_frozen_.load(std::memory_order_acquire)) {
            admit_position_symbol_(ev.code, "SetPosition");
        } else {
            note_symbol_if_open_(ev.code, mark_cache_.on_set_position(ev.code));
        }
        return;
    case XtAccountEvent::Type::TradeEx:
        if (symbols_frozen_.load(std::memory_order_acquire)) {
            admit_position_symbol_(ev.code, "TradeEx");
        } else {
            note_symbol_if_open_(ev.code, mark_cache_.on_trade(ev.code));
        }
        return;
    default:
        return;
    }
}

void AccountStateStore::on_event_after_apply(const XtAccountEvent& ev) {
    // 改仓事件（成交 / 持仓快照）之后成本价才是新的：必须等 reducer 把新持仓、新均价
    // 落进 AccountState 再按持仓终态重算。两条入口同路是转换表（单腿 / 双腿 / flat）
    // 唯一的执行点——事件里带的价格不参与，caller 无从绕过。
    if (ev.type != XtAccountEvent::Type::TradeEx &&
        ev.type != XtAccountEvent::Type::SetPosition) {
        return;
    }
    if (!ev.code || ev.code[0] == '\0') {
        return;
    }
    mark_cache_.update_cost_hint(ev.code, derive_cost_hint_(ev.code));
}

int64_t AccountStateStore::derive_cost_hint_(const char* code) const {
    const auto r = account_.position_readout(code);
    // 恰一条正持仓腿时该腿均价可作估值兜底；空仓 / 双腿锁仓下没有单一成本口径，
    // 写 0 表示显式失效（不留陈旧 hint）。
    if (r.long_qty > 0 && r.short_qty <= 0) {
        return r.long_avg_price > 0 ? r.long_avg_price : 0;
    }
    if (r.short_qty > 0 && r.long_qty <= 0) {
        return r.short_avg_price > 0 ? r.short_avg_price : 0;
    }
    return 0;
}

bool AccountStateStore::is_declared_(std::string_view code) const noexcept {
    const auto it = std::lower_bound(symbols_sorted_.begin(), symbols_sorted_.end(), code,
                                     [](const std::string& lhs, std::string_view rhs) {
                                         return std::string_view(lhs) < rhs;
                                     });
    return it != symbols_sorted_.end() && std::string_view(*it) == code;
}

bool AccountStateStore::is_in_universe_(std::string_view code) const noexcept {
    if (is_declared_(code)) return true;
    // declared 外的 code 只要拿到过 slot 就是 store 认可的成员（外来仓照常入账、
    // 按合约可见）。冻结前 SymbolTable 未 initialize，这一支恒不命中。
    return symbol_table_.find_slot(code) != kInvalidSymbolIndex;
}

void AccountStateStore::admit_position_symbol_(const char* code, const char* event_tag) {
    if (!code || code[0] == '\0') return;
    if (ensure_slot(code)) return;
    quarantine_slot_overflow_(code, event_tag);
}

void AccountStateStore::admit_pre_freeze_positions_() {
    // 冻结前落账的持仓（bootstrap 期先 apply 后 configure_symbols 的时序）此刻还没有
    // slot：它们的 code 已在惰性集合里，但 slot 语义是 initialize 才启用的。不在这里
    // 补 slot，gen-0 之后这些仓位对读侧就一直是零仓——per-symbol 读口不可见
    // （账户级聚合迭代持仓 map，有 mark id 即计入）。
    // 零仓的 declared symbol 不占 slot（等价于从未 touched）。
    std::vector<std::string_view> admit;
    for (const auto& code : symbols_sorted_) {
        if (code.empty()) continue;
        const auto r = account_.position_readout(code.c_str());
        if (r.long_qty == 0 && r.short_qty == 0) continue;
        if (admit.size() >= kMaxPositionSlots) {
            // 容量耗尽：与 recovery 路径同口径记名。静默丢弃会让这些持仓 per-symbol
            // 永远读零仓、无平仓通道且无任何告警面（declared 的 mark id 已预热，
            // 账户级聚合仍计入其市值/浮盈）。
            quarantine_slot_overflow_(code, "PreFreezePosition");
            continue;
        }
        admit.emplace_back(code);
    }
    if (admit.empty()) return;
    // declared 的 mark id 已由 register_symbol_ 预热（不变量 3 已满足）。
    std::vector<std::string> rejected;
    (void)symbol_table_.ensure_slots(admit, &rejected);
    // 锁内二次拒绝（容量在本批内被填满）同样记名，不静默。
    for (const auto& code : rejected) {
        quarantine_slot_overflow_(code, "PreFreezePosition");
    }
}

void AccountStateStore::admit_recovery_symbols_(
    const std::vector<XtPositionSnapshot>& positions) {
    // 锁外阶段：去重 + 滤掉已有 slot 的 code。不做容量预截：预截按锁外时刻的余量
    // 估算，而 ensure_slot 任意线程可调（submit 预留源）——锁外判定的「容量拒」与
    // 其它线程的 slot 授予存在 TOCTOU 窗口，会造出同时处于 PositionUniverse 与
    // quarantine 名单的 code。容量拒绝一律以批 admission 的锁内判定为准（下方
    // out_rejected）；锁内成本 O(pending × (log n + min(pending, 余量)) + n'·log n')——
    // 每个候选除二分外扫本批已追加尾部做批内重复防御；有新增 slot 时还整代
    // 重建并排序一次（n' = 追加后世代大小，≤ 1024）。受 1024/8192 硬上限约束，
    // recovery 冷路径可接受。
    std::vector<std::string_view> wanted;
    wanted.reserve(positions.size());
    for (const auto& p : positions) {
        if (!p.code.empty()) wanted.emplace_back(p.code);
    }
    std::sort(wanted.begin(), wanted.end());
    wanted.erase(std::unique(wanted.begin(), wanted.end()), wanted.end());

    std::vector<std::string_view> pending;
    pending.reserve(wanted.size());
    for (const auto code : wanted) {
        if (symbol_table_.find_slot(code) == kInvalidSymbolIndex) pending.push_back(code);
    }
    if (pending.empty()) return;

    std::vector<std::string_view> admit;
    admit.reserve(pending.size());
    for (const auto code : pending) {
        // 不变量 3：mark intern 先于 slot；intern 失败的 code 不进 admission。
        // intern 表 append-only 不回收 ⇒ 「intern 满被拒」单调成立，记名不会与
        // 未来的 slot 授予产生双集合。
        if (mark_cache_.intern_for_slot(code)) {
            admit.push_back(code);
        } else {
            quarantine_slot_overflow_(code, "PositionRecovery");
        }
    }
    if (admit.empty()) return;

    std::vector<std::string> rejected;
    (void)symbol_table_.ensure_slots(admit, &rejected);
    // 容量拒绝只认锁内判定：slot 容量满是单调不可逆的（append-only 不回收），
    // 被锁内拒的 code 此后任何线程的 ensure_slot 也必失败 ⇒
    // 「PositionUniverse 与 quarantine 互斥」由单调性成为定理，不再依赖任何
    // 锁外时刻的余量估算。
    for (const auto& code : rejected) {
        quarantine_slot_overflow_(code, "PositionRecovery");
    }
}

// slot 拿不到时该 symbol 的持仓/成交仍照常入账（资金守恒优先于可见性：丢 fill/持仓
// 事件会破坏账户级 cash/realized），但它没有 slot，snapshot_position 对它恒空——钱在
// 账上、仓不可见，且没有平仓通道（close quota 读零仓拒）。这里把它记名并大声暴露，
// 让停机对账和诊断能拿到确切 code 列表。
// 触发条件只剩容量溢出：PositionUniverse 满 1024，或 mark intern 表满 8192。
void AccountStateStore::quarantine_slot_overflow_(std::string_view code,
                                                  const char* event_tag) {
    if (code.empty()) return;
    xt_account_store_drops::position_slot_overflow().fetch_add(1, std::memory_order_relaxed);

    std::lock_guard<std::mutex> lk(quarantine_mu_);
    for (const auto& known : quarantined_codes_) {
        if (known == code) return;  // 每 code 只记名/告警一次
    }
    if (quarantined_codes_.size() >= kMaxQuarantinedCodes) {
        quarantined_overflow_.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    quarantined_codes_.emplace_back(code);
    quarantined_count_.store(quarantined_codes_.size(), std::memory_order_release);
    std::fprintf(stderr,
                 "[xt_account][error] symbol '%.*s' got no position slot (event=%s); "
                 "invisible per-symbol and not closeable (account-level aggregation still "
                 "includes it while it has a mark id) (repeats for this code suppressed)\n",
                 static_cast<int>(code.size()), code.data(), event_tag);
}

void AccountStateStore::note_symbol_if_open_(const char* code,
                                             MarkPriceTable::TrackResult tracked) {
    // intern 表满 = 该 code 永远拿不到 id：venue mark 与 cost hint 都无处可挂，
    // 聚合会按无价跳过它 ⇒ 成交照常扣现金、买到的资产不进 market_value，
    // 权益静默算错且零日志。本 throw 在 reducer 入账**之前**，事件不会落进 pos_。
    // 宁可中断长跑也不交出一份错的权益曲线。
    // 只在未冻结（回测惰性 intern）路径可达：冻结后事件走 slot admission，
    // 容量溢出退回 quarantine / 拒单路径而不 throw。
    if (tracked == MarkPriceTable::TrackResult::kInternFull) {
        throw std::runtime_error(
            "AccountStateStore::note_symbol_if_open_: instrument intern table exhausted at " +
            std::to_string(xt_core::kMaxInstruments) + " codes, code=" +
            std::string(code ? code : ""));
    }
    // freeze 后 declared 集合 immutable 且被跨线程读者查询，任何写入都是 race；
    // 未冻结（回测路径）时按 mark table 的注册结果惰性补齐该集合。
    if (tracked != MarkPriceTable::TrackResult::kTracked ||
        symbols_frozen_.load(std::memory_order_acquire)) {
        return;
    }
    insert_symbol_sorted_(code);
}

}  // namespace xt_account
