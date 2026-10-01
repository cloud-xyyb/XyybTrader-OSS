#include "xt_identity/OrderAttributionStore.h"
#include "order/XtOrderRuntimeBook.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "api/AliveInflightOrder.h"
#include "xt_core/order/XtTradeRecord.h"
#include "XtFixedPoint.h"
#include "xt_account/api/AccountCloseableView.h"
#include "xt_account/api/AccountSnapshot.h"  // CloseableView
#include "xt_util/time/XtSteadyClock.h"  // pre-registration 暂存 TTL（冷路径 steady ms）

#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif

namespace xt_runtime_book_drops {
// Process-local drop counter 保持 XtOrderRuntimeBook class layout 稳定，同时给
// 生产监控暴露异常账本状态信号。
std::atomic<uint64_t>& slot_pool_full_drops() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

// 非终态 update 未命中 slot 时递增；正常 submit/recovery 链路应先建立 slot。
std::atomic<uint64_t>& orphan_update_dropped() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

// 独立 Trade 未命中 slot 且超时未注册 / 表满时递增；正常 submit/recovery 链路应先建立 slot。
std::atomic<uint64_t>& orphan_trade_dropped() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

// orphan reaper 判定 venue 已无、直接 erase 的孤儿死单数（reap 第二轮仍命中即清）。
std::atomic<uint64_t>& orphan_reap_dropped() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

// T1.153 LEAK-2：stale venue 快照试图复活已见终态单时被 tombstone gate 拦下的条数。
std::atomic<uint64_t>& fillup_terminal_resurrect_blocked() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

// T1.153 tripwire：close_outstanding_ 与 slot 池重算值漂移被自愈的次数（正常恒 0；
// 非零 = 某写路径漏了 remove/add 括号，查根因）。
std::atomic<uint64_t>& close_outstanding_drift_healed() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

// snapshot pool 耗尽时 publish 被跳过；reader 静默读 stale，需生产告警。
std::atomic<uint64_t>& snapshot_publish_skipped() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

// 非 book 写线程的直读被 fail-closed 拦截；prod 调用图下应恒为 0。
std::atomic<uint64_t>& cross_thread_direct_read_blocked() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

// publish 时 tracked_codes_ 超 snapshot 数组容量被截断；reader 对截掉的合约静默失明。
std::atomic<uint64_t>& tracked_codes_truncated() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

// marshal 入队失败改走溢出侧道暂存的注册闭包数（现实主来源 = 停机窗口）。
std::atomic<uint64_t>& marshal_side_channel_used() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

// 溢出侧道打满被丢弃的注册闭包数（理论极限，worker 每轮 drain 下不可达）。
std::atomic<uint64_t>& marshal_side_channel_overflow_dropped() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

// on_order_submitted 命中仍存活 slot 被 fail-closed 拦下的注册数（同 id 二次注册无合法来源）。
std::atomic<uint64_t>& duplicate_submit_blocked() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

// recovery fillup 中 venue 回传定点量越界（负或超上限）被 clamp 的订单数。
std::atomic<uint64_t>& venue_fillup_qty_sanitized() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}
}  // namespace xt_runtime_book_drops

namespace xt_runtime {
namespace order {

namespace {
using OrderBookSnapshotPool = xt_util::sync::XtSnapshotPool<OrderBookSnapshot, 8>;

OrderBookSnapshot* acquire_order_book_snapshot(OrderBookSnapshotPool& pool,
                                               xt_util::sync::HazardDomain& domain) noexcept {
    auto* fresh = pool.acquire();
    if (!fresh) {
        domain.try_scan();
        fresh = pool.acquire();
    }
    return fresh;
}

void release_order_book_snapshot(void* ctx, const void* ptr) noexcept {
    auto* pool = static_cast<OrderBookSnapshotPool*>(ctx);
    pool->release(const_cast<OrderBookSnapshot*>(static_cast<const OrderBookSnapshot*>(ptr)));
}

// venue 回传定点量的 sanity 上限：1e15 fp = 1e9 手（fp scale 1e6），量级远超任何真实
// 合约的单笔委托 / 持仓（现实主力合约单笔以千手计），越界即视为 clientOrderId/OrderRef
// 反解或传输损坏。上限本身仍留足 __int128 头寸（cap² = 1e30 ≪ INT64 域外的 1.7e38），
// 让 fillup_alloc_filled_ 的宽整型乘法在最坏输入下也不溢出。
constexpr int64_t kMaxSaneFillupQtyFp = 1'000'000'000'000'000LL;  // 1e15

// 把 venue 回传定点量 clamp 到 [0, kMaxSaneFillupQtyFp]。返回 true = 发生 clamp（负值或
// 越界，视为垃圾输入）。fail-fast 语义：拒绝以垃圾继续记账，但保守 clamp 续跑恢复而非
// 弃整个 batch（漏一个真值的可平量比整批不恢复更安全，下轮 on_order_update/on_trade 收敛）。
bool clamp_fillup_venue_qty(int64_t& v) noexcept {
    if (v < 0) {
        v = 0;
        return true;
    }
    if (v > kMaxSaneFillupQtyFp) {
        v = kMaxSaneFillupQtyFp;
        return true;
    }
    return false;
}
}  // namespace

// ── 构造 ────────────────────────────────────────────────────────────────────
XtOrderRuntimeBook::XtOrderRuntimeBook() : XtOrderRuntimeBook(Config{}) {}

XtOrderRuntimeBook::XtOrderRuntimeBook(const Config& cfg)
    : pre_reg_ttl_ms_(cfg.pre_registration_ttl_ms) {
    init_slot_pool();
    publish_snapshot_();
}

XtOrderRuntimeBook::XtOrderRuntimeBook(XtOrderRuntimeBook&& other) noexcept
    : slot_pool_(std::move(other.slot_pool_)),
      free_stack_(other.free_stack_),
      free_top_(other.free_top_),
      id_to_slot_idx_(std::move(other.id_to_slot_idx_)),
      close_outstanding_(std::move(other.close_outstanding_)),
      open_order_seq_(other.open_order_seq_),
      order_final_before_submit_(std::move(other.order_final_before_submit_)),
      order_terminal_seen_(std::move(other.order_terminal_seen_)),
      order_terminal_fifo_(std::move(other.order_terminal_fifo_)),
      tracked_codes_(std::move(other.tracked_codes_)),
      instrument_registry_(std::move(other.instrument_registry_)),
      next_generation_(other.next_generation_),
      pre_reg_stash_(std::move(other.pre_reg_stash_)),
      pre_reg_trade_stash_(std::move(other.pre_reg_trade_stash_)),
      pre_reg_ttl_ms_(other.pre_reg_ttl_ms_) {
    // marshal 绑定捕获对象地址，不能随 move 转移；绑定后的对象移动会破坏
    // off-worker 注册栅栏并把目标对象退化为未绑定路径。
    // （溢出侧道不随 move：未绑定对象 post 不可达 → 侧道恒空。）
    assert(!other.marshal_bound_.load(std::memory_order_relaxed) &&
           "XtOrderRuntimeBook moved after bind_worker_marshal (marshal binding not transferred)");
    publish_snapshot_();
}

XtOrderRuntimeBook& XtOrderRuntimeBook::operator=(XtOrderRuntimeBook&& other) noexcept {
    if (this != &other) {
        assert(!other.marshal_bound_.load(std::memory_order_relaxed) &&
               "XtOrderRuntimeBook move-assigned after bind_worker_marshal");
        slot_pool_ = std::move(other.slot_pool_);
        free_stack_ = other.free_stack_;
        free_top_ = other.free_top_;
        id_to_slot_idx_ = std::move(other.id_to_slot_idx_);
        close_outstanding_ = std::move(other.close_outstanding_);
        open_order_seq_ = other.open_order_seq_;
        order_final_before_submit_ = std::move(other.order_final_before_submit_);
        order_terminal_seen_ = std::move(other.order_terminal_seen_);
        order_terminal_fifo_ = std::move(other.order_terminal_fifo_);
        tracked_codes_ = std::move(other.tracked_codes_);
        instrument_registry_ = std::move(other.instrument_registry_);
        next_generation_ = other.next_generation_;
        pre_reg_stash_ = std::move(other.pre_reg_stash_);
        pre_reg_trade_stash_ = std::move(other.pre_reg_trade_stash_);
        pre_reg_ttl_ms_ = other.pre_reg_ttl_ms_;
        publish_snapshot_();
    }
    return *this;
}

// ── Submit 侧 worker marshal（跨线程注册栅栏）──────────────────────────────

void XtOrderRuntimeBook::bind_worker_marshal(std::thread::id worker_tid, WorkerPostFn post) {
    marshal_worker_tid_ = worker_tid;
    marshal_post_ = std::move(post);
    // 绑定开启新 marshal 期：复位 quiescent 闩（shutdown 后重绑的卫生防线，防旧闩
    // 静默放行任意线程直写）。
    marshal_quiescent_.store(false, std::memory_order_release);
    // release 发布：off-worker 线程 acquire 读到 bound 后才会触碰 post_/tid_。
    marshal_bound_.store(static_cast<bool>(marshal_post_), std::memory_order_release);
}

void XtOrderRuntimeBook::unbind_worker_marshal() {
    // 先撤 bound 闸再清字段（@pre 保证无并发在途，顺序只为对称）。
    marshal_bound_.store(false, std::memory_order_release);
    marshal_post_ = nullptr;
    marshal_worker_tid_ = std::thread::id{};
    collect_first_touch_tid_.store(std::thread::id{}, std::memory_order_release);
    marshal_quiescent_.store(false, std::memory_order_release);
}

bool XtOrderRuntimeBook::is_bound_worker_thread() const noexcept {
    if (!marshal_bound_.load(std::memory_order_acquire)) return true;  // 单线程模式
    if (marshal_quiescent_.load(std::memory_order_acquire)) return true;  // worker 已 join，无并发
    return std::this_thread::get_id() == marshal_worker_tid_;
}

void XtOrderRuntimeBook::enter_quiescent() noexcept {
    marshal_quiescent_.store(true, std::memory_order_release);
}

bool XtOrderRuntimeBook::post_to_worker(std::function<void()> fn) const {
    if (!fn) return false;
    // 未绑定：单线程模式，caller 已按 is_bound_worker_thread 走直写（防御性 false）。
    if (!marshal_bound_.load(std::memory_order_acquire)) return false;
    // quiescent：worker 已 join，内联执行必达（闭包内 is_bound_worker_thread()→true 走直写）。
    if (marshal_quiescent_.load(std::memory_order_acquire)) {
        // 回调守卫政策：注册闭包异常不得逃逸控制/收尾线程；loud log 后吞掉继续。
        try {
            fn();
        } catch (...) {
            std::fprintf(stderr,
                         "[OrderBook][error] quiescent inline registration closure threw; "
                         "registration may be incomplete.\n");
        }
        return true;
    }
    // 首选：入队 worker（与 td rtn 同队列 FIFO）。WorkerTask 已 must_not_drop 绕软水位，
    // 仅 stop_requested_/not-running 或物理满（td 队列 65536 深，低频控制面下不可达）失败。
    // marshal_post_ 失败路径可能已 move-from fn（物理 push 失败），先留副本兜底侧道。
    std::function<void()> retained = fn;
    if (marshal_post_(std::move(fn))) return true;
    // 入队失败（stop 窗口 / 物理满）→ 溢出侧道暂存，worker 每轮 / quiescent 收尾 drain。
    return push_marshal_side_channel_(std::move(retained));
}

bool XtOrderRuntimeBook::push_marshal_side_channel_(std::function<void()> fn) const {
    {
        std::lock_guard<std::mutex> lk(marshal_side_channel_mu_);
        if (marshal_side_channel_.size() >= kMarshalSideChannelCap) {
            // 理论极限：低频控制面 producer + worker 每轮 drain 下不可达。打满 = worker 卡死
            // 或控制面异常高频；loud drop（注册丢失），counter + rate-limited stderr 告警。
            xt_runtime_book_drops::marshal_side_channel_overflow_dropped().fetch_add(
                1, std::memory_order_relaxed);
            static std::atomic<bool> overflow_warned{false};
            if (!overflow_warned.exchange(true)) {
                std::fprintf(stderr,
                             "[OrderBook][CRITICAL] marshal side-channel overflow (cap=%zu) — "
                             "order registration DROPPED; worker stuck draining? "
                             "recovery reverse-sync will backfill on restart.\n",
                             kMarshalSideChannelCap);
            }
            return false;
        }
        marshal_side_channel_.push_back(std::move(fn));
        marshal_side_channel_depth_.store(marshal_side_channel_.size(),
                                          std::memory_order_relaxed);
    }
    xt_runtime_book_drops::marshal_side_channel_used().fetch_add(1, std::memory_order_relaxed);
    // 关窗：post 在 quiescent 置位前读到 false、push 又落在 shutdown 最终 drain 之后，
    // 闭包会驻留到进程退出。push 后复查与 shutdown_quiesce 的最终 drain（无条件进
    // 临界区）经 marshal_side_channel_mu_ 定序：本 push 临界区若晚于 final drain 的，
    // 必能看到先行的 quiescent store → 看到闩即自清必达。
    if (marshal_quiescent_.load(std::memory_order_acquire)) {
        const_cast<XtOrderRuntimeBook*>(this)->drain_marshal_side_channel();
    }
    return true;
}

void XtOrderRuntimeBook::drain_marshal_side_channel() {
    if (marshal_side_channel_depth_.load(std::memory_order_relaxed) == 0) return;  // O(1) 早退
    drain_marshal_side_channel_final_();
}

void XtOrderRuntimeBook::shutdown_quiesce() {
    // 先置闩：此后 post 走内联直写，不再新增侧道暂存；再无条件 drain 清尾。
    enter_quiescent();
    drain_marshal_side_channel_final_();
}

void XtOrderRuntimeBook::drain_marshal_side_channel_final_() {
    std::vector<std::function<void()>> batch;
    {
        std::lock_guard<std::mutex> lk(marshal_side_channel_mu_);
        batch.swap(marshal_side_channel_);
        marshal_side_channel_depth_.store(0, std::memory_order_relaxed);
    }
    // 逐个执行暂存的注册闭包（worker / quiescent 线程直写 book）。闭包顺序 = 入侧道序，
    // 侧道整体先于本轮 td 队列事件被 worker 消费（drain 点在 process_td_events 之前）→
    // 溢出注册相对其后到的同队列回报仍保「注册先于回报」（残留跨队列乱序由
    // pre-registration 暂存表兑底）。闭包异常吞掉继续 drain（回调守卫政策，
    // 不得逃逸 worker/收尾线程）。
    for (auto& fn : batch) {
        if (!fn) continue;
        try {
            fn();
        } catch (...) {
            std::fprintf(stderr,
                         "[OrderBook][error] side-channel registration closure threw; "
                         "continuing drain.\n");
        }
    }
}

// ── Pre-registration 暂存表（先到的非终态 update；worker 单写路径 only）──────

void XtOrderRuntimeBook::stash_pre_registration_update_(const XtOrderExecUpdate& upd,
                                                        int64_t effective_remaining_fp) {
    sweep_expired_pre_registration_();
    if (pre_reg_stash_.size() >= kPreRegStashCap) {
        // 表满直达丢弃（真 orphan 口径）：计数 + metric 与旧直接丢弃路径同 label。
        xt_runtime_book_drops::orphan_update_dropped().fetch_add(1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance().inc_runtime_book_drop_total("orphan_update");
#endif
        std::fprintf(stderr,
                     "[OrderBook][error] on_order_update: pre-registration stash full (cap=%zu) — "
                     "unknown-id non-terminal update dropped as true orphan, id=%llu "
                     "(reverse sync missed / foreign order / userTag decode failure?).\n",
                     kPreRegStashCap, static_cast<unsigned long long>(upd.id));
        return;
    }
    PreRegStashEntry entry;
    entry.id = upd.id;
    entry.upd = upd;
    entry.effective_remaining_fp = effective_remaining_fp;
    entry.stash_ms = XtSteadyClock::steady_now_ms();
    pre_reg_stash_.push_back(std::move(entry));
}

void XtOrderRuntimeBook::sweep_expired_pre_registration_() {
    if (pre_reg_stash_.empty() && pre_reg_trade_stash_.empty()) return;
    const int64_t now_ms = XtSteadyClock::steady_now_ms();
    // update 暂存：超时未注册项按 orphan_update 计数。
    auto it = pre_reg_stash_.begin();
    while (it != pre_reg_stash_.end()) {
        if (now_ms - it->stash_ms >= pre_reg_ttl_ms_) {
            // 超时未注册 = 暂存没等到注册闭包（reverse sync 漏 / foreign order /
            // userTag 反解失败），按 orphan 计数（与旧直接丢弃同口径同 label）。
            xt_runtime_book_drops::orphan_update_dropped().fetch_add(1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
            ct::metrics::XtMetricRegistry::instance().inc_runtime_book_drop_total("orphan_update");
#endif
            std::fprintf(stderr,
                         "[OrderBook][error] pre-register race unresolved: update for order %llu "
                         "stashed but no registration arrived within %lldms — counted as orphan.\n",
                         static_cast<unsigned long long>(it->id),
                         static_cast<long long>(pre_reg_ttl_ms_));
            it = pre_reg_stash_.erase(it);
        } else {
            ++it;
        }
    }
    // trade 暂存：超时未注册项按 orphan_trade 计数（独立 label）。
    auto tit = pre_reg_trade_stash_.begin();
    while (tit != pre_reg_trade_stash_.end()) {
        if (now_ms - tit->stash_ms >= pre_reg_ttl_ms_) {
            xt_runtime_book_drops::orphan_trade_dropped().fetch_add(1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
            ct::metrics::XtMetricRegistry::instance().inc_runtime_book_drop_total("orphan_trade");
#endif
            std::fprintf(stderr,
                         "[OrderBook][error] pre-register race unresolved: trade for order %llu "
                         "stashed but no registration arrived within %lldms — counted as orphan_trade.\n",
                         static_cast<unsigned long long>(tit->id),
                         static_cast<long long>(pre_reg_ttl_ms_));
            tit = pre_reg_trade_stash_.erase(tit);
        } else {
            ++tit;
        }
    }
}

void XtOrderRuntimeBook::replay_pre_registration_updates_(uint64_t id) {
    if (pre_reg_stash_.empty()) return;
    sweep_expired_pre_registration_();
    // 收集-先删-后放：重放走 on_order_update 正常路径（slot 已建），先摘出匹配项
    // 防重入期间迭代器失效；vector 序 = 到达序。
    std::vector<PreRegStashEntry> matched;
    auto it = pre_reg_stash_.begin();
    while (it != pre_reg_stash_.end()) {
        if (it->id == id) {
            matched.push_back(std::move(*it));
            it = pre_reg_stash_.erase(it);
        } else {
            ++it;
        }
    }
    for (auto& e : matched) {
        on_order_update(e.upd, /*final_status=*/false, e.effective_remaining_fp);
    }
}

void XtOrderRuntimeBook::clear_pre_registration_for_id_(uint64_t id) {
    if (pre_reg_stash_.empty()) return;
    pre_reg_stash_.erase(
        std::remove_if(pre_reg_stash_.begin(), pre_reg_stash_.end(),
                       [id](const PreRegStashEntry& e) { return e.id == id; }),
        pre_reg_stash_.end());
}

// ── Pre-registration Trade 暂存表（先到的独立成交；worker 单写路径 only）──────

void XtOrderRuntimeBook::stash_pre_registration_trade_(uint64_t id, int64_t qty_fp,
                                                       bool is_today) {
    sweep_expired_pre_registration_();
    if (pre_reg_trade_stash_.size() >= kPreRegTradeStashCap) {
        // 表满直达丢弃（真 orphan_trade 口径）：计数 + metric 独立 label。
        xt_runtime_book_drops::orphan_trade_dropped().fetch_add(1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance().inc_runtime_book_drop_total("orphan_trade");
#endif
        std::fprintf(stderr,
                     "[OrderBook][error] on_trade: pre-registration trade stash full (cap=%zu) — "
                     "unknown-id trade dropped as true orphan, id=%llu "
                     "(reverse sync missed / foreign order / trade before registration timeout?).\n",
                     kPreRegTradeStashCap, static_cast<unsigned long long>(id));
        return;
    }
    PreRegTradeStashEntry entry;
    entry.id = id;
    entry.qty_fp = qty_fp;
    entry.is_today = is_today;
    entry.stash_ms = XtSteadyClock::steady_now_ms();
    pre_reg_trade_stash_.push_back(entry);
}

void XtOrderRuntimeBook::replay_pre_registration_trades_(uint64_t id) {
    if (pre_reg_trade_stash_.empty()) return;
    sweep_expired_pre_registration_();
    // 收集-先删-后放：先摘出匹配项防重入期间迭代器失效；vector 序 = 到达序。
    std::vector<PreRegTradeStashEntry> matched;
    auto it = pre_reg_trade_stash_.begin();
    while (it != pre_reg_trade_stash_.end()) {
        if (it->id == id) {
            matched.push_back(*it);
            it = pre_reg_trade_stash_.erase(it);
        } else {
            ++it;
        }
    }
    for (const auto& e : matched) {
        // F-2：每笔重放前显式 find_slot miss-guard——前笔全成 erase_slot 后，同 id 后续
        // stashed trade 已无 slot（终态已 remember），安全丢弃（同 id 必同状态，break）。
        OrderLifecycleSlot* atom = find_slot(id);
        if (!atom) break;
        apply_trade_fill_(*atom, id, e.qty_fp, e.is_today);
    }
}

void XtOrderRuntimeBook::clear_pre_registration_trades_for_id_(uint64_t id) {
    if (pre_reg_trade_stash_.empty()) return;
    pre_reg_trade_stash_.erase(
        std::remove_if(pre_reg_trade_stash_.begin(), pre_reg_trade_stash_.end(),
                       [id](const PreRegTradeStashEntry& e) { return e.id == id; }),
        pre_reg_trade_stash_.end());
}

void XtOrderRuntimeBook::init_slot_pool() {
    slot_pool_.fill(OrderLifecycleSlot{});  // id = 0 标识空闲
    free_top_ = 0;
    for (uint16_t i = 0; i < kMaxInflightOrders; ++i) {
        free_stack_[free_top_++] = i;
    }
    // 预 reserve 避免 submit 热路径触发 rehash。
    id_to_slot_idx_.clear();
    id_to_slot_idx_.reserve(kMaxInflightOrders);
}

uint16_t XtOrderRuntimeBook::alloc_slot() {
    if (free_top_ == 0) {
        // slot pool 满会导致 submit/force-flat 入口丢弃订单，生产必须告警。
        xt_runtime_book_drops::slot_pool_full_drops().fetch_add(1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance().inc_runtime_book_drop_total("slot_pool_full");
#endif
        return kInvalidSlotIdx;
    }
    return free_stack_[--free_top_];
}

void XtOrderRuntimeBook::free_slot_by_idx(uint16_t idx) {
    assert(idx < kMaxInflightOrders);
    slot_pool_[idx] = OrderLifecycleSlot{};  // id=0 是空闲标志。
    free_stack_[free_top_++] = idx;
}

XtOrderRuntimeBook::OrderLifecycleSlot* XtOrderRuntimeBook::find_slot(uint64_t id) {
    auto it = id_to_slot_idx_.find(id);
    if (it == id_to_slot_idx_.end()) return nullptr;
    return &slot_pool_[it->second];
}

const XtOrderRuntimeBook::OrderLifecycleSlot* XtOrderRuntimeBook::find_slot(uint64_t id) const {
    auto it = id_to_slot_idx_.find(id);
    if (it == id_to_slot_idx_.end()) return nullptr;
    return &slot_pool_[it->second];
}

XtOrderRuntimeBook::OrderLifecycleSlot* XtOrderRuntimeBook::upsert_slot(uint64_t id, bool& inserted) {
    auto it = id_to_slot_idx_.find(id);
    if (it != id_to_slot_idx_.end()) {
        inserted = false;
        return &slot_pool_[it->second];
    }
    const uint16_t idx = alloc_slot();
    if (idx == kInvalidSlotIdx) { inserted = false; return nullptr; }  // drop counter 已递增。
    id_to_slot_idx_[id] = idx;
    inserted = true;
    return &slot_pool_[idx];
}

void XtOrderRuntimeBook::erase_slot(uint64_t id) {
    auto it = id_to_slot_idx_.find(id);
    if (it == id_to_slot_idx_.end()) return;
    // 只回收 slot 与索引；归属擦除/退休按调用者语义显式分派（终态收尾走 retire —— 策略
    // 与 manual 统一入内存 retired 表，仅 sweep 过期 TTL 分档（manual 长 / 策略短）；
    // orphan reap / fillup-unowned / submit 失败回滚走 forget hard-erase）。见
    // apply_trade_fill_ / on_order_update / reap_recovered_unowned。
    free_slot_by_idx(it->second);
    id_to_slot_idx_.erase(it);
}

bool XtOrderRuntimeBook::terminal_order_seen_(uint64_t id) const noexcept {
    return id != 0 && order_terminal_seen_.find(id) != order_terminal_seen_.end();
}

void XtOrderRuntimeBook::remember_terminal_order_(uint64_t id) {
    if (id == 0) return;
    if (order_terminal_seen_.insert(id).second) {
        order_terminal_fifo_.push_back(id);
    }
    while (order_terminal_seen_.size() > kTerminalOrderCacheCap &&
           !order_terminal_fifo_.empty()) {
        const uint64_t old = order_terminal_fifo_.front();
        order_terminal_fifo_.pop_front();
        order_terminal_seen_.erase(old);
        // order_final_before_submit_ 是 terminal_seen 的子集；terminal FIFO 淘汰老 id 时
        // 联动清理，保证无对应 submit 的外部单 tombstone 同样受 cap 约束。
        order_final_before_submit_.erase(old);
    }
}

// ── 静态辅助 ──────────────────────────────────────────────────────────────────

int64_t XtOrderRuntimeBook::unfilled_today_fp(const OrderLifecycleSlot& slot) {
    return xt_fp::clamp_nonneg(slot.locked_today_fp - slot.filled_today_fp);
}

int64_t XtOrderRuntimeBook::unfilled_yesterday_fp(const OrderLifecycleSlot& slot) {
    return xt_fp::clamp_nonneg(slot.locked_yesterday_fp - slot.filled_yesterday_fp);
}

bool XtOrderRuntimeBook::has_close_contribution(const OrderLifecycleSlot& slot) {
    // instrument_id 整数比较替代原 string::empty()，零开销
    return slot.is_close && (slot.instrument_id != kInvalidInstrumentId);
}

// ── CloseOutstanding 增量维护 ──────────────────────────────────────────────────

void XtOrderRuntimeBook::add_close_outstanding_delta(XtInstrumentId id,
                                                      bool is_buy,
                                                      int64_t delta_today_fp,
                                                      int64_t delta_yesterday_fp) {
    if (id == kInvalidInstrumentId) return;
    if (delta_today_fp == 0 && delta_yesterday_fp == 0) return;

    const CloseOutstandingKey key{id, is_buy};
    auto it = close_outstanding_.find(key);
    if (it == close_outstanding_.end()) {
        const int64_t today_fp = xt_fp::clamp_nonneg(delta_today_fp);
        const int64_t yesterday_fp = xt_fp::clamp_nonneg(delta_yesterday_fp);
        if (today_fp <= 0 && yesterday_fp <= 0) return;
        close_outstanding_.emplace(key, CloseOutstandingAgg{today_fp, yesterday_fp});
        return;
    }

    it->second.today_fp = xt_fp::clamp_nonneg(it->second.today_fp + delta_today_fp);
    it->second.yesterday_fp = xt_fp::clamp_nonneg(it->second.yesterday_fp + delta_yesterday_fp);
    if (it->second.today_fp <= 0 && it->second.yesterday_fp <= 0) {
        close_outstanding_.erase(it);
    }
}

void XtOrderRuntimeBook::add_slot_close_contribution(const OrderLifecycleSlot& slot) {
    if (!has_close_contribution(slot)) return;
    add_close_outstanding_delta(slot.instrument_id,
                                slot.is_buy,
                                unfilled_today_fp(slot),
                                unfilled_yesterday_fp(slot));
}

void XtOrderRuntimeBook::remove_slot_close_contribution(const OrderLifecycleSlot& slot) {
    if (!has_close_contribution(slot)) return;
    add_close_outstanding_delta(slot.instrument_id,
                                slot.is_buy,
                                -unfilled_today_fp(slot),
                                -unfilled_yesterday_fp(slot));
}

size_t XtOrderRuntimeBook::reconcile_close_outstanding_from_slots_() {
    // T1.153 tripwire：close_outstanding_ 本质是 alive slot unfilled close 量的纯聚合，
    // 挂 reap 节拍从 slot 池重算比对。漂移 = 某写路径漏了 remove/add 括号（残留
    // hold 会把策略平仓死锁到重启，2026-07-17 22:42 实钓）→ loud + counter +
    // 采纳重算值自愈：平仓可用性优先，噪声保留供根因追踪，不静默。
    std::unordered_map<CloseOutstandingKey, CloseOutstandingAgg, CloseOutstandingKeyHash> fresh;
    for (const auto& kv : id_to_slot_idx_) {
        const uint16_t idx = kv.second;
        if (idx >= kMaxInflightOrders) continue;
        const OrderLifecycleSlot& slot = slot_pool_[idx];
        if (slot.id == 0 || !has_close_contribution(slot)) continue;
        const int64_t today_fp = unfilled_today_fp(slot);
        const int64_t yesterday_fp = unfilled_yesterday_fp(slot);
        if (today_fp <= 0 && yesterday_fp <= 0) continue;
        auto& agg = fresh[CloseOutstandingKey{slot.instrument_id, slot.is_buy}];
        agg.today_fp += today_fp;
        agg.yesterday_fp += yesterday_fp;
    }
    bool drift = (fresh.size() != close_outstanding_.size());
    if (!drift) {
        for (const auto& kv : fresh) {
            auto it = close_outstanding_.find(kv.first);
            if (it == close_outstanding_.end() ||
                it->second.today_fp != kv.second.today_fp ||
                it->second.yesterday_fp != kv.second.yesterday_fp) {
                drift = true;
                break;
            }
        }
    }
    if (!drift) return 0;
    std::fprintf(stderr,
                 "[xt_runtime_book] close_outstanding drift healed: entries %zu -> %zu\n",
                 close_outstanding_.size(), fresh.size());
    close_outstanding_ = std::move(fresh);
    xt_runtime_book_drops::close_outstanding_drift_healed().fetch_add(
        1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
    ct::metrics::XtMetricRegistry::instance().inc_runtime_book_drop_total(
        "close_outstanding_drift");
#endif
    return 1;
}

// ── 公共 API：报单受理 ──────────────────────────────────────────────────────────

OrderSnapshot
XtOrderRuntimeBook::build_order_snapshot_(const OrderLifecycleSlot& slot) const noexcept {
    OrderSnapshot out;
    out.id = slot.id;
    out.instrument_id = slot.instrument_id;
    copy_snapshot_code(out.code, instrument_registry_.code_of(slot.instrument_id));
    out.is_buy = slot.is_buy;
    out.is_close = slot.is_close;
    out.close_offset = slot.close_offset;
    out.price_fp = slot.price_fp;
    out.leaves_qty_fp = slot.leaves_qty_fp;
    out.locked_today_fp = slot.locked_today_fp;
    out.locked_yesterday_fp = slot.locked_yesterday_fp;
    out.filled_today_fp = slot.filled_today_fp;
    out.filled_yesterday_fp = slot.filled_yesterday_fp;
    out.seq = slot.seq;
    out.sid_id = slot.sid_id;
    return out;
}

void XtOrderRuntimeBook::publish_snapshot_() noexcept {
    auto* fresh = acquire_order_book_snapshot(snapshot_pool_, hp_domain_);
    if (!fresh) {
        // pool 耗尽 = reader 长持 guard 跨多次发布；跳过本次发布后 reader 持续读
        // stale snapshot，counter 是唯一可被 prom 采集的信号。
        xt_runtime_book_drops::snapshot_publish_skipped().fetch_add(1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance().inc_runtime_book_drop_total(
            "snapshot_publish_skipped");
#endif
        std::fprintf(stderr, "XtOrderRuntimeBook snapshot pool exhausted\n");
        return;
    }
    // 只重置可见计数：reader 契约只读 [0, *_count)，下方填充循环会对可见
    // 窗口内条目全字段显式赋值；count 之后的 pool 复用区域允许残留上一代内容。
    fresh->order_count = 0;
    fresh->tracked_code_count = 0;
    fresh->force_flat_pending_count = 0;
    fresh->close_outstanding_count = 0;
    fresh->generation = next_generation_++;

    // orders 数组截断结构性不可达：alive slot ≤ kMaxInflightOrders == kOrderSnapshotMaxOrders
    //（同幅 512），id_to_slot_idx_ 条目数不可能超过数组容量 → 不做截断观测。
    for (const auto& kv : id_to_slot_idx_) {
        if (fresh->order_count >= fresh->orders.size()) break;
        const auto idx = kv.second;
        if (idx >= kMaxInflightOrders) continue;
        const auto& slot = slot_pool_[idx];
        if (slot.id == 0) continue;
        fresh->orders[fresh->order_count++] = build_order_snapshot_(slot);
    }

    // tracked_codes_ 是唯一可真实超容量的数组（set 无上界，registry 上限 4096 >
    // kOrderSnapshotMaxCodes 512）；截断让跨线程 reader 对被截合约静默失明，必须可观测。
    bool tracked_codes_hit_truncation = false;
    for (const XtInstrumentId iid : tracked_codes_) {
        if (fresh->tracked_code_count >= fresh->tracked_codes.size()) {
            tracked_codes_hit_truncation = true;
            break;
        }
        const char* code = instrument_registry_.code_of(iid);
        if (!code || code[0] == '\0') continue;
        auto& out = fresh->tracked_codes[fresh->tracked_code_count++];
        out.instrument_id = iid;
        copy_snapshot_code(out.code, code);
        // pool 复用条目可能残留上一代值；tracked_codes 数组中该字段契约为 0
        //（仅 force_flat_pending_codes 数组有意义）。
        out.force_flat_sid_id = 0;
    }
    if (tracked_codes_hit_truncation) {
        xt_runtime_book_drops::tracked_codes_truncated().fetch_add(1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance().inc_runtime_book_drop_total(
            "tracked_codes_truncated");
#endif
        static std::atomic<bool> warned{false};
        if (!warned.exchange(true)) {
            std::fprintf(stderr,
                         "[OrderBook][critical] tracked codes exceed snapshot capacity "
                         "(%zu > %zu) — cross-thread readers blind to truncated codes\n",
                         tracked_codes_.size(), fresh->tracked_codes.size());
        }
    }

    // member scratch 复用容量：发布在 on_trade 等热路径上，避免每次构造临时 set 堆分配。
    publish_force_flat_keys_.clear();
    // O(alive) 迭代：id_to_slot_idx_ 即 alive slot 集合；(iid,sid) 去重后发布，
    // 数组顺序不作为契约，reader 只按集合语义线性查找。
    for (const auto& kv : id_to_slot_idx_) {
        const auto slot_idx = kv.second;
        if (slot_idx >= kMaxInflightOrders) continue;
        const auto& slot = slot_pool_[slot_idx];
        if (slot.id == 0 || !slot.is_close || slot.leaves_qty_fp <= 0) continue;
        const XtInstrumentId iid = slot.instrument_id;
        if (iid == kInvalidInstrumentId) continue;
        const uint32_t sid_id = slot.sid_id;
        const uint64_t key = (static_cast<uint64_t>(iid) << 32) ^ sid_id;
        auto seen = publish_force_flat_keys_.find(key);
        if (seen != publish_force_flat_keys_.end()) {
            // 同 (iid,sid) 多 slot：OR 合并 genuine 位（任一真·force-flat 即令该 key genuine
            // pending），不依赖 unordered_map 迭代序 —— 防策略普通平仓单与 panel/closeout 真·
            // force-flat 同 (iid,sid) 共存时 is_genuine 非确定致 panel 误 churn 自己的强平单。
            if (slot.is_force_flat) {
                fresh->force_flat_pending_codes[seen->second].is_genuine_force_flat = true;
            }
            continue;
        }
        if (fresh->force_flat_pending_count >= fresh->force_flat_pending_codes.size()) continue;
        const char* code = instrument_registry_.code_of(iid);
        if (!code || code[0] == '\0') continue;
        const std::size_t out_idx = fresh->force_flat_pending_count++;
        publish_force_flat_keys_.emplace(key, out_idx);
        auto& out = fresh->force_flat_pending_codes[out_idx];
        out.instrument_id = iid;
        copy_snapshot_code(out.code, code);
        out.force_flat_sid_id = sid_id;
        out.is_genuine_force_flat = slot.is_force_flat;
    }

    for (const auto& kv : close_outstanding_) {
        if (fresh->close_outstanding_count >= fresh->close_outstanding.size()) break;
        const char* code = instrument_registry_.code_of(kv.first.instrument_id);
        if (!code || code[0] == '\0') continue;
        auto& out = fresh->close_outstanding[fresh->close_outstanding_count++];
        out.instrument_id = kv.first.instrument_id;
        copy_snapshot_code(out.code, code);
        out.is_buy = kv.first.is_buy;
        out.today_fp = kv.second.today_fp;
        out.yesterday_fp = kv.second.yesterday_fp;
    }

    const auto* retired = current_.exchange(fresh, std::memory_order_acq_rel);
    if (retired) {
        hp_domain_.retire_raw(retired, &snapshot_pool_, release_order_book_snapshot);
    }
    hp_domain_.try_scan_if_due();
}

void XtOrderRuntimeBook::populate_submit_slot_(OrderLifecycleSlot& atom,
                                               uint64_t id,
                                               XtInstrumentId iid,
                                               bool is_buy,
                                               int64_t qty_fp,
                                               int64_t price_fp,
                                               XtOffset close_offset,
                                               StrategySid sid) {
    if (close_offset == XtOffset::Auto) {
        // XtOffset.h 契约：Auto 只是策略 submit-time sentinel，提交入口必须解析成
        // Open/Close 或拒单。落到这里说明上游解析漏了；沿用既有语义（Auto 与 Close
        // 同走昨仓优先分配）不改行为，只把违约打亮。
        static std::atomic<bool> warned{false};
        if (!warned.exchange(true, std::memory_order_relaxed)) {
            std::fprintf(stderr,
                         "[OrderBook][error] on_order_submitted: unresolved XtOffset::Auto "
                         "reached the runtime book (first occurrence, id=%llu) — submit entry "
                         "must resolve the sentinel; treating it as a close order.\n",
                         static_cast<unsigned long long>(id));
        }
        // 诊断信息先落盘，再让 debug 构建 abort。
        assert(false && "XtOffset::Auto must be resolved before reaching the runtime book");
    }
    atom.id = id;
    atom.instrument_id = iid;
    atom.is_buy = is_buy;
    // is_close 恒为 close_offset 的派生量，不接受 caller 传入的独立判据。
    atom.is_close = (close_offset != XtOffset::Open);
    atom.close_offset = close_offset;
    atom.price_fp = price_fp;
    atom.leaves_qty_fp = qty_fp > 0 ? qty_fp : 0;
    atom.qty_fp = qty_fp > 0 ? qty_fp : 0;
    atom.filled_upd_fp = 0;
    atom.filled_trd_fp = 0;
    atom.seq = ++open_order_seq_;
    atom.locked_today_fp = 0;
    atom.locked_yesterday_fp = 0;
    // slot.sid_id 来自 XtOrderRequest.sid_id；成交回报后续通过 order_id 反查归属。
    // 0 表示 no-sid / single-strategy 路径。
    atom.sid_id = sid.raw();
    xt_identity::OrderAttributionStore::remember_order_strategy_sid(id, sid);
    atom.filled_today_fp = 0;
    atom.filled_yesterday_fp = 0;
    atom.status = XtOrderStatus::Submitted;  // submit-visible 初值（collect_open_orders 用）
    atom.recovered_unowned = false;  // slot 复用重置：本进程 submit 必非孤儿
    atom.orphan_reap_attempted = false;  // 同上：复用 slot 清 reap 标记
    atom.is_force_flat = false;  // slot 复用重置：普通策略平仓单非 force-flat（区别 record_force_flat_order）
}

bool XtOrderRuntimeBook::should_skip_submit(uint64_t id) {
    assert(id != 0);
    if (id == 0) std::abort();
    // 终态早于 submit ack 到达时，submit 入口必须消费 tombstone，避免重新建 slot。
    return order_final_before_submit_.erase(id) > 0 || terminal_order_seen_(id);
}

XtOrderRuntimeBook::SubmitRegisterResult XtOrderRuntimeBook::on_order_submitted(
    uint64_t id,
    const char* code,
    bool is_buy,
    int64_t qty_fp,
    int64_t price_fp,
    XtOffset close_offset,
    const xt_account::CloseableView& closeable_view,
    StrategySid sid) {
    using Outcome = SubmitRegisterResult::Outcome;
    assert(id != 0);
    if (id == 0) std::abort();

    // closeout/panel 控制线程提交：注册整体必达投递到 worker（marshal 入队 / 溢出侧道 /
    // quiescent 内联），守 slot_pool_ 单写者契约——off-worker 绝不跨线程直写。
    if (!is_bound_worker_thread()) {
        std::string code_copy(code ? code : "");
        const bool posted =
            post_to_worker([this, id, code_copy = std::move(code_copy), is_buy, qty_fp,
                            price_fp, close_offset, closeable_view, sid] {
                (void)on_order_submitted(id, code_copy.c_str(), is_buy, qty_fp, price_fp,
                                         close_offset, closeable_view, sid);
            });
        return {posted ? Outcome::kDeferred : Outcome::kDropped, 0, 0};
    }

    // 终态先到（tombstone 已记录）：消费 tombstone 并跳过注册，不留 phantom slot。
    if (should_skip_submit(id)) return {Outcome::kSkippedTerminal, 0, 0};

    // submit 是 worker-owned mutable book 的单写者路径。
    // slot_pool_ + id_to_slot_idx_ + close_outstanding_ + tracked_codes_ 的
    // 批量写入，通过后续 immutable snapshot publish 暴露给
    // HftRunnerControl / collect_* 等外部 reader。

    // 同 id 命中仍存活的 slot：无合法来源（order id 由 per-instance 单调发号器产出，
    // recovery 反向灌走 fillup 独立入口），覆盖会抹掉旧单已推进的成交进度 / close 冻结 /
    // sid 归属。fail-closed 保留旧 slot，只留可观测信号。
    if (const OrderLifecycleSlot* live = find_slot(id)) {
        xt_runtime_book_drops::duplicate_submit_blocked().fetch_add(
            1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance().inc_runtime_book_drop_total(
            "duplicate_submit");
#endif
        std::fprintf(stderr,
                     "[OrderBook][error] on_order_submitted: duplicate id for a live slot — "
                     "registration dropped, existing slot kept. id=%llu code=%s "
                     "(existing: leaves_fp=%lld qty_fp=%lld filled_upd_fp=%lld "
                     "filled_trd_fp=%lld sid_id=%u) "
                     "(order-id generator collision / duplicate submit replay?).\n",
                     static_cast<unsigned long long>(id), code ? code : "",
                     static_cast<long long>(live->leaves_qty_fp),
                     static_cast<long long>(live->qty_fp),
                     static_cast<long long>(live->filled_upd_fp),
                     static_cast<long long>(live->filled_trd_fp),
                     live->sid_id);
        // 诊断信息先落盘（counter + 日志），再让 debug 构建 abort。
        assert(false && "duplicate on_order_submitted for a live order id");
        return {Outcome::kBlockedDuplicate, 0, 0};
    }

    // cold path：首次见到该合约时注册，之后 O(1) 查找
    const XtInstrumentId iid = (code && code[0] != '\0')
                                   ? instrument_registry_.register_or_get(code)
                                   : kInvalidInstrumentId;

    const bool is_close = (close_offset != XtOffset::Open);
    bool inserted = false;
    XtOrderRuntimeBook::OrderLifecycleSlot* atom = upsert_slot(id, inserted);
    // slot pool 满时 drop counter 已在 alloc_slot() 内递增；对 caller 等价于注册未落地。
    if (!atom) return {Outcome::kDropped, 0, 0};

    populate_submit_slot_(*atom, id, iid, is_buy, qty_fp, price_fp, close_offset, sid);

    if (is_close && qty_fp > 0) {
        init_close_locks_on_submit(iid, is_buy, qty_fp, close_offset, closeable_view, *atom);
    }

    add_slot_close_contribution(*atom);

    if (iid != kInvalidInstrumentId) {
        tracked_codes_.insert(iid);
    }
    publish_snapshot_();
    // 注册落地：按到达序重放先到的同 id 非终态 update（venue ack 跑赢注册的乱序吸收），
    // 再重放先到的独立 Trade（update 先、trade 后；leaves = qty − max(upd, trd) 对任意
    // 交错收敛，见 slot_leaves_from_filled）。空表 O(1) 早退。
    replay_pre_registration_updates_(id);
    replay_pre_registration_trades_(id);

    // replay 期间该单可能已走完终态（暂存 trade 全成 → erase slot + remember terminal）：
    // 二次消费 tombstone，caller 据此跳过冻结，对齐 terminal-first 无 phantom frozen（ORBR-1）。
    if (should_skip_submit(id)) return {Outcome::kSkippedTerminal, 0, 0};

    // replay 后的累计成交进度：caller 据此按增量释放 frozen（AccountState delta 语义幂等）。
    const OrderLifecycleSlot* live = find_slot(id);
    if (!live) return {Outcome::kSkippedTerminal, 0, 0};  // 终态已 erase（防御，上分支已覆盖）
    return {Outcome::kRegistered,
            std::max(live->filled_upd_fp, live->filled_trd_fp),
            slot_leaves_from_filled(*live)};
}

XtOrderRuntimeBook::SubmitRegisterResult XtOrderRuntimeBook::on_order_submitted(
    uint64_t id,
    const char* code,
    bool is_buy,
    int64_t qty_fp,
    int64_t price_fp,
    XtOffset close_offset,
    const xt_account::CloseableView& closeable_view) {
    return on_order_submitted(id, code, is_buy, qty_fp, price_fp, close_offset,
                              closeable_view, StrategySid::legacy());
}

// ── 平仓冻结初始化 ──────────────────────────────────────────────────────────────

void XtOrderRuntimeBook::init_close_locks_on_submit(XtInstrumentId instrument_id,
                                                    bool is_buy,
                                                    int64_t qty_fp,
                                                    XtOffset close_offset,
                                                    const xt_account::CloseableView& closeable_view,
                                                    OrderLifecycleSlot& slot) const {
    slot.locked_today_fp = 0;
    slot.locked_yesterday_fp = 0;
    slot.filled_today_fp = 0;
    slot.filled_yesterday_fp = 0;
    if (qty_fp <= 0) return;

    if (close_offset == XtOffset::CloseToday) {
        slot.locked_today_fp = qty_fp;
        return;
    }
    if (close_offset == XtOffset::CloseYesterday) {
        slot.locked_yesterday_fp = qty_fp;
        return;
    }

    // Auto 平仓：昨仓优先，按当前快照减在途冻结做分配。
    int64_t outstanding_today_fp = 0;
    int64_t outstanding_yesterday_fp = 0;
    // 内部调用 close_outstanding_ 直接查询（避免重新 id_of 查找）
    const CloseOutstandingKey key{instrument_id, is_buy};
    auto agg_it = close_outstanding_.find(key);
    if (agg_it != close_outstanding_.end()) {
        outstanding_today_fp = agg_it->second.today_fp;
        outstanding_yesterday_fp = agg_it->second.yesterday_fp;
    }

    const int64_t available_today_fp =
        closeable_view.today_fp > outstanding_today_fp
            ? (closeable_view.today_fp - outstanding_today_fp)
            : 0;
    const int64_t available_yesterday_fp =
        closeable_view.yesterday_fp > outstanding_yesterday_fp
            ? (closeable_view.yesterday_fp - outstanding_yesterday_fp)
            : 0;

    int64_t left_fp = qty_fp;
    const int64_t alloc_yesterday_fp = std::min(left_fp, available_yesterday_fp);
    left_fp -= alloc_yesterday_fp;
    const int64_t alloc_today_fp = std::min(left_fp, available_today_fp);
    left_fp -= alloc_today_fp;

    slot.locked_yesterday_fp = alloc_yesterday_fp;
    slot.locked_today_fp = alloc_today_fp;
    if (left_fp > 0) {
        // 极端乱序/快照边界下兜底：剩余量归入昨仓（可能过度锁定昨仓）
        slot.locked_yesterday_fp += left_fp;
#ifndef NDEBUG
        fprintf(stderr, "[xt_runtime] WARN: close_outstanding fallback — "
                "left_fp=%lld归入昨仓 (instrument_id=%u, is_buy=%d)\n",
                (long long)left_fp, instrument_id, (int)is_buy);
#endif
    }
}

// ── 成交处理 ──────────────────────────────────────────────────────────────────

void XtOrderRuntimeBook::accumulate_close_fills_on_trade(OrderLifecycleSlot& slot,
                                                         int64_t fill_qty_fp,
                                                         bool fill_today) {
    if (!slot.is_close || fill_qty_fp <= 0) return;

    auto consume = [&](int64_t& filled_fp, int64_t locked_fp, int64_t& left_fp) {
        if (left_fp <= 0) return;
        const int64_t room_fp = locked_fp > filled_fp ? (locked_fp - filled_fp) : 0;
        if (room_fp <= 0) return;
        const int64_t use_fp = std::min(left_fp, room_fp);
        filled_fp += use_fp;
        left_fp -= use_fp;
    };

    int64_t left_fp = fill_qty_fp;
    if (fill_today) {
        consume(slot.filled_today_fp, slot.locked_today_fp, left_fp);
        consume(slot.filled_yesterday_fp, slot.locked_yesterday_fp, left_fp);
        if (left_fp > 0) slot.filled_today_fp += left_fp;
    } else {
        consume(slot.filled_yesterday_fp, slot.locked_yesterday_fp, left_fp);
        consume(slot.filled_today_fp, slot.locked_today_fp, left_fp);
        if (left_fp > 0) slot.filled_yesterday_fp += left_fp;
    }
}

int64_t XtOrderRuntimeBook::slot_leaves_from_filled(const OrderLifecycleSlot& s) {
    const int64_t filled = std::max(s.filled_upd_fp, s.filled_trd_fp);
    return s.qty_fp > filled ? (s.qty_fp - filled) : 0;
}

// leaves 合流契约：slot 同时维护 update 绝对累计和 trade 增量累计，
// leaves = qty - max(filled_upd_fp, filled_trd_fp)。update/trade 任意到达顺序
// 均应幂等；stale NEW/Accepted update 不得回退已知成交进度。
void XtOrderRuntimeBook::on_trade(const XtTradeRecord& tr) {
    // 溢出侧道自清：worker rtn 入口兜底 drain，让 CTA/SEL 等无 event-loop drain hook
    // 的宿主也能及时落地暂存注册（「注册先于回报」）。侧道空时单次 relaxed load
    // 早退（BT 恒空）。
    drain_marshal_side_channel();
    const int64_t tr_qty_fp = tr.qty_fp;
    if (tr.order_id == 0 || tr_qty_fp <= 0) return;

    XtOrderRuntimeBook::OrderLifecycleSlot* atom = find_slot(tr.order_id);
    if (!atom) {
        // 独立 Trade 早于本地注册到达（XTP OnTradeEvent / Binance REST gap-fill Trade-only）：
        // 终态已见（进度已由 update 路径覆盖 / 注册已被 tombstone 吞掉）= 双源乱序的 stale
        // trade，不暂存——对称 on_order_update 的 stale 非终态丢弃分支（:984-989）。否则进
        // 最小投影 Trade 暂存表，注册落地时按到达序重放（gating 全在本 miss 分支内，
        // slot-found 主路径 0 新增指令）。
        if (terminal_order_seen_(tr.order_id)) return;
        stash_pre_registration_trade_(tr.order_id, tr_qty_fp, tr.is_today);
        return;
    }
    apply_trade_fill_(*atom, tr.order_id, tr_qty_fp, tr.is_today);
}

// 消费一笔成交到既有 slot：双估计器 trade 增量推进 + 今昨 close-lock 分配 +
// 终态 erase（remember_terminal） / 部成 status。on_trade 主路径与 trade replay 共用。
void XtOrderRuntimeBook::apply_trade_fill_(OrderLifecycleSlot& atom, uint64_t id,
                                           int64_t qty_fp, bool is_today) {
    // 包围 slot leaves_qty_fp/filled_*_fp 写 + close_outstanding_
    // 增量 + 终态 erase_slot（id_to_slot_idx_ 删 + slot_pool_ 槽位回收）。
    remove_slot_close_contribution(atom);
    if (atom.qty_fp > 0) {
        atom.filled_trd_fp = std::min(atom.qty_fp, atom.filled_trd_fp + qty_fp);
        atom.leaves_qty_fp = slot_leaves_from_filled(atom);
    } else {
        // qty 未知兜底（创建路径理论上必填 qty，防御保留）：按成交增量扣减。
        atom.leaves_qty_fp = std::max<int64_t>(0, atom.leaves_qty_fp - qty_fp);
    }
    accumulate_close_fills_on_trade(atom, qty_fp, is_today);

    if (atom.leaves_qty_fp <= 0) {
        remember_terminal_order_(id);
        // 终态收尾归属退休：策略与 manual 统一入内存 retired 表（终态→成交乱序窗内精确
        // lookup），仅 sweep 过期 TTL 分档（manual 长 / 策略短）。
        xt_identity::OrderAttributionStore::retire_order_strategy_sid(
            id, XtSteadyClock::steady_now_ms());
        erase_slot(id);
    } else {
        atom.status = XtOrderStatus::PartiallyFilled;  // 部分成交：slot 存活，status 反映真实进度
        add_slot_close_contribution(atom);
    }
    publish_snapshot_();
}

// ── 订单更新处理 ───────────────────────────────────────────────────────────────

XtInstrumentId XtOrderRuntimeBook::resolve_instrument_id_on_update(const XtOrderExecUpdate& upd) const {
    assert(upd.id != 0);
    if (upd.id == 0) std::abort();
    const XtOrderRuntimeBook::OrderLifecycleSlot* slot = find_slot(upd.id);
    if (!slot) return kInvalidInstrumentId;
    return slot->instrument_id;
}

void XtOrderRuntimeBook::upsert_order_atom_on_update(const XtOrderExecUpdate& upd,
                                                      XtInstrumentId upd_instrument_id,
                                                      int64_t effective_remaining_fp) {
    assert(upd.id != 0);
    if (upd.id == 0) std::abort();

    // on_order_update 入口 guard 保证非终态 update 的 slot 已存在；
    // 此函数只更新既有 slot，不创建 orphan slot。
    XtOrderRuntimeBook::OrderLifecycleSlot* atom = find_slot(upd.id);
    if (!atom) return;

    if (upd_instrument_id != kInvalidInstrumentId) atom->instrument_id = upd_instrument_id;
    atom->is_buy = upd.is_buy;
    atom->status = upd.status;  // 同步 venue 回报状态（collect_open_orders 用，非终态）
    // venue 回报可以把本地记为开仓的单翻成平仓语义：binance 从 venue reduceOnly 反推
    // close_offset，而 reduceOnly = ExecFlag::ReduceOnly || close_offset != Open，故
    // 「close_offset=Open + ExecFlag::ReduceOnly」提交的单，回报必带 Close。翻转只单向
    // （回报 Open 不撤销本地已知的平仓语义），is_close 随 close_offset 派生保持不变量；
    // close_outstanding 记账由 on_order_update 的 before/after 括号对称吸收。
    if (upd.close_offset != XtOffset::Open) {
        atom->close_offset = upd.close_offset;
    }
    atom->is_close = (atom->close_offset != XtOffset::Open);
    if (atom->sid_id == 0 && upd.sid_id != 0) {
        atom->sid_id = upd.sid_id;
    }
    if (atom->sid_id != 0) {
        xt_identity::OrderAttributionStore::remember_order_strategy_sid(
            upd.id, StrategySid::from_raw(atom->sid_id));
    }
    // venue 回报携带原始委托量时以 venue 为准（含 slot 创建路径缺 qty 的兜底回填）。
    if (upd.qty_fp > 0) atom->qty_fp = upd.qty_fp;
    if (atom->qty_fp > 0) {
        // update 是绝对口径：max-merge 使 stale/乱序 update（如 NEW 晚到 z=0）
        // 不回退已知成交进度；leaves 统一经双估计器重算。
        const int64_t remaining =
            effective_remaining_fp > 0 ? effective_remaining_fp : 0;
        const int64_t filled_from_upd =
            atom->qty_fp > remaining ? (atom->qty_fp - remaining) : 0;
        if (filled_from_upd > atom->filled_upd_fp) {
            atom->filled_upd_fp = filled_from_upd;
        }
        atom->leaves_qty_fp = slot_leaves_from_filled(*atom);
    } else {
        // qty 未知兜底（理论不可达）：保留 venue 绝对剩余直通。
        atom->leaves_qty_fp = effective_remaining_fp > 0 ? effective_remaining_fp : 0;
    }
}

void XtOrderRuntimeBook::on_order_update(const XtOrderExecUpdate& upd,
                                         bool final_status,
                                         int64_t effective_remaining_fp) {
    // 溢出侧道自清：同 on_trade 入口——暂存注册先于本条回报落地（侧道空时 O(1) 早退）。
    drain_marshal_side_channel();
    // 包围 slot_pool_ + id_to_slot_idx_ + tracked_codes_ +
    // order_final_before_submit_ 的多字段批量写。
    // 终态分支会触发 erase_slot（map.erase + slot 回收），非终态分支会
    // upsert_slot（map.insert 可能 rehash），均需 reader 一致性保护。

    const bool was_locally_submitted = (upd.id != 0) && (find_slot(upd.id) != nullptr);
    if (upd.id != 0 && final_status) {
        remember_terminal_order_(upd.id);
        // 终态一到，同 id 暂存的非终态 update + 独立 Trade 全部 stale → 静默清除（不计 orphan）。
        clear_pre_registration_for_id_(upd.id);
        clear_pre_registration_trades_for_id_(upd.id);
    }
    if (upd.id != 0 && final_status && !was_locally_submitted) {
        // 终态早于 submit ack 到达时记录 tombstone，should_skip_submit() 消费。
        order_final_before_submit_.insert(upd.id);
    }

    if (!final_status && upd.id != 0 && !was_locally_submitted &&
        terminal_order_seen_(upd.id)) {
        // Binance/WS 双信号源可能出现终态或 TRADE 先清 slot，stale NEW/Accepted
        // 后到的乱序。终态 tombstone 命中时忽略 stale 非终态,避免误报 orphan。
        return;
    }

    if (!final_status && upd.id != 0 && !was_locally_submitted) {
        // 注册闭包可能仍在 marshal 队列途中而 venue 同步 ack 先到（off-worker submit：
        // gateway 在返回前 emit Submitted，同 producer FIFO 下 update 先于注册被消费）。
        // 先进 pre-registration 暂存表，注册落地时按到达序重放；TTL 内无注册（真孤儿：
        // reverse sync 漏 / foreign order / userTag 反解失败）或表满才按 orphan 计数。
        // BT 单线程下注册严格先于任何 update → 此路径结构性不可达（唯一角落：
        // slot_pool 打满导致注册被拒后的 update 会入暂存，从即时 orphan 计数变
        // 延迟计数；计数/日志-only，不影响输出字节）。
        stash_pre_registration_update_(upd, effective_remaining_fp);
        return;
    }

    // 入口处直接解析 XtInstrumentId，避免 update 路径构造临时 std::string。
    const XtInstrumentId upd_iid = resolve_instrument_id_on_update(upd);
    if (upd_iid != kInvalidInstrumentId) tracked_codes_.insert(upd_iid);

    if (final_status && upd.id != 0) {
        XtOrderRuntimeBook::OrderLifecycleSlot* slot = find_slot(upd.id);
        if (slot) {
            remove_slot_close_contribution(*slot);
            // 终态收尾归属退休（同全成路径）：策略与 manual 统一入 retired 表（TTL 分档）。
            xt_identity::OrderAttributionStore::retire_order_strategy_sid(
                upd.id, XtSteadyClock::steady_now_ms());
            erase_slot(upd.id);
        }
        publish_snapshot_();
        return;
    }

    OrderLifecycleSlot before;
    bool has_before = false;
    if (upd.id != 0) {
        XtOrderRuntimeBook::OrderLifecycleSlot* slot = find_slot(upd.id);
        if (slot) {
            before = *slot;
            has_before = true;
        }
    }
    upsert_order_atom_on_update(upd, upd_iid, effective_remaining_fp);

    if (upd.id != 0) {
        XtOrderRuntimeBook::OrderLifecycleSlot* slot = find_slot(upd.id);
        if (slot) {
            if (has_before) remove_slot_close_contribution(before);
            add_slot_close_contribution(*slot);
        }
    }
    publish_snapshot_();
}

// ── CloseOutstanding 聚合查询 ──────────────────────────────────────────────────

void XtOrderRuntimeBook::accumulate_close_outstanding_fp(const char* code,
                                                          bool is_buy,
                                                          int64_t& out_today_fp,
                                                          int64_t& out_yesterday_fp) const {
    out_today_fp = 0;
    out_yesterday_fp = 0;

    auto guard = hp_domain_.protect(current_);
    if (!guard) return;

    for (std::size_t i = 0; i < guard->close_outstanding_count; ++i) {
        const auto& entry = guard->close_outstanding[i];
        if (entry.is_buy != is_buy || !snapshot_code_equals(entry.code, code)) continue;
        out_today_fp = entry.today_fp;
        out_yesterday_fp = entry.yesterday_fp;
        break;
    }
    out_today_fp = xt_fp::clamp_nonneg(out_today_fp);
    out_yesterday_fp = xt_fp::clamp_nonneg(out_yesterday_fp);
}

// ── 杂项公共方法 ───────────────────────────────────────────────────────────────

void XtOrderRuntimeBook::pre_warm_instrument(const char* code) {
    if (!code || code[0] == '\0') return;
    // pre_warm 通常在启动期单线程调用，无并发 reader；为统一所有
    // 公共 writer 入口（防御后续误用），发布 immutable snapshot。
    instrument_registry_.register_or_get(code);
    publish_snapshot_();
}

void XtOrderRuntimeBook::track_code(const char* code) {
    if (!code || code[0] == '\0') return;
    // tracked_codes_ insert 可能触发 rehash，跨线程 reader 通过
    // collect_tracked_codes 等 snapshot 访问时需 retry 重读。
    track_code_unlocked(code);
    publish_snapshot_();
}

// 内部 helper，调用方负责在单写者路径调用。供 sync_position_
// snapshot 等批量循环外部统一加 guard，避免每个 track 各起一个 writer 窗口
// 导致 reader retry 命中率上升。
void XtOrderRuntimeBook::track_code_unlocked(const char* code) {
    if (!code || code[0] == '\0') return;
    const XtInstrumentId iid = instrument_registry_.register_or_get(code);
    if (iid != kInvalidInstrumentId) tracked_codes_.insert(iid);
}

void XtOrderRuntimeBook::reset_for_recovery(const std::vector<std::string>& position_codes,
                                             bool is_cold_start) {
    if (is_cold_start) {
        // cold-start 路径全量重建 close_outstanding_/tracked_codes_/
        // order_final_before_submit_ + 槽位池重置，多字段批量写需原子可见性。
        close_outstanding_.clear();
        open_order_seq_ = 0;
        order_final_before_submit_.clear();
        order_terminal_seen_.clear();
        order_terminal_fifo_.clear();
        tracked_codes_.clear();
        // 槽位全量 wipe → 暂存的 pre-registration update + Trade 一并作废（旧 session stale）；
        // warm reconnect 保留 slot 与暂存表（注册仍可能落地重放，TTL 兜底）。
        pre_reg_stash_.clear();
        pre_reg_trade_stash_.clear();
        // 槽位池重置（保留 instrument_registry_，合约 ID 仍有效）
        init_slot_pool();
        for (const auto& code : position_codes) {
            if (code.empty()) continue;
            const XtInstrumentId iid = instrument_registry_.register_or_get(code.c_str());
            if (iid != kInvalidInstrumentId) tracked_codes_.insert(iid);
        }
        publish_snapshot_();
        return;
    }

    // warm reconnect: slot_pool_ / close_outstanding_ / order_final_before_submit_
    // 全部保留；仅去重式合并 tracked_codes_，让新合约
    // (recovery 后 venue 上的) 进入跟踪集合，旧合约保留不清。
    for (const auto& code : position_codes) {
        if (code.empty()) continue;
        const XtInstrumentId iid = instrument_registry_.register_or_get(code.c_str());
        if (iid != kInvalidInstrumentId) tracked_codes_.insert(iid);
    }
    publish_snapshot_();
}

void XtOrderRuntimeBook::collect_tracked_codes(std::vector<std::string>& out) const {
    out.clear();
    auto guard = hp_domain_.protect(current_);
    if (!guard) return;
    out.reserve(guard->tracked_code_count);
    for (std::size_t i = 0; i < guard->tracked_code_count; ++i) {
        const auto& entry = guard->tracked_codes[i];
        if (entry.code[0] != '\0') out.emplace_back(entry.code);
    }
}

std::vector<std::string> XtOrderRuntimeBook::sync_position_snapshot(
    const std::vector<std::string>& position_codes,
    bool is_recovery,
    bool is_cold_start) {
    std::vector<std::string> stale_codes;
    if (is_recovery) {
        // recovery 输入是 venue 完整持仓快照：已 tracked 但本次缺席的 code 视为
        // venue 已归零。stale code 必须在 reset_for_recovery() 合并新 tracked_codes_
        // 之前计算，否则旧 code 会被新集合覆盖后丢失清零信号。
        {
            std::unordered_set<std::string> seen;
            seen.reserve(position_codes.size());
            for (const auto& c : position_codes) {
                if (!c.empty()) seen.insert(c);
            }
            std::vector<std::string> tracked;
            collect_tracked_codes(tracked);
            for (auto& code : tracked) {
                if (!code.empty() && seen.count(code) == 0) {
                    stale_codes.push_back(std::move(code));
                }
            }
        }
        // cold-start 全量重建；warm reconnect 只合并 tracked_codes_，保留本进程
        // 已知 alive in-flight 订单视角。
        reset_for_recovery(position_codes, is_cold_start);
        return stale_codes;
    }
    // 非 recovery: 收集当前 tracked 但不在新快照中的合约。
    std::unordered_set<std::string> seen;
    seen.reserve(position_codes.size());
    for (const auto& c : position_codes) {
        if (!c.empty()) seen.insert(c);
    }
    std::vector<std::string> tracked;
    collect_tracked_codes(tracked);
    for (auto& code : tracked) {
        if (!code.empty() && seen.count(code) == 0) {
            stale_codes.push_back(std::move(code));
        }
    }
    // 跟踪快照中的新合约；末尾一次 publish，避免 reader 看到 partial tracked set。
    for (const auto& c : position_codes) {
        if (!c.empty()) track_code_unlocked(c.c_str());
    }
    publish_snapshot_();
    return stale_codes;
}

// ── 批量槽位迭代（改为遍历 slot_pool_ 数组，缓存友好）────────────────────────

int64_t XtOrderRuntimeBook::open_no_trade_qty_fp(const char* code, bool is_buy) const {
    if (!code || code[0] == '\0') return 0;
    auto guard = hp_domain_.protect(current_);
    if (!guard) return 0;

    int64_t out_fp = 0;
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        const auto& snap = guard->orders[i];
        if (!snapshot_code_equals(snap.code, code)) continue;
        if (snap.is_close || snap.is_buy != is_buy) continue;
        if (snap.leaves_qty_fp > 0) out_fp += snap.leaves_qty_fp;
    }
    return out_fp;
}

int64_t XtOrderRuntimeBook::undone_qty_fp(const char* code) const {
    const bool filter_by_code = (code && code[0] != '\0');
    auto guard = hp_domain_.protect(current_);
    if (!guard) return 0;
    int64_t sum_fp = 0;
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        const auto& snap = guard->orders[i];
        if (filter_by_code && !snapshot_code_equals(snap.code, code)) continue;
        if (snap.id == 0) continue;
        if (snap.leaves_qty_fp > 0) sum_fp += snap.leaves_qty_fp;
    }
    return xt_fp::clamp_nonneg(sum_fp);
}

int64_t XtOrderRuntimeBook::signed_inflight_fp(StrategySid sid, const char* code) const {
    const bool filter_by_code = (code && code[0] != '\0');
    const bool filter_by_sid  = sid.has_value();
    const uint32_t want_sid   = sid.raw();
    auto guard = hp_domain_.protect(current_);
    if (!guard) return 0;
    int64_t sum_fp = 0;
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        const auto& snap = guard->orders[i];
        if (snap.id == 0) continue;
        if (snap.leaves_qty_fp <= 0) continue;
        if (filter_by_code && !snapshot_code_equals(snap.code, code)) continue;
        if (filter_by_sid && snap.sid_id != want_sid) continue;
        // SIGNED + no clamp: this is a per-strategy net in-flight, can be negative.
        sum_fp += snap.is_buy ? snap.leaves_qty_fp : -snap.leaves_qty_fp;
    }
    return sum_fp;
}

void XtOrderRuntimeBook::collect_closeout_requests(std::vector<uint64_t>& cancel_ids) const {
    cancel_ids.clear();

    // 控制面可能 off-worker 调用；必须只读 owner-published immutable snapshot，
    // 不能直接遍历 mutable slot_pool_ / tracked_codes_。
    auto guard = hp_domain_.protect(current_);
    if (!guard) return;

    cancel_ids.reserve(guard->order_count);
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        const auto& snap = guard->orders[i];
        if (snap.leaves_qty_fp > 0) cancel_ids.push_back(snap.id);
    }
}

void XtOrderRuntimeBook::record_force_flat_order(uint64_t oid,
                                                  const std::string& code,
                                                  bool is_buy,
                                                  int64_t qty_fp,
                                                  XtOffset close_offset,
                                                  StrategySid sid) {
    assert(oid != 0);
    assert(!code.empty());
    if (oid == 0 || code.empty()) std::abort();

    // 同 on_order_submitted：off-worker（closeout/panel force-exit 控制线程）注册必达
    // 投递到 worker（marshal 入队 / 溢出侧道 / quiescent 内联），绝不跨线程直写。
    if (!is_bound_worker_thread()) {
        (void)post_to_worker([this, oid, code, is_buy, qty_fp, close_offset, sid] {
            record_force_flat_order(oid, code, is_buy, qty_fp, close_offset, sid);
        });
        return;
    }

    // 终态先到（tombstone 已记录）：不重建 phantom force-flat slot——与普通 submit
    // 注册入口（register_submitted_order_ → should_skip_submit）同一 tombstone 契约。
    if (should_skip_submit(oid)) return;

    // force-flat 单 slot 分配 + close_outstanding 增量 + tracked_codes_ 多字段批量写。

    const XtInstrumentId iid = instrument_registry_.register_or_get(code.c_str());

    bool inserted = false;
    XtOrderRuntimeBook::OrderLifecycleSlot* atom = upsert_slot(oid, inserted);
    if (!atom) return;

    if (!inserted) {
        remove_slot_close_contribution(*atom);
    }
    if (inserted) {
        atom->id = oid;
        atom->seq = ++open_order_seq_;
    }
    atom->instrument_id = iid;
    atom->is_buy = is_buy;
    atom->is_close = true;
    atom->is_force_flat = true;  // 真·force-flat 单：panel 重复强平守卫窄口径据此与策略普通平仓单区分。
    // force-flat 单按构造即平仓；Open 会破 `is_close == (close_offset != Open)` 不变量，
    // 收敛成 Close（下游 locked_* 分配对 Close/Open 本就同走昨仓分支，两 prod caller
    // 分别传 XtOffset::Close 与 closeout 已解析的 req.close_offset，均不触发本收敛）。
    assert(close_offset != XtOffset::Open);
    atom->close_offset =
        (close_offset == XtOffset::Open) ? XtOffset::Close : close_offset;
    atom->leaves_qty_fp = std::max<int64_t>(0, qty_fp);
    atom->qty_fp = std::max<int64_t>(0, qty_fp);
    atom->filled_upd_fp = 0;
    atom->filled_trd_fp = 0;
    atom->locked_today_fp = 0;
    atom->locked_yesterday_fp = 0;
    atom->filled_today_fp = 0;
    atom->filled_yesterday_fp = 0;
    atom->sid_id = sid.raw();
    // force-flat slot 与其余 4 条创建路径对称显式 populate status；
    // 复用既有 slot（极少见的合约切换）时一并重置可能残留的旧状态。
    atom->status = XtOrderStatus::Submitted;
    atom->recovered_unowned = false;
    atom->orphan_reap_attempted = false;  // slot 复用：清 reap 标记
    xt_identity::OrderAttributionStore::remember_order_strategy_sid(oid, sid);

    if (atom->leaves_qty_fp > 0) {
        if (close_offset == XtOffset::CloseToday) {
            atom->locked_today_fp = atom->leaves_qty_fp;
        } else {
            atom->locked_yesterday_fp = atom->leaves_qty_fp;
        }
    }
    add_slot_close_contribution(*atom);

    if (iid != kInvalidInstrumentId) {
        tracked_codes_.insert(iid);
    }
    publish_snapshot_();
    // 注册落地：按到达序重放先到的同 id 非终态 update + 独立 Trade（与 on_order_submitted 对称）。
    replay_pre_registration_updates_(oid);
    replay_pre_registration_trades_(oid);
}

// ── Recovery 反向灌入 alive in-flight orders（upsert + batch quota）──────────

void XtOrderRuntimeBook::fillup_populate_slot_(OrderLifecycleSlot& atom,
                                               const AliveInflightOrder& ord,
                                               XtInstrumentId iid,
                                               bool is_close,
                                               bool inserted_real,
                                               int64_t leaves_fp) {
    atom.instrument_id = iid;
    atom.is_buy = ord.is_buy;
    atom.is_close = is_close;
    atom.close_offset = ord.close_offset;
    atom.price_fp = ord.price_fp;
    atom.leaves_qty_fp = leaves_fp;
    // 双估计器以 venue 快照成交量为种子：insert 路径直接 SET；update 路径
    // max-merge（本地可能已应用 fence 分区后的 post-snapshot fill，进度不回退），
    // 并按估计器重算 leaves（本地知道更多时取更小的真实剩余）。
    {
        const int64_t seed_filled =
            ord.filled_qty_fp > 0 ? ord.filled_qty_fp : 0;
        if (ord.total_qty_fp > 0) atom.qty_fp = ord.total_qty_fp;
        if (inserted_real) {
            atom.filled_upd_fp = seed_filled;
            atom.filled_trd_fp = seed_filled;
        } else {
            atom.filled_upd_fp = std::max(atom.filled_upd_fp, seed_filled);
            atom.filled_trd_fp = std::max(atom.filled_trd_fp, seed_filled);
        }
        if (atom.qty_fp > 0) {
            atom.leaves_qty_fp = slot_leaves_from_filled(atom);
        }
    }
    atom.status = XtOrderStatus::Recovered;  // venue 反向灌：exch status 未知，显式标 Recovered
    atom.locked_today_fp = 0;
    atom.locked_yesterday_fp = 0;
    atom.filled_today_fp = 0;
    atom.filled_yesterday_fp = 0;
}

void XtOrderRuntimeBook::fillup_resolve_sid_(OrderLifecycleSlot& atom,
                                             const AliveInflightOrder& ord,
                                             bool inserted_real) {
    const StrategySid resolved_sid =
        ord.strategy_sid.has_value()
            ? ord.strategy_sid
            : xt_identity::OrderAttributionStore::lookup_order_strategy_sid(ord.local_id);
    const uint32_t incoming_sid_id = resolved_sid.raw();
    if (inserted_real || incoming_sid_id != 0) {
        atom.sid_id = incoming_sid_id;
    }
    if (atom.sid_id != 0) {
        xt_identity::OrderAttributionStore::remember_order_strategy_sid(
            ord.local_id, StrategySid::from_raw(atom.sid_id));
    } else if (inserted_real) {
        xt_identity::OrderAttributionStore::forget_order_strategy_sid(ord.local_id);
    }
    // cross-restart 孤儿判定：insert 路径（本进程 submit 流不认识该单）且 sid
    // 解析为 0 → 无任何在册策略归属；后续 batch 解析出 sid≠0 时清除。
    if (inserted_real) {
        atom.recovered_unowned = (incoming_sid_id == 0);
    } else if (incoming_sid_id != 0) {
        atom.recovered_unowned = false;
    }
}

void XtOrderRuntimeBook::fillup_alloc_close_locks_(OrderLifecycleSlot& atom,
                                                   XtOffset close_offset,
                                                   int64_t leaves_fp,
                                                   xt_account::CloseableView& rem) {
    // 分配 locked_today / locked_yesterday：昨仓优先 (与 init_close_locks_on_submit 对齐),
    // 但允许 explicit CloseToday / CloseYesterday 走显式分支。
    int64_t left_fp = leaves_fp;
    if (close_offset == XtOffset::CloseToday) {
        const int64_t take = std::min(left_fp, rem.today_fp);
        atom.locked_today_fp = take;
        rem.today_fp -= take;
        left_fp -= take;
        if (left_fp > 0) {
            // venue 已 lock 但本进程视角余量不足:兜底 lock_today 仍记 leaves
            atom.locked_today_fp += left_fp;
            left_fp = 0;
        }
    } else if (close_offset == XtOffset::CloseYesterday) {
        const int64_t take = std::min(left_fp, rem.yesterday_fp);
        atom.locked_yesterday_fp = take;
        rem.yesterday_fp -= take;
        left_fp -= take;
        if (left_fp > 0) {
            atom.locked_yesterday_fp += left_fp;
            left_fp = 0;
        }
    } else {
        // Auto Close:昨仓优先
        const int64_t take_y = std::min(left_fp, rem.yesterday_fp);
        atom.locked_yesterday_fp = take_y;
        rem.yesterday_fp -= take_y;
        left_fp -= take_y;

        const int64_t take_t = std::min(left_fp, rem.today_fp);
        atom.locked_today_fp = take_t;
        rem.today_fp -= take_t;
        left_fp -= take_t;

        if (left_fp > 0) {
            // 兜底:剩余归入昨仓 (与 init_close_locks_on_submit 对齐)
            atom.locked_yesterday_fp += left_fp;
            left_fp = 0;
        }
    }
}

void XtOrderRuntimeBook::fillup_alloc_filled_(OrderLifecycleSlot& atom, int64_t filled_fp) {
    // filled_qty_fp > 0 → 把 filled 按相同分配策略累加 (consume locked)
    if (filled_fp <= 0) return;
    // 简化策略:filled 按 lock 比例分配 (今 / 昨);若 lock_today=0 全归昨,
    // 若 lock_yesterday=0 全归今;否则按 lock 比例。
    if (atom.locked_today_fp == 0) {
        atom.filled_yesterday_fp = std::min(filled_fp, atom.locked_yesterday_fp);
    } else if (atom.locked_yesterday_fp == 0) {
        atom.filled_today_fp = std::min(filled_fp, atom.locked_today_fp);
    } else {
        // 按 lock 比例分配。中间量用 __int128：定点 1e6 scale 下几千手级仓位
        // （如 4000 手 = 4e9 fp）两两相乘即 > INT64_MAX（9.2e18），int64 乘法 signed
        // overflow → t 变负 → filled_today/yesterday 记账损坏。商 t = filled·lt/total
        // 天然 ≤ filled_fp（int64 可表示），余量 y = filled − t 归昨。对不溢出输入与旧
        // int64 实现逐位相同（同操作数、同截断除法，数学恒等）。
        const __int128 total_locked = static_cast<__int128>(atom.locked_today_fp) +
                                      static_cast<__int128>(atom.locked_yesterday_fp);
        if (total_locked > 0) {
            const __int128 prod = static_cast<__int128>(filled_fp) *
                                  static_cast<__int128>(atom.locked_today_fp);
            const int64_t t = static_cast<int64_t>(prod / total_locked);
            const int64_t y = filled_fp - t;
            atom.filled_today_fp = std::min(t, atom.locked_today_fp);
            atom.filled_yesterday_fp = std::min(y, atom.locked_yesterday_fp);
        }
    }
}

namespace {
// fillup batch 内顺序扣减 closeable quota 的临时余量 map key。
// key = (code, is_buy_to_close)，value = 剩余可平量。
struct QuotaKey {
    std::string code;
    bool is_buy_to_close;
    bool operator==(const QuotaKey& rhs) const noexcept {
        return is_buy_to_close == rhs.is_buy_to_close && code == rhs.code;
    }
};
struct QuotaKeyHash {
    size_t operator()(const QuotaKey& k) const noexcept {
        const auto h1 = std::hash<std::string>{}(k.code);
        const auto h2 = std::hash<bool>{}(k.is_buy_to_close);
        return h1 ^ (h2 << 1);
    }
};
}  // namespace

void XtOrderRuntimeBook::fillup_alive_inflight_orders_batch(
    const std::vector<AliveInflightOrder>& orders,
    std::function<xt_account::CloseableView(const std::string& code,
                                             bool is_buy_to_close)> view_for,
    std::vector<AcceptedRearmOrder>* accepted_out) {
    // Upsert 语义：
    // - 命中本地 slot：先撤旧 close_outstanding 贡献，再用 venue alive 状态覆盖重建。
    // - 未命中 slot：按 recovery insert 创建 Recovered 单，sid 解析失败时标记 recovered_unowned。
    // - 同一 batch 内的 closeable quota 按 (code, is_buy_to_close) 顺序扣减，避免多条同向
    //   close 单都按同一份 account 可平量初始化导致超额 lock。
    // - 末尾统一 publish snapshot，让 reader 看到完整 batch 而非 per-order 中间态。
    // 空 batch 也发布快照：cold-start 无 alive order 时，reader 仍需看到 ready 空态。
    std::unordered_map<QuotaKey, xt_account::CloseableView, QuotaKeyHash> remaining;
    remaining.reserve(orders.size() * 2);

    // venue 回传量 sanity 观测：本 batch 是否有 order 的定点量被 clamp（每批限频 stderr）。
    bool batch_qty_clamped = false;
    uint64_t batch_first_bad_id = 0;
    int64_t batch_first_bad_total = 0;
    int64_t batch_first_bad_filled = 0;

    for (const auto& raw : orders) {
        if (raw.local_id == 0 || raw.code.empty()) continue;

        // T1.153 LEAK-2：已见终态的单不得被 stale venue 快照复活（快照拍在 fill/终态
        // 落地前的竞态窗口）。本地 tombstone 是时序权威——跳过防 Recovered 幽灵 slot
        // 重上 close 账（归属已 forget → recovered_unowned → 恰落 reap 直删通道，
        // 与 LEAK-1 叠加成 22:42 残留 hold）。对照 submit 链 gateway should_skip_submit
        // 双闸与 record_force_flat_order 自查的同款先例。
        if (terminal_order_seen_(raw.local_id)) {
            xt_runtime_book_drops::fillup_terminal_resurrect_blocked().fetch_add(
                1, std::memory_order_relaxed);
            continue;
        }

        // venue 回传的 qty/filled 类定点量先过 sanity 上限（唯一 sanitize 点，下游 slot
        // 写入 / 双估计器 seed / close-lock 分摊全部读被 clamp 后的副本）。price_fp 是价格
        // 而非量，不同量级域，故不纳入此上限。
        AliveInflightOrder ord = raw;
        const bool bad_total = clamp_fillup_venue_qty(ord.total_qty_fp);
        const bool bad_filled = clamp_fillup_venue_qty(ord.filled_qty_fp);
        if (bad_total || bad_filled) {
            xt_runtime_book_drops::venue_fillup_qty_sanitized().fetch_add(
                1, std::memory_order_relaxed);
            if (!batch_qty_clamped) {
                batch_qty_clamped = true;
                batch_first_bad_id = raw.local_id;
                batch_first_bad_total = raw.total_qty_fp;
                batch_first_bad_filled = raw.filled_qty_fp;
            }
        }

        const XtInstrumentId iid = instrument_registry_.register_or_get(ord.code.c_str());
        if (iid == kInvalidInstrumentId) continue;

        const bool is_close = (ord.close_offset != XtOffset::Open);
        // closeable_view(pos, is_buy_to_close) 语义：
        //   is_buy_to_close=true  → cover view (close-short by buying)
        //   is_buy_to_close=false → sell view  (close-long by selling)
        // 与 XtLiveOrderGateway 一致:`is_buy_to_close == ord.is_buy`。
        const bool is_buy_to_close = ord.is_buy;

        bool inserted_real = false;
        OrderLifecycleSlot* atom = upsert_slot(ord.local_id, inserted_real);
        if (!atom) {
            // slot pool 满时 drop counter 已由 alloc_slot() 增加。
            continue;
        }

        // update 路径先撤销旧 close contribution，再用 venue alive 状态重建。
        if (!inserted_real) {
            remove_slot_close_contribution(*atom);
        } else {
            atom->id = ord.local_id;
            atom->seq = ++open_order_seq_;
        }

        const int64_t total_fp = ord.total_qty_fp > 0 ? ord.total_qty_fp : 0;
        const int64_t filled_fp = ord.filled_qty_fp > 0 ? ord.filled_qty_fp : 0;
        const int64_t leaves_fp = total_fp > filled_fp ? (total_fp - filled_fp) : 0;

        fillup_populate_slot_(*atom, ord, iid, is_close, inserted_real, leaves_fp);
        fillup_resolve_sid_(*atom, ord, inserted_real);

        if (is_close && leaves_fp > 0) {
            const QuotaKey key{ord.code, is_buy_to_close};
            auto it = remaining.find(key);
            if (it == remaining.end()) {
                // batch 内首次见此 (code, side) 时才读取 account closeable 初值。
                it = remaining.emplace(key, view_for ? view_for(ord.code, is_buy_to_close)
                                                     : xt_account::CloseableView{}).first;
            }
            // close-lock 配额按 venue 快照口径（leaves_fp）分配；update-merge 路径下
            // 本地估计器可能已知更多成交（slot.leaves 更小）→ lock 短暂偏大，方向
            // fail-safe（多锁不少锁），下个 on_order_update/on_trade 即收敛。
            fillup_alloc_close_locks_(*atom, ord.close_offset, leaves_fp, it->second);
            fillup_alloc_filled_(*atom, filled_fp);
        }

        add_slot_close_contribution(*atom);

        tracked_codes_.insert(iid);

        // book 实际接受此单（过 terminal-tombstone skip + 成功建/更新 slot）→ 收进 accepted
        // 清单，让 engine 逐单驱动 STP re-arm（跟随 book 裁决；上层只碰 STP，不回写 book）。
        if (accepted_out)
            accepted_out->push_back(
                AcceptedRearmOrder{ord.code, ord.local_id, ord.is_buy, ord.price_fp});
    }

    // 每批限频 loud（counter 已按 order 计数；日志每 batch 至多一行，避免垃圾输入刷屏）。
    if (batch_qty_clamped) {
        std::fprintf(stderr,
                     "[OrderBook][error] fillup: venue-reported qty out of sane bound "
                     "(<0 or >%lld fp) — clamped to [0,cap], continuing recovery. "
                     "first bad order id=%llu total_fp=%lld filled_fp=%lld "
                     "(clientOrderId/OrderRef decode or transport corruption?).\n",
                     static_cast<long long>(kMaxSaneFillupQtyFp),
                     static_cast<unsigned long long>(batch_first_bad_id),
                     static_cast<long long>(batch_first_bad_total),
                     static_cast<long long>(batch_first_bad_filled));
    }

    // batch 末尾一次性 publish_snapshot_，让 reader 看到完整新增/更新结果。
    publish_snapshot_();

    // reverse-sync fillup 也是一个注册入口：建 slot 后按到达序重放同 id 的早到暂存
    // update（与 on_order_submitted / record_force_flat_order 的注册重放对称）。否则
    // 只经 reverse-sync 注册（无本地 on_order_submitted）的单的早到 update 会滞留到
    // TTL sweep 被误计为 orphan（"reverse sync missed" 假告警）。
    // upsert_order_atom_on_update 的 filled max-merge 保证 stale/乱序 update 不回退已恢复
    // 的成交进度（venue 快照为准，重放仅 fail-safe 补齐 + 消费暂存）。
    // stash 常态为空（BT 结构性不可达，单线程注册严格先于 update/trade）→ 空表 O(1) 早退，
    // 不触发任何额外 publish。
    if (!pre_reg_stash_.empty() || !pre_reg_trade_stash_.empty()) {
        for (const auto& raw : orders) {
            if (raw.local_id == 0) continue;
            if (find_slot(raw.local_id) != nullptr) {
                // update 重放安全：update 是绝对累计，与 fillup 绝对量 max-merge 幂等。
                replay_pre_registration_updates_(raw.local_id);
                // 暂存 trade 是增量：fillup 的 venue 绝对量已吸收 pre-fill，重放会在
                // 绝对量之上双计 filled_trd（CtpExecDeliveryPartition 钉）——同 id 清除吸收，
                // 快照后新成交由正常 on_trade / 累计 update 覆盖。
                clear_pre_registration_trades_for_id_(raw.local_id);
            }
        }
    }
}

void XtOrderRuntimeBook::collect_active_order_prices(const char* code,
                                                      std::vector<int64_t>& bid_prices,
                                                      std::vector<int64_t>& ask_prices) const {
    bid_prices.clear();
    ask_prices.clear();
    if (!code || code[0] == '\0') return;
    auto guard = hp_domain_.protect(current_);
    if (!guard) return;
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        const auto& s = guard->orders[i];
        if (!snapshot_code_equals(s.code, code)) continue;
        if (s.id == 0 || s.leaves_qty_fp <= 0) continue;
        if (s.is_buy) bid_prices.push_back(s.price_fp);
        else          ask_prices.push_back(s.price_fp);
    }
}

// 控制面 force_exit 单 symbol 用。非热路径读取
// owner-published immutable snapshot，避免 off-worker 遍历 mutable slot_pool_。
void XtOrderRuntimeBook::collect_working_ids_for_code(const char* code,
                                                       std::vector<uint64_t>& ids) const {
    ids.clear();
    if (!code || code[0] == '\0') return;
    auto guard = hp_domain_.protect(current_);
    if (!guard) return;
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        const auto& s = guard->orders[i];
        if (!snapshot_code_equals(s.code, code)) continue;
        if (s.id == 0 || s.leaves_qty_fp <= 0) continue;
        ids.push_back(s.id);
    }
}

void XtOrderRuntimeBook::collect_working_ids_for_code_and_sid(const char* code,
                                                               StrategySid sid,
                                                               std::vector<uint64_t>& ids) const {
    ids.clear();
    if (!code || code[0] == '\0' || !sid.has_value()) return;
    const uint32_t sid_id = sid.raw();
    auto guard = hp_domain_.protect(current_);
    if (!guard) return;
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        const auto& s = guard->orders[i];
        if (!snapshot_code_equals(s.code, code)) continue;
        if (s.id == 0 || s.leaves_qty_fp <= 0) continue;
        if (s.sid_id != sid_id) continue;
        ids.push_back(s.id);
    }
}

// 宽口径：任意平仓挂单（is_close && leaves>0）在途。closeout 收集用，避免对已有 working
// close 的 code 再发 force-flat。含策略普通平仓单（panel 重复强平守卫改用窄口径 is_genuine_*）。
bool XtOrderRuntimeBook::is_force_flat_pending(const char* code) const {
    if (!code || code[0] == '\0') return false;
    auto guard = hp_domain_.protect(current_);
    if (!guard) return false;
    for (std::size_t i = 0; i < guard->force_flat_pending_count; ++i) {
        if (snapshot_code_equals(guard->force_flat_pending_codes[i].code, code)) return true;
    }
    return false;
}

// 反查 force-flat 发起方 sid_id；0 表示非 force-flat 或显式 global scope。
uint32_t XtOrderRuntimeBook::force_flat_pending_sid_for(const char* code) const {
    if (!code || code[0] == '\0') return 0;
    auto guard = hp_domain_.protect(current_);
    if (!guard) return 0;
    for (std::size_t i = 0; i < guard->force_flat_pending_count; ++i) {
        if (snapshot_code_equals(guard->force_flat_pending_codes[i].code, code)) {
            return guard->force_flat_pending_codes[i].force_flat_sid_id;
        }
    }
    return 0;
}

// 窄口径：只认 record_force_flat_order 发起的真·force-flat 单（is_genuine_force_flat）。
// panel 重复强平守卫用，让策略普通平仓挂单不阻断 operator runner-wide 接管。
bool XtOrderRuntimeBook::is_genuine_force_flat_pending(const char* code) const {
    if (!code || code[0] == '\0') return false;
    auto guard = hp_domain_.protect(current_);
    if (!guard) return false;
    for (std::size_t i = 0; i < guard->force_flat_pending_count; ++i) {
        if (guard->force_flat_pending_codes[i].is_genuine_force_flat
            && snapshot_code_equals(guard->force_flat_pending_codes[i].code, code)) {
            return true;
        }
    }
    return false;
}

bool XtOrderRuntimeBook::is_genuine_force_flat_pending_for_sid(const char* code, StrategySid sid) const {
    if (!code || code[0] == '\0') return false;
    auto guard = hp_domain_.protect(current_);
    if (!guard) return false;
    for (std::size_t i = 0; i < guard->force_flat_pending_count; ++i) {
        if (guard->force_flat_pending_codes[i].is_genuine_force_flat
            && snapshot_code_equals(guard->force_flat_pending_codes[i].code, code)
            && guard->force_flat_pending_codes[i].force_flat_sid_id == sid.raw()) {
            return true;
        }
    }
    return false;
}

bool XtOrderRuntimeBook::has_genuine_global_force_flat_pending(const char* code) const {
    return is_genuine_force_flat_pending_for_sid(code, StrategySid::legacy());
}

bool XtOrderRuntimeBook::is_collect_thread_allowed_() const noexcept {
    // bound marshal = 权威 worker tid（不靠 first-touch 猜测）。
    if (marshal_bound_.load(std::memory_order_acquire)) {
        // quiescent：worker 已 join，无并发写者 → 任意线程直读安全（shutdown 收尾）。
        if (marshal_quiescent_.load(std::memory_order_acquire)) return true;
        return std::this_thread::get_id() == marshal_worker_tid_;
    }
    // 未绑定（单线程/backtest/测试模式）：first-touch CAS 绑定，之后要求同线程。
    std::thread::id expected{};
    const std::thread::id self = std::this_thread::get_id();
    if (collect_first_touch_tid_.compare_exchange_strong(
            expected, self, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return true;
    }
    return expected == self;
}

void XtOrderRuntimeBook::on_cross_thread_direct_read_blocked_() const noexcept {
    // 跨线程直读 mutable slot_pool_ 是 data race；fail-closed 返回空结果比 race
    // 出垃圾 DTO 喂给策略安全（空列表最坏导致保守重试，被 budget/dedup 兜住）。
    xt_runtime_book_drops::cross_thread_direct_read_blocked().fetch_add(
        1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
    ct::metrics::XtMetricRegistry::instance().inc_runtime_book_drop_total(
        "cross_thread_direct_read");
#endif
    static std::atomic<bool> warned{false};
    if (!warned.exchange(true)) {
        std::fprintf(stderr,
                     "[OrderBook][error] cross-thread direct read blocked — "
                     "collect_* must run on the book worker thread\n");
    }
}

size_t XtOrderRuntimeBook::collect_open_orders(XtOrderExecUpdate* out, size_t max,
                                               const char* code_filter,
                                               uint32_t sid_filter) const {
    if (!out || max == 0) return 0;
    if (!is_collect_thread_allowed_()) {
        on_cross_thread_direct_read_blocked_();
        return 0;
    }

    // 同线程直读 slot_pool_（status 不在跨线程 OrderBookSnapshot，必须读 mutable 状态）。
    XtInstrumentId filter_iid = kInvalidInstrumentId;
    if (code_filter && code_filter[0] != '\0') {
        filter_iid = instrument_registry_.id_of(code_filter);
        if (filter_iid == kInvalidInstrumentId) return 0;  // 未知 code → 无匹配
    }

    size_t n = 0;
    for (const auto& kv : id_to_slot_idx_) {
        if (n >= max) break;
        const uint16_t idx = kv.second;
        if (idx >= kMaxInflightOrders) continue;
        const OrderLifecycleSlot& slot = slot_pool_[idx];
        if (slot.id == 0) continue;
        if (slot.leaves_qty_fp <= 0) continue;  // 对齐 view 的 effective_remaining<=0 erase + rollover 自身 filter
        if (sid_filter != 0 && slot.sid_id != sid_filter) continue;  // 截断前过滤，max 不被他策略单挤占
        if (filter_iid != kInvalidInstrumentId && slot.instrument_id != filter_iid) continue;

        XtOrderExecUpdate u{};
        u.id           = slot.id;
        u.is_buy       = slot.is_buy;
        u.close_offset = slot.close_offset;
        u.status       = slot.status;
        u.price_fp     = slot.price_fp;
        u.filled_fp    = slot.filled_today_fp + slot.filled_yesterday_fp;  // 派生，非承重
        u.leaves_fp    = slot.leaves_qty_fp;                              // 真值（rollover 用）
        u.qty_fp       = u.filled_fp + u.leaves_fp;                        // 派生，非承重
        u.sid_id       = slot.sid_id;
        // symbol_id / last_fill_fp / exch_ts_us / local_cb_us / local_cb_ns / local_strategy_us = 0
        // （open-order 枚举不带 exec 时间戳，要时取 on_order 回调）
        out[n++] = u;
    }
    return n;
}

uint32_t XtOrderRuntimeBook::order_sid_id(uint64_t id) const {
    if (id == 0) return 0;
    if (!is_collect_thread_allowed_()) {
        on_cross_thread_direct_read_blocked_();
        return 0;
    }
    const auto it = id_to_slot_idx_.find(id);
    if (it == id_to_slot_idx_.end()) return 0;
    const uint16_t idx = it->second;
    if (idx >= kMaxInflightOrders) return 0;
    const OrderLifecycleSlot& slot = slot_pool_[idx];
    if (slot.id != id) return 0;
    return slot.sid_id;
}

size_t XtOrderRuntimeBook::reap_recovered_unowned(uint64_t* out, size_t max,
                                                  std::vector<uint64_t>* reaped_ids) {
    if (!out || max == 0) return 0;
    if (!is_collect_thread_allowed_()) {
        on_cross_thread_direct_read_blocked_();
        return 0;
    }

    // 遍历中不能 erase（会让 id_to_slot_idx_ 迭代器失效）：先分流，循环后统一 erase。
    // 待清死单取固定小批量——孤儿单是 cross-restart 残留，实际远少于此上限。
    size_t n = 0;
    std::array<uint64_t, 256> dead{};
    size_t dead_n = 0;
    for (const auto& kv : id_to_slot_idx_) {
        const uint16_t idx = kv.second;
        if (idx >= kMaxInflightOrders) continue;
        OrderLifecycleSlot& slot = slot_pool_[idx];
        if (slot.id == 0 || !slot.recovered_unowned) continue;
        if (slot.leaves_qty_fp <= 0) continue;  // 对齐 open-set：effective_remaining<=0 不算 open
        if (!slot.orphan_reap_attempted) {
            // 首轮：仅当实际交付 caller 撤单（写入 out[]）才标记 attempted，给 venue 一次
            // 正常 cancel 的机会。超过 max 的孤儿本轮既不写 out[] 也不标记 —— 下一轮重进
            // 首轮分支再获撤单机会，杜绝「标了 attempted 却没交付撤单、下轮直接 erase
            // venue 活单」（RT-D2 链A）。
            if (n < max) {
                slot.orphan_reap_attempted = true;
                out[n++] = slot.id;
            }
        } else if (dead_n < dead.size()) {
            // 次轮仍命中：上轮撤单 venue 返 -2011、gateway kCancelNoop 保活未清 slot →
            // venue 已无此单（孤儿单非本进程 submit，无 cancel 跑赢注册的竞态），待 erase。
            dead[dead_n++] = slot.id;
        }
    }
    for (size_t i = 0; i < dead_n; ++i) {
        // T1.153 LEAK-1：erase 前先撤 close 贡献（全文件其它 erase 站点均有此配对，
        // 对照终态路径 remove→erase）；漏撤 = 残留 hold 永久卡死同向平仓。
        if (OrderLifecycleSlot* dead_slot = find_slot(dead[i])) {
            remove_slot_close_contribution(*dead_slot);
        }
        // orphan reap = 判无归属的孤儿单：硬删归属（双表 active+retired + 写 0-行），
        // 不得由 retired 表复活。
        xt_identity::OrderAttributionStore::forget_order_strategy_sid(dead[i]);
        // R4/L2：硬删路径不发 terminal notify → 暴露给 caller（engine）逐 id 清 STP 私有条目，
        // 否则 STP bound 残留 → 同价反向单永久过度拒绝（跨重启不愈）。
        if (reaped_ids) reaped_ids->push_back(dead[i]);
        erase_slot(dead[i]);
        xt_runtime_book_drops::orphan_reap_dropped().fetch_add(1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance().inc_runtime_book_drop_total("orphan_reap");
#endif
    }
    // T1.153 tripwire：挂 reap 节拍对账（worker 线程内，与 erase 同写者契约）。
    const size_t drift = reconcile_close_outstanding_from_slots_();
    if (dead_n > 0 || drift > 0) publish_snapshot_();
    return n;
}

size_t XtOrderRuntimeBook::open_order_count() const {
    auto guard = hp_domain_.protect(current_);
    if (!guard) return 0;
    size_t count = 0;
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        if (guard->orders[i].leaves_qty_fp > 0) ++count;
    }
    return count;
}

size_t XtOrderRuntimeBook::tracked_code_count() const {
    auto guard = hp_domain_.protect(current_);
    return guard ? guard->tracked_code_count : 0;
}

OrderSnapshot XtOrderRuntimeBook::snapshot_order_by_id(uint64_t id) const noexcept {
    auto guard = hp_domain_.protect(current_);
    if (!guard) return {};
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        if (guard->orders[i].id == id) return guard->orders[i];
    }
    return {};
}

xt_util::sync::HazardGuard<const OrderBookSnapshot>
XtOrderRuntimeBook::snapshot_all_orders() const noexcept {
    return hp_domain_.protect(current_);
}

}  // namespace order
}  // namespace xt_runtime
