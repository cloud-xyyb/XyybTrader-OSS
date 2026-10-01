#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "api/XtPositionSnapshot.h"
#include "state/AccountEvent.h"
#include "state/AccountState.h"
#include "state/MarkPriceTable.h"
#include "state/detail/AccountFullSnapshot.h"
#include "xt_account/api/AccountMarking.h"
#include "xt_account/api/AccountSnapshot.h"
#include "xt_util/sync/XtHazardDomain.h"
#include "xt_util/sync/XtSnapshotPool.h"

class XtCommodityCatalog;

namespace account_audit {

/// @brief AccountStateStore 冻结资金一致性审计结果。
///
/// 审计算法属于 store 内部；本 DTO 只承载调用方需要记录/告警的结果。
struct XtFrozenAuditReport {
    bool ok = true;                      ///< true 表示冻结资金与订单槽累计值一致。
    double frozen_cash = 0.0;            ///< 展示用（from_fp 转换后）。
    double frozen_cash_by_orders = 0.0;  ///< 展示用（from_fp 转换后）。
    std::string reason;                  ///< ok=false 的失败原因；或 ok=true 时的不可用说明（如首包未发布）。
};

} // namespace account_audit

namespace xt_account {

namespace detail {
struct AccountStoreSymbolTestSeam;
}  // namespace detail

/// @brief 账户账本的唯一写入口与快照发布门面。
///
/// 交易线程在不停地改账户——成交入账、下单冻结保证金、行情刷新持仓市值——而风控、策略
/// 和上报随时要读一份自洽的账户视图。AccountStateStore 把写和读隔开：写侧由单一 worker
/// 线程串行更新 AccountState 账本和 MarkPriceTable，读侧通过 snapshot_*() 拿到已发布的
/// 不可变快照，两者互不阻塞，读者也绝不会看到写到一半的中间态。
///
/// 硬契约：所有写入口（apply / on_position_snapshot）只能在绑定的 worker 线程调用。
/// Store 不持久化账本；进程恢复时由 broker 持仓快照重建仓位域、由 SetCash 重锚资金域。
class AccountStateStore {
public:
    /// @brief 创建账户状态 store。
    /// @param cfg AccountState 初始化配置；其中 catalog 指针生命周期由调用方保证。
    /// @pre futures_mode 为真时 cfg.catalog 必须非空（vol_scale 源）。
    /// @throws std::invalid_argument 当 futures_mode && catalog==nullptr（fail-fast 取代运行期静默 1× 退化）。
    explicit AccountStateStore(const AccountState::Config& cfg);

    // ── worker 绑定 ──────────────────────────────────────────────
    /// @brief 绑定调用线程为写者；等价于 bind_worker_thread(std::this_thread::get_id())。
    ///
    /// 两段式契约：bootstrap 期 composition root 先绑 main（本重载），随后 main
    /// 直接 configure/apply；event loop 起来后必须用带 tid 重载重绑到真实
    /// worker 线程（与 order_book bind_worker_marshal / market_cache
    /// bind_worker_thread 同锚点），否则 release 写闩把每笔 worker apply 记为
    /// 跨线程写。
    void bind_worker_thread() noexcept;
    /// @brief 绑定指定 tid 为唯一合法写者（release 写闩权威）；允许重绑覆盖。
    /// @pre 重绑必须在新写者开始写之前的 quiescent 期（loop start 处），
    ///      调用时不得有并发的 apply/on_position_snapshot 在途。
    void bind_worker_thread(std::thread::id tid) noexcept;
    /// @brief 清除 worker 绑定 + 重置 first-touch；仅测试在 event-loop 换线程后使用。
    /// @pre 调用时不得有并发的写入在途。
    void unbind_worker_thread() noexcept;

    /// @brief 标记 worker 已 join：此后任意线程 apply 视为合法（无并发写者）。
    ///
    /// runner 在 event loop stop/join 后调用（与 order_book enter_quiescent 同锚点）。
    /// shutdown 收尾 drain order_book 溢出侧道时，marshalled gateway 注册闭包会在
    /// 非 worker 线程 apply(order_accepted)；置位后 guard 不再误记跨线程写。
    /// @pre 调用后不得再有 worker 线程并发 apply。
    void enter_quiescent() noexcept;

    // ── 写入口 ──────────────────────────────────────────────
    /// @brief 应用 reducer 事件，并在 FullSnapshot 已发布后同步刷新受影响 slot。
    /// @param ev fixed-point reducer event；持仓/行情类事件的 `code` 调用期间必须有效。
    /// @note worker-thread only；TD 回报经 ingest/AccountEventCodec.h 转换后在此入账。
    void apply(const XtAccountEvent& ev);

    /// @brief 处理 broker 持仓快照；recovery=true 时先 reset 再重建持仓域。
    /// @param positions broker 回报的持仓集合；空 code 会被跳过。
    /// @param is_recovery true 表示权威恢复快照，成功后恢复 closeable-known。
    /// @param catalog 用于按合约 vol_scale 归一化均价；合约缺失或 vol_scale 无效时
    ///        按 1.0 兜底。
    /// @note 本入口不是普通批量 apply：它会写入 broker frozen / 今昨仓，并在末尾
    ///       全量发布 account / position snapshot。
    /// @note recovery 批的 closeable-known 置信在批尾发布之后才置位，读者不会在
    ///       尚未重建持仓的旧帧上看到可信的 closeable 字段。
    /// @note 整批只对读侧发布一次：批内逐事件的中间帧被抑制，off-worker 读者在
    ///       批次期间持续看到上一批次的自洽帧，不会读到「已 reset、尚未重建」的
    ///       零冻结/零持仓中间态。
    /// @note 冻结后先做整批 slot admission（一次锁内追加、只造一个新世代），再走
    ///       reset/重建序列：declared 外的恢复仓因此取得 slot 并对读侧可见，其市值
    ///       也随之计入 live equity。容量耗尽的 code 退回 quarantine 记名。
    void on_position_snapshot(const std::vector<XtPositionSnapshot>& positions,
                              bool is_recovery,
                              const XtCommodityCatalog& catalog);

    // ── symbol bootstrap ──────────────────────────────────────────────
    /// @brief 按 config symbol 集合完成注册、mark 预热和 gen-0 FullSnapshot 发布。
    ///
    /// 生产 composition root 的唯一 symbol 初始化入口；分步的注册/封口私有入口
    /// 只留给覆盖 pre-freeze gap 的测试（经 detail::AccountStoreSymbolTestSeam）。
    /// @param codes 启动期订阅的合约集合；空 code 会被跳过。
    /// @throws std::invalid_argument 有效 code 为 0 个，或注册总数超过 kMaxDeclaredSymbols。
    /// @throws std::logic_error symbol 集合已冻结（重复初始化），或首包快照池取不到帧。
    /// @note bootstrap 期 fail-fast：静默截断/静默降级会让被丢弃的 symbol 对读侧
    ///       永久不可见，money-path 无声退化。
    /// @note declared symbol 不预占 position slot：gen-0 是空世代，slot 由 recovery /
    ///       fill 与 set_position / submit 预留三个源按需创建。未 touched 的 declared
    ///       symbol 读侧返回零仓，与「零值 slot」值级等价。
    template <class CodesRange>
    void configure_symbols(const CodesRange& codes) {
        for (const auto& code : codes) {
            register_symbol_(code);
        }
        if (symbols_sorted_.empty()) {
            throw std::invalid_argument(
                "AccountStateStore::configure_symbols: empty symbol set");
        }
        freeze_symbols_();
    }

    /// @brief 返回 symbol 集合是否已冻结。
    [[nodiscard]] bool symbols_frozen() const noexcept;
    /// @brief 是否期货记账模式；ctor 后不可变，任意线程可读。
    [[nodiscard]] bool futures_mode() const noexcept;
    /// @brief 查询 code 是否属于 store 认可的 symbol 集合（DeclaredUniverse ∪ 已有 slot）。
    /// @param code 合约代码；空 code 返回 false。
    /// @note symbol 集合冻结前没有读侧查询语义，返回 false（冻结前 declared 集合仍可变，
    ///       跨线程读它是 race；BT 形态永远走这一支）。
    /// @note 任意线程可读：declared 集合冻结后 immutable，slot 判定读活世代单指针。
    [[nodiscard]] bool is_registered_symbol(std::string_view code) const noexcept;
    /// @brief 查询 code 是否属于 config 声明的 DeclaredUniverse（**不含**仅因持仓取得 slot 的外来合约）。
    /// @param code 合约代码；空 code 返回 false。
    /// @note 开仓面判据：恢复快照带入的外来仓必须能平（那走 is_registered_symbol），但不构成
    ///       在该合约上开新敞口的许可；两个判据必须分开，否则未声明合约的 Open 会穿过 fence。
    /// @note 冻结前没有读侧查询语义，返回 false（同 is_registered_symbol）。
    [[nodiscard]] bool is_declared_symbol(std::string_view code) const noexcept;
    /// @brief 查询 code 是否已在 PositionUniverse（= 已取得 position slot）。
    /// @param code 合约代码；空 code 或未冻结返回 false。
    /// @note submit fence 的 foreign 平仓前置：未声明合约只在**已有 slot** 时才可平（没有
    ///       slot 就没有可平量判据，放行等于允许对一个本进程看不见的仓位发平单）。
    /// @note 任意线程可读：读活世代单指针，无锁无分配。
    [[nodiscard]] bool has_position_slot(std::string_view code) const noexcept;
    /// @brief 返回当前 tracked symbol 列表副本（已发布帧世代的 slot 顺序）。
    /// @warning 成员面 = PositionUniverse（touched 集），**不覆盖 declared 全集**：
    ///          declared 但从未有持仓/成交/预留的 symbol 不在列表里。「不在列表里」
    ///          不等于「该 symbol 不可交易 / 无需处理」。需要遍历可交易集合的调用方
    ///          按 config 声明的 universe 枚举，再逐 code 查 `snapshot_position()`。
    [[nodiscard]] std::vector<std::string> snapshot_tracked_codes() const;
    /// @brief 返回当前**活世代**的 PositionUniverse code 副本（slot 顺序）。
    /// @note 与 `snapshot_tracked_codes()` 的差别 = 世代来源：本 API 取活世代（含尚未
    ///       进帧的新 slot），后者取已发布帧的世代。需要「谁可能有仓」的候选集枚举
    ///       （如 SEL sparse-weights 护栏）用本 API。
    [[nodiscard]] std::vector<std::string> snapshot_position_universe() const;

    // ── position slot 生命周期 ──────────────────────────────────────────────
    /// @brief 幂等地确保 code 拥有 position slot。
    /// @param code 合约代码；空 code 返回 false。
    /// @return true 表示该 code 已有 slot；false = mark intern 或 slot 容量耗尽。
    /// @pre `symbols_frozen()`；冻结前 slot 语义未启用，恒返回 false。
    /// @note 任意线程可调（submit 线程预留与 worker 线程入账走同一入口）：追加由
    ///       SymbolTable 内部锁串行，帧发布仍是 worker 独占。
    /// @note 先完成 mark registry 注册再建 slot（不变量 3）：没有 mark id 的 slot
    ///       持有者估值恒 0 且不可修复。
    /// @note false 的处置按来源分流：持仓/成交源退回 quarantine 记名 + critical
    ///       （资金守恒优先，事件照常入账），submit 预留源拒单。
    [[nodiscard]] bool ensure_slot(std::string_view code);
    /// @brief 已消耗的 position slot 数（活世代口径，`xt_position_slots_used` 数据源）。
    [[nodiscard]] std::size_t position_slots_used() const noexcept;
    /// @brief position slot 容量上限（= kMaxPositionSlots）。
    [[nodiscard]] static constexpr std::size_t position_slots_capacity() noexcept {
        return kMaxPositionSlots;
    }

    // ── mark 读口（估值 / 执行定价二分）────────────────────────────────
    /// @brief 返回 symbol 的 venue 真实行情价（fixed-point）；0 = 无真实行情。
    /// @param code 合约代码；空 code 或未注册 code 返回 0。
    /// @note **下单价格的唯一合法来源**：估值可以回退成本价，出单不行。取 0 的调用方
    ///       必须显式拒绝（NoMarkPrice），不得按成本价成交。估值/展示口径走快照的
    ///       `PositionSnapshot::mark_price`（venue 优先、次成本价）。
    /// @note 任意线程可读：mark 表内部是原子数组 + 无锁 intern 查找，不经发布帧。
    [[nodiscard]] int64_t venue_mark_price(std::string_view code) const noexcept;

    // ── quarantine 观测（冷路径）──────────────────────────────────────────
    // quarantine 语义 = **position slot 容量溢出**：持仓/成交事件的 symbol 拿不到 slot
    // 时记名。容量未耗尽时 declared 外的外来仓照常取得 slot 并对读侧可见，不进 quarantine。
    /// @brief 已记名的 quarantined symbol 数（distinct code，封顶 kMaxQuarantinedCodes）。
    /// @note 任意线程可读；>0 表示有合约的钱进了账户级聚合但按合约不可见。
    [[nodiscard]] std::size_t quarantined_symbol_count() const noexcept;
    /// @brief 超出记名上限后仍被观测到的 quarantine 事件数（不去重）。
    [[nodiscard]] uint64_t quarantined_overflow_count() const noexcept;
    /// @brief 返回已记名的 quarantined code 列表副本（诊断 / 停机对账用）。
    [[nodiscard]] std::vector<std::string> snapshot_quarantined_codes() const;

    // ── 风控 side-channel ──────────────────────────────────────────────
    /// @brief 冻结 session 起点 equity，供 daily_loss_pct 风控使用。
    /// @param equity_fp session 起点 equity（fixed-point）；0 表示未冻结。
    /// @note 任意线程可调；0 表示未冻结，风控侧按 fail-safe bypass。
    void on_session_begin_hook(int64_t equity_fp) noexcept;

    /// @brief 返回 frozen starting balance（fp）；0 表示未冻结。
    /// @note 该值不进入 hazard snapshot，caller 直接读 atomic 以避免 publish stale。
    [[nodiscard]] int64_t starting_balance_fp() const noexcept;

    /// @brief 标记今/昨仓可平量是否可信；false 时风控不得按快照直接放行。
    /// @param known true 表示 closeable 字段来自可信恢复/同步链路。
    /// @note 任意线程可调；snapshot_position() 会把该账户级标记覆盖进返回值。
    void set_closeable_known(bool known) noexcept;

    // ── snapshot 读取 ──────────────────────────────────────────────
    // 无 published snapshot（configure_symbols 前）时回退直读 mutable writer state：
    // 仅 pre-publish 单线程 bootstrap 阶段可达（runner 在 gRPC/off-worker reader
    // 启动前完成 configure_symbols），非跨线程安全路径。
    /// @brief 读取账户资金快照；无 published snapshot 时从 writer state 构建。
    [[nodiscard]] AccountSnapshot snapshot_account() const noexcept;
    /// @brief 直读 reducer 当前累计 realized PnL（fixed-point）。
    /// @note worker-thread only；跨线程读使用 snapshot_account().realized_pnl。
    [[nodiscard]] int64_t realized_pnl() const noexcept;
    /// @brief 读取单 symbol 持仓快照；冻结后未知 symbol 返回空快照。
    /// @param code 合约代码；空 code 返回空快照。
    /// @note 跨线程安全：内部会保护 FullSnapshot 和 per-symbol slot，并确认
    ///       FullSnapshot 仍是当前发布帧后返回。
    [[nodiscard]] PositionSnapshot snapshot_position(std::string_view code) const noexcept;

    /// @brief 同一发布边界下的账户 + 单 symbol 持仓快照。
    struct AccountPositionFrame {
        AccountSnapshot account;    ///< 该发布帧的账户资金快照。
        PositionSnapshot position;  ///< 同帧的单 symbol 持仓快照。
    };

    /// @brief 一次读出同帧的账户与持仓快照。
    /// @param code 合约代码；空 code 或冻结后未知 symbol 返回空 position + 当帧 account。
    /// @return 同一 FullSnapshot 发布边界下的 account/position 对。
    /// @note 需要两者自洽的读者（如风控按权益与持仓联合判据）必须走本 API：
    ///       分别调 snapshot_account() + snapshot_position() 可能横跨一次发布，
    ///       拿到不同时刻的权益与持仓。
    [[nodiscard]] AccountPositionFrame snapshot_account_and_position(
        std::string_view code) const noexcept;

    // ── 发布节奏（请求级完成协议）──────────────────────────────────
    // MarketTick 不发布帧（只原子写 mark 表），帧内 mark/equity 的新鲜度由「持仓/
    // 资金事件的即时发布 + 控制线程每秒一次的兜底请求」承担。需要「读到不早于此刻
    // 的帧」的等待方（session-begin 双闩）必须按**自己的 ticket** 判完成：全局
    // published_seq 会被任何一条无关发布路径推进，不是本请求的完成凭据。
    /// @brief 登记一次「全量重建并发布」请求。
    /// @return 该请求的 ticket（恒 >0）；已有未 ack 的在途请求时返回同一 ticket
    ///         （合并，至多一个在途请求）。
    /// @note 任意线程可调；登记本身不发布，发布由 worker 线程执行
    ///       run_publish_request() 完成。const 是读者侧语义（等待方只持 const 引用）。
    /// @pre 「ack 的帧不早于本次登记」这一新鲜度保证以单写者不变量为前提（apply 与
    ///      发布闭包同在 worker 线程串行）。quiescent 期放行 off-worker 直写，该前提
    ///      不成立，此时不得用 ticket 判新鲜度。
    [[nodiscard]] uint64_t request_publish() const noexcept;

    /// @brief worker 侧执行在册发布请求：全量重建 + 单次 head exchange，成功后 ack。
    /// @note worker-thread only；三 runner 控制线程经各自 worker 闭包队列投递本调用。
    ///       pool 耗尽致发布被跳过时**不 ack**（等待方按超时处理，投递方据
    ///       publish_request_attempts() 的推进识别该形态并重投）；在册请求已被先前
    ///       闭包 ack 时直接返回，不做冗余重建。
    /// @pre 同 request_publish()：新鲜度保证以单写者不变量为前提。
    void run_publish_request() noexcept;

    /// @brief 该 ticket 是否已由「携带它执行的发布闭包」成功发布完成。
    /// @param ticket request_publish() 的返回值；0 恒返回 false。
    [[nodiscard]] bool is_publish_acked(uint64_t ticket) const noexcept;

    /// @brief worker 侧真正为在册请求做过发布尝试的闭包次数（单调）。
    ///
    /// 只统计跑到发布这一步的闭包（无在册请求 / 请求已被先前闭包 ack 的多余投递不计），
    /// 成功与否都算。投递方据此把「闭包还在队列里」与「闭包已执行但 pool 耗尽没 ack」
    /// 区分开：后者必须重投，否则该 ticket 永不完成。
    [[nodiscard]] uint64_t publish_request_attempts() const noexcept;

    /// @brief 成功发布（head exchange 完成）的累计帧数。
    /// @note 任一发布路径成功都推进；**不是**请求完成凭据（见 request_publish）。
    [[nodiscard]] uint64_t published_seq() const noexcept;

    /// @brief 最近一次成功发布的 steady 时刻（ms）；0 表示尚未发布过。
    /// @note `xt_account_frame_age_ms` 的数据源 = now − 本值。
    [[nodiscard]] int64_t last_publish_steady_ms() const noexcept;

    // ── 诊断 ────────────────────────────────────────────
    /// @brief 校验 frozen_cash 与订单 slot 冻结量是否一致（直读 writer state，含 per-slot dump）。
    /// @param out 输出审计结果；函数会覆盖其内容。
    /// @warning 直读并迭代 worker 拥有的 order_slots_（mutable unordered_map）；仅在
    ///          account_store 无写者后安全（停机对账 post-join 路径）。与 worker 并发
    ///          调用 = UB race；在线路径用 frozen_audit_from_snapshot()。
    void frozen_audit_report(account_audit::XtFrozenAuditReport& out) const;

    /// @brief 跨线程安全的 frozen 审计：读 published snapshot 的同帧
    ///        frozen_cash / frozen_by_orders 自洽对（同一 publish 边界）。
    /// @param out 输出审计结果；函数会覆盖其内容。首包发布前 ok=true +
    ///        reason 标未发布（不回落直读 writer state）。
    /// @note mismatch 时仅带聚合差；per-slot dump 只在停机直读版提供。
    void frozen_audit_from_snapshot(account_audit::XtFrozenAuditReport& out) const;

private:
    friend struct detail::AccountStoreSymbolTestSeam;

    // ── symbol bootstrap 分步私有入口（生产一律经 configure_symbols）──────────
    /// @brief 启动期注册单个 symbol，同时预热 MarkPriceTable。
    /// @param code 合约代码；空 code 忽略。
    /// @throws std::logic_error symbol 集合已冻结。
    /// @throws std::invalid_argument 注册总数超过 kMaxDeclaredSymbols，或 intern 表容量耗尽。
    void register_symbol_(std::string_view code);
    /// @brief 锁定 symbol 集合、启用 slot 语义并发布首个 FullSnapshot。
    /// @note register_symbol_(...) 全部完成后调用；冻结后 declared 集合 immutable。
    /// @throws std::invalid_argument symbol 总数超过 kMaxDeclaredSymbols。
    /// @throws std::logic_error 首包 FullSnapshot 池取不到帧（fresh pool 结构性不可达）。
    void freeze_symbols_();
    /// @brief 把 code 按字典序插入 `symbols_sorted_`（已存在则不动）。
    /// @return true 表示本次真正插入。
    bool insert_symbol_sorted_(std::string_view code);

    AccountState account_;       ///< worker 单写者拥有的 reducer state；跨线程读必须走 snapshot API。
    MarkPriceTable mark_cache_;  ///< worker 单写者拥有的 mark price 表；Reset/recovery 不清空行情锚点。

    xt_util::sync::XtSnapshotPool<PositionSnapshot, kMaxPositionSlots * 4> position_pool_; ///< PositionSnapshot slot 池。
    xt_util::sync::XtSnapshotPool<FullSnapshot, 64> full_pool_;                     ///< FullSnapshot 头帧池。
    xt_util::sync::HazardDomain hp_domain_; ///< 延迟回收旧 snapshot，避免读侧 raw pointer 被复用。
    /// 当前 FullSnapshot 头指针；nullptr 表示尚未发布首包。
    std::atomic<const FullSnapshot*> current_{nullptr};
    // worker_tid_ 经 worker_bound_ release/acquire 发布；正确性依赖 bind/重绑
    // 仅在 quiescent 期调用（与 XtRuntimeMarketCache / XtOrderRuntimeBook marshal 同契约）。
    std::atomic<bool> worker_bound_{false};           ///< bind_worker_thread 已调
    std::thread::id   worker_tid_{};                  ///< 权威写者 tid
    mutable std::atomic<std::thread::id> write_first_touch_tid_{};  ///< 未绑定时 first-touch 写者
    /// worker join 后由 runner 置位：guard_worker_thread_ 恒放行（无并发写者）。
    /// BT/单测不置位（既有 thread-assert 契约不变）。
    std::atomic<bool> worker_quiescent_{false};
    /// 字典序 symbol 集合，两个阶段两种含义：freeze 后 = DeclaredUniverse（immutable，
    /// fence 与成员判定的 declared 半边）；未 freeze 时 = 回测惰性收编集合（回测不走
    /// SymbolTable：无 freeze、无帧，且 distinct code 规模不受 kMaxPositionSlots 约束）。
    /// 账户级聚合不再读本集合（统一迭代 reducer 持仓 map，见 build_account_snapshot_）。
    std::vector<std::string> symbols_sorted_;
    /// release/acquire 发布 symbol 集合冻结状态：置位后 declared 集合 immutable、
    /// SymbolTable 的 slot 语义启用。
    std::atomic<bool> symbols_frozen_{false};
    /// PositionUniverse 的唯一 owner（append-only 世代化）；freeze 时 initialize 出
    /// gen-0 空世代，此后由三个创建源按需增长。
    SymbolTable symbol_table_;

    /// 批次发布抑制闩：on_position_snapshot 的 apply 序列期间置位，publish_partial_
    /// 短路不发中间帧，批尾由 publish_all_snapshots_() 一次性收敛。
    /// worker 单写者私有状态（只在写路径读写、不跨线程），故不是 atomic。
    /// 仅供 on_position_snapshot 单层使用，不支持嵌套（RAII 作用域构造期断言）。
    bool suppress_partial_publish_ = false;
    /// 发布落后闩：某次 position-affecting 发布因 pool 耗尽被跳过后置位；
    /// 置位期间任何发布升级为全量原子重建（publish_all_snapshots_），成功后清除。
    /// 仅 worker 线程在 publish 路径读写，无并发访问。
    bool publish_dirty_ = false;

    // ── 发布完成协议（ticket）────────────────────────────────────────
    /// 已登记的最高发布请求 ticket；0 = 从未登记。控制线程写（request_publish
    /// 的 CAS），worker 与等待方读。mutable：登记是读者侧语义，等待方只持 const 引用。
    mutable std::atomic<uint64_t> requested_ticket_{0};
    /// 已完成的最高 ticket（单调）。**只有携带该 ticket 执行的发布闭包在自己成功
    /// head exchange 后**才推进；事件驱动的无关发布不得触碰（否则等待方会被假完成）。
    std::atomic<uint64_t> acked_ticket_{0};
    /// 已为在册请求做过发布尝试的闭包次数（单调）；worker 写、投递方读。
    /// 与 acked_ticket_ 的差别就是「执行过」与「发布成功」之差。
    std::atomic<uint64_t> publish_attempts_{0};
    /// 成功 head exchange 的累计次数；frame_age / 停滞观测口径，非请求完成凭据。
    std::atomic<uint64_t> published_seq_{0};
    /// 最近一次成功 head exchange 的 steady 毫秒；0 = 尚未发布。
    std::atomic<int64_t> last_publish_steady_ms_{0};

    /// starting_balance 走 caller-builds 模式，不进入 hazard snapshot。
    /// 独占 cache line，避免 GuardContext 热读与相邻 atomic 共享 cache line。
    alignas(64) std::atomic<int64_t> starting_balance_fp_{0};

    /// 账户级 closeable-known 标记；build_position_snapshot_ 会写入每个快照。
    alignas(64) std::atomic<bool> closeable_known_{true};

    // ── quarantine 注册表（冷路径，mutex 保护）────────────────────────────
    /// 记名上限：诊断用途，不做无界增长。
    static constexpr std::size_t kMaxQuarantinedCodes = 32;
    mutable std::mutex quarantine_mu_;             ///< 保护 quarantined_codes_
    std::vector<std::string> quarantined_codes_;   ///< 已记名的无 slot code
    std::atomic<std::size_t> quarantined_count_{0};   ///< quarantined_codes_.size() 的无锁镜像
    std::atomic<uint64_t> quarantined_overflow_{0};   ///< 超上限后仍观测到的事件数

    // ── mark / reducer 协调 ──────────────────────────────────────────────
    /// @brief 构造盯市计算输入；期货模式会从 AccountState::Config.catalog 取 vol_scale。
    xt_account::XtPositionMarkInput make_mark_input(const char* code) const;
    /// @brief readout 复用版：跳过额外的字符串键持仓查找（snapshot 折叠路径用）。
    xt_account::XtPositionMarkInput make_mark_input(
        const char* code, const AccountState::PositionReadout& readout) const;
    /// @brief 在 reducer apply 前同步 MarkPriceTable；Reset 只清成本价、保留 venue mark。
    void on_event_before_apply(const XtAccountEvent& ev);
    /// @brief 在 reducer apply 后按持仓终态重算成本价 fallback。
    /// @note 改仓事件（TradeEx / SetPosition）共用本派生点：成本价转换表只在这里执行，
    ///       事件里携带的价格不参与，caller 无从写入 hint。
    void on_event_after_apply(const XtAccountEvent& ev);
    /// @brief 按当前持仓推导该 code 的成本价 fallback。
    /// @return 恰一条正持仓腿时为该腿均价；空仓 / 双腿锁仓 / 均价无效时为 0（显式失效）。
    [[nodiscard]] int64_t derive_cost_hint_(const char* code) const;
    /// @brief 未冻结时把 mark table 认可的 code 并入惰性 symbol 集合（回测注册路径）。
    /// @param tracked MarkPriceTable on_* 的返回值。
    /// @throws std::runtime_error tracked == kInternFull（intern 表满，见函数体说明）。
    void note_symbol_if_open_(const char* code, MarkPriceTable::TrackResult tracked);
    /// @brief code 是否属于 declared 集合（字典序二分）。
    /// @note freeze 前该集合可变，仅 writer 侧可读；freeze 后 immutable，任意线程可读。
    [[nodiscard]] bool is_declared_(std::string_view code) const noexcept;
    /// @brief code 是否属于 store 认可的 symbol 集合（declared ∪ 活世代 slot）。
    [[nodiscard]] bool is_in_universe_(std::string_view code) const noexcept;
    /// @brief 冻结后为持仓/成交事件的 symbol 取 slot；取不到则 quarantine 记名（不拒账）。
    /// @param event_tag 事件类型标签，进日志便于定位来源。
    void admit_position_symbol_(const char* code, const char* event_tag);
    /// @brief 为冻结前已落账的持仓补 slot（bootstrap 期先 apply 后 configure_symbols）。
    /// @note freeze 内调用，gen-0 发布之后、首帧构建之前；零仓 symbol 不占 slot。
    void admit_pre_freeze_positions_();
    /// @brief recovery 批的整批 slot admission：锁外去重/滤已有 slot，锁内单世代追加。
    /// @param positions broker 持仓快照；空 code 跳过。
    /// @note 容量拒绝只认锁内判定（容量满单调不可逆 ⇒ 被拒 code 永不获 slot，
    ///       与 quarantine 名单互斥成定理）；溢出与 mark intern 失败逐个 quarantine 记名。
    void admit_recovery_symbols_(const std::vector<XtPositionSnapshot>& positions);
    /// @brief slot 取不到时记名该 code 并告警（每 code 一次）。
    /// @param event_tag 事件类型标签，进日志便于定位来源。
    void quarantine_slot_overflow_(std::string_view code, const char* event_tag);

    // ── writer guard ──────────────────────────────────────────────
    /// @brief 写路径线程闩（release 也生效）。
    ///
    /// 覆盖面 = reducer 写入与帧发布，两者仍是 worker 独占。position slot 追加不在
    /// 覆盖面内：它由 SymbolTable 内部锁串行，submit 线程持锁追加合法。
    ///
    /// bound 时以 worker_tid_ 为权威；未绑定时 first-touch 绑定当前线程
    /// （test-then-CAS：稳态只付 load+cmp）。mismatch = **fail-open**——计数
    /// + log-once 后照常入账（写路径丢 apply = 丢 fill/冻结记账，比一次
    /// torn write 更糟；与 market_cache/order_book 读路径 fail-closed 相反）。
    void guard_worker_thread_() const noexcept;
    /// @brief 跨线程写检测的统一出口：进程局部计数 + log-once；caller 照常继续。
    void on_cross_thread_write_() const noexcept;

    // ── snapshot 发布（单次 head exchange 协议）──────────────────────
    // 发布不变量：帧内 account 与全部 position slot 在未发布帧上建完后才做
    // 唯一一次 current_ exchange；已发布帧自此 immutable（slot 不再原地替换），
    // 读侧同帧读到的 account/position 恒属同一事件时刻。任一 pool 取不到则
    // 整次发布跳过（coherent-but-stale，计数 snapshot_publish_skipped）。
    //
    // 世代采纳规则：三条发布路径构帧时一律采纳**当时的活世代**（不是照搬上一帧的
    // 世代指针），否则 ensure_slot 建出的新 slot 永远进不了帧、该 code 的仓位对读侧
    // 永久不可见。slot 下标跨世代稳定 ⇒ 增量路径按下标逐位复制旧帧 [0, old_n)，
    // [old_n, new_n) 留 null（= untouched 零仓），事件所属 code 的 slot 本帧就地重建。
    /// @brief 按事件类型发布受影响的 account / position snapshot。
    /// @note MarketTick 零发布：只在 on_event_before_apply 写 mark 表，不构帧。
    void publish_partial_(const XtAccountEvent& ev) noexcept;
    /// @brief 重建并发布所有 tracked symbol 的快照（单次 exchange）。
    /// @return true 表示 head exchange 完成；false = 无已发布帧或 pool 耗尽跳过。
    bool publish_all_snapshots_() noexcept;
    /// @brief 发布新帧（仅 account 变化），复用既有 per-symbol slot 指针。
    void publish_account_snapshot_() noexcept;
    /// @brief 发布新帧：account + 受影响 symbol 的 position 同帧重建；
    ///        空 code / 未跟踪 code 退化为仅 account 发布。
    void publish_event_snapshot_(std::string_view code) noexcept;

    // ── snapshot 构建 ──────────────────────────────────────────────
    /// @brief 从 writer state 构造账户资金快照。
    [[nodiscard]] AccountSnapshot build_account_snapshot_() const noexcept;
    /// @brief 从 writer state 与 MarkPriceTable 构造单 symbol 持仓快照。
    [[nodiscard]] PositionSnapshot build_position_snapshot_(std::string_view code) const noexcept;

    /// @brief 记录一次成功的 head exchange：刷新发布时刻 + 推进 published_seq。
    /// @note gen-0 首包与三条发布路径的 exchange 之后各调一次；worker 线程独占。
    void note_published_() noexcept;
};


}  // namespace xt_account

namespace xt_account_store_drops {

/// @brief 非绑定写者线程调 apply()/on_position_snapshot() 被检测到的次数（fail-open，事件仍入账）。
///
/// 正常 prod 调用图（worker 单写 + off-worker submit 经 order_book marshal 整块
/// 序列化到 worker + shutdown 收尾 enter_quiescent 放行）应恒为 0；
/// > 0 表示有新代码从错误线程直写账本（未走 order_book marshal）。
std::atomic<uint64_t>& cross_thread_write();

/// @brief 快照发布因 pool 耗尽被跳过的次数（读者保持上一自洽帧，落后闩驱动
///        后续全量重建自愈）。正常应恒为 0；持续增长 = 慢读者 pin 帧阻塞回收。
/// prom metric 接线随 T1.117(a)。
std::atomic<uint64_t>& snapshot_publish_skipped();

/// @brief 控制线程向 account worker 投递发布请求闭包失败（队列满）的次数。
///
/// prom metric `xt_account_publish_post_failed_total`（advance_risk_frame 帧尾 emit）。投递失败
/// 不丢请求：ticket 仍在册，pending 闩保持到 ack，下一帧重投；>0 表示 worker
/// 队列在控制帧节拍上被打满，帧新鲜度已在退化。
std::atomic<uint64_t>& publish_post_failed();

/// @brief 持仓/成交事件的 symbol 拿不到 position slot 的次数（critical）。
///
/// prom metric `xt_account_position_slot_overflow_total`（advance_risk_frame 帧尾 emit）。>0 表示
/// PositionUniverse 容量（1024）耗尽或 mark intern 表耗尽：该 code 的钱进了账户级
/// 聚合但按合约不可见，且没有平仓通道（close quota 读零仓拒）。逐 code 记名见
/// `AccountStateStore::snapshot_quarantined_codes()`。
std::atomic<uint64_t>& position_slot_overflow();

#ifdef XT_BUILD_TESTS
/// @brief test-only 计数器重置 helper（process-global，测试 SetUp 显式重置防串扰）。
inline void reset_for_test() {
    cross_thread_write().store(0, std::memory_order_relaxed);
    snapshot_publish_skipped().store(0, std::memory_order_relaxed);
    publish_post_failed().store(0, std::memory_order_relaxed);
    position_slot_overflow().store(0, std::memory_order_relaxed);
}
#endif
}  // namespace xt_account_store_drops
