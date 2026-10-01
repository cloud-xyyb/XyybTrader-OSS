#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "api/IHftGateway.h"
#include "api/IHftEngineObserver.h"
#include "api/IOrderIdTranslator.h"
#include "api/ITradeGateway.h"
#include "xt_core/market/XtL2MarketStructs.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_strategy_engine/view/XtRuntimeView.h"
#include "xt_strategy_engine/IStrategyExecSink.h"
#include "xt_strategy_engine/XtStrategyContext.h"
#include "xt_strategy_engine/XtDeclaredSymbolRegistry.h"
#include "xt_core/market/XtRollEngine.h"
#include "xt_core/session/XtSessionFilter.h"
#include "xt_core/session/XtTradingDateInfer.h"
#include "HftLifecycleTelemetry.h"
#include "XtHftEngineStats.h"
#include "XtHftReplayExt.h"

struct AliveInflightOrder;

namespace xt_account {
class AccountStateStore;
}

class XtCommodityCatalog;
class XtStrategyBase;

namespace xt_runtime::order {
class XtOrderRuntimeBook;
}

/// @brief 提供 HFT 策略的行情、执行回报和账户事件处理。
///
/// 调用方通过 `on_tick()`、L2 order-flow 入口、`on_trade()`、`on_order_update()`
/// 和账户/recovery 入口推进 engine 状态。
/// Engine 更新 runtime/domain 状态后，按 context/sid 派发策略回调和 observer 通知。
///
/// 同一实例必须串行调用；Config 注入的依赖均为 non-owning。
/// 多策略成交和订单回报按 sid_id 路由。
class XtHftEngine {
public:
    // ── 类型与配置 (types/config) ──────────────────────────────────────

    /// @brief 回放扩展别名；中性定义见 XtHftReplayExt.h（engine 不反向依赖 backtest 模块）。
    using ReplayExt = xt_engine::hft::XtHftReplayExt;

    /// @brief lifecycle 事件类型与 callback 定义见 HftLifecycleTelemetry.h；
    /// 此处保留 XtHftEngine:: 别名，使既有 caller（reporter wiring / 测试）API 稳定。
    using PositionLifecycleEvent    = xt_engine::hft::PositionLifecycleEvent;
    using PositionLifecycleCallback = xt_engine::hft::PositionLifecycleCallback;
    using PositionMetricCallback    = xt_engine::hft::PositionMetricCallback;
    using LifecycleTelemetryInit    = xt_engine::hft::HftLifecycleTelemetry::Init;

    /// @brief Per-(sid, code) 单笔平仓 realized PnL 回调；空 callback 表示禁用。
    using FillRealizedCallback = std::function<void(const std::string& sid,
                                                    const std::string& code,
                                                    int64_t realized_fp)>;

    /// @brief broker 账户快照 set_cash 重锚前的 divergence 采样回调。
    /// @param divergence_fp broker balance(fp) − 重锚前 account_store cash(fp)，带符号；
    ///        即本次 set_cash 的校正量。空 callback 表示禁用（BT / 未接线 venue）。
    using CashReanchorProbe = std::function<void(int64_t divergence_fp)>;

    /// @brief 回测专用：每 tick 调用（用于逐笔 MAE/MFE 游程更新）；空 callback 表示禁用。
    using MarkTickCallback = std::function<void(const XtTick&)>;
    /// @brief 回测专用：每笔成交按 before/after 净仓分类（open/close/flip），供 excursion
    ///        tracker 落行；realized 使用 engine 传入的本 trade realized，不在 tracker 内重算。
    ///        空 callback 表示禁用。
    using TradeExcursionCallback = std::function<void(const XtTradeRecord& tr,
                                                      int64_t before_qty_fp,
                                                      int64_t after_qty_fp,
                                                      int64_t after_avg_fp,
                                                      int64_t realized_fp)>;

    /// @brief multi-context 下 sid 未命中任何 context 时的处置策略。
    ///
    /// 由组合根按 (是否多策略, kill-switch 入口是否可用) 派生后显式传入，
    /// engine 不再自行持有 kill-switch 依赖。
    enum class UnmatchedRoutingPolicy {
        WarnAndDrop,   ///< warn + 丢弃（single-context 语义下 unmatched 分支不可达）
        DropAndAlert,  ///< error + warn + 丢弃；上层由 kill-switch 入口另行处置
        FailFast,      ///< 抛 std::runtime_error，让装配/归因错误立即暴露
    };

    /// @brief engine 的 non-owning 依赖（required + optional）。
    ///
    /// gateway 与 view 为必填（构造期 fail-fast）；其余指针为空时，engine 跳过依赖
    /// 该组件的路径。
    struct Dependencies {
        xt_runtime::IHftGateway* gateway = nullptr;                  ///< required
        xt_strategy_engine::XtRuntimeView* view = nullptr;           ///< required
        xt_account::AccountStateStore* account_store = nullptr;
        xt_runtime::order::XtOrderRuntimeBook* order_book = nullptr;
        const XtCommodityCatalog* catalog = nullptr;
        xt_runtime::IOrderIdTranslator* order_id_translator = nullptr;
        /// sid 到声明品种的路由表；用于 code->sid 兜底和 resume 归因。
        xt_strategy_engine::XtDeclaredSymbolRegistry* declared_registry = nullptr;
    };

    /// @brief engine 行为选项（值语义，构造期拷入）。
    struct Options {
        xt_core::Config session = {};
        XtRollEngine::Config roll = {};
        UnmatchedRoutingPolicy unmatched_routing = UnmatchedRoutingPolicy::WarnAndDrop;
        /// 本地条件单 engine 侧总闸（与 ctx Config 同源 yaml；默认关保 golden）。
        /// false 时 tick hook / 终态喂入 / roll 清仓全部单分支早退。
        bool conditional_orders_enabled = false;
        /// 回放扩展（交易日历 / 夜盘 trading_date 推断）；live 装配留空。
        ReplayExt replay = {};
    };

    /// @brief 构造期一次性注入的 observer 与 callback 集合（注入即封口）。
    ///
    /// 各 callback 空值表示禁用对应输出。
    ///
    /// 生命周期：全部成员为 non-owning 引用/闭包，owner 归组合根，必须**覆盖 engine 的
    /// 事件派发期**（首次事件入口调用起，到 producer 停止/teardown quiesce 止；observer
    /// 指针、以及 callback 捕获的对象都是）。engine 析构本身不回调任何 hook（~XtHftEngine
    /// = default，只析构成员），故 owner 与 engine 的**对象析构序不受此约束**——两生产组合
    /// 根即为 engine 成员先于部分 hook owner 声明（反序析构 owner 先亡），在 quiesce 后
    /// 合法。engine 不做存活性检查。
    ///
    /// 异常契约：**engine 不给 observer/callback 加吞异常 guard**（区别于策略回调——策略侧
    /// 走 run_guarded）。observer 与本结构各 callback 均按 `noexcept` 期望调用，抛出即违约，
    /// 异常会沿事件入口向上穿透到组合根。理由：报表/遥测 hook 静默吞异常 = 产物静默残缺，
    /// 比 crash 更难发现。需要容错的 hook 自行在实现内 try/catch。
    struct Hooks {
        /// fan-out 顺序 == 本 vector 顺序（承重，勿重排）；不得含 nullptr、不得重复
        /// （重复指针 = 同一 observer 被 fan-out 两次，装配错误），违反即 ctor throw。
        std::vector<xt_runtime::IHftEngineObserver*> observers;
        LifecycleTelemetryInit lifecycle = {};
        /// per-fill realized PnL；空 callback 表示禁用。
        FillRealizedCallback fill_realized;
        /// @brief set_cash 重锚 divergence 采样；空 callback 表示禁用。
        /// @note 采样发生在 on_account_snapshot 内、apply(set_cash) **之前**：set_cash 会把
        ///       store cash 直接覆盖为 broker balance，事后比较恒 0（假报健康）。首个有效
        ///       broker 快照是 anchor（derived 尚未锚定，全额差值不是校正），跳过不采样。
        ///       回调在 worker 线程、账户快照冷路径（周期 ~account_refresh_ms）触发。
        CashReanchorProbe cash_reanchor;
        /// 逐 tick mark 回调（回测 MAE/MFE）；空 callback 表示禁用，live 路径零开销。
        MarkTickCallback mark_tick;
        /// 逐笔成交 excursion 分类回调（回测 MAE/MFE）；空 callback 表示禁用。
        TradeExcursionCallback trade_excursion;
        /// 参数版本更新的外部扩展 hook；context 通知后调用。
        std::function<void(uint64_t /*param_version*/)> on_params_extra;
        /// @brief 策略 on_init 失败回调（per-strategy fail-closed）。
        ///
        /// on_init 抛异常时 engine 把该 context 标记 disabled（后续所有回调 fan-out
        /// 跳过该 ctx），并同步调用本回调（sid_id + 策略名）。空回调（BT / 未接线装配）
        /// 时仅本地 disable + 计数；live 组合根注入以置 per-sid kill latch。
        std::function<void(uint32_t /*sid_id*/, const std::string& /*strategy_name*/)>
            on_strategy_init_failure;
    };

    /// @brief 单个策略启动失败记录。
    struct StrategyStartFailure {
        uint32_t sid_id = 0;
        std::string strategy_id;
    };

    /// @brief start() 的结构化结果。
    struct StartReport {
        bool all_started = false;                       ///< 全部 context on_init 成功
        std::vector<StrategyStartFailure> failures;     ///< 被 fail-closed disable 的 context
    };

    // ── 构造与生命周期 (construction/lifecycle) ──────────────────────

    /// @brief 原子装配：构造即完整（依赖、选项、context 集合、hook 一次性注入并封口）。
    ///
    /// 语义顺序：逐 context `ensure_sid_resolved()`（registry 此刻已 freeze）→ 全量校验。
    /// 构造成功后 engine 的路由集合不再变化，运行期只剩 `start()` 与事件端口。
    ///
    /// @param deps    non-owning 依赖；gateway/view 必填。
    /// @param opts    session/roll/routing policy 等行为选项。
    /// @param contexts engine 接管所有权；顺序即路由下标顺序，不可为空、不可含空指针。
    /// @param hooks   observer 与各类 callback；observer 按注册序 fan-out，生命周期与
    ///                异常契约见 Hooks 注释。
    /// @throws std::invalid_argument 装配契约违反：gateway/view 为空；contexts 为空或含空指针；
    ///         strategy_id 为空/重复/落保留 `_` 域；非 WarnAndDrop policy 用于 single-context；
    ///         multi-context 下存在未 resolved 的 sid；conditional_orders 开关与 context 不一致；
    ///         hooks.observers 含空指针或重复指针。
    XtHftEngine(Dependencies deps,
                Options opts,
                std::vector<std::unique_ptr<XtStrategyContext>> contexts,
                Hooks hooks);

    /// @brief 释放 engine 自有 context/lifecycle 状态；不释放注入的 non-owning 依赖。
    ~XtHftEngine();

    XtHftEngine(const XtHftEngine&) = delete;
    XtHftEngine& operator=(const XtHftEngine&) = delete;
    XtHftEngine(XtHftEngine&&) = delete;
    XtHftEngine& operator=(XtHftEngine&&) = delete;

    /// @brief 启动全部 strategy context（exactly-once），返回结构化启动结果。
    ///
    /// `started_` 在函数入口即被消费（先于任何策略回调）：即使某 context 的
    /// on_init 失败回调中途抛出逃逸，本 engine 亦不可重启——二次调用一律抛异常，
    /// 杜绝「部分初始化后重跑副作用」。单 context 的 on_init 失败只 disable 该
    /// context（同步触发 on_strategy_init_failure）并计入 report，兄弟策略照常启动。
    ///
    /// live 由 EventLoop worker 线程调用；backtest 由 runner 在 reporter 绑定后调用。
    /// @throws std::logic_error 重复调用。
    StartReport start();

    // ── 配置注入 (configuration) ──────────────────────────────────────

    /// @brief engine 级（replayer 交付之后）丢弃计数；live/backtest 两端同口径自增。
    ///
    /// 计数只在既有 early-return 丢弃分支自增（accepted 热路径零新增指令）。
    /// roll = roll should_dispatch 拒绝；session = session gate + pre-session 非
    /// preopen 拒绝；calendar = 节假日过滤拒绝。
    /// @note 当前唯一消费者是 backtest consumed manifest；live 自增但无人读。
    struct EngineDropCounters {
        uint64_t roll     = 0;
        uint64_t session  = 0;
        uint64_t calendar = 0;
    };
    const EngineDropCounters& engine_drop_counters() const noexcept {
        return drops_;
    }

    // ── Context 查询 (context queries) ────────────────────────────────

    /// @brief 当前已注入的 strategy context 数量。
    std::size_t context_count() const noexcept { return slots_.size(); }

    /// @brief 按装配顺序返回只读 strategy context；idx 越界时返回 nullptr。
    /// @note 只读：engine 是 context 集合的唯一 owner，可变操作一律走下方窄口，
    ///       避免调用方绕过 engine 直改运行期状态。
    const XtStrategyContext* context(std::size_t idx) const noexcept;

    // ── Context 窄口 (context mutation ports) ────────────────────────
    //
    // context 可变操作的全部合法入口；每个端口自带调用时机契约，engine 内部
    // 遍历 slots_，调用方不再持可变 context 指针。

    /// @brief 控制面按 strategy_id 启停策略提交（gRPC binder / panel）。
    /// @param strategy_id 目标策略；空串直接返回 false。
    /// @return 是否命中至少一个 context。
    /// @note enabled=false 时该 context 的 can_submit() fail-closed。
    bool set_strategy_control_enabled(const std::string& strategy_id, bool enabled);

    /// @brief 装配期回灌 userdata：loader 返回 true 时用其出参覆盖该 context。
    /// @param loader runner 提供的读回策略（入参 strategy_id，出参 kv）；落盘 IO 与
    ///        失败告警全归 runner，engine 与 SDK 保持零 IO。
    /// @pre 必须在 start()（策略 on_init）之前、worker 线程未启动时调用；
    ///      与运行中的 save_userdata/load_userdata 并发是 data race。
    void restore_strategy_userdata(
        const std::function<bool(const std::string& strategy_id,
                                 std::unordered_map<std::string, std::string>& out)>& loader);

    /// @brief graceful stop 后导出各 context 的 userdata（与 restore 对称）。
    /// @param writer runner 提供的落盘策略（入参 strategy_id 与快照）；失败告警归 runner。
    /// @pre 必须在 worker 线程已 join 后调用（snapshot 本身无同步）。
    void dump_strategy_userdata(
        const std::function<void(const std::string& strategy_id,
                                 std::unordered_map<std::string, std::string> kv)>& writer) const;

    /// @brief 排出全部 context 的策略指标快照（drain 语义：读后清零）。
    /// @param out 按装配顺序 append `{strategy_id, snapshot}`；调用方负责清空/预留容量。
    /// @note 调用方是 RunnerControl 的 1Hz frame loop（`flush_strategy_metrics_if_needed`
    ///       → `XtHftEventLoop::drain_all_strategy_metrics`，XtHftControlReportEmit.cpp:804），
    ///       跑在**控制线程而非 worker**。跨线程只读遍历 `slots_` 安全的依据：原子装配封口后
    ///       slots_ 结构不再变化（无 push_back/erase，无 ctx 指针重绑）；per-context 指标本身的
    ///       并发安全由 ctx 的并发指标模式（双缓冲 + seqlock）承担——live 组合根必须在
    ///       EventLoop 启动前 `enable_concurrent_metrics()`。
    void drain_all_strategy_metrics(
        std::vector<std::pair<std::string, ct::metrics::XtStrategyMetricsSnapshot>>& out);

    // ── 行情与控制事件 (market/control events) ───────────────────────

    /// @brief 行情 tick 入口；更新 runtime/account 视图后再分发给 strategy。
    void on_tick(const XtTick& tick);

    /// @brief L2 委托队列入口；更新 runtime market cache 后分发给 strategy。
    void on_order_queue(const XtOrderQueue& oq);

    /// @brief L2 逐笔委托入口；更新 runtime market cache 后分发给 strategy。
    void on_order_detail(const XtOrderDetail& od);

    /// @brief L2 逐笔成交入口；更新 runtime market cache 后分发给 strategy。
    void on_transaction(const XtTransaction& tr);

    /// @brief 通道 ready 事件；同步 lifecycle 状态并广播给可见 strategy。
    void on_channel_ready(const char* channel);

    /// @brief 通道 lost 事件；同步 lifecycle 状态并广播给可见 strategy。
    void on_channel_lost(const char* channel, int reason);

    /// @brief 参数版本更新事件；先通知 context，再执行外部扩展回调。
    void on_params_updated(uint64_t param_version);

    // ── 执行与恢复事件 (execution/recovery events) ───────────────────

    /// @brief 处理成交回报：更新 domain 状态并派发策略 on_fill。
    /// @param trade            已由数据源层归一化的成交。
    /// @param td_state         Recovering 阶段直接忽略并返回 true。
    /// @param out_realized_fp  非空时写入本 trade 的 realized PnL（account_store apply 前后差）；
    ///                         Recovering 早退不写。
    /// @param out_effective_sid 非空时写入恢复后的 strategy_id；不可归因时为空。
    /// @return true 表示策略 on_fill 派发成功或无需派发；false 表示策略回调失败。
    /// @note unmatched fill（multi-strategy sid 归因失败）时 book/account 已记账且不回滚，
    ///       仅跳过策略 on_fill——账户以 venue 为准（account-truth 单源），策略仓位
    ///       必须走 pull（get_position_fp），不得维护本地 shadow 仓位。
    /// @param elig latency eligibility（gateway 边界盖章；仅 gate observer 的 latency
    ///        tracker feed，kIneligible=延迟重放/合成不喂）；BT caller 默认 kEligible。
    bool on_trade(const XtTradeRecord& trade, ITradeGateway::TdState td_state,
                  int64_t* out_realized_fp = nullptr,
                  std::string* out_effective_sid = nullptr,
                  XtLatencyEligibility elig = XtLatencyEligibility::kEligible);

    /// @brief 处理订单回报：更新 domain 状态并按 sid 派发 strategy on_order。
    /// @details
    /// - runtime book/account 先消费物理 order id；策略和 observer 随后看到 logical id。
    /// - 策略回调失败返回 false；已写入的 domain 状态不回滚。
    /// - multi-strategy unmatched 会记录并跳过策略派发；strict 且无 live kill-switch 入口时 fail-fast。
    /// - 终态回报会清理 order lifecycle 辅助索引并通知 gateway terminal tracking。
    /// @return true 表示 domain 更新完成，且 strategy on_order 成功或 unmatched 已记录并跳过策略派发；
    ///         false 表示策略回调失败。
    /// @param elig latency eligibility（gateway 边界盖章；仅 gate observer latency feed）；
    ///        BT caller / reconcile 合成终态默认由 caller 显式传（见 reconcile_lost_terminals）。
    bool on_order_update(const XtOrderExecUpdate& upd,
                         XtLatencyEligibility elig = XtLatencyEligibility::kEligible);

    /// @brief 导入 venue 仍 alive 的 in-flight orders；空集合合法。
    /// @param is_cold_start true=进程冷启首次 recovery；false=warm reconnect。
    void on_reverse_sync_batch(const std::vector<AliveInflightOrder>& orders,
                               bool is_cold_start);

    /// @brief 回收 recovered-unowned 在途单：grace 后扫描 book 并逐个撤单。
    /// @details
    /// - recovered-unowned 表示 reverse-sync 灌入、但当前进程没有 strategy 归属的订单。
    /// - reverse-sync 后 grace_ms 内不处理，给 single-strategy 恢复单自行收尾窗口。
    /// - 撤单幂等；未收敛的订单由后续 sweep 重试。
    /// @param now_ms     调用方 steady ms（与 on_reverse_sync_batch 的戳同源）。
    /// @param grace_ms   reverse-sync 后的静默窗口 ms。
    /// @param max_cancels 单次 sweep 撤单上限（venue 限频保护）。
    /// @return 本次实际发出的撤单数；start() 之前调用返回 0（计入 pre-start drop）。
    /// @note 仅 worker 线程可调（book 单写者契约）；backtest 从不 reverse-sync → 永远 no-op。
    size_t reap_recovered_orphans(int64_t now_ms, int64_t grace_ms, size_t max_cancels);

    /// @brief 修复 venue 已终态但本地 book 仍 open 的本进程订单。
    /// @details
    /// - 只处理 gateway 确认 terminal 的 owned 订单，recovered-unowned 交给 orphan reap。
    /// - 命中后合成终态回报，复用 on_order_update 的 book/account/strategy/observer 收尾。
    /// - 合成终态路径幂等；重复终态由 book/account/strategy 辅助索引吸收。
    /// - 每轮全量扫 open 集（不按 max 截断），避免 venue-terminal 单排在预算窗口外被饿死。
    /// @param max  单轮合成（heal）上限（burst 防护）；open 集全量扫描不受此截断。
    /// @return 本次合成的终态数；start() 之前调用返回 0（计入 pre-start drop）。
    /// @note 仅 worker 线程可调（book 单写者契约）；gateway is_order_terminal 非 CTP 默认 false → no-op。
    size_t reconcile_lost_terminals(size_t max);

    /// @brief 导入 venue 持仓快照。
    /// @param positions      venue 返回的完整持仓快照。
    /// @param td_state       Recovering 表示 recovery 快照；Live 表示普通同步快照。
    /// @param is_cold_start  true=进程冷启首次 recovery；false=warm reconnect。
    /// @param session_gen    TD session generation（gen fence，live 透传给 observer 做 stale 判定）；
    ///                       backtest/无 gen 语境传默认 0。
    void on_position_snapshot(const std::vector<ITradeGateway::PositionSnapshot>& positions,
                              ITradeGateway::TdState td_state,
                              bool is_cold_start = false,
                              uint32_t session_gen = 0);

    /// @brief 导入账户资金快照；更新 account store 后广播给所有策略和 observer。
    void on_account_snapshot(const ITradeGateway::AccountSnapshot& snap);

    /// @brief 导入日终结算增量；提交为 account stream 的 DailySettlement 事件。
    void on_daily_settlement(const ITradeGateway::DailySettlementDelta& delta);

    /// @brief 导入出入金增量；提交为 account stream 的 CashTransfer 事件。
    void on_cash_transfer(const ITradeGateway::CashTransferDelta& delta);

    // ── 诊断导出 (diagnostics) ────────────────────────────────────────

    /// @brief 诊断快照别名；POD 定义在 XtHftEngineStats.h（轻头，供只
    ///        forward-declare engine 的 caller 使用完整类型）。
    using UnmatchedStats = XtHftUnmatchedStats;

    /// @brief 一次性 relaxed 读出全部路由/恢复诊断计数。
    UnmatchedStats unmatched_stats() const noexcept {
        UnmatchedStats s;
        s.fill_empty_sid = dropped_fill_empty_sid_count_.load(std::memory_order_relaxed);
        s.fill_unknown_sid = dropped_fill_unknown_sid_count_.load(std::memory_order_relaxed);
        s.order_update_empty_sid =
            dropped_order_update_empty_sid_count_.load(std::memory_order_relaxed);
        s.order_update_unknown_sid =
            dropped_order_update_unknown_sid_count_.load(std::memory_order_relaxed);
        s.exec_dispatch = exec_dispatch_count_.load(std::memory_order_relaxed);
        s.sid_fallback_recovered =
            unmatched_sid_recovered_count_.load(std::memory_order_relaxed);
        s.lost_terminal_reconciled =
            lost_terminal_reconcile_total_.load(std::memory_order_relaxed);
        return s;
    }

    /// @brief 因 multi-strategy sid 未归因而丢弃的 fill 累计数（empty+unknown 两桶之和）。
    uint64_t dropped_fill_count() const noexcept {
        return dropped_fill_empty_sid_count_.load(std::memory_order_relaxed) +
               dropped_fill_unknown_sid_count_.load(std::memory_order_relaxed);
    }

    /// @brief 因 multi-strategy sid 未归因而丢弃的 order update 累计数（两桶之和）。
    uint64_t dropped_order_update_count() const noexcept {
        return dropped_order_update_empty_sid_count_.load(std::memory_order_relaxed) +
               dropped_order_update_unknown_sid_count_.load(std::memory_order_relaxed);
    }

    /// @brief recovered-unowned 孤儿在途单被自动撤单的累计数。
    uint64_t orphan_reap_cancel_count() const noexcept {
        return orphan_reap_cancel_total_.load(std::memory_order_relaxed);
    }

    /// @brief 因归属 journal 持久化降级而被抑制（切告警-only）的 orphan reap sweep 累计数。
    uint64_t orphan_reap_suppressed_count() const noexcept {
        return orphan_reap_suppressed_total_.load(std::memory_order_relaxed);
    }

    /// @brief venue 已终态但本地 book 仍 open 的订单被合成终态修复的累计数。
    uint64_t lost_terminal_reconcile_count() const noexcept {
        return lost_terminal_reconcile_total_.load(std::memory_order_relaxed);
    }

    /// @brief 因 on_init 失败被 fail-closed disable 的 context 累计数。
    /// @note 组合根据此判定：backtest fail-fast（>0 即 run 失败），live 观测。
    uint64_t strategy_init_failure_count() const noexcept {
        return strategy_init_failure_total_.load(std::memory_order_relaxed);
    }

    /// @brief 本 engine 实例的策略回调（on_tick/on_fill/on_order 等）被 run_guarded
    ///        捕获的累计异常数。
    /// @note 组合根据此判定：**backtest 收尾 >0 即 run 失败**——策略回调内抛出的
    ///       fail-fast 异常（如 bt-time-axis P4 的 latency 越界校验）被 guard 吞掉
    ///       （仅 error log + 计数），engine 层 throw 才穿透；此计数是唯一可观测信号。
    ///       live 观测。
    uint64_t strategy_callback_exception_count() const noexcept {
        return strategy_exception_total_.load(std::memory_order_relaxed);
    }

    /// @brief start() 之前到达而被丢弃的 runtime 事件累计数。
    /// @note 生产两端事件流均在 start() 之后开闸，>0 表示装配时序被破坏。
    uint64_t pre_start_dropped_count() const noexcept {
        return pre_start_dropped_total_.load(std::memory_order_relaxed);
    }

    /// @brief 本地 order book 当前 alive 在途单数（含 recovered 孤儿）；无 book 时 0。
    /// @note 上层控制面周期读取，导出为 xt_open_orders gauge（venue per-symbol cap 预警）。
    uint32_t open_order_count() const noexcept;

private:
    struct FillPositionBefore {
        int64_t qty_fp = 0;
        int64_t avg_price_fp = 0;
    };

    // ── 初始化 (initialization) ────────────────────────────────────────

    /// @brief 应用回放扩展：calendar filter 与夜盘 trading_date 推断器。
    void apply_replay_ext_(const ReplayExt& ext);

    /// @brief 原子构造的装配校验；失败即抛，engine 不进入半装配态。
    /// @throws std::invalid_argument 见构造函数契约。
    void validate_assembly_(const Options& opts, const Hooks& hooks) const;

    /// @brief 记录 start() 之前到达的 runtime 事件（计数 + 节流 error 日志）。
    /// @note 冷路径；热路径只保留调用点的一个可预测分支。
    void note_pre_start_drop_(const char* entry) noexcept;

    // ── Tick 与 session pipeline ──────────────────────────────────────

    /// @brief 将 tick 价格写入 gateway/account 视图。
    void apply_strategy_tick_state_(const XtTick& tick);

    /// @brief 准备正常交易 tick 的共享状态。
    /// @details
    /// - 归一化本轮 `trading_date`；数据未提供时按 `td_infer_`，
    ///   再按 `action_date/action_time` fallback 推断。
    /// - 检查 `calendar_filter_`，处理 roll 切换与 session begin/end 边界。
    /// - 推进 market state、gateway clock、account mark，并触发可选 `mark_tick_cb_`。
    /// - false 条件：calendar filter 拒绝该交易日，或 session gate 拒绝该 tick。
    bool prepare_cycle_(XtTick& tick);

    /// @brief 将 tick fan-out 到所有 strategy context。
    void dispatch_tick_to_contexts_(const XtTick& tick,
                                    const char* stage,
                                    const char* error_message);

    /// @brief 本地条件单 per-tick 触发判定（dispatch 之前：stop 优先于策略新意图，
    ///        同 tick 同批进 process_tick 撮合）；空 book ctx header-inline 跳过。
    void check_conditional_stops_(const XtTick& tick);

    /// @brief 条件单终态喂入：与 dispatch_order_update_to_strategy_ 同一 sid
    ///        路由口径定位 ctx（on_order_update 是 live/BT 共同入口）。
    /// @param book_sid_hint stamp-miss（upd.sid_id==0）时的 book slot 反查值（在
    ///        终态 erase 前快照）；只参与 stop 路由选 ctx，不回写 upd。
    void notify_stops_for_update_(const XtOrderExecUpdate& upd, uint32_t book_sid_hint);

    /// @brief 从 gateway 拉取订单状态更新并路由给策略。
    void drain_order_updates_();

    /// @brief 处理一个正常交易 tick：准备状态、派发策略、推进撮合。
    /// @details
    /// - `prepare_cycle_` 先更新 session、market、account 状态。
    /// - 调整后的 tick 随后派发给 strategy context，策略可在该 tick 内下单。
    /// - 策略派发后调用 `gateway_->process_tick()`，并同步订单状态更新。
    /// - fill 可见性由 backtest clock / `drain_fills` 控制，不在本函数内 drain。
    /// @param tick 策略和撮合共用的正常交易 tick；按值接收以允许本轮归一化。
    void run_tick_cycle_(XtTick tick);

    /// @brief 取得 tick 所属 trading_date；优先使用 tick.trading_date，缺失时按配置推断。
    uint32_t infer_trading_date_(const XtTick& tick) const;
    /// @brief 判断当前 roll 配置是否允许该 tick 分发给策略。
    bool roll_accepts_tick_(const XtTick& tick) const;

    /// @brief 夜盘 pre-open auction tick 仅 dispatch 给策略，不驱动撮合。
    /// @details
    /// - 只处理跨午夜 session 开盘前一分钟的 tick。
    /// - 命中后更新 market/runtime view，并分发给 strategy context。
    /// - 不进入 `run_tick_cycle_` 的撮合和订单回报链。
    void handle_pre_session_tick_(const XtTick& tick);

    /// @brief 推进 session 边界状态，并在交易日切换时广播 session begin/end。
    bool update_session_state_(uint32_t session_date, uint32_t action_time,
                               const XtTick& prev_tick);
    /// @brief 执行 session-begin 副作用组：pre-warm → trading_date_ → account session_begin
    ///        → gateway on_session_begin → 逐 ctx fan-out；顺序承重勿重排。
    void enter_new_session_(uint32_t trading_date, const XtTick& prev_tick);
    /// @brief 写入最新 market/cache/clock 状态，供 strategy context 和 runtime view 读取。
    void update_market_state_(const XtTick& tick);
    /// @brief session_begin 前预热 last tick snapshot；不生成完整行情事件。
    void pre_warm_snapshot_(const XtTick& tick);
    /// @brief 按 trading_date 检查主力合约切换并广播 on_roll。
    void check_roll_change_(uint32_t trading_date);

    // ── 成交/订单/恢复辅助 (fill/order/recovery helpers) ─────────────

    /// @brief 发出 fill 后 lifecycle / position metric。
    void emit_fill_telemetry_(const XtTradeRecord& tr,
                              int64_t pos_before_qty_fp,
                              int64_t pos_before_avg_fp);
    /// @brief 在 account_store apply 前采样本 code 仓位；不需要输出时返回零值。
    FillPositionBefore capture_fill_position_before_(const XtTradeRecord& tr,
                                                     bool need_realized_output) const;
    /// @brief 调用回测 MAE/MFE 逐笔成交 excursion callback。
    void emit_trade_excursion_(const XtTradeRecord& tr,
                               const FillPositionBefore& pos_before,
                               int64_t fill_realized_fp);

    /// @brief 将 strategy on_fill 路由到对应的策略；优先 gateway 预 stamp 的 `tr.sid_id`，
    ///        0 时回退 strategy_id 解析。false 表示策略回调失败。
    /// @param out_logical_order_id 非空且命中 matched sink 时，写回本次 build_strategy_fill_event_
    ///        已算出的 logical order id（L-2 fill-link，供 trade fan 传 observer；不做二次
    ///        lookup）；manual/unmatched/no-sink 边缘路径保持 0（不新增锁）。
    bool dispatch_fill_to_strategy_(const XtTradeRecord& tr,
                                    uint64_t* out_logical_order_id = nullptr);
    /// @brief 将 fill 的 strategy_id 解析为 sid_id；缺失或未注册返回 kNoSidId。
    uint32_t resolve_fill_sid_id_(const std::string& strategy_id) const;
    /// @brief 处理 multi-context 下未匹配的 fill；返回 true 表示主流程继续 observer 通知。
    bool handle_unmatched_fill_(const XtTradeRecord& tr);
    /// @brief 构造策略可见 fill，并将多切片单物理 order id 翻译为 logical clord_id。
    XtFillEvent build_strategy_fill_event_(const XtTradeRecord& tr) const;

    /// @brief 让 book/account 消费物理 order id；策略视图转换在此之后发生。
    void apply_order_update_state_(const XtOrderExecUpdate& applied_upd,
                                   bool is_terminal);
    /// @brief 构造策略可见 order update，并翻译 logical clord_id。
    XtOrderExecUpdate build_strategy_order_update_(
        const XtOrderExecUpdate& applied_upd) const;
    /// @brief 将 strategy on_order 路由到对应的策略；false 表示策略回调失败。
    bool dispatch_order_update_to_strategy_(const XtOrderExecUpdate& strategy_upd);
    /// @brief 处理 multi-context 下未匹配的 order update；返回 true 表示 observer 仍继续通知。
    bool handle_unmatched_order_update_(const XtOrderExecUpdate& strategy_upd);
    /// @brief 终态回报完成 observer 通知后清理辅助索引和 gateway terminal tracking。
    void finish_terminal_order_update_(const XtOrderExecUpdate& applied_upd);

    /// @brief recovery/cold boot 时按 declared registry / context 归因继承仓位，
    /// 再委托 lifecycle_.on_resume 补发 resume 事件。
    void emit_resume_for_recovery_(
        const std::vector<ITradeGateway::PositionSnapshot>& positions);

    // ── Fail-closed disable (per-strategy) ───────────────────────────

    /// @brief ctx[i] 是否已因 on_init 失败被 fail-closed disable。
    /// @note disabled 集恒空（正常启动路径）时对每个 fan-out 只多一个廉价 false 分支，
    ///       行为与旧代码逐位等价。
    bool is_ctx_disabled_(std::size_t i) const noexcept {
        return i < slots_.size() && slots_[i].disabled;
    }
    /// @brief 标记 ctx[i] fail-closed disabled：置位 + 计数 + critical 日志 + 触发失败回调。
    /// @note 幂等；回调携带 (resolved_sid_id, strategy_id)，空回调时仅本地 disable。
    void disable_context_(std::size_t i);

    // ── 策略路由 (strategy routing) ──────────────────────────────────

    /// @brief 按 sid_id 查 context 下标（uint32 路由，免 name() round-trip）；未命中返回 slots_.size()。
    std::size_t find_ctx_idx_by_sid_id_(uint32_t sid_id) const noexcept;

    /// @brief 下标 i 的执行回调目标（context 即 sink）；越界返回 nullptr。
    xt_strategy_engine::IStrategyExecSink* sink_at_(std::size_t i) const noexcept;

    /// @brief 按 sid_id 选择执行回报的策略回调目标。
    /// @details
    /// - manual 保留 sid 与 disabled context 在入口早退（返回 nullptr、不置 skip）。
    /// - single-context 模式返回唯一 context，不要求 sid 命中。
    /// - multi-context 模式按 sid_id 找到 context 下标，再返回同下标 context。
    /// - multi-context 下 `kNoSidId` 或未命中时返回 nullptr，并置 `skip_unmatched=true`。
    /// @param skip_unmatched 输出参数；true 表示调用方应跳过策略派发并走 unmatched 诊断路径。
    /// @return 目标 sink；返回 nullptr 且 `skip_unmatched=false` 表示当前没有 callback 目标。
    xt_strategy_engine::IStrategyExecSink* select_routing_sink_(uint32_t sid_id,
                                          bool& skip_unmatched) const noexcept;

    /// @brief 为缺失 strategy_id 的 fill 恢复唯一 sid；成功时写回 `tr.strategy_id` 并回填 `tr.sid_id`。
    bool recover_missing_fill_sid_(XtTradeRecord& tr);
    /// @brief 记录 fill sid fallback 成功的诊断计数和 rate-limited warn。
    void record_recovered_fill_sid_(const XtTradeRecord& tr, const std::string& recovered_sid);

    /// @brief fill 缺 sid 时恢复唯一 strategy_id：single 归唯一 context，multi 走 code->sid。
    ///        无唯一归属时返回空 sid。
    /// @note single-context fallback 的前提是 runner 独占账户：账户内出现非本进程
    ///       策略的 foreign fill 会被误归 contexts_[0]，污染 per-strategy 归因。
    std::string effective_sid_(const std::string& sid, const std::string& code);
    /// @brief 懒构建 code->sid fallback 表。
    void build_code_to_sid_();

    // ── 核心依赖 (core dependencies) ─────────────────────────────────

    xt_runtime::IHftGateway* gateway_ = nullptr;  ///< non-owning；构造函数要求非空。
    xt_strategy_engine::XtRuntimeView* runtime_view_ = nullptr;  ///< non-owning；构造函数要求非空。

    // ── 策略上下文状态 (strategy contexts) ───────────────────────────

    /// @brief engine 拥有的 strategy context 及其 fail-closed 状态。
    struct ContextSlot {
        std::unique_ptr<XtStrategyContext> ctx;
        bool disabled = false;  ///< on_init 失败置位，后续 fan-out 跳过该 ctx。
    };
    /// 唯一 routing truth：装配顺序即路由下标顺序，context 本身即执行回调 sink。
    std::vector<ContextSlot> slots_;

    // ── Session 与行情状态 (session/market state) ───────────────────

    xt_core::Config session_;  ///< Config.session 拷贝；disabled 或空 sections 表示不启用 session gate。
    XtRollEngine roll_;        ///< 主力合约切换 resolver；未配置 roll 时 inactive。
    XtTradingDateInfer td_infer_;  ///< 回测夜盘 trading_date 推断器；默认 disabled。
    std::function<bool(uint32_t)> calendar_filter_;  ///< 可选 trading_date filter；false 会丢弃 tick。
    EngineDropCounters drops_;  ///< engine 级丢弃计数（见 engine_drop_counters()）。

    std::string last_primary_symbol_;   ///< 已通知策略的上一主力合约；空表示尚未通知。
    uint32_t last_roll_check_date_ = 0; ///< 上次执行 roll resolve 的 trading_date；0=尚未检查。

    XtTick last_any_core_tick_{};  ///< runtime clock / session_begin pre-warm 的最近 core tick。
    bool has_last_any_core_tick_ = false;  ///< 区分无 snapshot 与零值 tick。
    uint32_t trading_date_ = 0;     ///< 当前 engine trading_date；0=尚未进入有效交易日。
    uint32_t last_action_date_ = 0; ///< 已触发 session 边界的日期状态；0=尚未 session_begin。

    // ── Domain 服务 (domain services) ────────────────────────────────

    xt_account::AccountStateStore* account_store_ = nullptr;  ///< non-owning；空值关闭账户写入/PnL 采样。
    xt_runtime::order::XtOrderRuntimeBook* order_book_ = nullptr;  ///< non-owning；空值关闭 book/recovery 路径。
    const XtCommodityCatalog* catalog_ = nullptr;  ///< non-owning；空值跳过合约元数据依赖路径。
    xt_strategy_engine::XtDeclaredSymbolRegistry* declared_registry_ = nullptr;  ///< non-owning sid->symbols 路由表。
    xt_runtime::IOrderIdTranslator* order_id_translator_ = nullptr;  ///< 可选物理/逻辑 order id 适配器。

    // ── Lifecycle 与 telemetry 状态 ──────────────────────────────────

    xt_engine::hft::HftLifecycleTelemetry lifecycle_;  ///< engine-owned lifecycle/open_ts 状态；callback 可为空。

    // ── 回调与 observer (callbacks/observers) ────────────────────────

    FillRealizedCallback fill_realized_cb_;        ///< 可选 per-fill realized callback；空值关闭输出。
    CashReanchorProbe cash_reanchor_probe_;        ///< 可选 set_cash 重锚 divergence 采样；空值关闭。
    bool cash_anchored_ = false;                   ///< 首个有效 broker 快照落锚后置 true；首锚不算校正。
    MarkTickCallback mark_tick_cb_;                ///< 可选 per-tick mark callback；live 默认空值。
    TradeExcursionCallback trade_excursion_cb_;    ///< 可选 per-trade excursion callback；空值关闭输出。
    std::vector<xt_runtime::IHftEngineObserver*> observers_;  ///< non-owning；按注册顺序接收事件。

    std::function<void(uint64_t)> on_params_extra_;  ///< 可选外部扩展 hook；context 通知后调用。
    std::function<void(uint32_t, const std::string&)> on_strategy_init_failure_;  ///< on_init 失败回调；空则仅本地 disable（live 注入置 per-sid kill latch）。

    // ── 路由诊断状态 (routing diagnostics) ───────────────────────────

    // engine 线程写；diagnostic getter relaxed 读取，计数只用于观测和告警。
    // fill/order update 丢弃按 reason 分桶：empty_sid=事件无归因，unknown_sid=归因未命中。
    std::atomic<uint64_t> dropped_fill_empty_sid_count_{0};
    std::atomic<uint64_t> dropped_fill_unknown_sid_count_{0};
    std::atomic<uint64_t> dropped_order_update_empty_sid_count_{0};
    std::atomic<uint64_t> dropped_order_update_unknown_sid_count_{0};
    std::atomic<uint64_t> exec_dispatch_count_{0};  ///< fill+order update 派发尝试总数（unmatched budget 分母，派发计数先于 drop）。
    std::atomic<uint64_t> unmatched_sid_recovered_count_{0};  ///< sid fallback 成功恢复归因次数。
    std::atomic<uint64_t> orphan_reap_cancel_total_{0};  ///< recovered-unowned 孤儿单撤单请求累计数。
    std::atomic<uint64_t> orphan_reap_suppressed_total_{0};  ///< journal 降级下 orphan reap 被抑制（告警-only）的累计数。
    std::atomic<uint64_t> lost_terminal_reconcile_total_{0};  ///< venue-terminal/book-open 修复累计数。
    std::atomic<uint64_t> strategy_init_failure_total_{0};  ///< on_init 失败被 fail-closed disable 的 ctx 累计数。
    std::atomic<uint64_t> strategy_exception_total_{0};  ///< 本实例策略回调被 guard 捕获的累计异常数。
    std::atomic<uint64_t> pre_start_dropped_total_{0};  ///< start() 前到达被丢弃的 runtime 事件累计数。
    int64_t last_reverse_sync_steady_ms_ = 0;  ///< 上次 reverse-sync steady ms；0=从未发生。

    std::unordered_map<std::string, std::string> code_to_sid_;  ///< lazy code->sid fallback 表。
    bool code_to_sid_built_ = false;  ///< true 表示已尝试构建，含无 declared_registry_ 场景。

    UnmatchedRoutingPolicy unmatched_routing_ = UnmatchedRoutingPolicy::WarnAndDrop;  ///< sid 未命中时的处置。

    bool conditional_orders_enabled_ = false;  ///< 本地条件单总闸；false 时三个 hook 单分支早退。

    bool started_ = false;  ///< start() 已消费；false 时 runtime 事件入口一律丢弃。
};
