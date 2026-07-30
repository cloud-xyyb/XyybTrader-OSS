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

    using ReplayExt = xt_engine::hft::XtHftReplayExt;

    using PositionLifecycleEvent    = xt_engine::hft::PositionLifecycleEvent;
    using PositionLifecycleCallback = xt_engine::hft::PositionLifecycleCallback;
    using PositionMetricCallback    = xt_engine::hft::PositionMetricCallback;
    using LifecycleTelemetryInit    = xt_engine::hft::HftLifecycleTelemetry::Init;

    using FillRealizedCallback = std::function<void(const std::string& sid,
                                                    const std::string& code,
                                                    int64_t realized_fp)>;

    using CashReanchorProbe = std::function<void(int64_t divergence_fp)>;

    using MarkTickCallback = std::function<void(const XtTick&)>;

    using TradeExcursionCallback = std::function<void(const XtTradeRecord& tr,
                                                      int64_t before_qty_fp,
                                                      int64_t after_qty_fp,
                                                      int64_t after_avg_fp,
                                                      int64_t realized_fp)>;

    enum class UnmatchedRoutingPolicy {
        WarnAndDrop,
        DropAndAlert,
        FailFast,
    };

    /// @brief engine 的 non-owning 依赖（required + optional）。
    ///
    /// gateway 与 view 为必填（构造期 fail-fast）；其余指针为空时，engine 跳过依赖
    /// 该组件的路径。
    struct Dependencies {
        xt_runtime::IHftGateway* gateway = nullptr;
        xt_strategy_engine::XtRuntimeView* view = nullptr;
        xt_account::AccountStateStore* account_store = nullptr;
        xt_runtime::order::XtOrderRuntimeBook* order_book = nullptr;
        const XtCommodityCatalog* catalog = nullptr;
        xt_runtime::IOrderIdTranslator* order_id_translator = nullptr;

        xt_strategy_engine::XtDeclaredSymbolRegistry* declared_registry = nullptr;
    };

    /// @brief engine 行为选项（值语义，构造期拷入）。
    struct Options {
        xt_core::Config session = {};
        XtRollEngine::Config roll = {};
        UnmatchedRoutingPolicy unmatched_routing = UnmatchedRoutingPolicy::WarnAndDrop;

        bool conditional_orders_enabled = false;

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

        std::vector<xt_runtime::IHftEngineObserver*> observers;
        LifecycleTelemetryInit lifecycle = {};

        FillRealizedCallback fill_realized;

        CashReanchorProbe cash_reanchor;

        MarkTickCallback mark_tick;

        TradeExcursionCallback trade_excursion;

        std::function<void(uint64_t  )> on_params_extra;

        std::function<void(uint32_t  , const std::string&  )>
            on_strategy_init_failure;
    };

    /// @brief 单个策略启动失败记录。
    struct StrategyStartFailure {
        uint32_t sid_id = 0;
        std::string strategy_id;
    };

    /// @brief start() 的结构化结果。
    struct StartReport {
        bool all_started = false;
        std::vector<StrategyStartFailure> failures;
    };

    XtHftEngine(Dependencies deps,
                Options opts,
                std::vector<std::unique_ptr<XtStrategyContext>> contexts,
                Hooks hooks);

    ~XtHftEngine();

    XtHftEngine(const XtHftEngine&) = delete;
    XtHftEngine& operator=(const XtHftEngine&) = delete;
    XtHftEngine(XtHftEngine&&) = delete;
    XtHftEngine& operator=(XtHftEngine&&) = delete;

    StartReport start();

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

    std::size_t context_count() const noexcept { return slots_.size(); }

    const XtStrategyContext* context(std::size_t idx) const noexcept;

    bool set_strategy_control_enabled(const std::string& strategy_id, bool enabled);

    void restore_strategy_userdata(
        const std::function<bool(const std::string& strategy_id,
                                 std::unordered_map<std::string, std::string>& out)>& loader);

    void dump_strategy_userdata(
        const std::function<void(const std::string& strategy_id,
                                 std::unordered_map<std::string, std::string> kv)>& writer) const;

    void drain_all_strategy_metrics(
        std::vector<std::pair<std::string, ct::metrics::XtStrategyMetricsSnapshot>>& out);

    void on_tick(const XtTick& tick);

    void on_order_queue(const XtOrderQueue& oq);

    void on_order_detail(const XtOrderDetail& od);

    void on_transaction(const XtTransaction& tr);

    void on_channel_ready(const char* channel);

    void on_channel_lost(const char* channel, int reason);

    void on_params_updated(uint64_t param_version);

    bool on_trade(const XtTradeRecord& trade, ITradeGateway::TdState td_state,
                  int64_t* out_realized_fp = nullptr,
                  std::string* out_effective_sid = nullptr,
                  XtLatencyEligibility elig = XtLatencyEligibility::kEligible);

    bool on_order_update(const XtOrderExecUpdate& upd,
                         XtLatencyEligibility elig = XtLatencyEligibility::kEligible);

    void on_reverse_sync_batch(const std::vector<AliveInflightOrder>& orders,
                               bool is_cold_start);

    size_t reap_recovered_orphans(int64_t now_ms, int64_t grace_ms, size_t max_cancels);

    size_t reconcile_lost_terminals(size_t max);

    void on_position_snapshot(const std::vector<ITradeGateway::PositionSnapshot>& positions,
                              ITradeGateway::TdState td_state,
                              bool is_cold_start = false,
                              uint32_t session_gen = 0);

    void on_account_snapshot(const ITradeGateway::AccountSnapshot& snap);

    void on_daily_settlement(const ITradeGateway::DailySettlementDelta& delta);

    void on_cash_transfer(const ITradeGateway::CashTransferDelta& delta);

    using UnmatchedStats = XtHftUnmatchedStats;

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

    uint64_t dropped_fill_count() const noexcept {
        return dropped_fill_empty_sid_count_.load(std::memory_order_relaxed) +
               dropped_fill_unknown_sid_count_.load(std::memory_order_relaxed);
    }

    uint64_t dropped_order_update_count() const noexcept {
        return dropped_order_update_empty_sid_count_.load(std::memory_order_relaxed) +
               dropped_order_update_unknown_sid_count_.load(std::memory_order_relaxed);
    }

    uint64_t orphan_reap_cancel_count() const noexcept {
        return orphan_reap_cancel_total_.load(std::memory_order_relaxed);
    }

    uint64_t orphan_reap_suppressed_count() const noexcept {
        return orphan_reap_suppressed_total_.load(std::memory_order_relaxed);
    }

    uint64_t lost_terminal_reconcile_count() const noexcept {
        return lost_terminal_reconcile_total_.load(std::memory_order_relaxed);
    }

    uint64_t strategy_init_failure_count() const noexcept {
        return strategy_init_failure_total_.load(std::memory_order_relaxed);
    }

    uint64_t strategy_callback_exception_count() const noexcept {
        return strategy_exception_total_.load(std::memory_order_relaxed);
    }

    uint64_t pre_start_dropped_count() const noexcept {
        return pre_start_dropped_total_.load(std::memory_order_relaxed);
    }

    uint32_t open_order_count() const noexcept;

private:
    struct FillPositionBefore {
        int64_t qty_fp = 0;
        int64_t avg_price_fp = 0;
    };

    void apply_replay_ext_(const ReplayExt& ext);

    void validate_assembly_(const Options& opts, const Hooks& hooks) const;

    void note_pre_start_drop_(const char* entry) noexcept;

    void apply_strategy_tick_state_(const XtTick& tick);

    bool prepare_cycle_(XtTick& tick);

    void dispatch_tick_to_contexts_(const XtTick& tick,
                                    const char* stage,
                                    const char* error_message);

    void check_conditional_stops_(const XtTick& tick);

    void notify_stops_for_update_(const XtOrderExecUpdate& upd, uint32_t book_sid_hint);

    void drain_order_updates_();

    void run_tick_cycle_(XtTick tick);

    uint32_t infer_trading_date_(const XtTick& tick) const;

    bool roll_accepts_tick_(const XtTick& tick) const;

    void handle_pre_session_tick_(const XtTick& tick);

    bool update_session_state_(uint32_t session_date, uint32_t action_time,
                               const XtTick& prev_tick);

    void enter_new_session_(uint32_t trading_date, const XtTick& prev_tick);

    void update_market_state_(const XtTick& tick);

    void pre_warm_snapshot_(const XtTick& tick);

    void check_roll_change_(uint32_t trading_date);

    void emit_fill_telemetry_(const XtTradeRecord& tr,
                              int64_t pos_before_qty_fp,
                              int64_t pos_before_avg_fp);

    FillPositionBefore capture_fill_position_before_(const XtTradeRecord& tr,
                                                     bool need_realized_output) const;

    void emit_trade_excursion_(const XtTradeRecord& tr,
                               const FillPositionBefore& pos_before,
                               int64_t fill_realized_fp);

    bool dispatch_fill_to_strategy_(const XtTradeRecord& tr,
                                    uint64_t* out_logical_order_id = nullptr);

    uint32_t resolve_fill_sid_id_(const std::string& strategy_id) const;

    bool handle_unmatched_fill_(const XtTradeRecord& tr);

    XtFillEvent build_strategy_fill_event_(const XtTradeRecord& tr) const;

    void apply_order_update_state_(const XtOrderExecUpdate& applied_upd,
                                   bool is_terminal);

    XtOrderExecUpdate build_strategy_order_update_(
        const XtOrderExecUpdate& applied_upd) const;

    bool dispatch_order_update_to_strategy_(const XtOrderExecUpdate& strategy_upd);

    bool handle_unmatched_order_update_(const XtOrderExecUpdate& strategy_upd);

    void finish_terminal_order_update_(const XtOrderExecUpdate& applied_upd);

    void emit_resume_for_recovery_(
        const std::vector<ITradeGateway::PositionSnapshot>& positions);

    bool is_ctx_disabled_(std::size_t i) const noexcept {
        return i < slots_.size() && slots_[i].disabled;
    }

    void disable_context_(std::size_t i);

    std::size_t find_ctx_idx_by_sid_id_(uint32_t sid_id) const noexcept;

    xt_strategy_engine::IStrategyExecSink* sink_at_(std::size_t i) const noexcept;

    xt_strategy_engine::IStrategyExecSink* select_routing_sink_(uint32_t sid_id,
                                          bool& skip_unmatched) const noexcept;

    bool recover_missing_fill_sid_(XtTradeRecord& tr);

    void record_recovered_fill_sid_(const XtTradeRecord& tr, const std::string& recovered_sid);

    std::string effective_sid_(const std::string& sid, const std::string& code);

    void build_code_to_sid_();

    xt_runtime::IHftGateway* gateway_ = nullptr;
    xt_strategy_engine::XtRuntimeView* runtime_view_ = nullptr;

    /// @brief engine 拥有的 strategy context 及其 fail-closed 状态。
    struct ContextSlot {
        std::unique_ptr<XtStrategyContext> ctx;
        bool disabled = false;
    };

    std::vector<ContextSlot> slots_;

    xt_core::Config session_;
    XtRollEngine roll_;
    XtTradingDateInfer td_infer_;
    std::function<bool(uint32_t)> calendar_filter_;
    EngineDropCounters drops_;

    std::string last_primary_symbol_;
    uint32_t last_roll_check_date_ = 0;

    XtTick last_any_core_tick_{};
    bool has_last_any_core_tick_ = false;
    uint32_t trading_date_ = 0;
    uint32_t last_action_date_ = 0;

    xt_account::AccountStateStore* account_store_ = nullptr;
    xt_runtime::order::XtOrderRuntimeBook* order_book_ = nullptr;
    const XtCommodityCatalog* catalog_ = nullptr;
    xt_strategy_engine::XtDeclaredSymbolRegistry* declared_registry_ = nullptr;
    xt_runtime::IOrderIdTranslator* order_id_translator_ = nullptr;

    xt_engine::hft::HftLifecycleTelemetry lifecycle_;

    FillRealizedCallback fill_realized_cb_;
    CashReanchorProbe cash_reanchor_probe_;
    bool cash_anchored_ = false;
    MarkTickCallback mark_tick_cb_;
    TradeExcursionCallback trade_excursion_cb_;
    std::vector<xt_runtime::IHftEngineObserver*> observers_;

    std::function<void(uint64_t)> on_params_extra_;
    std::function<void(uint32_t, const std::string&)> on_strategy_init_failure_;

    std::atomic<uint64_t> dropped_fill_empty_sid_count_{0};
    std::atomic<uint64_t> dropped_fill_unknown_sid_count_{0};
    std::atomic<uint64_t> dropped_order_update_empty_sid_count_{0};
    std::atomic<uint64_t> dropped_order_update_unknown_sid_count_{0};
    std::atomic<uint64_t> exec_dispatch_count_{0};
    std::atomic<uint64_t> unmatched_sid_recovered_count_{0};
    std::atomic<uint64_t> orphan_reap_cancel_total_{0};
    std::atomic<uint64_t> orphan_reap_suppressed_total_{0};
    std::atomic<uint64_t> lost_terminal_reconcile_total_{0};
    std::atomic<uint64_t> strategy_init_failure_total_{0};
    std::atomic<uint64_t> strategy_exception_total_{0};
    std::atomic<uint64_t> pre_start_dropped_total_{0};
    int64_t last_reverse_sync_steady_ms_ = 0;

    std::unordered_map<std::string, std::string> code_to_sid_;
    bool code_to_sid_built_ = false;

    UnmatchedRoutingPolicy unmatched_routing_ = UnmatchedRoutingPolicy::WarnAndDrop;

    bool conditional_orders_enabled_ = false;

    bool started_ = false;
};
