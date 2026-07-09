#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "api/IHftGateway.h"
#include "api/IHftEngineObserver.h"
#include "api/IKillSwitchSink.h"
#include "api/IOrderIdTranslator.h"
#include "api/ITradeGateway.h"
#include "xt_core/market/XtL2MarketStructs.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_strategy_engine/view/XtRuntimeView.h"
#include "xt_strategy_engine/IStrategyExecSink.h"
#include "xt_strategy_engine/XtStrategyContext.h"
#include "xt_strategy_engine/XtStrategyInitConfig.h"
#include "xt_strategy_engine/XtDeclaredSymbolRegistry.h"
#include "xt_core/market/XtRollEngine.h"
#include "xt_core/session/XtSessionFilter.h"
#include "xt_core/session/XtTradingDateInfer.h"
#include "HftLifecycleTelemetry.h"
#include "XtHftEngineStats.h"

struct AliveInflightOrder;

namespace xt_account {
class AccountStateStore;
}

class XtCommodityCatalog;
class XtStrategyBase;

namespace xt_backtest::hft {
struct XtHftBacktestExtConfig;
}

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

    /// @brief 配置 HFT engine 的 gateway、runtime view 和 domain 依赖。
    ///
    /// gateway 与 view 为必填；其余指针为空时，engine 跳过依赖该组件的路径。
    /// 执行回调 sink 不走 Config；启动期统一经 `set_exec_sinks()` 注入。
    struct Config {
        xt_runtime::IHftGateway* gateway = nullptr;
        xt_strategy_engine::XtRuntimeView* view = nullptr;
        xt_core::Config session = {};
        XtRollEngine::Config roll = {};
        std::string strategy_id;
        size_t history_ticks = 2048;
        size_t history_bars = 512;
        XtStrategyContext::IParamPort* param_port = nullptr;
        const XtStrategyInitConfig* init_config = nullptr;

        std::function<void(uint64_t  )> on_params_extra;

        xt_account::AccountStateStore* account_store = nullptr;
        xt_runtime::order::XtOrderRuntimeBook* order_book = nullptr;
        const XtCommodityCatalog* catalog = nullptr;
        xt_runtime::IOrderIdTranslator* order_id_translator = nullptr;

        xt_strategy_engine::XtDeclaredSymbolRegistry* declared_registry = nullptr;

        xt_runtime::IKillSwitchSink* kill_switch_sink = nullptr;

        bool strict_unmatched_routing = false;

        bool conditional_orders_enabled = false;
    };

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

    explicit XtHftEngine(XtStrategyBase* stra, const Config& cfg);

    ~XtHftEngine();

    XtHftEngine(const XtHftEngine&) = delete;
    XtHftEngine& operator=(const XtHftEngine&) = delete;
    XtHftEngine(XtHftEngine&&) = delete;
    XtHftEngine& operator=(XtHftEngine&&) = delete;

    void on_init();

    void configure_backtest_ext(const xt_backtest::hft::XtHftBacktestExtConfig& ext);

    void set_exec_sinks(xt_strategy_engine::IStrategyExecSink* fallback,
                        std::vector<xt_strategy_engine::IStrategyExecSink*> sinks);

    void add_context(std::unique_ptr<XtStrategyContext> ctx);

    void reserve_contexts(std::size_t n) { contexts_.reserve(n); }

    std::size_t context_count() const noexcept { return contexts_.size(); }

    XtStrategyContext*       context(std::size_t idx) noexcept;

    const XtStrategyContext* context(std::size_t idx) const noexcept;

    void on_tick(const XtTick& tick);

    void on_order_queue(const XtOrderQueue& oq);

    void on_order_detail(const XtOrderDetail& od);

    void on_transaction(const XtTransaction& tr);

    void on_channel_ready(const char* channel);

    void on_channel_lost(const char* channel, int reason);

    void on_params_updated(uint64_t param_version);

    bool on_trade(const XtTradeRecord& trade, ITradeGateway::TdState td_state,
                  int64_t* out_realized_fp = nullptr,
                  std::string* out_effective_sid = nullptr);

    bool on_order_update(const XtOrderExecUpdate& upd);

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

    uint64_t lost_terminal_reconcile_count() const noexcept {
        return lost_terminal_reconcile_total_.load(std::memory_order_relaxed);
    }

    uint32_t open_order_count() const noexcept;

    void add_observer(xt_runtime::IHftEngineObserver* observer) {
        if (observer) observers_.push_back(observer);
    }

    void init_lifecycle_telemetry(LifecycleTelemetryInit init);

    void set_fill_realized_callback(FillRealizedCallback cb) noexcept {
        fill_realized_cb_ = std::move(cb);
    }

    void set_cash_reanchor_probe(CashReanchorProbe cb) noexcept {
        cash_reanchor_probe_ = std::move(cb);
    }

    void set_mark_tick_callback(MarkTickCallback cb) noexcept { mark_tick_cb_ = std::move(cb); }

    void set_trade_excursion_callback(TradeExcursionCallback cb) noexcept {
        trade_excursion_cb_ = std::move(cb);
    }

private:
    struct FillPositionBefore {
        int64_t qty_fp = 0;
        int64_t avg_price_fp = 0;
    };

    void init_(const Config& cfg);

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

    bool dispatch_fill_to_strategy_(const XtTradeRecord& tr);

    uint32_t resolve_fill_sid_id_(const std::string& strategy_id) const;

    bool handle_unmatched_fill_(const XtTradeRecord& tr);

    XtFillEvent build_strategy_fill_event_(const XtTradeRecord& tr) const;

    std::string prefetch_terminal_order_code_(const XtOrderExecUpdate& upd,
                                              bool is_terminal);

    void apply_order_update_state_(const XtOrderExecUpdate& applied_upd,
                                   bool is_terminal);

    XtOrderExecUpdate build_strategy_order_update_(
        const XtOrderExecUpdate& applied_upd) const;

    bool dispatch_order_update_to_strategy_(const XtOrderExecUpdate& strategy_upd);

    bool handle_unmatched_order_update_(const XtOrderExecUpdate& strategy_upd);

    void finish_terminal_order_update_(const XtOrderExecUpdate& applied_upd,
                                       const std::string& stp_code);

    void emit_resume_for_recovery_(
        const std::vector<ITradeGateway::PositionSnapshot>& positions);

    std::size_t find_ctx_idx_by_sid_id_(uint32_t sid_id) const noexcept;

    xt_strategy_engine::IStrategyExecSink* select_routing_sink_(uint32_t sid_id,
                                          bool& skip_unmatched) const noexcept;

    bool recover_missing_fill_sid_(XtTradeRecord& tr);

    void record_recovered_fill_sid_(const XtTradeRecord& tr, const std::string& recovered_sid);

    std::string effective_sid_(const std::string& sid, const std::string& code);

    void build_code_to_sid_();

    XtStrategyBase* stra_ = nullptr;
    xt_runtime::IHftGateway* gateway_ = nullptr;
    xt_strategy_engine::XtRuntimeView* runtime_view_ = nullptr;

    std::vector<std::unique_ptr<XtStrategyContext>> contexts_;
    XtStrategyContext* active_ctx_ = nullptr;
    std::vector<xt_strategy_engine::IStrategyExecSink*> exec_sinks_;

    xt_core::Config session_;
    XtRollEngine roll_;
    XtTradingDateInfer td_infer_;
    std::function<bool(uint32_t)> calendar_filter_;

    std::string last_primary_symbol_;
    uint32_t last_roll_check_date_ = 0;

    XtTick last_any_core_tick_{};
    bool has_last_any_core_tick_ = false;
    uint32_t trading_date_ = 0;
    uint32_t last_action_date_ = 0;

    xt_account::AccountStateStore* account_store_ = nullptr;
    xt_runtime::order::XtOrderRuntimeBook* order_book_ = nullptr;
    const XtCommodityCatalog* catalog_ = nullptr;
    xt_strategy_engine::IStrategyExecSink* exec_sink_ = nullptr;
    xt_strategy_engine::XtDeclaredSymbolRegistry* declared_registry_ = nullptr;
    xt_runtime::IKillSwitchSink* kill_switch_sink_ = nullptr;
    xt_runtime::IOrderIdTranslator* order_id_translator_ = nullptr;

    xt_engine::hft::HftLifecycleTelemetry lifecycle_;

    FillRealizedCallback fill_realized_cb_;
    CashReanchorProbe cash_reanchor_probe_;
    bool cash_anchored_ = false;
    MarkTickCallback mark_tick_cb_;
    TradeExcursionCallback trade_excursion_cb_;
    std::vector<xt_runtime::IHftEngineObserver*> observers_;

    std::function<void(uint64_t)> on_params_extra_;

    std::atomic<uint64_t> dropped_fill_empty_sid_count_{0};
    std::atomic<uint64_t> dropped_fill_unknown_sid_count_{0};
    std::atomic<uint64_t> dropped_order_update_empty_sid_count_{0};
    std::atomic<uint64_t> dropped_order_update_unknown_sid_count_{0};
    std::atomic<uint64_t> exec_dispatch_count_{0};
    std::atomic<uint64_t> unmatched_sid_recovered_count_{0};
    std::atomic<uint64_t> orphan_reap_cancel_total_{0};
    std::atomic<uint64_t> lost_terminal_reconcile_total_{0};
    int64_t last_reverse_sync_steady_ms_ = 0;

    std::unordered_map<std::string, std::string> code_to_sid_;
    bool code_to_sid_built_ = false;

    bool strict_unmatched_routing_ = false;

    bool conditional_orders_enabled_ = false;
};
