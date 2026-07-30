#pragma once

#include "XtStrategyBase.h"
#include "XtStrategyInitConfig.h"
#include "api/IExecutionGateway.h"
#include "api/IStrategyContext.h"
#include "api/IntentResult.h"
#include "xt_metrics/XtStrategyMetricsAggregator.h"
#include "xt_strategy_engine/IStrategyExecSink.h"
#include "xt_strategy_engine/XtStopOrderBook.h"
#include "xt_strategy_engine/view/XtRuntimeView.h"

// XtRunnerSignals 由 runner 持有；context 只保存 non-owning pointer，避免 header 循环依赖。
struct XtRunnerSignals;

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>

class XtRollEngine;

namespace xt_strategy_engine {
class XtDeclaredSymbolRegistry;
class IStrategyTelemetrySink;
}

/// @brief 策略回调内访问 runtime 的统一 context。
///
/// Context 是策略调用下单、行情/账户查询、参数读取、指标上报和 userdata 的入口。
///
/// @note 引擎在同一 event-loop 线程内串行调用策略回调与 context API；默认路径无锁。
///       HFT 实盘若由指标采集线程读取 metrics，需在 EventLoop 启动前调用
///       `enable_concurrent_metrics()`。
/// @note `roll_engine` 非空时，buy_fp/sell_fp 与持仓查询按逻辑标的映射到物理合约
///       （如 ag@main → ag2206）。
class XtStrategyContext final : public IStrategyContext,
                                public xt_strategy_engine::IStrategyExecSink {
public:
    /**
     * @brief 策略参数读写端口（parameter read/write port）。
     *
     * @warning 实现方必须保证 `get` / `set` / `version` 线程安全；热更新可能来自
     *          控制面或配置监控线程，策略回调在 event-loop 线程读取参数。
     * @note `version()` 必须单调递增，用于 context 检测参数变更。
     */
    class IParamPort {
    public:
        virtual ~IParamPort() = default;

        virtual bool get(const std::string& key, std::string& out) const = 0;

        virtual bool set(const std::string& key, const std::string& value) = 0;

        virtual uint64_t version() const = 0;
    };

    /// @brief 构造配置聚合体（construction config aggregate）。
    ///
    /// 调用方必须提供 strategy；HFT/CTA 直连或 intent 模式还必须提供 runtime_view。
    /// SEL selector 可以只提供 on_weights_fn；此时 read/query API 返回安全默认值
    /// （nullptr/0）并 warn-once，submit/cancel fail-closed——任何路径不做 null deref。
    struct Config {

        using IntentFn = std::function<xt_runtime::IntentResult(
            const char* strategy_id, const char* code,
            double prev_virtual_pos, double new_virtual_pos,
            int64_t price_fp, XtTimeInForce tif, const char* tag)>;
        using WeightsFn = std::function<void(
            const std::string& strategy_id,
            const std::unordered_map<std::string, double>& weights)>;

        XtStrategyBase* strategy = nullptr;
        xt_runtime::IExecutionGateway* exec_gateway = nullptr;
        IntentFn on_intent_fn;
        WeightsFn on_weights_fn;
        xt_strategy_engine::XtRuntimeView* runtime_view = nullptr;
        const XtStrategyInitConfig* init_config = nullptr;
        IParamPort* param_port = nullptr;
        const ::XtRollEngine* roll_engine = nullptr;

        xt_strategy_engine::IStrategyTelemetrySink* telemetry_sink = nullptr;

        std::string strategy_id;

        bool route_by_sid = false;

        xt_strategy_engine::XtDeclaredSymbolRegistry* declared_registry = nullptr;

        bool project_inflight = false;

        bool conditional_orders_enabled = false;
    };

    explicit XtStrategyContext(const Config& cfg);
    ~XtStrategyContext() override = default;

    void enable_concurrent_metrics() noexcept;

    uint64_t buy_fp(const char* code,
                    int64_t price_fp,
                    int64_t qty_fp,
                    XtTimeInForce tif,
                    const char* tag,
                    XtOffset offset) override;

    uint64_t sell_fp(const char* code,
                     int64_t price_fp,
                     int64_t qty_fp,
                     XtTimeInForce tif,
                     const char* tag,
                     XtOffset offset) override;

    void cancel(uint64_t order_id) override;

    uint64_t submit_stop_fp(const char* code, bool is_buy, int64_t trigger_price_fp,
                            int64_t qty_fp, XtOffset offset, const char* tag) override;
    uint64_t submit_trailing_stop_fp(const char* code, bool is_buy, int64_t distance_fp,
                                     int64_t qty_fp, XtOffset offset, const char* tag,
                                     int64_t seed_price_fp) override;
    bool cancel_stop(uint64_t stop_id) override;

    const XtTick* get_last_tick(const char* code) override;

    int64_t get_last_price_fp(const char* code) override;

    size_t get_ticks(const char* code, XtTick* buf, size_t max) override;

    size_t get_bars(const char* code, const char* period, XtBar* buf, size_t max) override;

    size_t get_open_orders(const char* code, XtOrderExecUpdate* buf, size_t max) override;

    int64_t get_position_fp(const char* code) override;

    int64_t get_position_avg_price_fp(const char* code) override;

    int64_t get_position_unrealized_fp(const char* code) override;

    int64_t projected_inflight_fp(const char* code) override;

    size_t get_position_detail(const char* code,
                               xt_runtime::XtStraPositionDetail* buf,
                               size_t max) override;

    int64_t get_cash_fp() override;

    int64_t get_equity_fp() override;

    const xt_runtime::XtStraContractInfo* get_contract_info(const char* code) override;

    uint32_t get_date() override;

    uint32_t get_trading_date() override;

    uint32_t get_time_ms() override;

    std::string get_param_str(const char* key, const char* def) override;

    int64_t get_param_int(const char* key, int64_t def) override;

    double get_param_double(const char* key, double def) override;

    bool get_param_bool(const char* key, bool def) override;

    void sub_tick(const char* code) override;

    void sub_bar(const char* code, const char* period, size_t max) override;

    void declare_position_interest(const char* code) override;

    void log_debug(const char* msg) override;
    void log_info(const char* msg) override;
    void log_warn(const char* msg) override;

    void report_metric(const char* name, double val) override;

    void set_next_order_reason(const char* reason) override;

    void save_userdata(const char* key, const char* val) override;

    const char* load_userdata(const char* key, const char* def) override;

    void on_init();

    void dispatch_tick(const XtTick& tick);

    void dispatch_bar(const char* code, const char* period, const XtBar& bar);

    void dispatch_bar_open(const char* code, const char* period, int64_t open_px_fp);

    void on_order_queue(const XtOrderQueue& q);

    void on_order_detail(const XtOrderDetail& d);

    void on_transaction(const XtTransaction& t);

    void on_session_begin(uint32_t trading_date);

    void on_session_end(uint32_t trading_date);

    void on_roll(const char* old_code, const char* new_code, uint32_t trading_date,
                 int64_t old_last_price_fp = 0);

    void on_channel_ready(const char* channel);

    void on_channel_lost(const char* channel, int reason);

    void on_params_updated(uint64_t version);

    void check_conditional_orders(const XtTick& tick);

    void notify_order_update_for_stops(const XtOrderExecUpdate& upd);

    std::size_t clear_stops_for_code(const char* code);

    [[nodiscard]] bool has_armed_stops() const noexcept {
        return conditional_orders_enabled_ && !stop_book_.empty();
    }

    [[nodiscard]] bool conditional_orders_enabled() const noexcept {
        return conditional_orders_enabled_;
    }

    void on_order(const XtOrderExecUpdate& upd) override;

    void on_fill(const XtFillEvent& fill) override;

    void on_account(const XtAccountUpdate& upd) override;

    void set_initial_virtual_pos(const char* code, int64_t qty_fp);

    std::unordered_map<std::string, int64_t> snapshot_virtual_pos() const {
        return virtual_pos_fp_;
    }

    std::unordered_map<std::string, std::string> snapshot_userdata() const {
        return userdata_;
    }

    void restore_userdata(std::unordered_map<std::string, std::string> kv);

    bool has_tick_subscription() const noexcept { return tick_subscribed_; }

    const std::string& strategy_id() const noexcept { return strategy_id_; }

    uint32_t resolved_sid_id() const noexcept { return resolved_sid_id_; }

    void ensure_sid_resolved() { (void)resolved_sid_raw_(); }

    ct::metrics::XtStrategyMetricsSnapshot drain_strategy_metrics();

    void set_signals(const XtRunnerSignals* s) noexcept { signals_ = s; }

    bool can_submit() const override;

    [[nodiscard]] xt_runtime::XtRejectCode last_reject_code() const noexcept override {
        return last_reject_code_;
    }

    void set_control_enabled(bool enabled) noexcept {
        control_enabled_.store(enabled, std::memory_order_release);
    }

    void set_target_weights(
        const std::unordered_map<std::string, double>& weights) override;

    [[nodiscard]] int64_t current_fire_time_ms() const noexcept override {
        return current_fire_time_ms_;
    }
    void set_current_fire_time_ms(int64_t fire_ms) noexcept {
        current_fire_time_ms_ = fire_ms;
    }

private:
    static XtStrategyBase& require_strategy(XtStrategyBase* strategy, const char* where);
    static bool is_valid_fp_input(const char* code, int64_t price_fp, int64_t qty_fp);

    uint64_t submit_impl_(const char* code, bool is_buy, int64_t price_fp, int64_t qty_fp,
                          XtTimeInForce tif, const char* tag, XtOffset offset);

    xt_runtime::XtRejectCode guard_submit(const char* code, int64_t price_fp, int64_t qty_fp, bool is_buy) const;
    bool is_subscribed(const char* code) const;

    const std::string* find_param_value(const char* key) const;

    uint32_t resolved_sid_raw_();
    static bool parse_bool_value(const std::string& value, bool def);

    const char* resolve_physical_(const char* code) const;

    bool view_or_warn_();

    void fire_stop_(uint64_t stop_id);

    int64_t stop_closeable_qty_(const xt_strategy_engine::StopSlot& s);

    void stop_emit_event_(const char* event, uint64_t delta = 1) const;
    void stop_emit_armed_gauge_() const;

    XtStrategyBase* strategy_ = nullptr;
    xt_runtime::IExecutionGateway* exec_gateway_ = nullptr;
    Config::IntentFn on_intent_fn_;
    Config::WeightsFn on_weights_fn_;
    xt_strategy_engine::XtRuntimeView* runtime_view_ = nullptr;
    const XtStrategyInitConfig* init_config_ = nullptr;
    IParamPort* param_port_ = nullptr;
    const ::XtRollEngine* roll_engine_ = nullptr;
    xt_strategy_engine::IStrategyTelemetrySink* telemetry_sink_ = nullptr;

    bool route_by_sid_ = false;

    xt_strategy_engine::XtDeclaredSymbolRegistry* declared_registry_ = nullptr;

    const XtRunnerSignals* signals_ = nullptr;

    std::string strategy_id_;
    bool project_inflight_ = false;
    bool init_phase_ = false;
    uint64_t last_param_version_ = 0;
    uint32_t resolved_sid_id_ = 0;
    bool sid_resolved_ = false;

    std::atomic<bool> control_enabled_{true};

    xt_runtime::XtRejectCode last_reject_code_ = xt_runtime::XtRejectCode::NoError;

    std::string next_order_reason_;

    int64_t current_fire_time_ms_ = 0;

    bool cancel_unroutable_warned_ = false;

    bool null_view_warned_ = false;

    std::unordered_set<std::string> subscriptions_;
    bool tick_subscribed_ = false;
    mutable std::unordered_map<std::string, bool> sub_cache_;

    std::unordered_map<std::string, int64_t> virtual_pos_fp_;
    uint64_t next_virtual_id_ = 0;

    bool conditional_orders_enabled_ = false;
    xt_strategy_engine::XtStopOrderBook stop_book_;
    std::vector<uint64_t> stop_fired_scratch_;

    std::unordered_map<std::string, std::string> userdata_;
    std::unordered_map<std::string, xt_runtime::XtStraContractInfo> contract_cache_;
    std::string load_userdata_buf_;
    mutable std::string param_value_buf_;
    mutable std::string resolve_phys_buf_;
    ct::metrics::XtStrategyMetricsAggregator metrics_;
};
