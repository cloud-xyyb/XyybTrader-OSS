#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <prometheus/metric_family.h>

namespace prometheus {
class Registry;
class Exposer;
class Histogram;
class Counter;
}

namespace ct::metrics {

/// @brief runner 进程里的 Prometheus metrics 总入口
///
/// runner 启动时用 XtMetricRegistry 开一个 /metrics HTTP endpoint，Prometheus 来 scrape；
/// 运行中各模块把已经发生的事实打进去，例如下单数、成交数、拒单数、行情 tick 数、latency histogram 等
class XtMetricRegistry {
public:

    static XtMetricRegistry& instance();

    XtMetricRegistry(const XtMetricRegistry&) = delete;
    XtMetricRegistry& operator=(const XtMetricRegistry&) = delete;

    bool start_exposer(uint16_t port = 9101,
                       std::string_view bind_addr = "127.0.0.1");

    void stop_exposer();

    bool is_running() const noexcept;

    std::vector<prometheus::MetricFamily> collect_metric_families_for_test() const;

    void on_order_submit(std::string_view strategy,
                         std::string_view book,
                         std::string_view venue,
                         std::string_view symbol,
                         std::string_view side);

    void on_md_tick(std::string_view venue,
                    std::string_view symbol);

    void on_order_rtn(std::string_view strategy,
                      std::string_view book,
                      std::string_view venue,
                      std::string_view symbol,
                      std::string_view status);

    void on_trade(std::string_view strategy,
                  std::string_view book,
                  std::string_view venue,
                  std::string_view symbol,
                  std::string_view side,
                  int64_t qty_fp,
                  int64_t price_fp,
                  int64_t fee_fp,
                  int64_t realized_pnl_fp,
                  bool is_maker);

    void on_normalized_trade(std::string_view strategy,
                             std::string_view book,
                             std::string_view venue,
                             std::string_view symbol,
                             std::string_view side,
                             std::string_view route_status,
                             bool strategy_accounted);

    void set_strategy_ledger_today(std::string_view strategy,
                                   std::string_view book,
                                   std::string_view venue,
                                   std::string_view symbol,
                                   int64_t today_realized_pnl_fp,
                                   int64_t today_fees_fp,
                                   uint64_t today_fills);

    void on_risk_reject(std::string_view strategy,
                        std::string_view book,
                        std::string_view venue,
                        std::string_view symbol,
                        std::string_view reason);

    void inc_risk_rejects_total(std::string_view venue, uint64_t delta);

    void on_order_reject(std::string_view venue,
                         std::string_view err_code);

    void observe_md_event_to_recv_us(std::string_view channel,
                                      uint64_t latency_us);

    void observe_md_internal_queue_us(std::string_view channel,
                                       uint64_t queue_us);

    void set_md_clock_offset_us(std::string_view channel, double offset_us);

    void set_md_clock_offset_confidence(std::string_view channel, bool locked);

    void set_md_clock_offset_window_samples(std::string_view channel,
                                              uint64_t samples);

    void set_md_clock_offset_window_min_age_seconds(std::string_view channel,
                                                      double age_s);

    void inc_md_clock_step(std::string_view channel);

    void inc_md_negative_latency(std::string_view channel, uint64_t count);

    void inc_md_ws_reconnect(std::string_view channel);

    void inc_md_aggtrade_gap(std::string_view channel);

    void inc_md_aggtrade_backfill(std::string_view result, uint64_t n);

    void on_param_update(std::string_view strategy,
                         std::string_view actor,
                         std::string_view key);

    void on_kill_switch(std::string_view venue,
                        std::string_view actor,
                        std::string_view state,
                        std::string_view reason);

    void set_pnl_net(std::string_view venue,
                     std::string_view account,
                     double pnl_net);

    void set_position_qty_fp(std::string_view strategy,
                             std::string_view book,
                             std::string_view venue,
                             std::string_view symbol,
                             int64_t qty_fp);

    void set_strategy_position_qty_fp(std::string_view venue,
                                      std::string_view account,
                                      std::string_view strategy,
                                      std::string_view book,
                                      std::string_view symbol,
                                      int64_t qty_fp);

    void set_strategy_position_avg_price_fp(std::string_view venue,
                                            std::string_view account,
                                            std::string_view strategy,
                                            std::string_view book,
                                            std::string_view symbol,
                                            int64_t v);

    void set_strategy_position_unrealized_pnl_fp(std::string_view venue,
                                                 std::string_view account,
                                                 std::string_view strategy,
                                                 std::string_view book,
                                                 std::string_view symbol,
                                                 int64_t v);

    void set_kill_switch_active(std::string_view venue, bool active);

    void set_td_ready(std::string_view venue, bool ready);

    void set_trading_ready(std::string_view venue, bool ready);

    void set_td_recovery_wedged(std::string_view venue, bool wedged);

    void inc_md_ticks_total(std::string_view venue, uint64_t delta);

    void set_strategy_enabled(std::string_view strategy,
                              std::string_view venue,
                              bool enabled);

    void set_strategy_custom_metric(std::string_view strategy,
                                    std::string_view name,
                                    double value);

    void set_today_pnl_net_v2(std::string_view strategy,
                              std::string_view book,
                              std::string_view venue,
                              std::string_view symbol,
                              double pnl_net);

    void set_today_fills_total_v2(std::string_view strategy,
                                  std::string_view venue,
                                  std::string_view symbol,
                                  uint64_t fills_count);

    void inc_strategy_baseline_new_total(std::string_view strategy,
                                         std::string_view venue);

    void inc_baseline_rotated_total(std::string_view venue);

    void inc_strategy_baseline_load_failed_total(std::string_view venue,
                                                 std::string_view reason);

    struct AccountTruthSample {
        double wallet_balance              = 0.0;
        double equity                      = 0.0;
        double position_value              = 0.0;
        double unrealized_pnl              = 0.0;
        double fees_today                  = 0.0;
        double external_cashflow_today     = 0.0;
        double trading_day_id              = 0.0;
        double available_balance           = 0.0;
        double maint_margin                = 0.0;
        double margin_ratio                = 0.0;
    };

    void set_account_truth_snapshot(std::string_view venue,
                                    std::string_view account,
                                    const AccountTruthSample& sample,
                                    std::string_view source = "runtime");

    void set_venue_equity_raw(std::string_view venue,
                              std::string_view account,
                              double value);

    void observe_cash_reanchor(std::string_view venue,
                               std::string_view account,
                               double divergence);

    void set_state_divergence(std::string_view field,
                              std::string_view symbol,
                              double value);

    void inc_state_divergence_emit_heartbeat();

    void set_reconcile_snapshot_age_ms(double age_ms);

    void on_fill_maker_taker(std::string_view strategy,
                             std::string_view book,
                             std::string_view venue,
                             std::string_view symbol,
                             bool is_maker);

    struct RiskLimitsSample {
        double max_order_qty           = 0.0;
        double max_pos_qty             = 0.0;
        double max_drawdown_pct        = 0.0;
        double max_position_value_usd  = 0.0;
        double max_orders_per_min      = 0.0;
        bool   allow_short             = true;
    };

    void set_strategy_limits(std::string_view strategy,
                             std::string_view book,
                             std::string_view venue,
                             const RiskLimitsSample& limits);

    void set_strategy_auto_liquidating(std::string_view strategy, bool latched);

    void set_strategy_auto_liquidation_trigger_pnl_fp(std::string_view strategy,
                                                      int64_t trigger_pnl_fp);

    void inc_strategy_auto_liquidation_breach_total(std::string_view strategy);

    void inc_strategy_auto_liquidation_submit_total(std::string_view strategy);

    void inc_strategy_auto_liquidation_deferred_total(std::string_view strategy);

    void inc_strategy_auto_liquidation_1to1_violation_total(std::string_view strategy);

    void inc_stop_order_total(std::string_view event, uint64_t delta = 1);

    void set_stop_order_armed(std::string_view strategy, uint32_t count);

    void observe_control_rpc_us(std::string_view action,
                                 std::string_view result,
                                 uint64_t latency_us);

    void inc_control_rpc_total(std::string_view action,
                                std::string_view actor,
                                std::string_view result);

    void inc_gateway_disconnect_total(std::string_view venue,
                                       std::string_view role,
                                       std::string_view reason);

    void inc_close_quota_disagreement_total(std::string_view venue);

    void inc_recovery_rebuild_alive_total(std::string_view venue,
                                          std::string_view outcome);

    void inc_recovery_rebuild_alive_skipped_foreign_total(std::string_view venue);

    void inc_recovery_orphan_reaped_total(std::string_view venue, uint64_t delta);

    void inc_td_position_snapshot_drop_total(std::string_view venue, uint64_t delta);

    void inc_td_control_event_drop_total(std::string_view venue, uint64_t delta);

    void inc_td_other_event_drop_total(std::string_view venue, uint64_t delta);

    void inc_hft_worker_guard_fail_total(std::string_view venue, std::string_view stage,
                                         uint64_t delta);

    void set_open_orders(std::string_view venue, uint32_t count);

    void set_shm_md_attach_failing(std::string_view ring, uint32_t failing);

    void set_shm_md_attach_fail_count(std::string_view ring, uint64_t count);

    void set_stp_registered_symbols(std::string_view venue, uint32_t count);

    void inc_runtime_book_drop_total(std::string_view reason);

    void inc_unmatched_drop_total(std::string_view kind, std::string_view reason,
                                  uint64_t delta);

    void inc_engine_recovery_total(std::string_view kind, uint64_t delta);

    void inc_runtime_market_drop_total(std::string_view reason);

    void inc_latency_meta_miss_total(std::string_view venue);

    void inc_latency_eligible_total(std::string_view stage, std::string_view strategy,
                                    std::string_view book, std::string_view symbol);

    void inc_latency_tap_dropped_total(std::string_view reason);

    void inc_broker_frozen_stale_total(std::string_view venue);

    void inc_position_aggregator_jump_total(std::string_view sid,
                                            std::string_view code);

    void set_position_avg_price_fp(std::string_view venue, std::string_view symbol,
                                   int64_t v);

    void set_position_mark_price_fp(std::string_view venue, std::string_view symbol,
                                    int64_t v);

    void set_position_unrealized_pnl_fp(std::string_view venue, std::string_view symbol,
                                        int64_t v);

    void set_position_margin_fp(std::string_view venue, std::string_view symbol,
                                int64_t v);

    void set_account_store_mark_price_fp(std::string_view venue,
                                          std::string_view symbol,
                                          int64_t v);

    void set_broker_position_qty_fp(std::string_view venue,
                                    std::string_view account,
                                    std::string_view symbol,
                                    int64_t v);

    void set_broker_position_avg_price_fp(std::string_view venue,
                                          std::string_view account,
                                          std::string_view symbol,
                                          int64_t v);

    void set_broker_position_mark_price_fp(std::string_view venue,
                                           std::string_view account,
                                           std::string_view symbol,
                                           int64_t v);

    void set_broker_position_unrealized_pnl_fp(std::string_view venue,
                                               std::string_view account,
                                               std::string_view symbol,
                                               int64_t v);

    void set_broker_position_margin_fp(std::string_view venue,
                                       std::string_view account,
                                       std::string_view symbol,
                                       int64_t v);

    /// @brief runner-owned 1Hz KPI snapshot。
    struct RuntimeKpiSnapshot {
        double md_gap_ms_p99 = 0.0;
        bool has_md_gap_ms_p99 = true;
        double md_gap_ms_peak = 0.0;
        bool has_md_gap_ms_peak = true;
        double md_age_ms = 0.0;
        bool has_md_age_ms = false;
        double event_drop = 0.0;
        double queue_depth_p99 = 0.0;
        double risk_block_count = 0.0;
    };

    void emit_contract_meta();

    void set_runtime_kpi_snapshot(std::string_view strategy,
                                  std::string_view book,
                                  std::string_view venue,
                                  std::string_view symbol,
                                  const RuntimeKpiSnapshot& sample);

    [[nodiscard]] prometheus::Histogram* get_submit_to_ack_us_histogram(
        std::string_view strategy, std::string_view book,
        std::string_view venue, std::string_view symbol);
    [[nodiscard]] prometheus::Histogram* get_submit_to_fill_us_histogram(
        std::string_view strategy, std::string_view book,
        std::string_view venue, std::string_view symbol);
    [[nodiscard]] prometheus::Histogram* get_submit_to_ack_paired_us_histogram(
        std::string_view strategy, std::string_view book,
        std::string_view venue, std::string_view symbol);
    [[nodiscard]] prometheus::Histogram* get_strategy_us_histogram(
        std::string_view strategy, std::string_view book,
        std::string_view venue, std::string_view symbol);
    [[nodiscard]] prometheus::Histogram* get_local_submit_us_histogram(
        std::string_view strategy, std::string_view book,
        std::string_view venue, std::string_view symbol);

    [[nodiscard]] prometheus::Counter* get_latency_eligible_counter(
        std::string_view stage, std::string_view strategy,
        std::string_view book, std::string_view symbol);

private:
    XtMetricRegistry();
    ~XtMetricRegistry();

    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}
