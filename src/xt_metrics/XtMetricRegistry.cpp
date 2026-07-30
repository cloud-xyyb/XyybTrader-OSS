#include "xt_metrics/XtMetricRegistry.h"

#include <atomic>
#include <cmath>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include "xt_metrics/_generated/zero_init.h"

namespace ct::metrics {

namespace {

const std::vector<double>& bucket_family_vector(std::string_view family_name) {
    static std::vector<std::vector<double>> caches;
    static std::once_flag init_flag;
    std::call_once(init_flag, [] {
        for (const auto& fam : xt::metrics::contract::kBucketFamilies) {
            std::vector<double> v;
            v.reserve(fam.bucket_count);
            for (std::size_t i = 0; i < fam.bucket_count; ++i) {
                v.push_back(fam.buckets[i]);
            }
            caches.push_back(std::move(v));
        }
    });
    for (std::size_t i = 0; i < xt::metrics::contract::kBucketFamilies.size(); ++i) {
        if (xt::metrics::contract::kBucketFamilies[i].name == family_name) {
            return caches[i];
        }
    }

    static const std::vector<double> kEmpty;
    return kEmpty;
}

const std::vector<double>& order_us_buckets() {
    return bucket_family_vector("order_us");
}

const std::vector<double>& rpc_us_buckets() {
    return bucket_family_vector("rpc_us");
}

prometheus::Labels make_labels_5(std::string_view k1, std::string_view v1,
                                  std::string_view k2, std::string_view v2,
                                  std::string_view k3, std::string_view v3,
                                  std::string_view k4, std::string_view v4,
                                  std::string_view k5, std::string_view v5) {
    return {
        {std::string(k1), std::string(v1)},
        {std::string(k2), std::string(v2)},
        {std::string(k3), std::string(v3)},
        {std::string(k4), std::string(v4)},
        {std::string(k5), std::string(v5)},
    };
}

prometheus::Labels make_labels_4(std::string_view k1, std::string_view v1,
                                  std::string_view k2, std::string_view v2,
                                  std::string_view k3, std::string_view v3,
                                  std::string_view k4, std::string_view v4) {
    return {
        {std::string(k1), std::string(v1)},
        {std::string(k2), std::string(v2)},
        {std::string(k3), std::string(v3)},
        {std::string(k4), std::string(v4)},
    };
}

prometheus::Labels make_labels_2(std::string_view k1, std::string_view v1,
                                  std::string_view k2, std::string_view v2) {
    return {
        {std::string(k1), std::string(v1)},
        {std::string(k2), std::string(v2)},
    };
}

prometheus::Labels make_labels_3(std::string_view k1, std::string_view v1,
                                  std::string_view k2, std::string_view v2,
                                  std::string_view k3, std::string_view v3) {
    return {
        {std::string(k1), std::string(v1)},
        {std::string(k2), std::string(v2)},
        {std::string(k3), std::string(v3)},
    };
}

}

struct XtMetricRegistry::Impl {
    std::shared_ptr<prometheus::Registry> registry;
    std::unique_ptr<prometheus::Exposer> exposer;
    std::mutex mu;
    std::atomic<bool> running{false};

    prometheus::Family<prometheus::Counter>* fam_submits_total          = nullptr;
    prometheus::Family<prometheus::Counter>* fam_md_ticks_total          = nullptr;
    prometheus::Family<prometheus::Counter>* fam_order_rtns_total       = nullptr;
    prometheus::Family<prometheus::Counter>* fam_fills_total            = nullptr;
    prometheus::Family<prometheus::Counter>* fam_today_fills_total      = nullptr;
    prometheus::Family<prometheus::Counter>* fam_risk_rejects_total     = nullptr;
    prometheus::Family<prometheus::Counter>* fam_order_rejects_total    = nullptr;
    prometheus::Family<prometheus::Counter>* fam_param_updates_total    = nullptr;
    prometheus::Family<prometheus::Counter>* fam_kill_switch_total      = nullptr;
    prometheus::Family<prometheus::Counter>* fam_realized_pnl_sum       = nullptr;
    prometheus::Family<prometheus::Counter>* fam_realized_loss_sum      = nullptr;
    prometheus::Family<prometheus::Counter>* fam_fee_sum                = nullptr;
    prometheus::Family<prometheus::Counter>* fam_fee_rebate_sum         = nullptr;
    prometheus::Family<prometheus::Counter>* fam_trade_route_status_total = nullptr;
    prometheus::Family<prometheus::Counter>* fam_strategy_ledger_fills_total = nullptr;

    prometheus::Family<prometheus::Histogram>* fam_submit_to_ack_us     = nullptr;
    prometheus::Family<prometheus::Histogram>* fam_submit_to_fill_us    = nullptr;
    prometheus::Family<prometheus::Histogram>* fam_control_rpc_us       = nullptr;

    prometheus::Family<prometheus::Histogram>* fam_strategy_us          = nullptr;
    prometheus::Family<prometheus::Histogram>* fam_local_submit_us      = nullptr;

    prometheus::Family<prometheus::Histogram>* fam_md_event_to_recv_us    = nullptr;
    prometheus::Family<prometheus::Histogram>* fam_md_internal_queue_us   = nullptr;
    prometheus::Family<prometheus::Gauge>*     fam_md_clock_offset_us     = nullptr;
    prometheus::Family<prometheus::Gauge>*     fam_md_clock_offset_confidence = nullptr;
    prometheus::Family<prometheus::Gauge>*     fam_md_clock_offset_window_samples = nullptr;
    prometheus::Family<prometheus::Gauge>*     fam_md_clock_offset_window_min_age_seconds = nullptr;
    prometheus::Family<prometheus::Counter>*   fam_md_clock_step_total    = nullptr;
    prometheus::Family<prometheus::Counter>*   fam_md_negative_latency_total = nullptr;
    prometheus::Family<prometheus::Counter>*   fam_md_ws_reconnect_total  = nullptr;
    prometheus::Family<prometheus::Counter>*   fam_md_aggtrade_gap_total  = nullptr;
    prometheus::Family<prometheus::Counter>*   fam_md_aggtrade_backfill_total = nullptr;

    prometheus::Family<prometheus::Gauge>*   fam_pnl_net                = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_position_qty_fp        = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_kill_switch_active     = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_td_ready               = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_td_recovery_wedged     = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_trading_ready          = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_strategy_enabled       = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_strategy_custom_metric = nullptr;

    prometheus::Family<prometheus::Gauge>*   fam_today_pnl_net_v2       = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_today_fills_total_v2   = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_strategy_ledger_today_fills_total = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_strategy_ledger_today_realized_pnl_fp = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_strategy_ledger_today_fees_fp = nullptr;
    prometheus::Family<prometheus::Counter>* fam_strategy_baseline_new_total = nullptr;
    prometheus::Family<prometheus::Counter>* fam_baseline_rotated_total      = nullptr;
    prometheus::Family<prometheus::Counter>* fam_strategy_baseline_load_failed_total = nullptr;

    prometheus::Family<prometheus::Gauge>*   fam_strategy_auto_liquidating = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_strategy_auto_liquidation_trigger_pnl_fp = nullptr;
    prometheus::Family<prometheus::Counter>* fam_strategy_auto_liquidation_breach_total = nullptr;
    prometheus::Family<prometheus::Counter>* fam_strategy_auto_liquidation_submit_total = nullptr;
    prometheus::Family<prometheus::Counter>* fam_strategy_auto_liquidation_deferred_total = nullptr;
    prometheus::Family<prometheus::Counter>* fam_strategy_auto_liquidation_1to1_violation_total = nullptr;

    prometheus::Family<prometheus::Counter>* fam_stop_order_total = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_stop_order_armed = nullptr;

    prometheus::Family<prometheus::Gauge>* fam_account_truth_wallet_balance          = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_account_truth_equity                  = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_account_equity_derived               = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_venue_equity_raw                       = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_venue_cash_reanchor_divergence       = nullptr;
    prometheus::Family<prometheus::Counter>* fam_venue_cash_reanchor_correction_total = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_account_truth_position_value          = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_account_truth_unrealized_pnl          = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_account_truth_fees_today              = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_account_truth_external_cashflow_today = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_account_truth_trading_day_id          = nullptr;

    prometheus::Family<prometheus::Gauge>*   fam_state_divergence                = nullptr;
    prometheus::Family<prometheus::Counter>* fam_state_divergence_emit_heartbeat = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_reconcile_snapshot_age_ms       = nullptr;

    prometheus::Family<prometheus::Counter>* fam_maker_fills_total                    = nullptr;
    prometheus::Family<prometheus::Counter>* fam_taker_fills_total                    = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_strategy_limit_max_order_qty         = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_strategy_limit_max_pos_qty           = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_strategy_limit_max_drawdown_pct      = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_strategy_limit_max_position_value_usd = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_strategy_limit_max_orders_per_min    = nullptr;
    prometheus::Family<prometheus::Gauge>*   fam_strategy_limit_allow_short           = nullptr;

    prometheus::Family<prometheus::Counter>* fam_control_rpc_total      = nullptr;

    prometheus::Family<prometheus::Counter>* fam_gateway_disconnect_total = nullptr;

    prometheus::Family<prometheus::Counter>* fam_close_quota_disagreement_total = nullptr;

    prometheus::Family<prometheus::Counter>* fam_recovery_rebuild_alive_total = nullptr;
    prometheus::Family<prometheus::Counter>* fam_recovery_rebuild_alive_skipped_foreign_total = nullptr;
    prometheus::Family<prometheus::Counter>* fam_recovery_orphan_reaped_total = nullptr;
    prometheus::Family<prometheus::Counter>* fam_td_position_snapshot_drop_total = nullptr;
    prometheus::Family<prometheus::Counter>* fam_td_control_event_drop_total = nullptr;
    prometheus::Family<prometheus::Counter>* fam_td_other_event_drop_total = nullptr;
    prometheus::Family<prometheus::Counter>* fam_hft_worker_guard_fail_total = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_open_orders = nullptr;

    prometheus::Family<prometheus::Gauge>* fam_shm_md_attach_failing = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_shm_md_attach_fail_count = nullptr;

    prometheus::Family<prometheus::Gauge>* fam_stp_registered_symbols = nullptr;

    prometheus::Family<prometheus::Counter>* fam_runtime_book_drop_total = nullptr;

    prometheus::Family<prometheus::Counter>* fam_dropped_total = nullptr;
    prometheus::Family<prometheus::Counter>* fam_engine_recovery_total = nullptr;

    prometheus::Family<prometheus::Counter>* fam_runtime_market_drop_total = nullptr;

    prometheus::Family<prometheus::Counter>* fam_latency_meta_miss_total = nullptr;
    prometheus::Family<prometheus::Counter>* fam_latency_eligible_total = nullptr;
    prometheus::Family<prometheus::Counter>* fam_latency_tap_dropped_total = nullptr;

    prometheus::Family<prometheus::Counter>* fam_broker_frozen_stale_total = nullptr;

    prometheus::Family<prometheus::Counter>* fam_position_aggregator_jump_total = nullptr;

    prometheus::Family<prometheus::Gauge>* fam_position_avg_price_fp       = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_position_mark_price_fp      = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_position_unrealized_pnl_fp  = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_position_margin_fp          = nullptr;

    prometheus::Family<prometheus::Gauge>* fam_account_store_mark_price_fp = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_broker_position_qty_fp       = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_broker_position_avg_price_fp = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_broker_position_mark_price_fp = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_broker_position_unrealized_pnl_fp = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_broker_position_margin_fp    = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_strategy_position_qty_fp     = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_strategy_position_avg_price_fp = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_strategy_position_unrealized_pnl_fp = nullptr;

    prometheus::Family<prometheus::Gauge>* fam_account_available_balance   = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_account_maint_margin        = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_account_margin_ratio        = nullptr;

    prometheus::Family<prometheus::Histogram>* fam_submit_to_ack_paired_us = nullptr;

    prometheus::Family<prometheus::Gauge>* fam_runner_contract_version_info = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_runner_bucket_schema_info    = nullptr;

    prometheus::Family<prometheus::Gauge>* fam_md_gap_ms_p99 = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_md_gap_ms_peak = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_md_age_ms = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_event_drop = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_queue_depth = nullptr;
    prometheus::Family<prometheus::Gauge>* fam_risk_block_count = nullptr;

    Impl() {
        registry = std::make_shared<prometheus::Registry>();

        fam_submits_total = &prometheus::BuildCounter()
            .Name("xt_submits_total")
            .Help("Total order submits emitted by runner")
            .Register(*registry);

        fam_md_ticks_total = &prometheus::BuildCounter()
            .Name("xt_md_ticks_total")
            .Help("Market data tick events received per (venue, symbol). "
                  "Cross-engine emit via XtLiveRecorder::emit_md_tick (CTP/binance/XTP). "
                  "Panel uses rate(xt_md_ticks_total[1m]) for true TICK rate.")
            .Register(*registry);
        fam_order_rtns_total = &prometheus::BuildCounter()
            .Name("xt_order_rtns_total")
            .Help("Total order status returns received by runner")
            .Register(*registry);
        fam_fills_total = &prometheus::BuildCounter()
            .Name("xt_fills_total")
            .Help("Lifetime fill count")
            .Register(*registry);
        fam_today_fills_total = &prometheus::BuildCounter()
            .Name("xt_today_fills_total")
            .Help("Today's fill count (resets at trading_day boundary)")
            .Register(*registry);
        fam_risk_rejects_total = &prometheus::BuildCounter()
            .Name("xt_risk_rejects_total")
            .Help("Total pre-trade risk rejects")
            .Register(*registry);
        fam_order_rejects_total = &prometheus::BuildCounter()
            .Name("xt_order_rejects_total")
            .Help("Total venue/exchange business rejects by err_code (e.g. -2022 ReduceOnly, -4164 notional<min); distinct from xt_risk_rejects_total (local pre-trade risk)")
            .Register(*registry);
        fam_param_updates_total = &prometheus::BuildCounter()
            .Name("xt_param_updates_total")
            .Help("Total hot-param updates applied via control plane")
            .Register(*registry);
        fam_kill_switch_total = &prometheus::BuildCounter()
            .Name("xt_kill_switch_total")
            .Help("Total kill-switch state transitions (tripped + resumed)")
            .Register(*registry);
        fam_realized_pnl_sum = &prometheus::BuildCounter()
            .Name("xt_realized_pnl_fp_sum")
            .Help("Sum of positive realized PnL ×1e6 (close fills only); losses use xt_realized_loss_fp_sum")
            .Register(*registry);
        fam_realized_loss_sum = &prometheus::BuildCounter()
            .Name("xt_realized_loss_fp_sum")
            .Help("Sum of absolute realized losses ×1e6 (close fills only)")
            .Register(*registry);
        fam_fee_sum = &prometheus::BuildCounter()
            .Name("xt_fee_fp_sum")
            .Help("Sum of positive fees ×1e6; rebates use xt_fee_rebate_fp_sum")
            .Register(*registry);
        fam_fee_rebate_sum = &prometheus::BuildCounter()
            .Name("xt_fee_rebate_fp_sum")
            .Help("Sum of absolute maker rebates ×1e6")
            .Register(*registry);
        fam_trade_route_status_total = &prometheus::BuildCounter()
            .Name("xt_trade_route_status_total")
            .Help("Normalized trade route triage count (labels: venue, symbol, route_status)")
            .Register(*registry);
        fam_strategy_ledger_fills_total = &prometheus::BuildCounter()
            .Name("xt_strategy_ledger_fills_total")
            .Help("Routed normalized ledger fills (labels: strategy, book, venue, symbol, side)")
            .Register(*registry);

        fam_submit_to_ack_us = &prometheus::BuildHistogram()
            .Name("xt_submit_to_ack_us")
            .Help("Submit→Ack latency in microseconds")
            .Register(*registry);
        fam_submit_to_fill_us = &prometheus::BuildHistogram()
            .Name("xt_submit_to_fill_us")
            .Help("Submit→Fill latency in microseconds")
            .Register(*registry);

        fam_strategy_us = &prometheus::BuildHistogram()
            .Name("xt_strategy_us")
            .Help("Strategy callback duration (tick→submit decision)")
            .Register(*registry);
        fam_local_submit_us = &prometheus::BuildHistogram()
            .Name("xt_local_submit_us")
            .Help("Local submit path (strategy decision → userspace write complete)")
            .Register(*registry);

        fam_md_event_to_recv_us = &prometheus::BuildHistogram()
            .Name("xt_md_event_to_recv_us")
            .Help("Binance WS event_time→local recv single-trip latency (μs), clock-offset corrected")
            .Register(*registry);
        fam_md_internal_queue_us = &prometheus::BuildHistogram()
            .Name("xt_md_internal_queue_us")
            .Help("Binance internal queue (matching engine T → server send E), per message")
            .Register(*registry);
        fam_md_clock_offset_us = &prometheus::BuildGauge()
            .Name("xt_md_clock_offset_us")
            .Help("XtClockSkewEstimator current offset estimate (μs); NaN during STEPPED state")
            .Register(*registry);
        fam_md_clock_offset_confidence = &prometheus::BuildGauge()
            .Name("xt_md_clock_offset_confidence")
            .Help("XtClockSkewEstimator confidence (0 = WARMING/STEPPED, 1 = LOCKED)")
            .Register(*registry);
        fam_md_clock_offset_window_samples = &prometheus::BuildGauge()
            .Name("xt_md_clock_offset_window_samples")
            .Help("Sample count in the current 1h rolling window")
            .Register(*registry);
        fam_md_clock_offset_window_min_age_seconds = &prometheus::BuildGauge()
            .Name("xt_md_clock_offset_window_min_age_seconds")
            .Help("Age (seconds) of oldest sample in the trimmed-max window")
            .Register(*registry);
        fam_md_clock_step_total = &prometheus::BuildCounter()
            .Name("xt_md_clock_step_total")
            .Help("NTP clock-step events detected (wall vs monotonic divergence > 10ms)")
            .Register(*registry);
        fam_md_negative_latency_total = &prometheus::BuildCounter()
            .Name("xt_md_negative_latency_total")
            .Help("Samples that came out negative after clock-offset correction (clipped to 0)")
            .Register(*registry);
        fam_md_ws_reconnect_total = &prometheus::BuildCounter()
            .Name("xt_md_ws_reconnect_total")
            .Help("Binance WS reconnect events; each reconnect resets the clock skew window")
            .Register(*registry);
        fam_md_aggtrade_gap_total = &prometheus::BuildCounter()
            .Name("xt_md_aggtrade_gap_total")
            .Help("aggTrade aggregate-id discontinuity (missed-trade) events; >0 mostly from reconnect gap")
            .Register(*registry);
        fam_md_aggtrade_backfill_total = &prometheus::BuildCounter()
            .Name("xt_md_aggtrade_backfill_total")
            .Help("aggTrade gap backfill outcome (labels: result=replayed|failed|truncated)")
            .Register(*registry);

        fam_pnl_net = &prometheus::BuildGauge()
            .Name("xt_pnl_net")
            .Help("Cumulative session-pnl_net (= last_equity - session_baseline_equity); "
                  "today/window deltas derived via prometheus recording rules, not in-runner snapshot")
            .Register(*registry);
        fam_position_qty_fp = &prometheus::BuildGauge()
            .Name("xt_position_qty_fp")
            .Help("Current position quantity ×1e6 (per strategy/book/symbol)")
            .Register(*registry);
        fam_kill_switch_active = &prometheus::BuildGauge()
            .Name("xt_kill_switch_active")
            .Help("Kill switch state (1 = active/halt, 0 = inactive)")
            .Register(*registry);
        fam_td_ready = &prometheus::BuildGauge()
            .Name("xt_td_ready")
            .Help("Trade gateway readiness from XtRunnerSignals (1 = ready, 0 = not ready/recovering)")
            .Register(*registry);
        fam_td_recovery_wedged = &prometheus::BuildGauge()
            .Name("xt_td_recovery_wedged")
            .Help("TD recovery re-arm exceeded alert rounds while front connected and not ready (1 = wedged, operator restart needed; re-arm keeps retrying)")
            .Register(*registry);
        fam_trading_ready = &prometheus::BuildGauge()
            .Name("xt_trading_ready")
            .Help("Trading readiness gate from XtRunnerSignals (1 = MD+TD ready, 0 = gated)")
            .Register(*registry);

        fam_today_pnl_net_v2 = &prometheus::BuildGauge()
            .Name("xt_today_pnl_net_v2")
            .Help("Today's realized PNL net per (strategy, book, venue, symbol), "
                  "baseline-aware (XtStrategyPnlBaseline manages per-day reset + restart-safe).")
            .Register(*registry);
        fam_today_fills_total_v2 = &prometheus::BuildGauge()
            .Name("xt_today_fills_total_v2")
            .Help("Today's fills count per (strategy, venue, symbol), baseline-aware "
                  "(gauge not counter because cross-day rotation resets to 0).")
            .Register(*registry);
        fam_strategy_ledger_today_fills_total = &prometheus::BuildGauge()
            .Name("xt_strategy_ledger_today_fills_total")
            .Help("Today's routed fill count derived from normalized trade ledger")
            .Register(*registry);
        fam_strategy_ledger_today_realized_pnl_fp = &prometheus::BuildGauge()
            .Name("xt_strategy_ledger_today_realized_pnl_fp")
            .Help("Today's realized PnL ×1e6 derived from normalized trade ledger")
            .Register(*registry);
        fam_strategy_ledger_today_fees_fp = &prometheus::BuildGauge()
            .Name("xt_strategy_ledger_today_fees_fp")
            .Help("Today's fees ×1e6 derived from normalized trade ledger")
            .Register(*registry);
        fam_strategy_baseline_new_total = &prometheus::BuildCounter()
            .Name("xt_strategy_baseline_new_total")
            .Help("Strategy 首次出现 (baseline 没该 key) 触发 — alert 预防 hot-add mid-day 假爆赚 PNL.")
            .Register(*registry);
        fam_baseline_rotated_total = &prometheus::BuildCounter()
            .Name("xt_baseline_rotated_total")
            .Help("Per-venue day rotation 触发计数 — ops 验证 daily reset 准时.")
            .Register(*registry);
        fam_strategy_baseline_load_failed_total = &prometheus::BuildCounter()
            .Name("xt_strategy_baseline_load_failed_total")
            .Help("Strategy PNL baseline load failed; baseline-derived today PNL is fail-closed.")
            .Register(*registry);
        fam_strategy_auto_liquidating = &prometheus::BuildGauge()
            .Name("xt_strategy_auto_liquidating")
            .Help("Per-sid auto-liquidation latch (1 = armed/active force-flat, 0 = not latched).")
            .Register(*registry);
        fam_strategy_auto_liquidation_trigger_pnl_fp = &prometheus::BuildGauge()
            .Name("xt_strategy_auto_liquidation_trigger_pnl_fp")
            .Help("Per-sid trigger PnL x1e6 (today_realized + unrealized) vs liquidation threshold.")
            .Register(*registry);
        fam_strategy_auto_liquidation_breach_total = &prometheus::BuildCounter()
            .Name("xt_strategy_auto_liquidation_breach_total")
            .Help("Per-sid auto-liquidation rising-edge arm events (breach detected -> latch set).")
            .Register(*registry);
        fam_strategy_auto_liquidation_submit_total = &prometheus::BuildCounter()
            .Name("xt_strategy_auto_liquidation_submit_total")
            .Help("Per-sid auto-liquidation force-exit IOC close submissions (ForceClosed outcome).")
            .Register(*registry);
        fam_strategy_auto_liquidation_deferred_total = &prometheus::BuildCounter()
            .Name("xt_strategy_auto_liquidation_deferred_total")
            .Help("Per-sid auto-liquidation actuation deferred (NoTd/NoMarkPrice/SubmitFailed/CancelledOnly); latch held.")
            .Register(*registry);
        fam_strategy_auto_liquidation_1to1_violation_total = &prometheus::BuildCounter()
            .Name("xt_strategy_auto_liquidation_1to1_violation_total")
            .Help("Per-sid auto-liquidation skipped: declared_symbols != 1 (warn-once per sid).")
            .Register(*registry);

        fam_stop_order_total = &prometheus::BuildCounter()
            .Name("xt_stop_order_total")
            .Help("Engine-hosted stop-order lifecycle events (labels: event="
                  "fired|refired|consumed|clamped|dropped_flat|dropped_roll|"
                  "dropped_exhausted|arm_rejected_full). clamped > 0 表示保护性平仓量"
                  "因侧向可平/volscale 缩水;dropped_exhausted > 0 表示 venue fire 达"
                  "上限放弃,仓位可能无保护。")
            .Register(*registry);
        fam_stop_order_armed = &prometheus::BuildGauge()
            .Name("xt_stop_order_armed")
            .Help("Per-strategy live stop-order slot count (Armed + Fired); "
                  "day-order 语义, 进程重启即失效。")
            .Register(*registry);

        fam_account_truth_wallet_balance = &prometheus::BuildGauge()
            .Name("xt_account_truth_wallet_balance")
            .Help("Account wallet balance: account_store cash (reconciled to venue wallet, "
                  "NOT a direct venue read). runtime-emitted from XtRunner cold path 1Hz.")
            .Register(*registry);
        fam_account_truth_equity = &prometheus::BuildGauge()
            .Name("xt_account_truth_equity")
            .Help("Account equity = account_store cash + unrealized_pnl (account_store-derived, "
                  "NOT real venue/broker equity; runtime-emitted 1Hz).")
            .Register(*registry);

        fam_account_equity_derived = &prometheus::BuildGauge()
            .Name("xt_account_equity_derived")
            .Help("Account equity = account_store cash + unrealized_pnl (account_store-derived, "
                  "NOT real venue/broker equity; runtime-emitted 1Hz). Rename target of "
                  "xt_account_truth_equity (dual-emitted during deprecation window).")
            .Register(*registry);
        fam_venue_equity_raw = &prometheus::BuildGauge()
            .Name("xt_venue_equity_raw")
            .Help("Real venue/broker equity = total_margin_balance read from venue REST/TD "
                  "(binance /fapi/v2/account totalMarginBalance; ctp OnRspQryTradingAccount "
                  "Balance). runtime-emitted 1Hz, only when the venue provides it "
                  "(binance + ctp); XTP/CTA/SEL leave 0 (no emit). "
                  "Cross-check vs xt_account_truth_equity (account_store-derived).")
            .Register(*registry);
        fam_venue_cash_reanchor_divergence = &prometheus::BuildGauge()
            .Name("xt_venue_cash_reanchor_divergence")
            .Help("Signed broker-minus-derived cash divergence sampled BEFORE each periodic "
                  "set_cash re-anchor (post-apply comparison is always 0). CTP HFT emit, "
                  "cadence = account_refresh_ms; first anchor after start not sampled.")
            .Register(*registry);
        fam_venue_cash_reanchor_correction_total = &prometheus::BuildCounter()
            .Name("xt_venue_cash_reanchor_correction_total")
            .Help("Cumulative sum of abs(set_cash correction) applied by periodic broker "
                  "account re-anchor. With open positions each sample structurally contains "
                  "delta PositionProfit (expected growth); real drift = bias in the signed "
                  "divergence gauge / growth while flat.")
            .Register(*registry);
        fam_account_truth_position_value = &prometheus::BuildGauge()
            .Name("xt_account_truth_position_value")
            .Help("Account total position notional value (runtime-emitted 1Hz).")
            .Register(*registry);
        fam_account_truth_unrealized_pnl = &prometheus::BuildGauge()
            .Name("xt_account_truth_unrealized_pnl")
            .Help("Account unrealized PnL (runtime-emitted 1Hz, broker snapshot value).")
            .Register(*registry);
        fam_account_truth_fees_today = &prometheus::BuildGauge()
            .Name("xt_account_truth_fees_today")
            .Help("Account fees accumulated today (per-trading-day, runtime-emitted 1Hz).")
            .Register(*registry);
        fam_account_truth_external_cashflow_today = &prometheus::BuildGauge()
            .Name("xt_account_truth_external_cashflow_today")
            .Help("Account external cashflow today (deposit/withdraw/transfer; "
                  "polled async from broker REST every 60s, runtime emits cached atomic).")
            .Register(*registry);
        fam_account_truth_trading_day_id = &prometheus::BuildGauge()
            .Name("xt_account_truth_trading_day_id")
            .Help("Current trading day ID (yyyymmdd integer in double, runtime-emitted 1Hz).")
            .Register(*registry);

        fam_state_divergence = &prometheus::BuildGauge()
            .Name("xt_state_divergence")
            .Help("Account-state divergence |account_store.field − broker_snapshot.field| "
                  "(state divergence telemetry, 1Hz; venue/host via external_labels)")
            .Register(*registry);
        fam_state_divergence_emit_heartbeat = &prometheus::BuildCounter()
            .Name("xt_state_divergence_emit_heartbeat")
            .Help("Increments on every divergence-telemetry emit tick; "
                  "rate≈1/s steady state, rate==0 ⇒ safety net itself is dead")
            .Register(*registry);
        fam_reconcile_snapshot_age_ms = &prometheus::BuildGauge()
            .Name("xt_reconcile_snapshot_age_ms")
            .Help("Age (ms) of the last successful exchange position snapshot in the "
                  "reconcile loop; 0 on each successful fetch, grows across consecutive "
                  "fetch failures (loop fail-closed, silently skipping compare cycles)")
            .Register(*registry);

        fam_maker_fills_total = &prometheus::BuildCounter()
            .Name("xt_maker_fills_total")
            .Help("Total maker-side fills (passive — order resting on book before match). "
                  "Cross-engine HFT/CTA/SEL emit. Recording rule derives ratio.")
            .Register(*registry);
        fam_taker_fills_total = &prometheus::BuildCounter()
            .Name("xt_taker_fills_total")
            .Help("Total taker-side fills (aggressive — order crossed book). "
                  "Cross-engine HFT/CTA/SEL emit.")
            .Register(*registry);

        fam_strategy_limit_max_order_qty = &prometheus::BuildGauge()
            .Name("xt_strategy_limit_max_order_qty")
            .Help("Strategy max order qty risk limit (gauge, 1Hz emit from XtLiveParamDomain).")
            .Register(*registry);
        fam_strategy_limit_max_pos_qty = &prometheus::BuildGauge()
            .Name("xt_strategy_limit_max_pos_qty")
            .Help("Strategy max position qty risk limit (gauge, 1Hz emit).")
            .Register(*registry);
        fam_strategy_limit_max_drawdown_pct = &prometheus::BuildGauge()
            .Name("xt_strategy_limit_max_drawdown_pct")
            .Help("Strategy max drawdown pct risk limit (gauge, 1Hz emit).")
            .Register(*registry);
        fam_strategy_limit_max_position_value_usd = &prometheus::BuildGauge()
            .Name("xt_strategy_limit_max_position_value_usd")
            .Help("Strategy max position value USD risk limit (gauge, 1Hz emit).")
            .Register(*registry);
        fam_strategy_limit_max_orders_per_min = &prometheus::BuildGauge()
            .Name("xt_strategy_limit_max_orders_per_min")
            .Help("Strategy max orders per minute risk limit (gauge, 1Hz emit).")
            .Register(*registry);
        fam_strategy_limit_allow_short = &prometheus::BuildGauge()
            .Name("xt_strategy_limit_allow_short")
            .Help("Strategy allow-short permission (gauge 1=allowed / 0=blocked, 1Hz emit).")
            .Register(*registry);

        fam_strategy_enabled = &prometheus::BuildGauge()
            .Name("xt_strategy_enabled")
            .Help("Strategy enable gate from XtLiveParamDomain (1=enabled, 0=paused).")
            .Register(*registry);
        fam_strategy_custom_metric = &prometheus::BuildGauge()
            .Name("xt_strategy_custom_metric")
            .Help("Strategy-reported custom gauge via report_metric (name label = metric key); "
                  "HFT main context only, 1Hz control-loop emit.")
            .Register(*registry);

        fam_control_rpc_us = &prometheus::BuildHistogram()
            .Name("xt_control_rpc_us")
            .Help("Control plane gRPC RPC latency in microseconds (server-side, "
                  "from PRE_RECV_INITIAL_METADATA to PRE_SEND_STATUS)")
            .Register(*registry);
        fam_control_rpc_total = &prometheus::BuildCounter()
            .Name("xt_control_rpc_total")
            .Help("Total control plane gRPC RPC calls (labels: action, actor, result)")
            .Register(*registry);

        fam_gateway_disconnect_total = &prometheus::BuildCounter()
            .Name("xt_gateway_disconnect_total")
            .Help("Total gateway disconnect events (labels: venue, role, reason). "
                  "reason is finite enum: front_disconnect|ws_close|uds_close|stale_timeout|auth_fail|unknown")
            .Register(*registry);

        fam_close_quota_disagreement_total = &prometheus::BuildCounter()
            .Name("xt_risk_close_quota_disagreement_total")
            .Help("Risk close path 双源 disagreement events: |local_outstanding - "
                  "broker_frozen| > 1 lot fp. CTP-specific (binance 无 frozen 推送, "
                  "default 0 让 max() 退化为 local single-source).")
            .Register(*registry);

        fam_recovery_rebuild_alive_total = &prometheus::BuildCounter()
            .Name("xt_recovery_rebuild_alive_total")
            .Help("Recovery reverse-sync alive in-flight rebuild calls "
                  "(labels: venue, outcome=success|failure)")
            .Register(*registry);
        fam_recovery_rebuild_alive_skipped_foreign_total = &prometheus::BuildCounter()
            .Name("xt_recovery_rebuild_alive_skipped_foreign_total")
            .Help("Recovery reverse-sync skipped foreign / unparseable "
                  "client_order_id (labels: venue)")
            .Register(*registry);
        fam_recovery_orphan_reaped_total = &prometheus::BuildCounter()
            .Name("xt_recovery_orphan_reaped_total")
            .Help("Orphan reaper cancels issued for recovered-unowned in-flight "
                  "orders (cross-restart leftovers, labels: venue)")
            .Register(*registry);

        fam_td_position_snapshot_drop_total = &prometheus::BuildCounter()
            .Name("xt_td_position_snapshot_drop_total")
            .Help("HFT position snapshot events dropped at the td event queue "
                  "(labels: venue). PositionSnapshot 已加入 must_not_drop, 软水位丢弃消除; "
                  "本 counter > 0 表示物理队列满丢首帧 → td_position_ready fail-closed "
                  "wedge 全部 submit (loud CRITICAL log 同点触发, 需运维关注)。")
            .Register(*registry);

        fam_td_control_event_drop_total = &prometheus::BuildCounter()
            .Name("xt_td_control_event_drop_total")
            .Help("HFT control events (ChannelReady/ChannelLost) dropped at the td "
                  "event queue (labels: venue). 两者均 must_not_drop, 增长 = 物理队列满丢弃; "
                  "丢 ChannelReady 无自愈 (worker td_channel_up_ 直到下次断连边沿不 re-arm "
                  "→ td_position_ready fail-closed 拒全部 submit), 需运维关注。")
            .Register(*registry);

        fam_td_other_event_drop_total = &prometheus::BuildCounter()
            .Name("xt_td_other_event_drop_total")
            .Help("HFT td events outside the position/control buckets (exec/account "
                  "plus non-edge control events, labels: venue). 非 must_not_drop 残余桶: "
                  "软水位 shedding (风暴背压 by design) / 物理满 / loop 未运行拒绝皆计入; "
                  "critical trade/order 事件绕软水位仅物理满落此桶 (同点有 CRITICAL log + "
                  "critical 聚合)。背压诊断可见性, 非 readiness-wedge 信号。")
            .Register(*registry);

        fam_hft_worker_guard_fail_total = &prometheus::BuildCounter()
            .Name("xt_hft_worker_guard_fail_total")
            .Help("HFT event-loop worker guard failures (labels: venue, stage=body|"
                  "normalize|record). body = backstop 接住的逃逸异常 (随后 fail-stop, "
                  "由 liveness/up 告警接手); normalize = 成交归一化抛异常, raw fill "
                  "passthrough 落账; record = telemetry 记录抛异常被 swallow。")
            .Register(*registry);

        fam_open_orders = &prometheus::BuildGauge()
            .Name("xt_open_orders")
            .Help("Local order-book alive in-flight orders (incl. recovered "
                  "orphans); proxy for venue per-symbol open-order cap "
                  "(labels: venue)")
            .Register(*registry);

        fam_shm_md_attach_failing = &prometheus::BuildGauge()
            .Name("xt_shm_md_attach_failing")
            .Help("shm MD reader attach state (1=most recent attach attempt failed "
                  "and not yet recovered, 0=attached/never-failed; labels: ring). "
                  "Only present when md.transport=shm (multibook reader).")
            .Register(*registry);
        fam_shm_md_attach_fail_count = &prometheus::BuildGauge()
            .Name("xt_shm_md_attach_fail_count")
            .Help("Cumulative shm MD reader attach failures since process start "
                  "(monotonic gauge; labels: ring). Diagnostic companion to "
                  "xt_shm_md_attach_failing.")
            .Register(*registry);

        fam_stp_registered_symbols = &prometheus::BuildGauge()
            .Name("xt_stp_registered_symbols")
            .Help("Number of symbols armed in the STP self-trade guard "
                  "(labels: venue). 0 = STP not pre-warmed = self-trade guard "
                  "bypassed. Set once at runner assembly after pre_warm_risk.")
            .Register(*registry);

        fam_runtime_book_drop_total = &prometheus::BuildCounter()
            .Name("xt_runtime_book_drop_total")
            .Help("XtOrderRuntimeBook drop events (labels: reason="
                  "slot_pool_full|orphan_update|orphan_trade|orphan_reap|duplicate_submit|"
                  "snapshot_publish_skipped|cross_thread_direct_read|tracked_codes_truncated|"
                  "close_outstanding_drift). slot_pool_full 应恒为 0 "
                  "(kMaxInflightOrders=512 远大于正常 in-flight);orphan_update / orphan_trade > 0 "
                  "表示 reverse 灌路径漏单 / foreign order / 本进程 submit race;"
                  "orphan_reap > 0 表示 cross-restart recovered-unowned 孤儿单 venue 已无被兜底清;"
                  "duplicate_submit > 0 表示同 id 撞上仍存活的 slot,注册被 fail-closed 丢弃,"
                  "需查上游发号链;"
                  "snapshot_publish_skipped > 0 表示 snapshot pool 耗尽,reader 读 stale;"
                  "cross_thread_direct_read > 0 表示非 worker 线程误用直读接口被拦截;"
                  "tracked_codes_truncated > 0 表示 tracked code 集合触顶被截断;"
                  "close_outstanding_drift > 0 表示 close 在途额度与 slot 派生量分歧被自愈。")
            .Register(*registry);

        fam_dropped_total = &prometheus::BuildCounter()
            .Name("xt_dropped_total")
            .Help("Multi-strategy unmatched exec-event drops (labels: kind=fill|order_rtn, "
                  "reason=empty_sid|unknown_sid). 归因丢失的 money-path 事件丢弃，期望恒 0；"
                  "偶发 = cancel/fill race（snapshot/reconcile 兜底），持续增长 = 归因链损坏，"
                  "超 unmatched_budget 预算会 trip kill_switch。")
            .Register(*registry);

        fam_engine_recovery_total = &prometheus::BuildCounter()
            .Name("xt_engine_recovery_total")
            .Help("Engine self-heal recovery actions (labels: kind=sid_fallback|"
                  "lost_terminal). sid_fallback = 缺 sid fill 经 code->sid 唯一归因恢复；"
                  "lost_terminal = venue 已终态但本地 book 仍 open 的订单被合成终态修复。")
            .Register(*registry);

        fam_runtime_market_drop_total = &prometheus::BuildCounter()
            .Name("xt_runtime_market_drop_total")
            .Help("XtRuntimeMarketCache drop events (labels: reason="
                  "cross_thread_direct_read). cross_thread_direct_read > 0 表示"
                  "非 worker 线程误用行情直读接口（last_tick/ticks/bars/last_price_fp）"
                  "被 fail-closed 拦截,off-worker 应走 snapshot_last_tick。期望恒 0。")
            .Register(*registry);

        fam_latency_meta_miss_total = &prometheus::BuildCounter()
            .Name("xt_latency_meta_miss_total")
            .Help("Latency tracker emit skipped: order strategy/code meta unresolved "
                  "even via order-book fallback (labels: venue; once per order).")
            .Register(*registry);

        fam_latency_eligible_total = &prometheus::BuildCounter()
            .Name("xt_latency_eligible_total")
            .Help("Latency sample eligibility (denominator for latency coverage). "
                  "Labels: stage=ack, strategy, book, symbol. Counted once per "
                  "successful gateway submit with known strategy/symbol.")
            .Register(*registry);

        fam_latency_tap_dropped_total = &prometheus::BuildCounter()
            .Name("xt_latency_tap_dropped_total")
            .Help("Latency tracker tap dropped events (labels: reason="
                  "ring_full|overflow_full|meta_evicted|orders_evicted|trace_oversize|"
                  "slot_exhausted|label_cache_full). rate > 0 表示延迟采样传输面"
                  "背压 / 容器驱逐，样本覆盖率有损。")
            .Register(*registry);

        fam_broker_frozen_stale_total = &prometheus::BuildCounter()
            .Name("xt_risk_broker_frozen_stale_total")
            .Help("CTP broker_frozen 推送停滞 > 30s,risk path 降级 local-only "
                  "(labels: venue). binance/XTP 不推 broker_frozen,不触发。")
            .Register(*registry);

        fam_position_aggregator_jump_total = &prometheus::BuildCounter()
            .Name("xt_position_aggregator_jump_total")
            .Help("PositionAggregator on_update saw an implausibly-large jump "
                  "(delta > max_observed_order_qty_fp_*2). Labels: sid, code. "
                  "Preventive observability against multi-emitter "
                  "REPLACE-semantics regressions. Non-fatal in release; debug "
                  "build asserts.")
            .Register(*registry);

        fam_position_avg_price_fp = &prometheus::BuildGauge()
            .Name("xt_position_avg_price_fp")
            .Help("Per-position avg entry price ×1e6 (venue truth, binance /fapi/v2/account)")
            .Register(*registry);
        fam_position_mark_price_fp = &prometheus::BuildGauge()
            .Name("xt_position_mark_price_fp")
            .Help("Per-position mark price ×1e6 (venue truth, binance /fapi/v2/positionRisk)")
            .Register(*registry);

        fam_account_store_mark_price_fp = &prometheus::BuildGauge()
            .Name("xt_account_store_mark_price_fp")
            .Help("Account_store mark_cache_ internal mark per tracked code ×1e6 fp "
                  "(diagnostic; if 0 with live venue mark = MarketTick path broken)")
            .Register(*registry);
        fam_position_unrealized_pnl_fp = &prometheus::BuildGauge()
            .Name("xt_position_unrealized_pnl_fp")
            .Help("Per-position unrealized PnL ×1e6 (venue truth, signed)")
            .Register(*registry);
        fam_position_margin_fp = &prometheus::BuildGauge()
            .Name("xt_position_margin_fp")
            .Help("Per-position initial margin ×1e6 (positionInitialMargin, binance)")
            .Register(*registry);
        fam_broker_position_qty_fp = &prometheus::BuildGauge()
            .Name("xt_broker_position_qty_fp")
            .Help("Broker/account position quantity ×1e6 (labels: venue, account, symbol)")
            .Register(*registry);
        fam_broker_position_avg_price_fp = &prometheus::BuildGauge()
            .Name("xt_broker_position_avg_price_fp")
            .Help("Broker/account position average price ×1e6")
            .Register(*registry);
        fam_broker_position_mark_price_fp = &prometheus::BuildGauge()
            .Name("xt_broker_position_mark_price_fp")
            .Help("Broker/account position mark price ×1e6")
            .Register(*registry);
        fam_broker_position_unrealized_pnl_fp = &prometheus::BuildGauge()
            .Name("xt_broker_position_unrealized_pnl_fp")
            .Help("Broker/account position unrealized PnL ×1e6")
            .Register(*registry);
        fam_broker_position_margin_fp = &prometheus::BuildGauge()
            .Name("xt_broker_position_margin_fp")
            .Help("Broker/account position margin ×1e6")
            .Register(*registry);
        fam_strategy_position_qty_fp = &prometheus::BuildGauge()
            .Name("xt_strategy_position_qty_fp")
            .Help("Strategy/book position quantity ×1e6")
            .Register(*registry);
        fam_strategy_position_avg_price_fp = &prometheus::BuildGauge()
            .Name("xt_strategy_position_avg_price_fp")
            .Help("Strategy/book position average price ×1e6")
            .Register(*registry);
        fam_strategy_position_unrealized_pnl_fp = &prometheus::BuildGauge()
            .Name("xt_strategy_position_unrealized_pnl_fp")
            .Help("Strategy/book position unrealized PnL ×1e6")
            .Register(*registry);

        fam_account_available_balance = &prometheus::BuildGauge()
            .Name("xt_account_available_balance")
            .Help("Account available balance (availableBalance binance / balance CTP), "
                  "runtime-emitted 1Hz. labels: {venue, account, source}")
            .Register(*registry);
        fam_account_maint_margin = &prometheus::BuildGauge()
            .Name("xt_account_maint_margin")
            .Help("Account total maintenance margin (totalMaintMargin binance; 0 CTP), "
                  "runtime-emitted 1Hz. labels: {venue, account, source}")
            .Register(*registry);
        fam_account_margin_ratio = &prometheus::BuildGauge()
            .Name("xt_account_margin_ratio")
            .Help("Account margin ratio = maint_margin / total_margin_balance "
                  "[0,∞); 0 if denom=0. runtime-emitted 1Hz. labels: {venue, account, source}")
            .Register(*registry);

        fam_submit_to_ack_paired_us = &prometheus::BuildHistogram()
            .Name("xt_submit_to_ack_paired_us")
            .Help("Submit→Ack paired latency in microseconds (min of UDS + REST ack when both arrive). "
                  "SLA truth source; 1Hz CSV Snapshot::submit_to_ack_paired is ops-correlation-only.")
            .Register(*registry);

        fam_runner_contract_version_info = &prometheus::BuildGauge()
            .Name("xt_runner_contract_version_info")
            .Help("Runner contract manifest version. Value=1; version label "
                  "matches the manifest_version field in metrics.yaml.")
            .Register(*registry);
        fam_runner_bucket_schema_info = &prometheus::BuildGauge()
            .Name("xt_runner_bucket_schema_info")
            .Help("Bucket schema fingerprint (sha256[:16] over canonical_json"
                  "({family,bucket_version,buckets_sorted})). Gate 1 cross-checks "
                  "against yaml-computed hash. Value=1; carries identity labels only.")
            .Register(*registry);

        fam_md_gap_ms_p99 = &prometheus::BuildGauge()
            .Name("xt_md_gap_ms_p99")
            .Help("Runner MD queue lag p99 in ms (histogram bucket upper bound, conservative). "
                  "HFT value is the window-max of per-frame p99 (no cross-frame histogram merge; conservative upper bound).")
            .Register(*registry);
        fam_md_gap_ms_peak = &prometheus::BuildGauge()
            .Name("xt_md_gap_ms_peak")
            .Help("Runner reporter-window peak MD queue lag in milliseconds")
            .Register(*registry);
        fam_md_age_ms = &prometheus::BuildGauge()
            .Name("xt_md_age_ms")
            .Help("Runner observed age of the last market-data tick in milliseconds")
            .Register(*registry);
        fam_event_drop = &prometheus::BuildGauge()
            .Name("xt_event_drop")
            .Help("Runner event drops from MD tick drops plus rejected TD flow events")
            .Register(*registry);
        fam_queue_depth = &prometheus::BuildGauge()
            .Name("xt_queue_depth")
            .Help("Runner internal event queue depth (instant snapshot, not a percentile): "
                  "HFT=TD event queue, CTA=tick queue, SEL=ctrl queue")
            .Register(*registry);
        fam_risk_block_count = &prometheus::BuildGauge()
            .Name("xt_risk_block_count")
            .Help("Runner pre-trade risk blocks in the current rolling snapshot")
            .Register(*registry);
    }
};

XtMetricRegistry& XtMetricRegistry::instance() {
    static XtMetricRegistry inst;
    return inst;
}

XtMetricRegistry::XtMetricRegistry() : impl_(std::make_unique<Impl>()) {}

XtMetricRegistry::~XtMetricRegistry() {
    stop_exposer();
}

bool XtMetricRegistry::start_exposer(uint16_t port, std::string_view bind_addr) {
    std::lock_guard<std::mutex> lk(impl_->mu);
    if (impl_->running.load(std::memory_order_acquire)) {
            return false;
    }
    try {
        const std::string endpoint = std::string(bind_addr) + ":" + std::to_string(port);
        impl_->exposer = std::make_unique<prometheus::Exposer>(endpoint);
        impl_->exposer->RegisterCollectable(impl_->registry);
        impl_->running.store(true, std::memory_order_release);

        emit_contract_meta();
        return true;
    } catch (const std::exception&) {
        impl_->exposer.reset();
        return false;
    }
}

void XtMetricRegistry::stop_exposer() {
    std::lock_guard<std::mutex> lk(impl_->mu);
    if (impl_->exposer) {
        impl_->exposer.reset();
    }
    impl_->running.store(false, std::memory_order_release);
}

bool XtMetricRegistry::is_running() const noexcept {
    return impl_->running.load(std::memory_order_acquire);
}

std::vector<prometheus::MetricFamily> XtMetricRegistry::collect_metric_families_for_test() const {
    return impl_->registry->Collect();
}

void XtMetricRegistry::on_order_submit(std::string_view strategy,
                                       std::string_view book,
                                       std::string_view venue,
                                       std::string_view symbol,
                                       std::string_view side) {
    (void)venue;
    impl_->fam_submits_total->Add(make_labels_4(
        "strategy", strategy, "book", book, "symbol", symbol, "side", side)).Increment();
}

void XtMetricRegistry::on_md_tick(std::string_view venue,
                                  std::string_view symbol) {

    impl_->fam_md_ticks_total->Add(make_labels_2(
        "venue", venue, "symbol", symbol)).Increment();
}

void XtMetricRegistry::on_order_rtn(std::string_view strategy,
                                    std::string_view book,
                                    std::string_view venue,
                                    std::string_view symbol,
                                    std::string_view status) {
    impl_->fam_order_rtns_total->Add(make_labels_5(
        "strategy", strategy, "book", book, "venue", venue,
        "symbol", symbol, "status", status)).Increment();
}

void XtMetricRegistry::on_trade(std::string_view strategy,
                                std::string_view book,
                                std::string_view venue,
                                std::string_view symbol,
                                std::string_view side,
                                int64_t qty_fp,
                                int64_t price_fp,
                                int64_t fee_fp,
                                int64_t realized_pnl_fp,
                                bool is_maker) {
    (void)price_fp;
    auto labels = make_labels_5(
        "strategy", strategy, "book", book, "venue", venue,
        "symbol", symbol, "side", side);

    impl_->fam_fills_total->Add(labels).Increment();
    impl_->fam_today_fills_total->Add(labels).Increment();

    if (fee_fp > 0) {
        impl_->fam_fee_sum->Add(labels).Increment(static_cast<double>(fee_fp));
    } else if (fee_fp < 0) {
        impl_->fam_fee_rebate_sum->Add(labels).Increment(static_cast<double>(-fee_fp));
    }
    if (realized_pnl_fp > 0) {
        impl_->fam_realized_pnl_sum->Add(labels).Increment(static_cast<double>(realized_pnl_fp));
    } else if (realized_pnl_fp < 0) {
        impl_->fam_realized_loss_sum->Add(labels).Increment(static_cast<double>(-realized_pnl_fp));
    }

    auto fills_labels = make_labels_4(
        "strategy", strategy, "book", book, "venue", venue, "symbol", symbol);
    if (is_maker) {
        impl_->fam_maker_fills_total->Add(fills_labels).Increment();
    } else {
        impl_->fam_taker_fills_total->Add(fills_labels).Increment();
    }

    (void)qty_fp;
}

void XtMetricRegistry::on_normalized_trade(std::string_view strategy,
                                           std::string_view book,
                                           std::string_view venue,
                                           std::string_view symbol,
                                           std::string_view side,
                                           std::string_view route_status,
                                           bool strategy_accounted) {

    impl_->fam_trade_route_status_total->Add(
        make_labels_3("venue", venue, "symbol", symbol, "route_status", route_status)
    ).Increment();
    if (!strategy_accounted) return;

    impl_->fam_strategy_ledger_fills_total->Add(make_labels_5(
        "strategy", strategy, "book", book, "venue", venue,
        "symbol", symbol, "side", side)).Increment();
}

void XtMetricRegistry::set_strategy_ledger_today(std::string_view strategy,
                                                 std::string_view book,
                                                 std::string_view venue,
                                                 std::string_view symbol,
                                                 int64_t today_realized_pnl_fp,
                                                 int64_t today_fees_fp,
                                                 uint64_t today_fills) {

    const auto labels = make_labels_4(
        "strategy", strategy, "book", book, "venue", venue, "symbol", symbol);
    impl_->fam_strategy_ledger_today_fills_total->Add(labels)
        .Set(static_cast<double>(today_fills));
    impl_->fam_strategy_ledger_today_realized_pnl_fp->Add(labels)
        .Set(static_cast<double>(today_realized_pnl_fp));
    impl_->fam_strategy_ledger_today_fees_fp->Add(labels)
        .Set(static_cast<double>(today_fees_fp));
}

void XtMetricRegistry::on_risk_reject(std::string_view strategy,
                                      std::string_view book,
                                      std::string_view venue,
                                      std::string_view symbol,
                                      std::string_view reason) {
    impl_->fam_risk_rejects_total->Add(make_labels_5(
        "strategy", strategy, "book", book, "venue", venue,
        "symbol", symbol, "reason", reason)).Increment();
}

void XtMetricRegistry::inc_risk_rejects_total(std::string_view venue,
                                              uint64_t delta) {

    auto& counter = impl_->fam_risk_rejects_total->Add(make_labels_4(
        "strategy", "_runtime", "venue", venue,
        "symbol", "_all", "reason", "_all"));
    if (delta > 0) counter.Increment(static_cast<double>(delta));
}

void XtMetricRegistry::on_order_reject(std::string_view venue,
                                       std::string_view err_code) {

    impl_->fam_order_rejects_total->Add(make_labels_2(
        "venue", venue, "err_code", err_code)).Increment();
}

void XtMetricRegistry::observe_md_event_to_recv_us(std::string_view channel,
                                                    uint64_t latency_us) {
    impl_->fam_md_event_to_recv_us->Add(
        {{"channel", std::string(channel)}},
        bucket_family_vector("md_us")
    ).Observe(static_cast<double>(latency_us));
}

void XtMetricRegistry::observe_md_internal_queue_us(std::string_view channel,
                                                     uint64_t queue_us) {
    impl_->fam_md_internal_queue_us->Add(
        {{"channel", std::string(channel)}},
        bucket_family_vector("md_us")
    ).Observe(static_cast<double>(queue_us));
}

void XtMetricRegistry::set_md_clock_offset_us(std::string_view channel,
                                               double offset_us) {

    impl_->fam_md_clock_offset_us->Add(
        {{"channel", std::string(channel)}}
    ).Set(offset_us);
}

void XtMetricRegistry::set_md_clock_offset_confidence(std::string_view channel,
                                                       bool locked) {
    impl_->fam_md_clock_offset_confidence->Add(
        {{"channel", std::string(channel)}}
    ).Set(locked ? 1.0 : 0.0);
}

void XtMetricRegistry::set_md_clock_offset_window_samples(std::string_view channel,
                                                            uint64_t samples) {
    impl_->fam_md_clock_offset_window_samples->Add(
        {{"channel", std::string(channel)}}
    ).Set(static_cast<double>(samples));
}

void XtMetricRegistry::set_md_clock_offset_window_min_age_seconds(
    std::string_view channel, double age_s) {
    impl_->fam_md_clock_offset_window_min_age_seconds->Add(
        {{"channel", std::string(channel)}}
    ).Set(age_s);
}

void XtMetricRegistry::inc_md_clock_step(std::string_view channel) {
    impl_->fam_md_clock_step_total->Add(
        {{"channel", std::string(channel)}}
    ).Increment();
}

void XtMetricRegistry::inc_md_negative_latency(std::string_view channel,
                                                 uint64_t count) {
    impl_->fam_md_negative_latency_total->Add(
        {{"channel", std::string(channel)}}
    ).Increment(static_cast<double>(count));
}

void XtMetricRegistry::inc_md_ws_reconnect(std::string_view channel) {
    impl_->fam_md_ws_reconnect_total->Add(
        {{"channel", std::string(channel)}}
    ).Increment();
}

void XtMetricRegistry::inc_md_aggtrade_gap(std::string_view channel) {
    impl_->fam_md_aggtrade_gap_total->Add(
        {{"channel", std::string(channel)}}
    ).Increment();
}

void XtMetricRegistry::inc_md_aggtrade_backfill(std::string_view result, uint64_t n) {
    impl_->fam_md_aggtrade_backfill_total->Add(
        {{"result", std::string(result)}}
    ).Increment(static_cast<double>(n));
}

void XtMetricRegistry::on_param_update(std::string_view strategy,
                                       std::string_view actor,
                                       std::string_view key) {
    impl_->fam_param_updates_total->Add({
        {"strategy", std::string(strategy)},
        {"actor",    std::string(actor)},
        {"key",      std::string(key)},
    }).Increment();
}

void XtMetricRegistry::on_kill_switch(std::string_view venue,
                                      std::string_view actor,
                                      std::string_view state,
                                      std::string_view reason) {
    impl_->fam_kill_switch_total->Add({
        {"venue",  std::string(venue)},
        {"actor",  std::string(actor)},
        {"state",  std::string(state)},
        {"reason", std::string(reason)},
    }).Increment();
}

void XtMetricRegistry::set_pnl_net(std::string_view venue,
                                   std::string_view account,
                                   double pnl_net) {

    impl_->fam_pnl_net->Add(
        make_labels_2("venue", venue, "account", account)
    ).Set(pnl_net);
}

void XtMetricRegistry::set_position_qty_fp(std::string_view strategy,
                                           std::string_view book,
                                           std::string_view venue,
                                           std::string_view symbol,
                                           int64_t qty_fp) {
    impl_->fam_position_qty_fp->Add(make_labels_4(
        "strategy", strategy, "book", book, "venue", venue, "symbol", symbol)
    ).Set(static_cast<double>(qty_fp));
}

void XtMetricRegistry::set_strategy_position_qty_fp(std::string_view venue,
                                                    std::string_view account,
                                                    std::string_view strategy,
                                                    std::string_view book,
                                                    std::string_view symbol,
                                                    int64_t qty_fp) {
    impl_->fam_strategy_position_qty_fp->Add(make_labels_5(
        "venue", venue, "account", account, "strategy", strategy,
        "book", book, "symbol", symbol)
    ).Set(static_cast<double>(qty_fp));
}

void XtMetricRegistry::set_strategy_position_avg_price_fp(std::string_view venue,
                                                          std::string_view account,
                                                          std::string_view strategy,
                                                          std::string_view book,
                                                          std::string_view symbol,
                                                          int64_t v) {
    impl_->fam_strategy_position_avg_price_fp->Add(make_labels_5(
        "venue", venue, "account", account, "strategy", strategy,
        "book", book, "symbol", symbol)
    ).Set(static_cast<double>(v));
}

void XtMetricRegistry::set_strategy_position_unrealized_pnl_fp(std::string_view venue,
                                                               std::string_view account,
                                                               std::string_view strategy,
                                                               std::string_view book,
                                                               std::string_view symbol,
                                                               int64_t v) {
    impl_->fam_strategy_position_unrealized_pnl_fp->Add(make_labels_5(
        "venue", venue, "account", account, "strategy", strategy,
        "book", book, "symbol", symbol)
    ).Set(static_cast<double>(v));
}

void XtMetricRegistry::set_kill_switch_active(std::string_view venue, bool active) {
    impl_->fam_kill_switch_active->Add({{"venue", std::string(venue)}})
        .Set(active ? 1.0 : 0.0);
}

void XtMetricRegistry::set_td_ready(std::string_view venue, bool ready) {
    impl_->fam_td_ready->Add({{"venue", std::string(venue)}})
        .Set(ready ? 1.0 : 0.0);
}

void XtMetricRegistry::set_trading_ready(std::string_view venue, bool ready) {
    impl_->fam_trading_ready->Add({{"venue", std::string(venue)}})
        .Set(ready ? 1.0 : 0.0);
}

void XtMetricRegistry::set_td_recovery_wedged(std::string_view venue, bool wedged) {
    impl_->fam_td_recovery_wedged->Add({{"venue", std::string(venue)}})
        .Set(wedged ? 1.0 : 0.0);
}

void XtMetricRegistry::inc_md_ticks_total(std::string_view venue, uint64_t delta) {
    if (delta == 0) return;
    impl_->fam_md_ticks_total->Add(make_labels_2("venue", venue, "symbol", "_all"))
        .Increment(static_cast<double>(delta));
}

void XtMetricRegistry::set_strategy_enabled(std::string_view strategy,
                                            std::string_view venue,
                                            bool enabled) {
    impl_->fam_strategy_enabled->Add(make_labels_2(
        "strategy", strategy, "venue", venue)
    ).Set(enabled ? 1.0 : 0.0);
}

void XtMetricRegistry::set_strategy_custom_metric(std::string_view strategy,
                                                  std::string_view name,
                                                  double value) {

    impl_->fam_strategy_custom_metric->Add(make_labels_2(
        "strategy", strategy, "name", name)
    ).Set(value);
}

void XtMetricRegistry::set_today_pnl_net_v2(std::string_view strategy,
                                            std::string_view book,
                                            std::string_view venue,
                                            std::string_view symbol,
                                            double pnl_net) {
    impl_->fam_today_pnl_net_v2->Add(make_labels_4(
        "strategy", strategy, "book", book, "venue", venue, "symbol", symbol)
    ).Set(pnl_net);
}

void XtMetricRegistry::set_today_fills_total_v2(std::string_view strategy,
                                                std::string_view venue,
                                                std::string_view symbol,
                                                uint64_t fills_count) {
    impl_->fam_today_fills_total_v2->Add(make_labels_3(
        "strategy", strategy, "venue", venue, "symbol", symbol)
    ).Set(static_cast<double>(fills_count));
}

void XtMetricRegistry::inc_strategy_baseline_new_total(std::string_view strategy,
                                                       std::string_view venue) {
    impl_->fam_strategy_baseline_new_total->Add(
        make_labels_2("strategy", strategy, "venue", venue)
    ).Increment();
}

void XtMetricRegistry::inc_baseline_rotated_total(std::string_view venue) {
    impl_->fam_baseline_rotated_total->Add({{"venue", std::string(venue)}})
        .Increment();
}

void XtMetricRegistry::inc_strategy_baseline_load_failed_total(std::string_view venue,
                                                               std::string_view reason) {
    impl_->fam_strategy_baseline_load_failed_total->Add(
        make_labels_2("venue", venue, "reason", reason)
    ).Increment();
}

void XtMetricRegistry::set_strategy_auto_liquidating(std::string_view strategy,
                                                     bool latched) {
    impl_->fam_strategy_auto_liquidating->Add({{"strategy", std::string(strategy)}})
        .Set(latched ? 1.0 : 0.0);
}

void XtMetricRegistry::set_strategy_auto_liquidation_trigger_pnl_fp(std::string_view strategy,
                                                                    int64_t trigger_pnl_fp) {
    impl_->fam_strategy_auto_liquidation_trigger_pnl_fp->Add({{"strategy", std::string(strategy)}})
        .Set(static_cast<double>(trigger_pnl_fp));
}

void XtMetricRegistry::inc_strategy_auto_liquidation_breach_total(std::string_view strategy) {
    impl_->fam_strategy_auto_liquidation_breach_total->Add({{"strategy", std::string(strategy)}})
        .Increment();
}

void XtMetricRegistry::inc_strategy_auto_liquidation_submit_total(std::string_view strategy) {
    impl_->fam_strategy_auto_liquidation_submit_total->Add({{"strategy", std::string(strategy)}})
        .Increment();
}

void XtMetricRegistry::inc_strategy_auto_liquidation_deferred_total(std::string_view strategy) {
    impl_->fam_strategy_auto_liquidation_deferred_total->Add({{"strategy", std::string(strategy)}})
        .Increment();
}

void XtMetricRegistry::inc_strategy_auto_liquidation_1to1_violation_total(std::string_view strategy) {
    impl_->fam_strategy_auto_liquidation_1to1_violation_total->Add({{"strategy", std::string(strategy)}})
        .Increment();
}

void XtMetricRegistry::inc_stop_order_total(std::string_view event, uint64_t delta) {
    if (delta == 0) return;
    impl_->fam_stop_order_total->Add({{"event", std::string(event)}})
        .Increment(static_cast<double>(delta));
}

void XtMetricRegistry::set_stop_order_armed(std::string_view strategy, uint32_t count) {
    impl_->fam_stop_order_armed->Add({{"strategy", std::string(strategy)}})
        .Set(static_cast<double>(count));
}

void XtMetricRegistry::set_account_truth_snapshot(std::string_view venue,
                                                   std::string_view account,
                                                   const AccountTruthSample& sample,
                                                   std::string_view source) {

    auto labels = make_labels_3("venue", venue,
                                 "account", account,
                                 "source", source);
    impl_->fam_account_truth_wallet_balance->Add(labels).Set(sample.wallet_balance);
    impl_->fam_account_truth_equity->Add(labels).Set(sample.equity);
    impl_->fam_account_equity_derived->Add(labels).Set(sample.equity);
    impl_->fam_account_truth_position_value->Add(labels).Set(sample.position_value);
    impl_->fam_account_truth_unrealized_pnl->Add(labels).Set(sample.unrealized_pnl);
    impl_->fam_account_truth_fees_today->Add(labels).Set(sample.fees_today);
    impl_->fam_account_truth_external_cashflow_today->Add(labels).Set(sample.external_cashflow_today);
    impl_->fam_account_truth_trading_day_id->Add(labels).Set(sample.trading_day_id);
    impl_->fam_account_available_balance->Add(labels).Set(sample.available_balance);
    impl_->fam_account_maint_margin->Add(labels).Set(sample.maint_margin);
    impl_->fam_account_margin_ratio->Add(labels).Set(sample.margin_ratio);
}

void XtMetricRegistry::set_venue_equity_raw(std::string_view venue,
                                            std::string_view account,
                                            double value) {
    impl_->fam_venue_equity_raw->Add(
        make_labels_2("venue", venue, "account", account)
    ).Set(value);
}

void XtMetricRegistry::observe_cash_reanchor(std::string_view venue,
                                             std::string_view account,
                                             double divergence) {
    auto labels = make_labels_2("venue", venue, "account", account);
    impl_->fam_venue_cash_reanchor_divergence->Add(labels).Set(divergence);
    impl_->fam_venue_cash_reanchor_correction_total->Add(labels)
        .Increment(std::abs(divergence));
}

void XtMetricRegistry::set_state_divergence(std::string_view field,
                                            std::string_view symbol,
                                            double value) {
    impl_->fam_state_divergence->Add(
        make_labels_2("field", field, "symbol", symbol)
    ).Set(value);
}

void XtMetricRegistry::inc_state_divergence_emit_heartbeat() {

    impl_->fam_state_divergence_emit_heartbeat->Add(prometheus::Labels{})
        .Increment();
}

void XtMetricRegistry::set_reconcile_snapshot_age_ms(double age_ms) {

    impl_->fam_reconcile_snapshot_age_ms->Add(prometheus::Labels{}).Set(age_ms);
}

void XtMetricRegistry::on_fill_maker_taker(std::string_view strategy,
                                            std::string_view book,
                                            std::string_view venue,
                                            std::string_view symbol,
                                            bool is_maker) {
    auto labels = make_labels_4("strategy", strategy,
                                 "book", book,
                                 "venue", venue,
                                 "symbol", symbol);
    if (is_maker) {
        impl_->fam_maker_fills_total->Add(labels).Increment();
    } else {
        impl_->fam_taker_fills_total->Add(labels).Increment();
    }
}

void XtMetricRegistry::set_strategy_limits(std::string_view strategy,
                                            std::string_view book,
                                            std::string_view venue,
                                            const RiskLimitsSample& limits) {
    auto labels = make_labels_3("strategy", strategy,
                                 "book", book,
                                 "venue", venue);
    impl_->fam_strategy_limit_max_order_qty->Add(labels).Set(limits.max_order_qty);
    impl_->fam_strategy_limit_max_pos_qty->Add(labels).Set(limits.max_pos_qty);
    impl_->fam_strategy_limit_max_drawdown_pct->Add(labels).Set(limits.max_drawdown_pct);
    impl_->fam_strategy_limit_max_position_value_usd->Add(labels).Set(limits.max_position_value_usd);
    impl_->fam_strategy_limit_max_orders_per_min->Add(labels).Set(limits.max_orders_per_min);
    impl_->fam_strategy_limit_allow_short->Add(labels).Set(limits.allow_short ? 1.0 : 0.0);
}

void XtMetricRegistry::observe_control_rpc_us(std::string_view action,
                                                std::string_view result,
                                                uint64_t latency_us) {

    impl_->fam_control_rpc_us->Add(
        make_labels_2("action", action, "result", result),
        rpc_us_buckets()
    ).Observe(static_cast<double>(latency_us));
}

void XtMetricRegistry::inc_control_rpc_total(std::string_view action,
                                              std::string_view actor,
                                              std::string_view result) {
    impl_->fam_control_rpc_total->Add(
        make_labels_3("action", action, "actor", actor, "result", result)
    ).Increment();
}

void XtMetricRegistry::inc_gateway_disconnect_total(std::string_view venue,
                                                     std::string_view role,
                                                     std::string_view reason) {
    impl_->fam_gateway_disconnect_total->Add(
        make_labels_3("venue", venue, "role", role, "reason", reason)
    ).Increment();
}

void XtMetricRegistry::inc_close_quota_disagreement_total(std::string_view venue) {
    impl_->fam_close_quota_disagreement_total->Add(
        prometheus::Labels{{"venue", std::string(venue)}}
    ).Increment();
}

void XtMetricRegistry::inc_recovery_rebuild_alive_total(std::string_view venue,
                                                         std::string_view outcome) {
    impl_->fam_recovery_rebuild_alive_total->Add(
        make_labels_2("venue", venue, "outcome", outcome)
    ).Increment();
}

void XtMetricRegistry::inc_recovery_rebuild_alive_skipped_foreign_total(
    std::string_view venue) {
    impl_->fam_recovery_rebuild_alive_skipped_foreign_total->Add(
        prometheus::Labels{{"venue", std::string(venue)}}
    ).Increment();
}

void XtMetricRegistry::inc_recovery_orphan_reaped_total(std::string_view venue,
                                                        uint64_t delta) {
    if (delta == 0) return;
    impl_->fam_recovery_orphan_reaped_total->Add(
        prometheus::Labels{{"venue", std::string(venue)}}
    ).Increment(static_cast<double>(delta));
}

void XtMetricRegistry::inc_td_position_snapshot_drop_total(std::string_view venue,
                                                           uint64_t delta) {
    if (delta == 0) return;
    impl_->fam_td_position_snapshot_drop_total->Add(
        prometheus::Labels{{"venue", std::string(venue)}}
    ).Increment(static_cast<double>(delta));
}

void XtMetricRegistry::inc_td_control_event_drop_total(std::string_view venue,
                                                       uint64_t delta) {
    if (delta == 0) return;
    impl_->fam_td_control_event_drop_total->Add(
        prometheus::Labels{{"venue", std::string(venue)}}
    ).Increment(static_cast<double>(delta));
}

void XtMetricRegistry::inc_td_other_event_drop_total(std::string_view venue,
                                                     uint64_t delta) {
    if (delta == 0) return;
    impl_->fam_td_other_event_drop_total->Add(
        prometheus::Labels{{"venue", std::string(venue)}}
    ).Increment(static_cast<double>(delta));
}

void XtMetricRegistry::inc_hft_worker_guard_fail_total(std::string_view venue,
                                                       std::string_view stage,
                                                       uint64_t delta) {
    if (delta == 0) return;
    impl_->fam_hft_worker_guard_fail_total->Add(
        prometheus::Labels{{"venue", std::string(venue)}, {"stage", std::string(stage)}}
    ).Increment(static_cast<double>(delta));
}

void XtMetricRegistry::set_open_orders(std::string_view venue, uint32_t count) {
    impl_->fam_open_orders->Add(
        prometheus::Labels{{"venue", std::string(venue)}}
    ).Set(static_cast<double>(count));
}

void XtMetricRegistry::set_shm_md_attach_failing(std::string_view ring, uint32_t failing) {
    impl_->fam_shm_md_attach_failing->Add(
        prometheus::Labels{{"ring", std::string(ring)}}
    ).Set(static_cast<double>(failing));
}

void XtMetricRegistry::set_shm_md_attach_fail_count(std::string_view ring, uint64_t count) {
    impl_->fam_shm_md_attach_fail_count->Add(
        prometheus::Labels{{"ring", std::string(ring)}}
    ).Set(static_cast<double>(count));
}

void XtMetricRegistry::set_stp_registered_symbols(std::string_view venue, uint32_t count) {
    impl_->fam_stp_registered_symbols->Add(
        prometheus::Labels{{"venue", std::string(venue)}}
    ).Set(static_cast<double>(count));
}

void XtMetricRegistry::inc_runtime_book_drop_total(std::string_view reason) {
    impl_->fam_runtime_book_drop_total->Add(
        prometheus::Labels{{"reason", std::string(reason)}}
    ).Increment();
}

void XtMetricRegistry::inc_unmatched_drop_total(std::string_view kind,
                                                std::string_view reason,
                                                uint64_t delta) {
    if (delta == 0) return;
    impl_->fam_dropped_total->Add(
        prometheus::Labels{{"kind", std::string(kind)}, {"reason", std::string(reason)}}
    ).Increment(static_cast<double>(delta));
}

void XtMetricRegistry::inc_engine_recovery_total(std::string_view kind, uint64_t delta) {
    if (delta == 0) return;
    impl_->fam_engine_recovery_total->Add(
        prometheus::Labels{{"kind", std::string(kind)}}
    ).Increment(static_cast<double>(delta));
}

void XtMetricRegistry::inc_runtime_market_drop_total(std::string_view reason) {
    impl_->fam_runtime_market_drop_total->Add(
        prometheus::Labels{{"reason", std::string(reason)}}
    ).Increment();
}

void XtMetricRegistry::inc_latency_meta_miss_total(std::string_view venue) {
    impl_->fam_latency_meta_miss_total->Add(
        prometheus::Labels{{"venue", std::string(venue)}}
    ).Increment();
}

void XtMetricRegistry::inc_latency_eligible_total(std::string_view stage,
                                                  std::string_view strategy,
                                                  std::string_view book,
                                                  std::string_view symbol) {
    impl_->fam_latency_eligible_total->Add(
        prometheus::Labels{{"stage", std::string(stage)},
                           {"strategy", std::string(strategy)},
                           {"book", std::string(book)},
                           {"symbol", std::string(symbol)}}
    ).Increment();
}

void XtMetricRegistry::inc_latency_tap_dropped_total(std::string_view reason) {
    impl_->fam_latency_tap_dropped_total->Add(
        prometheus::Labels{{"reason", std::string(reason)}}
    ).Increment();
}

void XtMetricRegistry::inc_broker_frozen_stale_total(std::string_view venue) {
    impl_->fam_broker_frozen_stale_total->Add(
        prometheus::Labels{{"venue", std::string(venue)}}
    ).Increment();
}

void XtMetricRegistry::inc_position_aggregator_jump_total(std::string_view sid,
                                                          std::string_view code) {
    impl_->fam_position_aggregator_jump_total->Add(
        make_labels_2("sid", sid, "code", code)
    ).Increment();
}

void XtMetricRegistry::set_position_avg_price_fp(std::string_view venue,
                                                  std::string_view symbol,
                                                  int64_t v) {
    impl_->fam_position_avg_price_fp->Add(
        make_labels_2("venue", venue, "symbol", symbol)
    ).Set(static_cast<double>(v));
}

void XtMetricRegistry::set_position_mark_price_fp(std::string_view venue,
                                                   std::string_view symbol,
                                                   int64_t v) {
    impl_->fam_position_mark_price_fp->Add(
        make_labels_2("venue", venue, "symbol", symbol)
    ).Set(static_cast<double>(v));
}

void XtMetricRegistry::set_position_unrealized_pnl_fp(std::string_view venue,
                                                       std::string_view symbol,
                                                       int64_t v) {
    impl_->fam_position_unrealized_pnl_fp->Add(
        make_labels_2("venue", venue, "symbol", symbol)
    ).Set(static_cast<double>(v));
}

void XtMetricRegistry::set_position_margin_fp(std::string_view venue,
                                               std::string_view symbol,
                                               int64_t v) {
    impl_->fam_position_margin_fp->Add(
        make_labels_2("venue", venue, "symbol", symbol)
    ).Set(static_cast<double>(v));
}

void XtMetricRegistry::set_account_store_mark_price_fp(std::string_view venue,
                                                        std::string_view symbol,
                                                        int64_t v) {
    impl_->fam_account_store_mark_price_fp->Add(
        make_labels_2("venue", venue, "symbol", symbol)
    ).Set(static_cast<double>(v));
}

void XtMetricRegistry::set_broker_position_qty_fp(std::string_view venue,
                                                  std::string_view account,
                                                  std::string_view symbol,
                                                  int64_t v) {
    impl_->fam_broker_position_qty_fp->Add(
        make_labels_3("venue", venue, "account", account, "symbol", symbol)
    ).Set(static_cast<double>(v));
}

void XtMetricRegistry::set_broker_position_avg_price_fp(std::string_view venue,
                                                        std::string_view account,
                                                        std::string_view symbol,
                                                        int64_t v) {
    impl_->fam_broker_position_avg_price_fp->Add(
        make_labels_3("venue", venue, "account", account, "symbol", symbol)
    ).Set(static_cast<double>(v));
}

void XtMetricRegistry::set_broker_position_mark_price_fp(std::string_view venue,
                                                         std::string_view account,
                                                         std::string_view symbol,
                                                         int64_t v) {
    impl_->fam_broker_position_mark_price_fp->Add(
        make_labels_3("venue", venue, "account", account, "symbol", symbol)
    ).Set(static_cast<double>(v));
}

void XtMetricRegistry::set_broker_position_unrealized_pnl_fp(std::string_view venue,
                                                             std::string_view account,
                                                             std::string_view symbol,
                                                             int64_t v) {
    impl_->fam_broker_position_unrealized_pnl_fp->Add(
        make_labels_3("venue", venue, "account", account, "symbol", symbol)
    ).Set(static_cast<double>(v));
}

void XtMetricRegistry::set_broker_position_margin_fp(std::string_view venue,
                                                     std::string_view account,
                                                     std::string_view symbol,
                                                     int64_t v) {
    impl_->fam_broker_position_margin_fp->Add(
        make_labels_3("venue", venue, "account", account, "symbol", symbol)
    ).Set(static_cast<double>(v));
}

void XtMetricRegistry::emit_contract_meta() {
    namespace contract = xt::metrics::contract;
    impl_->fam_runner_contract_version_info
        ->Add({{"version", std::string(contract::kManifestVersion)}})
        .Set(1.0);
    for (const auto& fam : contract::kBucketFamilies) {
        impl_->fam_runner_bucket_schema_info
            ->Add({
                {"family", std::string(fam.name)},
                {"bucket_version", std::string(fam.version)},
                {"hash", std::string(fam.hash)},
            })
            .Set(1.0);
    }
}

void XtMetricRegistry::set_runtime_kpi_snapshot(std::string_view strategy,
                                                std::string_view book,
                                                std::string_view venue,
                                                std::string_view symbol,
                                                const RuntimeKpiSnapshot& sample) {
    const auto labels = make_labels_4(
        "strategy", strategy, "book", book, "venue", venue, "symbol", symbol);
    auto set_gauge = [&](prometheus::Family<prometheus::Gauge>* fam, double value) {
        fam->Add(labels).Set(std::isfinite(value) ? value : 0.0);
    };
    if (sample.has_md_gap_ms_p99) {
        set_gauge(impl_->fam_md_gap_ms_p99, sample.md_gap_ms_p99);
    }
    if (sample.has_md_gap_ms_peak) {
        set_gauge(impl_->fam_md_gap_ms_peak, sample.md_gap_ms_peak);
    }
    if (sample.has_md_age_ms) {
        set_gauge(impl_->fam_md_age_ms, sample.md_age_ms);
    }
    set_gauge(impl_->fam_event_drop, sample.event_drop);
    set_gauge(impl_->fam_queue_depth, sample.queue_depth_p99);
    set_gauge(impl_->fam_risk_block_count, sample.risk_block_count);
}

prometheus::Histogram* XtMetricRegistry::get_submit_to_ack_us_histogram(
    std::string_view strategy, std::string_view book,
    std::string_view venue, std::string_view symbol) {
    (void)venue;
    return &impl_->fam_submit_to_ack_us->Add(
        make_labels_3("strategy", strategy, "book", book, "symbol", symbol),
        order_us_buckets()
    );
}

prometheus::Histogram* XtMetricRegistry::get_submit_to_fill_us_histogram(
    std::string_view strategy, std::string_view book,
    std::string_view venue, std::string_view symbol) {
    (void)venue;
    return &impl_->fam_submit_to_fill_us->Add(
        make_labels_3("strategy", strategy, "book", book, "symbol", symbol),
        order_us_buckets()
    );
}

prometheus::Histogram* XtMetricRegistry::get_submit_to_ack_paired_us_histogram(
    std::string_view strategy, std::string_view book,
    std::string_view venue, std::string_view symbol) {
    (void)venue;
    return &impl_->fam_submit_to_ack_paired_us->Add(
        make_labels_3("strategy", strategy, "book", book, "symbol", symbol),
        order_us_buckets()
    );
}

prometheus::Histogram* XtMetricRegistry::get_strategy_us_histogram(
    std::string_view strategy, std::string_view book,
    std::string_view venue, std::string_view symbol) {
    (void)venue;
    return &impl_->fam_strategy_us->Add(
        make_labels_3("strategy", strategy, "book", book, "symbol", symbol),
        order_us_buckets()
    );
}

prometheus::Histogram* XtMetricRegistry::get_local_submit_us_histogram(
    std::string_view strategy, std::string_view book,
    std::string_view venue, std::string_view symbol) {
    (void)venue;
    return &impl_->fam_local_submit_us->Add(
        make_labels_3("strategy", strategy, "book", book, "symbol", symbol),
        order_us_buckets()
    );
}

prometheus::Counter* XtMetricRegistry::get_latency_eligible_counter(
    std::string_view stage, std::string_view strategy,
    std::string_view book, std::string_view symbol) {
    return &impl_->fam_latency_eligible_total->Add(
        make_labels_4("stage", stage, "strategy", strategy, "book", book, "symbol", symbol)
    );
}

}
