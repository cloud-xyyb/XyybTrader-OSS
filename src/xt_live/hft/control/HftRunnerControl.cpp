#include "xt_live/hft/control/HftRunnerControl.h"

#include "xt_util/runtime/SdNotify.h"
#include "xt_util/runtime/XtShutdownSignal.h"

#include <chrono>

#include "XtLiveOrderGateway.h"
#include "xt_report/live/XtLiveReporter.h"
#include "xt_report/core/XtReportTypes.h"
#include "api/ITradeGateway.h"
#include "api/XtRunnerSignals.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_risk/state/SafetyStateStore.h"
#include "xt_util/time/XtSteadyClock.h"

#include "xt_live/hft/control/HftControlStages.h"
#include "xt_live/hft/control/HftRunnerControlState.h"
#include "xt_live/hft/telemetry/XtHftControlReportEmit.h"

using namespace xt_live::control::detail;

XtHftRunnerControlContext make_runner_control_context(ITradeGateway* td_gateway,
                                                   XtHftEventLoop& strategy_event_loop,
                                                   XtLiveOrderGateway& live_order_gateway,
                                                   XtRunnerSignals& runtime_shared_state,
                                                   const xt_account::AccountStateStore& account_store,
                                                   xt_runtime::order::XtOrderRuntimeBook& order_book,
                                                   const XtLiveOrderGatewayCfg& order_gateway_cfg,
                                                   XtLiveLatencyTracker* latency_tracker,
                                                   XtLiveParamDomain* param_domain,
                                                   IAccountTruthSource* account_truth_source) {
    XtHftRunnerControlContext ctx{td_gateway,
                               strategy_event_loop,
                               live_order_gateway,
                               runtime_shared_state,
                               &account_store,
                               &order_book,
                               &order_gateway_cfg,
                               latency_tracker,
                               param_domain,
                               account_truth_source};
    return ctx;
}

HftRunnerControl::HftRunnerControl(const XtRunnerControlConfig& cfg,
                             XtHftRunnerControlContext& ctx,
                             const Deps& deps)
    : cfg_(cfg),
      ctx_(ctx),
      live_reporter_(deps.live_reporter) {}

namespace {

void update_last_snapshot_state(const LoopFrame& frame,
                                bool& has_last_snapshot,
                                ControlSnapshot& last_snapshot,
                                int64_t& last_snapshot_ms) {
    has_last_snapshot = true;
    last_snapshot = frame.snapshot;
    last_snapshot_ms = frame.now_ms;
}

void finalize_control_loop(const XtRunnerControlConfig& cfg,
                           XtHftRunnerControlContext& ctx,
                           xt_report::XtLiveReporter& live_tracker,
                           bool has_last_snapshot,
                           const ControlSnapshot& last_snapshot,
                           int64_t last_snapshot_ms,
                           const ControlLoopState& st) {
    if (!has_last_snapshot) return;

    live_tracker.on_equity(last_snapshot.equity);

    xt_report::OrderRatios ratios;
    if (last_snapshot.risk_stats.submit_attempts > 0) {
        const double d = static_cast<double>(last_snapshot.risk_stats.submit_attempts);
        ratios.fill_ratio   = static_cast<double>(last_snapshot.risk_stats.submit_success) / d;
        const uint64_t rejects = last_snapshot.risk_stats.submit_attempts > last_snapshot.risk_stats.submit_success
                                     ? last_snapshot.risk_stats.submit_attempts - last_snapshot.risk_stats.submit_success : 0;
        ratios.reject_ratio = static_cast<double>(rejects) / d;
    }
    if (last_snapshot.risk_stats.cancel_attempts > 0) {
        ratios.cancel_ratio = static_cast<double>(last_snapshot.risk_stats.cancel_success) /
                              static_cast<double>(last_snapshot.risk_stats.cancel_attempts);
    }
    if (last_snapshot.risk_stats.submit_success > 0) {
        ratios.order_cancel_ratio =
            static_cast<double>(last_snapshot.risk_stats.canceled_terminal_total) /
            static_cast<double>(last_snapshot.risk_stats.submit_success);
    }
    xt_report::LatencyStats latency;
    latency.submit_to_ack_us_avg  = last_snapshot.latency_stats.submit_to_ack.avg_us;
    latency.submit_to_ack_us_p50  = last_snapshot.latency_stats.submit_to_ack.p50_us;
    latency.submit_to_ack_us_p90  = last_snapshot.latency_stats.submit_to_ack.p90_us;
    latency.submit_to_ack_us_p95  = last_snapshot.latency_stats.submit_to_ack.p95_us;
    latency.submit_to_ack_us_p99  = last_snapshot.latency_stats.submit_to_ack.p99_us;
    latency.submit_to_ack_paired_us_avg = last_snapshot.latency_stats.submit_to_ack_paired.avg_us;
    latency.submit_to_ack_paired_us_p50 = last_snapshot.latency_stats.submit_to_ack_paired.p50_us;
    latency.submit_to_ack_paired_us_p90 = last_snapshot.latency_stats.submit_to_ack_paired.p90_us;
    latency.submit_to_ack_paired_us_p95 = last_snapshot.latency_stats.submit_to_ack_paired.p95_us;
    latency.submit_to_ack_paired_us_p99 = last_snapshot.latency_stats.submit_to_ack_paired.p99_us;
    latency.submit_to_fill_us_avg = last_snapshot.latency_stats.submit_to_fill.avg_us;
    latency.submit_to_fill_us_p50 = last_snapshot.latency_stats.submit_to_fill.p50_us;
    latency.submit_to_fill_us_p90 = last_snapshot.latency_stats.submit_to_fill.p90_us;
    latency.submit_to_fill_us_p95 = last_snapshot.latency_stats.submit_to_fill.p95_us;
    latency.submit_to_fill_us_p99 = last_snapshot.latency_stats.submit_to_fill.p99_us;
    latency.strategy_us_avg       = last_snapshot.latency_stats.strategy_processing.avg_us;
    latency.strategy_us_p50       = last_snapshot.latency_stats.strategy_processing.p50_us;
    latency.strategy_us_p90       = last_snapshot.latency_stats.strategy_processing.p90_us;
    latency.strategy_us_p99       = last_snapshot.latency_stats.strategy_processing.p99_us;
    latency.local_submit_us_avg   = last_snapshot.latency_stats.submit_wire.avg_us;
    latency.local_submit_us_p50   = last_snapshot.latency_stats.submit_wire.p50_us;
    latency.local_submit_us_p90   = last_snapshot.latency_stats.submit_wire.p90_us;
    latency.local_submit_us_p99   = last_snapshot.latency_stats.submit_wire.p99_us;
    latency.ack_path_us_avg       = last_snapshot.latency_stats.ack_path.avg_us;
    latency.ack_path_us_p50       = last_snapshot.latency_stats.ack_path.p50_us;
    latency.ack_path_us_p90       = last_snapshot.latency_stats.ack_path.p90_us;
    latency.ack_path_us_p99       = last_snapshot.latency_stats.ack_path.p99_us;
    latency.fill_after_ack_us_avg = last_snapshot.latency_stats.ack_to_fill.avg_us;
    latency.fill_after_ack_us_p50 = last_snapshot.latency_stats.ack_to_fill.p50_us;
    latency.fill_after_ack_us_p90 = last_snapshot.latency_stats.ack_to_fill.p90_us;
    latency.fill_after_ack_us_p99 = last_snapshot.latency_stats.ack_to_fill.p99_us;
    latency.event_to_ack_us_avg   = last_snapshot.latency_stats.event_to_ack.avg_us;
    latency.event_to_ack_us_p50   = last_snapshot.latency_stats.event_to_ack.p50_us;
    latency.event_to_ack_us_p90   = last_snapshot.latency_stats.event_to_ack.p90_us;
    latency.event_to_ack_us_p99   = last_snapshot.latency_stats.event_to_ack.p99_us;
    latency.event_to_fill_us_avg  = last_snapshot.latency_stats.event_to_fill.avg_us;
    latency.event_to_fill_us_p50  = last_snapshot.latency_stats.event_to_fill.p50_us;
    latency.event_to_fill_us_p90  = last_snapshot.latency_stats.event_to_fill.p90_us;
    latency.event_to_fill_us_p99  = last_snapshot.latency_stats.event_to_fill.p99_us;
    latency.risk_block_count      = last_snapshot.risk_stats.submit_attempts > last_snapshot.risk_stats.submit_success
                                        ? last_snapshot.risk_stats.submit_attempts - last_snapshot.risk_stats.submit_success : 0;
    latency.md_gap_ms_p99         = static_cast<double>(last_snapshot.tick_lag_p99_us) / 1000.0;
    latency.event_drop            = last_snapshot.tick_drop + last_snapshot.td_flow_event_rejected;
    latency.queue_depth_p99       = static_cast<double>(last_snapshot.td_event_peak_backlog);

    const bool kill_switch_active = ctx.runtime_shared_state.kill_switch.load(std::memory_order_acquire);
    live_tracker.force_flush_kpi(last_snapshot_ms, 0.0, ratios, latency, kill_switch_active);
    live_tracker.flush_run_summary();
}

}

void HftRunnerControl::run(std::atomic<bool>& shutdown) {
    ControlLoopState st;
    const int64_t start_ms = live_reporter_.start_ms();
    bool has_last_snapshot = false;
    ControlSnapshot last_snapshot;
    int64_t last_snapshot_ms = start_ms;

    const ConsistencyThresholds thresholds = make_consistency_thresholds(cfg_);

    bootstrap_safety_state(ctx_, cfg_.safety_state_path);
    xt_risk::state::SafetyState last_safety_state = collect_safety_state(ctx_);
    int64_t last_safety_save_ms = XtSteadyClock::steady_now_ms();

    save_safety_state_if_due(cfg_.safety_state_path, ctx_,
                             last_safety_state, last_safety_save_ms,
                             last_safety_save_ms,
                             cfg_.safety_state_save_interval_ms,
                             "bootstrap");

    xt_util::sd_notify_ready();

    for (;;) {

        if (xt_util::XtShutdownSignal::wait_for(
                shutdown, std::chrono::milliseconds(cfg_.sleep_ms))) {
            break;
        }

        const LoopFrame frame = collect_loop_frame(cfg_, ctx_, st);
        log_control_stats(frame.snapshot, st.latency_sla_breach_total);
        update_last_snapshot_state(frame, has_last_snapshot, last_snapshot, last_snapshot_ms);

        if (!apply_policy_chain(shutdown, cfg_, ctx_, thresholds, frame, st)) break;

        xt_live::control::emit_live_reports(cfg_, ctx_, live_reporter_, frame, st);
        xt_live::control::flush_strategy_metrics_if_needed(cfg_, ctx_, live_reporter_, frame, st);

        liquidator_.step(ctx_);

        run_force_close_once(ctx_, cfg_, st);

        maybe_auto_release_closeout_latch(ctx_, cfg_, st);

        {
            const int64_t safety_now_ms = XtSteadyClock::steady_now_ms();
            save_safety_state_if_due(cfg_.safety_state_path, ctx_,
                                     last_safety_state, last_safety_save_ms,
                                     safety_now_ms,
                                     cfg_.safety_state_save_interval_ms,
                                     "frame");
        }

        xt_util::sd_notify_watchdog();
    }

    xt_util::sd_notify_stopping();

    {
        const int64_t shutdown_now_ms = XtSteadyClock::steady_now_ms();
        save_safety_state_if_due(cfg_.safety_state_path, ctx_,
                                 last_safety_state, last_safety_save_ms,
                                 shutdown_now_ms,
                                 cfg_.safety_state_save_interval_ms,
                                 "shutdown");
    }

    finalize_control_loop(
        cfg_, ctx_, live_reporter_, has_last_snapshot, last_snapshot, last_snapshot_ms, st);
}
