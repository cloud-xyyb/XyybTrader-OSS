
#include "xt_live/hft/control/HftControlStages.h"

#include <algorithm>
#include <utility>

#include "XtLiveOrderGateway.h"
#include "XtLiveParamDomain.h"
#include "api/ITradeGateway.h"
#include "api/XtRunnerSignals.h"
#include "latency/XtLiveLatencyTracker.h"
#include "state/AccountStateStore.h"
#include "xt_account/api/AccountSnapshot.h"
#include "XtFixedPoint.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_live/control/AdvanceRiskFrame.h"
#include "xt_live/hft/runtime/XtHftEventLoop.h"
#include "xt_runtime/order/XtOrderRuntimeBook.h"
#include "xt_util/time/XtSteadyClock.h"

namespace xt_live::control::detail {
namespace {

ConsistencyReport build_consistency_report(const XtHftRunnerControlContext& ctx) {
    ConsistencyReport out;
    if (!ctx.order_book || !ctx.account_store) {
        return out;
    }
    out.open_order_count   = ctx.order_book->open_order_count();
    out.tracked_code_count = ctx.order_book->tracked_code_count();
    account_audit::XtFrozenAuditReport audit;

    ctx.account_store->frozen_audit_from_snapshot(audit);
    out.ok                    = audit.ok;
    out.frozen_cash           = audit.frozen_cash;
    out.frozen_cash_by_orders = audit.frozen_cash_by_orders;
    out.reason                = std::move(audit.reason);
    return out;
}

int64_t now_ms_steady() {
    return XtSteadyClock::steady_now_ms();
}

ControlParamSnapshot read_control_params(const XtRunnerControlConfig& cfg,
                                         const XtHftRunnerControlContext& ctx) {
    ControlParamSnapshot out;
    out.log_stats_enabled = cfg.log_stats;
    out.strategy_metrics_flush_sec = std::max(1, cfg.strategy_metrics_flush_sec);
    if (ctx.latency_tracker) {
        out.latency_enabled = ctx.latency_tracker->enabled();
        out.latency_stale_ttl_us = ctx.latency_tracker->stale_ttl_us();
    }
    if (!ctx.param_domain) return out;

    XtLiveParamDomain* pd = ctx.param_domain;
    out.param_ver = pd->version();
    out.param_count = pd->size();
    out.log_stats_enabled = pd->get_bool_or("control.log_stats", cfg.log_stats);
    out.latency_enabled = pd->get_bool_or("latency.enabled", out.latency_enabled);
    const int64_t ttl_us = pd->get_int_or(
        "latency.stale_ttl_us",
        static_cast<int64_t>(out.latency_stale_ttl_us));
    if (ttl_us > 0) {
        out.latency_stale_ttl_us = static_cast<uint64_t>(ttl_us);
    }
    out.latency_sla.ack_sla_p99_us = static_cast<uint64_t>(
        std::max<int64_t>(0, pd->get_int_or("latency.sla_ack_p99_us", 0)));
    out.latency_sla.fill_sla_p99_us = static_cast<uint64_t>(
        std::max<int64_t>(0, pd->get_int_or("latency.sla_fill_p99_us", 0)));
    out.latency_sla.ack_fill_sla_p99_us = static_cast<uint64_t>(
        std::max<int64_t>(0, pd->get_int_or("latency.sla_ack_fill_p99_us", 0)));
    out.latency_sla.warn_throttle_ms = std::max<int64_t>(0, pd->get_int_or("latency.sla_warn_throttle_ms", 5000));
    out.strategy_metrics_flush_sec = std::max<int>(
        1,
        static_cast<int>(pd->get_int_or("strategy_metrics.flush_sec", cfg.strategy_metrics_flush_sec)));
    return out;
}

void apply_control_params(const ControlParamSnapshot& params,
                          XtLiveLatencyTracker* latency_tracker) {
    if (!latency_tracker) return;
    latency_tracker->set_enabled(params.latency_enabled);
    if (params.latency_stale_ttl_us > 0) {
        latency_tracker->set_stale_ttl_us(params.latency_stale_ttl_us);
    }
}

void advance_frame_side_effects(XtHftRunnerControlContext& ctx,
                                const std::string& venue_label,
                                ControlLoopState& st) {
    xt_live::control::RiskFrameDeps deps;
    deps.risk_monitor  = ctx.risk_monitor;
    deps.account_store = ctx.account_store;
    deps.signals       = &ctx.runtime_shared_state;
    deps.order_gateway = &ctx.live_order_gateway;
    deps.sid_registry  = xt_identity::StrategySidRegistry::global();
    deps.md            = ctx.md_gateway;
    deps.td            = ctx.td_gateway;
    deps.venue_label   = venue_label;
    xt_live::control::advance_risk_frame(deps, st.risk_frame);
}

ControlSnapshot sample_control_snapshot(const XtHftRunnerControlContext& ctx,
                                        const ControlParamSnapshot& params,
                                        ITradeGateway* td) {
    ControlSnapshot out;
    const auto tick_lag_hist     = ctx.strategy_event_loop.drain_tick_lag_hist_stats();
    out.tick_lag_p50_us          = tick_lag_hist.p50_us;
    out.tick_lag_p99_us          = tick_lag_hist.p99_us;
    out.tick_lag_peak_us         = ctx.strategy_event_loop.drain_tick_lag_peak_us();
    out.td_flow_lag_p99_us       = ctx.strategy_event_loop.drain_td_flow_lag_hist_stats().p99_us;
    out.td_event_backlog         = ctx.strategy_event_loop.td_event_backlog();
    out.td_event_peak_backlog    = ctx.strategy_event_loop.td_event_peak_backlog_val();
    out.td_flow_event_lag_us_max = ctx.strategy_event_loop.td_flow_event_lag_us_max_val();
    out.td_flow_event_rejected   = ctx.runtime_shared_state.td_event_drop_cnt.load(std::memory_order_relaxed)
                                 + ctx.runtime_shared_state.flow_event_drop_cnt.load(std::memory_order_relaxed);
    out.trade_count              = ctx.runtime_shared_state.trade_fill_cnt.load(std::memory_order_relaxed);
    out.risk_stats = ctx.live_order_gateway.risk_stats_snapshot();
    out.td_query_stats = td ? td->query_control_stats() : ITradeGateway::QueryControlStats{};
    out.consistency = build_consistency_report(ctx);
    out.latency_stats = ctx.latency_tracker ? ctx.latency_tracker->snapshot() : XtLiveLatencyTracker::Snapshot{};
    if (ctx.account_store) {
        const auto m = ctx.account_store->snapshot_account();
        out.cash       = xt_fp::from_fp(m.available_cash);
        out.equity     = xt_fp::from_fp(m.equity);
        out.unrealized = xt_fp::from_fp(m.unrealized_pnl);
    }

    out.md_in = ctx.runtime_shared_state.md_in_cnt.load(std::memory_order_relaxed);
    out.enq = ctx.runtime_shared_state.enq_cnt.load(std::memory_order_relaxed);
    out.proc = ctx.runtime_shared_state.tick_proc_cnt.load(std::memory_order_relaxed);
    out.tick_drop = ctx.runtime_shared_state.drop_cnt.load(std::memory_order_relaxed);
    out.td_event_drop = ctx.runtime_shared_state.td_event_drop_cnt.load(std::memory_order_relaxed);
    out.flow_event_drop = ctx.runtime_shared_state.flow_event_drop_cnt.load(std::memory_order_relaxed);
    out.critical_td_event_drop = ctx.runtime_shared_state.critical_td_event_drop_cnt.load(std::memory_order_relaxed);
    out.ready = ctx.runtime_shared_state.trading_ready.load(std::memory_order_relaxed);
    out.md_ready = ctx.runtime_shared_state.md_ready.load(std::memory_order_relaxed);
    out.td_ready = ctx.runtime_shared_state.td_ready.load(std::memory_order_relaxed);

    out.last_tick_date = ctx.runtime_shared_state.last_tick_date.load(std::memory_order_acquire);
    out.last_tick_time = ctx.runtime_shared_state.last_tick_time.load(std::memory_order_acquire);
    out.param_ver = params.param_ver;
    out.param_count = params.param_count;
    out.log_stats_enabled = params.log_stats_enabled;

    if (ctx.order_update_drain_q) {
        XtOrderExecUpdate upd;
        while (ctx.order_update_drain_q->pop(upd))
            out.recent_order_updates.push_back(std::move(upd));
    }
    if (ctx.trade_drain_q) {
        XtTradeRecord tr;
        while (ctx.trade_drain_q->pop(tr))
            out.recent_trades.push_back(std::move(tr));
    }
    return out;
}

}

LoopFrame collect_loop_frame(const XtRunnerControlConfig& cfg,
                             XtHftRunnerControlContext& ctx,
                             ControlLoopState& st) {
    LoopFrame out;
    ITradeGateway* td = ctx.td_gateway;
    out.params = read_control_params(cfg, ctx);
    apply_control_params(out.params, ctx.latency_tracker);
    advance_frame_side_effects(ctx, cfg.venue_label, st);
    out.snapshot = sample_control_snapshot(ctx, out.params, td);
    out.now_ms = now_ms_steady();
    return out;
}

}
