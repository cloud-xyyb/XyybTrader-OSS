#include "xt_live/hft/telemetry/XtHftControlReportEmit.h"

#include "xt_live/hft/control/HftRunnerControlState.h"
#include "xt_live/hft/telemetry/HftRunnerControlStrategyPnlEmit.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "XtLogger.h"
#include "latency/XtLiveLatencyTracker.h"
#include "XtLiveOrderGateway.h"
#include "XtLiveParamDomain.h"
#include "xt_report/live/XtLiveReporter.h"
#include "xt_report/live/XtLiveReportAdapter.h"
#include "xt_report/util/XtReportEventBuilder.h"
#include "xt_report/core/XtReportTypes.h"
#include "api/XtRunnerSignals.h"
#include "xt_util/time/XtSteadyClock.h"
#include "xt_live/hft/runtime/XtHftEventLoop.h"
#include "XtFixedPoint.h"
#include "market/XtRuntimeMarketCache.h"
#include "xt_account/api/AccountSnapshot.h"
#include "xt_account/api/IAccountTruthSource.h"
#include "state/AccountStateStore.h"
#include "xt_runtime/order/XtOrderRuntimeBook.h"
#include "api/ITradeGateway.h"
#include "xt_core/session/XtTradingDay.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_live/hft/control/HftCloseoutGate.h"
#include "XtLiveOrderGatewayCfg.h"

#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#include "xt_metrics/XtStrategyPnlAggregator.h"
#include "xt_metrics/XtStrategyPnlBaseline.h"
#include "xt_metrics/XtVenueLabel.h"
#include "xt_ipc/XtShmMdGateway.h"
#include "XtDeclaredSymbolRegistry.h"
#include "xt_risk/config/XtRiskCfg.h"
#endif

namespace xt_live::control {

using namespace xt_live::control::detail;

#ifdef XT_HAVE_METRICS
bool strategy_enabled_value(std::string_view value) {
    std::string s(value);
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return s == "1" || s == "true" || s == "yes" || s == "on";
}

void maybe_emit_strategy_enabled_metrics(const XtHftRunnerControlContext& ctx,
                                         const XtRunnerControlConfig& cfg) {
    if (!ctx.param_domain || cfg.venue_label.empty()) return;

    const auto snap = ctx.param_domain->snapshot();
    const auto venue = xt_util::canonical_venue_label(cfg.venue_label);
    constexpr std::string_view kPrefix = "strategy.";
    constexpr std::string_view kSuffix = ".enabled";

    for (const auto& kv : snap.values) {
        const std::string& key = kv.first;
        if (key.rfind(kPrefix, 0) != 0) continue;
        if (key.size() <= kPrefix.size() + kSuffix.size()) continue;
        if (key.compare(key.size() - kSuffix.size(), kSuffix.size(), kSuffix) != 0) {
            continue;
        }
        const std::string sid = key.substr(
            kPrefix.size(), key.size() - kPrefix.size() - kSuffix.size());
        ct::metrics::XtMetricRegistry::instance().set_strategy_enabled(
            sid, venue, strategy_enabled_value(kv.second));
    }
}
#endif
bool should_flush_strategy_metrics(const ControlParamSnapshot& params,
                                   int64_t now_ms,
                                   ControlLoopState& st) {
    const int flush_sec = std::max(1, params.strategy_metrics_flush_sec);
    const int64_t now_sec = now_ms / 1000;
    if (st.last_strategy_metrics_flush_sec == 0) {
        st.last_strategy_metrics_flush_sec = now_sec;
        return true;
    }
    if (now_sec - st.last_strategy_metrics_flush_sec < flush_sec) return false;
    st.last_strategy_metrics_flush_sec = now_sec;
    return true;
}

void log_strategy_metrics_snapshot(std::string_view sid,
                                   const ct::metrics::XtStrategyMetricsSnapshot& snap,
                                   int window_sec) {
    if (snap.tick_count == 0 || snap.metrics.empty()) return;

    auto find_metric = [&](const char* key, const char* agg) -> std::string {
        if (!key || !agg) return "na";
        for (const auto& m : snap.metrics) {
            if (m.metric != key) continue;
            if (std::strcmp(agg, "last") == 0) return std::to_string(m.last);
            if (std::strcmp(agg, "avg") == 0) {
                return m.count > 0 ? std::to_string(m.sum / static_cast<double>(m.count)) : "0";
            }
            if (std::strcmp(agg, "min") == 0) return std::to_string(m.min);
            if (std::strcmp(agg, "max") == 0) return std::to_string(m.max);
            if (std::strcmp(agg, "sum") == 0) return std::to_string(m.sum);
        }
        return "na";
    };

    const std::string score_last = find_metric("signal.score", "last");
    const std::string score_avg = find_metric("signal.score", "avg");
    const std::string score_min = find_metric("signal.score", "min");
    const std::string score_max = find_metric("signal.score", "max");
    const std::string trigger_buy = find_metric("signal.trigger_buy", "sum");
    const std::string trigger_sell = find_metric("signal.trigger_sell", "sum");
    const std::string trigger_hits = find_metric("signal.trigger_hit", "sum");
    XtLogger::info_m("RT",
                     "stra sid={} window_sec={} tick_n={} score(last/avg/min/max)={}/{}/{}/{} trigger_hits(buy/sell/all)={}/{}/{}",
                     sid,
                     window_sec,
                     snap.tick_count,
                     score_last,
                     score_avg,
                     score_min,
                     score_max,
                     trigger_buy,
                     trigger_sell,
                     trigger_hits);
}

void build_per_symbol_snapshots(const XtHftRunnerControlContext& ctx,
                                std::vector<xt_report::XtPositionRow>& pos_rows) {
    pos_rows.clear();
    if (!ctx.account_store) return;

    const auto codes = ctx.account_store->snapshot_tracked_codes();
    const size_t n = codes.size();
    pos_rows.reserve(n);

    for (const auto& code : codes) {
        if (code.empty()) continue;
        const auto pf = ctx.account_store->snapshot_position(code);

        xt_report::XtPositionRow pos;
        pos.symbol         = code;
        pos.qty            = xt_fp::from_fp(pf.qty);
        pos.long_qty       = xt_fp::from_fp(pf.long_qty);
        pos.short_qty      = xt_fp::from_fp(pf.short_qty);
        pos.avg_price      = xt_fp::from_fp(pf.avg_price);
        pos.mark_price     = xt_fp::from_fp(pf.mark_price);
        pos.unrealized_pnl = xt_fp::from_fp(pf.unrealized_pnl);

        pos.last_price = xt_fp::from_fp(pf.mark_price);
        int64_t total_volume_fp = 0;
        if (ctx.market_cache != nullptr) {

            XtTick q;
            if (ctx.market_cache->snapshot_last_tick(code.c_str(), q)) {
                pos.bid_price = xt_fp::from_fp(q.bid_price_fp[0]);
                pos.ask_price = xt_fp::from_fp(q.ask_price_fp[0]);
                total_volume_fp = q.total_volume_fp;
            } else {
                pos.bid_price = 0.0;
                pos.ask_price = 0.0;
            }
        } else {
            pos.bid_price = 0.0;
            pos.ask_price = 0.0;
        }

        pos.volume = static_cast<int64_t>(xt_fp::from_fp(total_volume_fp));
        pos_rows.push_back(std::move(pos));
    }
}

void maybe_flush_per_symbol_snapshots(const XtHftRunnerControlContext& ctx,
                                      xt_report::XtLiveReporter& live_tracker,
                                      const LoopFrame& frame,
                                      ControlLoopState& st) {
    const int64_t ts_sec = frame.now_ms / 1000;
    if (ts_sec == st.last_per_symbol_snap_sec) return;
    st.last_per_symbol_snap_sec = ts_sec;

    std::vector<xt_report::XtPositionRow> pos_rows;
    build_per_symbol_snapshots(ctx, pos_rows);

    if (ts_sec - st.last_lock_warn_sec >= 60) {
        bool any_lock = false;
        for (const auto& r : pos_rows) {
            if (r.long_qty > 0.0 && r.short_qty > 0.0) {
                any_lock = true;
                XtLogger::warn_m("RT",
                    "LOCKED position symbol={} long={} short={} net={} gross={} "
                    "(broker holds both legs; net-only strategy/risk views hide this)",
                    r.symbol, r.long_qty, r.short_qty, r.qty, r.long_qty + r.short_qty);
            }
        }
        if (any_lock) st.last_lock_warn_sec = ts_sec;
    }

    if (const XtLiveOrderGatewayCfg* gw = ctx.order_gateway_cfg;
        gw && gw->closeout_enabled && !gw->closeout_windows.empty()
        && ts_sec - st.last_closeout_miss_warn_sec >= 1800) {
        std::vector<int> force_list;
        force_list.reserve(gw->closeout_windows.size());
        for (const auto& w : gw->closeout_windows) force_list.push_back(w.force_hhmm);
        if (closeout_boundary_trap(force_list, gw->session_gate_sections)) {
            XtLogger::warn_m("RT",
                "closeout MISCONFIG: a force_flat_hhmm is not inside any session section "
                "(e.g. boundary 230 vs session end 0230) -> closeout will NEVER fire for that "
                "window and positions won't auto-flatten. Fix: set force_flat_hhmm strictly "
                "inside the session (e.g. 229 not 230).");
            st.last_closeout_miss_warn_sec = ts_sec;
        }
    }

    live_tracker.flush_position_snapshot(ts_sec, pos_rows);
}

xt_report::XtAccountTruthRow to_account_truth_row(const AccountTruth& truth) {
    xt_report::XtAccountTruthRow row;
    row.wallet_balance = xt_fp::from_fp(truth.wallet_balance_fp);
    row.position_value = xt_fp::from_fp(truth.position_value_fp);
    row.unrealized_pnl = xt_fp::from_fp(truth.unrealized_pnl_fp);
    row.fees_today = xt_fp::from_fp(truth.fees_today_fp);
    row.external_cashflow_today = xt_fp::from_fp(truth.external_cashflow_today_fp);
    row.truth_ts_ns = truth.ts_ns;
    row.trading_day = truth.trading_day_id;
    return row;
}

void emit_per_symbol_truth_gauges(std::string_view venue,
                                  std::string_view account,
                                  const std::vector<PerSymbolTruth>& per_symbol,
                                  std::vector<std::string>& last_emitted_symbols) {
    auto& reg = ct::metrics::XtMetricRegistry::instance();
    for (const auto& stale : last_emitted_symbols) {
        const bool still_present =
            std::any_of(per_symbol.begin(), per_symbol.end(),
                        [&](const PerSymbolTruth& ps) { return ps.symbol == stale; });
        if (still_present) continue;
        reg.set_position_avg_price_fp(venue, stale, 0);
        reg.set_position_mark_price_fp(venue, stale, 0);
        reg.set_position_unrealized_pnl_fp(venue, stale, 0);
        reg.set_position_margin_fp(venue, stale, 0);
        reg.set_broker_position_qty_fp(venue, account, stale, 0);
        reg.set_broker_position_avg_price_fp(venue, account, stale, 0);
        reg.set_broker_position_mark_price_fp(venue, account, stale, 0);
        reg.set_broker_position_unrealized_pnl_fp(venue, account, stale, 0);
        reg.set_broker_position_margin_fp(venue, account, stale, 0);
    }
    last_emitted_symbols.clear();
    for (const auto& ps : per_symbol) {
        if (ps.symbol.empty()) continue;
        reg.set_position_avg_price_fp(venue, ps.symbol, ps.avg_price_fp);
        reg.set_position_mark_price_fp(venue, ps.symbol, ps.mark_price_fp);
        reg.set_position_unrealized_pnl_fp(venue, ps.symbol, ps.unrealized_pnl_fp);
        reg.set_position_margin_fp(venue, ps.symbol, ps.margin_fp);
        reg.set_broker_position_qty_fp(venue, account, ps.symbol, ps.qty_fp);
        reg.set_broker_position_avg_price_fp(venue, account, ps.symbol, ps.avg_price_fp);
        reg.set_broker_position_mark_price_fp(venue, account, ps.symbol, ps.mark_price_fp);
        reg.set_broker_position_unrealized_pnl_fp(venue, account, ps.symbol,
                                                  ps.unrealized_pnl_fp);
        reg.set_broker_position_margin_fp(venue, account, ps.symbol, ps.margin_fp);
        last_emitted_symbols.push_back(ps.symbol);
    }
}

void maybe_flush_account_truth_snapshot(XtHftRunnerControlContext& ctx,
                                        const XtRunnerControlConfig& cfg,
                                        xt_report::XtLiveReporter& live_tracker,
                                        const LoopFrame& frame,
                                        ControlLoopState& st) {
    if (!ctx.account_truth_source) return;
    if (!ctx.runtime_shared_state.has_account_snapshot.load(std::memory_order_acquire)) return;

    const int64_t ts_sec = frame.now_ms / 1000;
    if (ts_sec == st.last_account_truth_snap_sec) return;

    ctx.account_truth_source->poll();
    AccountTruth truth = ctx.account_truth_source->fetch_snapshot();
    if (truth.trading_day_id > 0 &&
        st.last_account_truth_day > 0 &&
        truth.trading_day_id != st.last_account_truth_day) {
        ctx.account_truth_source->on_trading_day_rollover(
            st.last_account_truth_day, truth.trading_day_id);
        ctx.account_truth_source->poll();
        truth = ctx.account_truth_source->fetch_snapshot();
    }
    if (truth.trading_day_id > 0) {
        st.last_account_truth_day = truth.trading_day_id;
    }

    live_tracker.flush_account_truth(ts_sec, to_account_truth_row(truth));

    if (!cfg.venue_label.empty()) {

        const auto venue = xt_util::canonical_venue_label(cfg.venue_label);

        ct::metrics::XtMetricRegistry::AccountTruthSample sample{};
        sample.wallet_balance          = xt_fp::from_fp(truth.wallet_balance_fp);
        sample.equity                  = xt_fp::from_fp(account_truth_equity_fp(truth));
        sample.position_value          = xt_fp::from_fp(truth.position_value_fp);
        sample.unrealized_pnl          = xt_fp::from_fp(truth.unrealized_pnl_fp);
        sample.fees_today              = xt_fp::from_fp(truth.fees_today_fp);
        sample.external_cashflow_today = xt_fp::from_fp(truth.external_cashflow_today_fp);
        sample.trading_day_id          = static_cast<double>(truth.trading_day_id);

        sample.available_balance = xt_fp::from_fp(truth.available_balance_fp);
        sample.maint_margin      = xt_fp::from_fp(truth.maint_margin_fp);
        sample.margin_ratio      = truth.total_margin_balance_fp > 0
            ? static_cast<double>(truth.maint_margin_fp) /
              static_cast<double>(truth.total_margin_balance_fp)
            : 0.0;

        const std::string account = cfg.account_label.empty() ? "_default" : cfg.account_label;
        auto& reg = ct::metrics::XtMetricRegistry::instance();
        reg.set_account_truth_snapshot(venue, account, sample, "runtime");

        if (truth.total_margin_balance_fp > 0) {
            reg.set_venue_equity_raw(venue, account,
                                     xt_fp::from_fp(truth.total_margin_balance_fp));
        }

        emit_per_symbol_truth_gauges(venue, account, truth.per_symbol,
                                     st.last_truth_symbols);

        for (const auto& [code, mark_fp] : truth.runtime_per_symbol_marks_fp) {
            if (code.empty()) continue;
            reg.set_account_store_mark_price_fp(venue, code, mark_fp);
        }

        if (ctx.account_store && truth.per_symbol_is_broker_truth) {
            const auto store_account = ctx.account_store->snapshot_account();
            int64_t broker_unrealized_sum_fp = 0;
            for (const auto& code : ctx.account_store->snapshot_tracked_codes()) {
                if (code.empty()) continue;
                const auto pos = ctx.account_store->snapshot_position(code);
                int64_t broker_qty_fp = 0;
                int64_t broker_unrealized_fp = 0;
                for (const auto& ps : truth.per_symbol) {
                    if (ps.symbol == code) {
                        broker_qty_fp = ps.qty_fp;
                        broker_unrealized_fp = ps.unrealized_pnl_fp;
                        break;
                    }
                }
                const double qty_div = std::abs(
                    xt_fp::from_fp(pos.qty - broker_qty_fp));
                reg.set_state_divergence("qty", code, qty_div);
                const double sym_unrealized_div = std::abs(
                    xt_fp::from_fp(pos.unrealized_pnl - broker_unrealized_fp));
                reg.set_state_divergence("unrealized_pnl", code, sym_unrealized_div);
            }
            for (const auto& ps : truth.per_symbol) {
                broker_unrealized_sum_fp += ps.unrealized_pnl_fp;
            }
            const double agg_unrealized_div = std::abs(
                xt_fp::from_fp(store_account.unrealized_pnl - broker_unrealized_sum_fp));
            reg.set_state_divergence("unrealized_pnl", "_aggregate", agg_unrealized_div);
        }

        reg.inc_state_divergence_emit_heartbeat();
    }
    st.last_account_truth_snap_sec = ts_sec;
}

void maybe_init_reporter_baseline(XtHftRunnerControlContext& ctx,
                                   xt_report::XtLiveReporter& live_tracker,
                                   const LoopFrame& frame,
                                   ControlLoopState& st) {

    if (!st.reporter_baseline_set &&
        ctx.runtime_shared_state.trading_ready.load(std::memory_order_acquire) &&
        ctx.runtime_shared_state.has_account_snapshot.load(std::memory_order_acquire) &&
        std::isfinite(frame.snapshot.equity) && frame.snapshot.equity > 1.0) {
        live_tracker.set_baseline_equity(frame.snapshot.equity);
        st.reporter_baseline_set = true;
        XtLogger::info_m("RT",
                          "live reporter baseline set: start_equity={:.6f} "
                          "(trading_ready=true, has_account_snapshot=true)",
                          frame.snapshot.equity);
    }
}

double emit_live_fills_and_get_turnover(xt_report::XtLiveReporter& live_tracker,
                                         const LoopFrame& frame) {

    const int64_t fallback_wall_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    std::vector<xt_report::XtLiveFillEvent> live_fills;
    xt_report::build_live_fill_events(frame.snapshot.recent_trades, fallback_wall_ms, live_fills);
    double interval_turnover = 0.0;
    for (const auto& fill : live_fills) {
        auto fill_row = xt_report::XtReportEventBuilder::build_fill(fill);
        const double to = std::fabs(fill.qty * fill.price);
        live_tracker.on_fill(fill_row, fill.fee, fill.slippage);
        interval_turnover += to;
    }
    return interval_turnover;
}

void build_kpi_ratios(const ControlSnapshot& snap, xt_report::OrderRatios& ratios) {
    if (snap.risk_stats.submit_attempts > 0) {
        const double d = static_cast<double>(snap.risk_stats.submit_attempts);
        ratios.fill_ratio   = static_cast<double>(snap.risk_stats.submit_success) / d;
        const uint64_t rejects = snap.risk_stats.submit_attempts > snap.risk_stats.submit_success
                                     ? snap.risk_stats.submit_attempts - snap.risk_stats.submit_success : 0;
        ratios.reject_ratio = static_cast<double>(rejects) / d;
    }
    if (snap.risk_stats.cancel_attempts > 0) {
        ratios.cancel_ratio = static_cast<double>(snap.risk_stats.cancel_success) /
                              static_cast<double>(snap.risk_stats.cancel_attempts);
    }

    if (snap.risk_stats.submit_success > 0) {
        ratios.order_cancel_ratio =
            static_cast<double>(snap.risk_stats.canceled_terminal_total) /
            static_cast<double>(snap.risk_stats.submit_success);
    }
}

void build_kpi_latency(const ControlSnapshot& snap, xt_report::LatencyStats& latency) {
    latency.submit_to_ack_us_avg  = snap.latency_stats.submit_to_ack.avg_us;
    latency.submit_to_ack_us_p50  = snap.latency_stats.submit_to_ack.p50_us;
    latency.submit_to_ack_us_p90  = snap.latency_stats.submit_to_ack.p90_us;
    latency.submit_to_ack_us_p95  = snap.latency_stats.submit_to_ack.p95_us;
    latency.submit_to_ack_us_p99  = snap.latency_stats.submit_to_ack.p99_us;
    latency.submit_to_ack_paired_us_avg = snap.latency_stats.submit_to_ack_paired.avg_us;
    latency.submit_to_ack_paired_us_p50 = snap.latency_stats.submit_to_ack_paired.p50_us;
    latency.submit_to_ack_paired_us_p90 = snap.latency_stats.submit_to_ack_paired.p90_us;
    latency.submit_to_ack_paired_us_p95 = snap.latency_stats.submit_to_ack_paired.p95_us;
    latency.submit_to_ack_paired_us_p99 = snap.latency_stats.submit_to_ack_paired.p99_us;
    static int64_t last_empty_paired_warn_ms = 0;
    if (snap.latency_stats.submit_to_ack.samples > 0 &&
        snap.latency_stats.submit_to_trade_ack.samples > 0 &&
        snap.latency_stats.submit_to_ack_paired.samples == 0) {
        const int64_t now_ms = static_cast<int64_t>(XtSteadyClock::steady_now_ms());
        if (last_empty_paired_warn_ms == 0 || now_ms - last_empty_paired_warn_ms >= 300000) {
            last_empty_paired_warn_ms = now_ms;
            XtLogger::warn_m("RT",
                "paired histogram empty in rolling window - single-path fallback only "
                "(submit_to_ack_samples={} trade_ack_samples={})",
                snap.latency_stats.submit_to_ack.samples,
                snap.latency_stats.submit_to_trade_ack.samples);
        }
    }

    latency.submit_to_trade_ack_us_avg = snap.latency_stats.submit_to_trade_ack.avg_us;
    latency.submit_to_trade_ack_us_p50 = snap.latency_stats.submit_to_trade_ack.p50_us;
    latency.submit_to_trade_ack_us_p90 = snap.latency_stats.submit_to_trade_ack.p90_us;
    latency.submit_to_trade_ack_us_p95 = snap.latency_stats.submit_to_trade_ack.p95_us;
    latency.submit_to_trade_ack_us_p99 = snap.latency_stats.submit_to_trade_ack.p99_us;

    latency.uds_first_savings_us_avg = snap.latency_stats.uds_first_savings.avg_us;
    latency.uds_first_savings_us_p50 = snap.latency_stats.uds_first_savings.p50_us;
    latency.uds_first_savings_us_p90 = snap.latency_stats.uds_first_savings.p90_us;
    latency.uds_first_savings_us_p95 = snap.latency_stats.uds_first_savings.p95_us;
    latency.uds_first_savings_us_p99 = snap.latency_stats.uds_first_savings.p99_us;
    latency.submit_to_fill_us_avg = snap.latency_stats.submit_to_fill.avg_us;
    latency.submit_to_fill_us_p50 = snap.latency_stats.submit_to_fill.p50_us;
    latency.submit_to_fill_us_p90 = snap.latency_stats.submit_to_fill.p90_us;
    latency.submit_to_fill_us_p95 = snap.latency_stats.submit_to_fill.p95_us;
    latency.submit_to_fill_us_p99 = snap.latency_stats.submit_to_fill.p99_us;
    latency.strategy_us_avg       = snap.latency_stats.strategy_processing.avg_us;
    latency.strategy_us_p50       = snap.latency_stats.strategy_processing.p50_us;
    latency.strategy_us_p90       = snap.latency_stats.strategy_processing.p90_us;
    latency.strategy_us_p99       = snap.latency_stats.strategy_processing.p99_us;
    latency.local_submit_us_avg   = snap.latency_stats.submit_wire.avg_us;
    latency.local_submit_us_p50   = snap.latency_stats.submit_wire.p50_us;
    latency.local_submit_us_p90   = snap.latency_stats.submit_wire.p90_us;
    latency.local_submit_us_p99   = snap.latency_stats.submit_wire.p99_us;
    latency.ack_path_us_avg       = snap.latency_stats.ack_path.avg_us;
    latency.ack_path_us_p50       = snap.latency_stats.ack_path.p50_us;
    latency.ack_path_us_p90       = snap.latency_stats.ack_path.p90_us;
    latency.ack_path_us_p99       = snap.latency_stats.ack_path.p99_us;
    latency.fill_after_ack_us_avg = snap.latency_stats.ack_to_fill.avg_us;
    latency.fill_after_ack_us_p50 = snap.latency_stats.ack_to_fill.p50_us;
    latency.fill_after_ack_us_p90 = snap.latency_stats.ack_to_fill.p90_us;
    latency.fill_after_ack_us_p99 = snap.latency_stats.ack_to_fill.p99_us;
    latency.event_to_ack_us_avg   = snap.latency_stats.event_to_ack.avg_us;
    latency.event_to_ack_us_p50   = snap.latency_stats.event_to_ack.p50_us;
    latency.event_to_ack_us_p90   = snap.latency_stats.event_to_ack.p90_us;
    latency.event_to_ack_us_p99   = snap.latency_stats.event_to_ack.p99_us;
    latency.event_to_fill_us_avg  = snap.latency_stats.event_to_fill.avg_us;
    latency.event_to_fill_us_p50  = snap.latency_stats.event_to_fill.p50_us;
    latency.event_to_fill_us_p90  = snap.latency_stats.event_to_fill.p90_us;
    latency.event_to_fill_us_p99  = snap.latency_stats.event_to_fill.p99_us;

    latency.submit_to_ack_us_samples  = snap.latency_stats.submit_to_ack.samples;
    latency.fill_after_ack_us_samples = snap.latency_stats.ack_to_fill.samples;
    latency.local_submit_us_samples   = snap.latency_stats.submit_wire.samples;
    latency.submit_to_ack_paired_us_samples = snap.latency_stats.submit_to_ack_paired.samples;
    latency.submit_to_trade_ack_us_samples  = snap.latency_stats.submit_to_trade_ack.samples;
    latency.uds_first_savings_us_samples    = snap.latency_stats.uds_first_savings.samples;
    latency.submit_to_fill_us_samples       = snap.latency_stats.submit_to_fill.samples;
    latency.strategy_us_samples             = snap.latency_stats.strategy_processing.samples;
    latency.ack_path_us_samples             = snap.latency_stats.ack_path.samples;
    latency.event_to_ack_us_samples         = snap.latency_stats.event_to_ack.samples;
    latency.event_to_fill_us_samples        = snap.latency_stats.event_to_fill.samples;

    latency.risk_block_count      = snap.risk_stats.total_rejects();
    latency.md_gap_ms_p99         = static_cast<double>(snap.tick_lag_p99_us) / 1000.0;
    latency.md_gap_ms_peak        = static_cast<double>(snap.tick_lag_peak_us) / 1000.0;
    latency.event_drop            = snap.tick_drop + snap.td_flow_event_rejected;

    latency.queue_depth_p99       = static_cast<double>(snap.td_event_backlog);
}

void emit_td_drop_metrics(std::string_view venue,
                          const XtRunnerSignals& signals,
                          ControlLoopState& st) {
#ifdef XT_HAVE_METRICS
    auto& reg = ct::metrics::XtMetricRegistry::instance();

    const uint64_t possnap_drop_total =
        signals.td_drop_position_cnt.load(std::memory_order_relaxed);
    const uint64_t possnap_drop_delta =
        possnap_drop_total >= st.last_td_position_snapshot_drop_prom_total
            ? (possnap_drop_total - st.last_td_position_snapshot_drop_prom_total)
            : possnap_drop_total;
    reg.inc_td_position_snapshot_drop_total(venue, possnap_drop_delta);
    st.last_td_position_snapshot_drop_prom_total = possnap_drop_total;

    const uint64_t ctrl_drop_total =
        signals.td_drop_control_cnt.load(std::memory_order_relaxed);
    const uint64_t ctrl_drop_delta =
        ctrl_drop_total >= st.last_td_control_event_drop_prom_total
            ? (ctrl_drop_total - st.last_td_control_event_drop_prom_total)
            : ctrl_drop_total;
    reg.inc_td_control_event_drop_total(venue, ctrl_drop_delta);
    st.last_td_control_event_drop_prom_total = ctrl_drop_total;

    const uint64_t other_drop_total =
        signals.td_drop_other_cnt.load(std::memory_order_relaxed);
    const uint64_t other_drop_delta =
        other_drop_total >= st.last_td_other_event_drop_prom_total
            ? (other_drop_total - st.last_td_other_event_drop_prom_total)
            : other_drop_total;
    reg.inc_td_other_event_drop_total(venue, other_drop_delta);
    st.last_td_other_event_drop_prom_total = other_drop_total;
#else
    (void)venue;
    (void)signals;
    (void)st;
#endif
}

void emit_unmatched_drop_metrics(const XtRunnerSignals& signals,
                                 ControlLoopState& st) {
#ifdef XT_HAVE_METRICS
    auto& reg = ct::metrics::XtMetricRegistry::instance();

    const auto emit_delta = [&](uint64_t total, uint64_t& last, auto&& inc) {
        const uint64_t delta = total >= last ? (total - last) : total;
        inc(delta);
        last = total;
    };
    emit_delta(signals.multi_strategy_dropped_fill_empty_sid_cnt.load(std::memory_order_relaxed),
               st.last_dropped_fill_empty_prom_total,
               [&](uint64_t d) { reg.inc_unmatched_drop_total("fill", "empty_sid", d); });
    emit_delta(signals.multi_strategy_dropped_fill_unknown_sid_cnt.load(std::memory_order_relaxed),
               st.last_dropped_fill_unknown_prom_total,
               [&](uint64_t d) { reg.inc_unmatched_drop_total("fill", "unknown_sid", d); });
    emit_delta(signals.multi_strategy_dropped_order_update_empty_sid_cnt.load(std::memory_order_relaxed),
               st.last_dropped_order_empty_prom_total,
               [&](uint64_t d) { reg.inc_unmatched_drop_total("order_rtn", "empty_sid", d); });
    emit_delta(signals.multi_strategy_dropped_order_update_unknown_sid_cnt.load(std::memory_order_relaxed),
               st.last_dropped_order_unknown_prom_total,
               [&](uint64_t d) { reg.inc_unmatched_drop_total("order_rtn", "unknown_sid", d); });
    emit_delta(signals.engine_sid_fallback_recovered_cnt.load(std::memory_order_relaxed),
               st.last_sid_fallback_prom_total,
               [&](uint64_t d) { reg.inc_engine_recovery_total("sid_fallback", d); });
    emit_delta(signals.engine_lost_terminal_reconciled_cnt.load(std::memory_order_relaxed),
               st.last_lost_terminal_prom_total,
               [&](uint64_t d) { reg.inc_engine_recovery_total("lost_terminal", d); });
#else
    (void)signals;
    (void)st;
#endif
}

void emit_live_reports(const XtRunnerControlConfig& cfg,
                       XtHftRunnerControlContext& ctx,
                       xt_report::XtLiveReporter& live_tracker,
                       const LoopFrame& frame,
                       ControlLoopState& st) {

    live_tracker.on_equity(frame.snapshot.equity);
    maybe_init_reporter_baseline(ctx, live_tracker, frame, st);

    maybe_flush_per_symbol_snapshots(ctx, live_tracker, frame, st);
    maybe_flush_account_truth_snapshot(ctx, cfg, live_tracker, frame, st);

    const double interval_turnover = emit_live_fills_and_get_turnover(live_tracker, frame);

    xt_report::OrderRatios ratios;
    xt_report::LatencyStats latency;
    build_kpi_ratios(frame.snapshot, ratios);
    build_kpi_latency(frame.snapshot, latency);

    st.kpi_window_tick_lag_us_peak =
        std::max(st.kpi_window_tick_lag_us_peak, frame.snapshot.tick_lag_peak_us);
    st.kpi_window_tick_lag_us_p99 =
        std::max(st.kpi_window_tick_lag_us_p99, frame.snapshot.tick_lag_p99_us);
    xt_report::LatencyStats emit_latency = latency;
    emit_latency.md_gap_ms_p99 =
        static_cast<double>(st.kpi_window_tick_lag_us_p99) / 1000.0;
    emit_latency.md_gap_ms_peak =
        static_cast<double>(st.kpi_window_tick_lag_us_peak) / 1000.0;

    const bool kill_switch_active = ctx.runtime_shared_state.kill_switch.load(std::memory_order_acquire);
    const bool kpi_flushed =
        live_tracker.try_flush_kpi(frame.now_ms, interval_turnover, ratios, emit_latency, kill_switch_active);

#ifdef XT_HAVE_METRICS
    if (!cfg.venue_label.empty()) {
        const auto venue = xt_util::canonical_venue_label(cfg.venue_label);
        auto& reg = ct::metrics::XtMetricRegistry::instance();
        reg.set_kill_switch_active(venue, kill_switch_active);
        reg.set_td_ready(
            venue, ctx.runtime_shared_state.td_ready.load(std::memory_order_acquire));
        reg.set_trading_ready(
            venue, ctx.runtime_shared_state.trading_ready.load(std::memory_order_acquire));
        if (kpi_flushed) {

            ct::metrics::XtMetricRegistry::RuntimeKpiSnapshot kpi_sample{};
            kpi_sample.md_gap_ms_p99 = emit_latency.md_gap_ms_p99;
            kpi_sample.md_gap_ms_peak = emit_latency.md_gap_ms_peak;
            const auto last_md_tick_ms =
                ctx.runtime_shared_state.last_tick_steady_ms.load(std::memory_order_relaxed);
            kpi_sample.has_md_age_ms = last_md_tick_ms > 0;
            if (kpi_sample.has_md_age_ms) {
                const uint32_t now_ms = static_cast<uint32_t>(frame.now_ms);
                kpi_sample.md_age_ms = static_cast<double>(
                    std::max<uint32_t>(1, now_ms - last_md_tick_ms));
            }
            kpi_sample.event_drop = static_cast<double>(emit_latency.event_drop);
            kpi_sample.queue_depth_p99 = emit_latency.queue_depth_p99;
            kpi_sample.risk_block_count = static_cast<double>(emit_latency.risk_block_count);
            reg.set_runtime_kpi_snapshot(
                "_runtime", "_runtime", venue, "_all", kpi_sample);

            const uint64_t risk_rejects_total =
                static_cast<uint64_t>(kpi_sample.risk_block_count);
            const uint64_t risk_rejects_delta =
                risk_rejects_total >= st.last_risk_rejects_prom_total
                    ? (risk_rejects_total - st.last_risk_rejects_prom_total)
                    : risk_rejects_total;
            reg.inc_risk_rejects_total(venue, risk_rejects_delta);
            st.last_risk_rejects_prom_total = risk_rejects_total;
        }
        const uint64_t md_total = frame.snapshot.md_in;
        const uint64_t md_delta = md_total >= st.last_md_ticks_prom_total
            ? (md_total - st.last_md_ticks_prom_total)
            : md_total;
        reg.inc_md_ticks_total(venue, md_delta);
        st.last_md_ticks_prom_total = md_total;

        if (ctx.shm_md_gateway) {
            const auto& ring = ctx.shm_md_gateway->ring_name();
            reg.set_shm_md_attach_failing(ring, ctx.shm_md_gateway->attach_failing());
            reg.set_shm_md_attach_fail_count(ring, ctx.shm_md_gateway->attach_fail_count());
        }

        const uint64_t reap_total = ctx.strategy_event_loop.engine_orphan_reap_cancel_count();
        const uint64_t reap_delta = reap_total >= st.last_orphan_reap_prom_total
            ? (reap_total - st.last_orphan_reap_prom_total)
            : reap_total;
        reg.inc_recovery_orphan_reaped_total(venue, reap_delta);
        st.last_orphan_reap_prom_total = reap_total;

        emit_td_drop_metrics(venue, ctx.runtime_shared_state, st);

        emit_unmatched_drop_metrics(ctx.runtime_shared_state, st);

        const auto emit_guard_fail = [&](const char* stage, uint64_t total, uint64_t& last) {
            const uint64_t delta = total >= last ? (total - last) : total;
            reg.inc_hft_worker_guard_fail_total(venue, stage, delta);
            last = total;
        };
        emit_guard_fail("body", ctx.strategy_event_loop.worker_body_exception_count(),
                        st.last_worker_guard_body_prom_total);
        emit_guard_fail("normalize", ctx.strategy_event_loop.normalize_fail_count(),
                        st.last_worker_guard_normalize_prom_total);
        emit_guard_fail("record", ctx.strategy_event_loop.record_fail_count(),
                        st.last_worker_guard_record_prom_total);

        reg.set_open_orders(venue, ctx.strategy_event_loop.engine_open_order_count());
        maybe_emit_strategy_enabled_metrics(ctx, cfg);
    }
#endif
    if (kpi_flushed) {
        st.kpi_window_tick_lag_us_peak = 0;
        st.kpi_window_tick_lag_us_p99 = 0;
    }

#ifdef XT_HAVE_METRICS
    if (!cfg.venue_label.empty() && live_tracker.equity_initialized()) {
        ct::metrics::XtMetricRegistry::instance().set_pnl_net(
            cfg.venue_label,
            cfg.account_label.empty() ? "_default" : cfg.account_label,
            live_tracker.session_pnl_net());
    }

    if (ctx.strategy_pnl_aggr && !cfg.venue_label.empty()) {
        const auto rows = ctx.strategy_pnl_aggr->snapshot();
        xt_live::control::emit_strategy_pnl_metrics(
            cfg, ctx.strategy_pnl_baseline, ctx.declared_registry,
            rows, st.last_baseline_day_id);

        {
            const XtRiskCfg risk_cfg_snap = ctx.live_order_gateway.risk_config();
            for (const auto& r : rows) {
                const std::string& sid = r.sid.empty() ? std::string("_default") : r.sid;
                double max_pos_qty = risk_cfg_snap.max_pos_qty;
                auto it = risk_cfg_snap.per_strategy_max_pos_qty_by_sid_string.find(sid);
                if (it != risk_cfg_snap.per_strategy_max_pos_qty_by_sid_string.end()) {
                    max_pos_qty = it->second;
                }
                ct::metrics::XtMetricRegistry::RiskLimitsSample sample{};
                sample.max_order_qty           = risk_cfg_snap.max_order_qty;
                sample.max_pos_qty             = max_pos_qty;
                sample.max_drawdown_pct        = 0.0;
                sample.max_position_value_usd  = 0.0;
                sample.max_orders_per_min      = 0.0;
                sample.allow_short             = risk_cfg_snap.allow_short;
                ct::metrics::XtMetricRegistry::instance().set_strategy_limits(
                    sid, sid  , cfg.venue_label, sample);
            }
        }
    }
#endif
}

void flush_strategy_metrics_if_needed(const XtRunnerControlConfig& cfg,
                                      XtHftRunnerControlContext& ctx,
                                      xt_report::XtLiveReporter& live_tracker,
                                      const LoopFrame& frame,
                                      ControlLoopState& st) {
    if (!should_flush_strategy_metrics(frame.params, frame.now_ms, st)) return;

    (void)live_tracker;
    const auto per_sid_metrics = ctx.strategy_event_loop.drain_all_strategy_metrics();
    for (const auto& [sid, snap] : per_sid_metrics) {
        if (snap.tick_count == 0 && snap.metrics.empty()) continue;
        if (frame.snapshot.log_stats_enabled) {
            log_strategy_metrics_snapshot(sid, snap, frame.params.strategy_metrics_flush_sec);
        }
#ifdef XT_HAVE_METRICS
        if (!cfg.venue_label.empty()) {
            const std::string_view strategy = sid.empty() ? std::string_view("_default")
                                                           : std::string_view(sid);
            auto& reg = ct::metrics::XtMetricRegistry::instance();
            for (const auto& m : snap.metrics) {
                reg.set_strategy_custom_metric(strategy, m.metric, m.last);
            }
        }
#endif
    }
}

}
