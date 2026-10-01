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
#include "xt_report/live/XtLiveReportAdapter.h"   // build_live_fill_events
#include "xt_report/util/XtReportEventBuilder.h"
#include "xt_report/core/XtReportTypes.h"
#include "api/XtRunnerSignals.h"
#include "xt_util/time/XtSteadyClock.h"
#include "xt_live/hft/runtime/XtHftEventLoop.h"
#include "XtFixedPoint.h"
#include "market/XtRuntimeMarketCache.h"
#include "xt_account/api/AccountSnapshot.h"
#include "xt_account/api/IAccountTruthSource.h"  // AccountTruth / IAccountTruthSource / account_truth_equity_fp
#include "state/AccountStateStore.h"
#include "xt_runtime/order/XtOrderRuntimeBook.h"
#include "api/ITradeGateway.h"
#include "xt_core/session/XtTradingDay.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_live/hft/control/HftCloseoutGate.h"            // closeout_boundary_trap
#include "XtLiveOrderGatewayCfg.h"      // closeout_enabled / closeout_windows / session_gate_sections

// XT_HAVE_METRICS PUBLIC compile def 由 xt_live_telemetry 透传过来
#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#include "xt_metrics/XtStrategyPnlAggregator.h"
#include "xt_metrics/XtStrategyPnlBaseline.h"
#include "xt_metrics/XtVenueLabel.h"                // canonical venue label
#include "xt_ipc/XtShmMdGateway.h"                  // shm attach 观测 accessor（T1.141①）
#include "XtDeclaredSymbolRegistry.h"
#include "xt_risk/config/XtRiskCfg.h"
#endif

namespace xt_live::control {

// 共享 struct（detail ns）可不带限定引用。
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
        return true;  // 首次 flush
    }
    if (now_sec - st.last_strategy_metrics_flush_sec < flush_sec) return false;
    st.last_strategy_metrics_flush_sec = now_sec;
    return true;
}

// 输出策略指标摘要日志：从 metrics vector 中按 key+agg 查找指定维度。
// 线性扫描 metrics（通常 < 20 个），不需要建索引。
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

// 收集 per-symbol 持仓快照（含 bid/ask/volume 回填），供 1Hz position snapshot。
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
        // XtPositionRow 同时承载 price fields；bid/ask 优先从 market_cache 回填。
        pos.last_price = xt_fp::from_fp(pf.mark_price);
        int64_t total_volume_fp = 0;
        if (ctx.market_cache != nullptr) {
            // off-worker（control 线程）读：经 HP 快照一次取全 tick，避免直读 cache 的 data race
            // （worker on_tick 并发写 last_ticks_ → rehash-during-find UB + torn read）。
            XtTick q;
            if (ctx.market_cache->snapshot_last_tick(code.c_str(), q)) {
                pos.bid_price = xt_fp::from_fp(q.bid_price_fp[0]);
                pos.ask_price = xt_fp::from_fp(q.ask_price_fp[0]);
                total_volume_fp = q.total_volume_fp;  // 未命中 → 保持 0 fallback
            } else {
                pos.bid_price = 0.0;
                pos.ask_price = 0.0;
            }
        } else {
            pos.bid_price = 0.0;
            pos.ask_price = 0.0;
        }
        // volume 在 XtPositionRow 是 int64 (手数, 不反 fp), CTP 累计成交量 fp ×1e6 反 = 手数
        pos.volume = static_cast<int64_t>(xt_fp::from_fp(total_volume_fp));
        pos_rows.push_back(std::move(pos));
    }
}

// 每秒最多写一次 per-symbol 快照。
void maybe_flush_per_symbol_snapshots(const XtHftRunnerControlContext& ctx,
                                      xt_report::XtLiveReporter& live_tracker,
                                      const LoopFrame& frame,
                                      ControlLoopState& st) {
    const int64_t ts_sec = frame.now_ms / 1000;
    if (ts_sec == st.last_per_symbol_snap_sec) return;
    st.last_per_symbol_snap_sec = ts_sec;

    std::vector<xt_report::XtPositionRow> pos_rows;
    build_per_symbol_snapshots(ctx, pos_rows);
    // broker 同时持多空腿 = CTP 锁仓（毛仓≠净仓）。strategy/risk 只读净仓，
    // 锁仓时 net≈0 把毛敞口藏住 → 此冷路径是唯一能看见毛锁仓的地方。60s 节流避免刷屏。
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
    // closeout "边界陷阱"：某 force_flat_hhmm 不落在任何 session 半开区间内
    // （如右端点 0230 ∉ [2100,0230)）时，该窗口永不触发。纯配置判定：
    // 配置正确时恒静默，错配时每 30min 提醒一次。
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
    // position snapshot 兼容承载 last/bid/ask/volume 字段，不再单独 flush price。
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

    // poll() 承担 source 的外部拉取（binance income REST），fetch_snapshot() 只读缓存；
    // 每次 fetch 前配一次 poll，rollover 后的重取窗口才不会落空（source 自带节流）。
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

    // Prometheus 直接 emit account_truth 字段；external_cashflow_today 来自
    // BinanceAccountTruthSource 的 income 缓存（poll() 刷新），这里只读不做 IO。
    if (!cfg.venue_label.empty()) {
        // canonical venue label 单源（消除 cfg.venue_label vs md_provider 分叉）
        const auto venue = xt_util::canonical_venue_label(cfg.venue_label);

        ct::metrics::XtMetricRegistry::AccountTruthSample sample{};
        sample.wallet_balance          = xt_fp::from_fp(truth.wallet_balance_fp);
        sample.equity                  = xt_fp::from_fp(account_truth_equity_fp(truth));
        sample.position_value          = xt_fp::from_fp(truth.position_value_fp);
        sample.unrealized_pnl          = xt_fp::from_fp(truth.unrealized_pnl_fp);
        sample.fees_today              = xt_fp::from_fp(truth.fees_today_fp);
        sample.external_cashflow_today = xt_fp::from_fp(truth.external_cashflow_today_fp);
        sample.trading_day_id          = static_cast<double>(truth.trading_day_id);
        // account_truth gap fields。
        sample.available_balance = xt_fp::from_fp(truth.available_balance_fp);
        sample.maint_margin      = xt_fp::from_fp(truth.maint_margin_fp);
        sample.margin_ratio      = truth.total_margin_balance_fp > 0
            ? static_cast<double>(truth.maint_margin_fp) /
              static_cast<double>(truth.total_margin_balance_fp)
            : 0.0;

        const std::string account = cfg.account_label.empty() ? "_default" : cfg.account_label;
        auto& reg = ct::metrics::XtMetricRegistry::instance();
        reg.set_account_truth_snapshot(venue, account, sample, "runtime");

        // venue equity raw 取 venue 真值 total_margin_balance：binance REST
        // totalMarginBalance / CTP TD Balance（动态权益，overlay 透传）。
        // 填充后才 emit；XTP/CTA/SEL 不提供该字段，保持 0 时跳过，避免污染
        // cross-check。risk/latch 不消费该 metric。
        if (truth.total_margin_balance_fp > 0) {
            reg.set_venue_equity_raw(venue, account,
                                     xt_fp::from_fp(truth.total_margin_balance_fp));
        }

        // per-symbol emit；Binance 填充，CTP per_symbol 通常为空向量。消失
        // symbol（持仓归零）由 helper 写 0 清残留。
        emit_per_symbol_truth_gauges(venue, account, truth.per_symbol,
                                     st.last_truth_symbols);

        // 导出 account_store mark_cache 内部 mark；当 runtime mark 与 venue truth
        // mark 分叉时，可直接定位 MarketTick → mark_cache 更新链路是否断开。
        // 该 metric 仅用于诊断，不参与 account truth 或 risk 计算。
        for (const auto& [code, mark_fp] : truth.runtime_per_symbol_marks_fp) {
            if (code.empty()) continue;
            reg.set_account_store_mark_price_fp(venue, code, mark_fp);
        }

        // broker per-symbol truth 可用时 emit store-vs-broker divergence telemetry。
        // 仅当 per_symbol 是 venue/broker 真值(binance REST)才比对;CTP per_symbol 来自
        // account_store 镜像(per_symbol_is_broker_truth=false),自比恒 0 会假报 I1 健康 → 跳过。
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
    // Baseline 必须等 first real account snapshot，避免 trading_ready 早于异步账户快照时
    // 把 PnL 起点锁到配置默认值。
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
    // 写入 fills 并返回本帧累计 turnover。
    // frame.now_ms 是 steady_clock 节拍 ms (monotonic since boot),
    // 不带 wall clock 含义。fills.csv.ts_ms 字段需 wall clock UTC epoch ms,
    // 因此在 emit 侧单独取 system_clock。
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
    // order_cancel_ratio: 订单终态 Canceled / 已报出订单. 含 IOC 超时, 与 cancel_ratio 正交.
    if (snap.risk_stats.submit_success > 0) {
        ratios.order_cancel_ratio =
            static_cast<double>(snap.risk_stats.canceled_terminal_total) /
            static_cast<double>(snap.risk_stats.submit_success);
    }
}

void build_kpi_latency(const ControlSnapshot& snap, xt_report::LatencyStats& latency) {
    latency.submit_to_ack_us_avg  = snap.latency_stats.submit_to_ack.avg_us;
    latency.submit_to_ack_us_p50  = snap.latency_stats.submit_to_ack.p50_us;   // p50 与 avg 分离，避免长尾污染中位数。
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
    // raw transport ACK (不经 UDS-first dedup)
    latency.submit_to_trade_ack_us_avg = snap.latency_stats.submit_to_trade_ack.avg_us;
    latency.submit_to_trade_ack_us_p50 = snap.latency_stats.submit_to_trade_ack.p50_us;
    latency.submit_to_trade_ack_us_p90 = snap.latency_stats.submit_to_trade_ack.p90_us;
    latency.submit_to_trade_ack_us_p95 = snap.latency_stats.submit_to_trade_ack.p95_us;
    latency.submit_to_trade_ack_us_p99 = snap.latency_stats.submit_to_trade_ack.p99_us;
    // per-order UDS-first savings
    latency.uds_first_savings_us_avg = snap.latency_stats.uds_first_savings.avg_us;
    latency.uds_first_savings_us_p50 = snap.latency_stats.uds_first_savings.p50_us;
    latency.uds_first_savings_us_p90 = snap.latency_stats.uds_first_savings.p90_us;
    latency.uds_first_savings_us_p95 = snap.latency_stats.uds_first_savings.p95_us;
    latency.uds_first_savings_us_p99 = snap.latency_stats.uds_first_savings.p99_us;
    latency.submit_to_fill_us_avg = snap.latency_stats.submit_to_fill.avg_us;
    latency.submit_to_fill_us_p50 = snap.latency_stats.submit_to_fill.p50_us;  // 新增, 对称 ack
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
    // Per-stage 样本计数（fail-closed budget）：0 → NO_DATA 而非"低延迟绿灯"。
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
    // 真 pre-trade risk reject 累计（record_reject 单调计数）。旧 attempts−success 跨计数域
    // （attempts 按逻辑 submit / success 按物理 slice），multi-slice 会把 reject 吞成 0。
    latency.risk_block_count      = snap.risk_stats.total_rejects();
    latency.md_gap_ms_p99         = static_cast<double>(snap.tick_lag_p99_us) / 1000.0;
    latency.md_gap_ms_peak        = static_cast<double>(snap.tick_lag_peak_us) / 1000.0;
    latency.event_drop            = snap.tick_drop + snap.td_flow_event_rejected;
    // queue depth = TD 事件在途 instant（非 lifetime peak）。字段名 queue_depth_p99 是
    // report CSV 契约不改，值口径切 instant 与 CTA/SEL 统一（xt_queue_depth gauge）。
    latency.queue_depth_p99       = static_cast<double>(snap.td_event_backlog);
}

void emit_td_drop_metrics(std::string_view venue,
                          const XtRunnerSignals& signals,
                          ControlLoopState& st) {
#ifdef XT_HAVE_METRICS
    auto& reg = ct::metrics::XtMetricRegistry::instance();
    // position snapshot 丢弃（A2 后仅物理队列满触发）→ td_position_ready fail-closed
    // wedge 全部 submit 的唯一结构化告警信号；同 md_ticks 的 cumulative→delta emit 模式。
    const uint64_t possnap_drop_total =
        signals.td_drop_position_cnt.load(std::memory_order_relaxed);
    const uint64_t possnap_drop_delta =
        possnap_drop_total >= st.last_td_position_snapshot_drop_prom_total
            ? (possnap_drop_total - st.last_td_position_snapshot_drop_prom_total)
            : possnap_drop_total;
    reg.inc_td_position_snapshot_drop_total(venue, possnap_drop_delta);
    st.last_td_position_snapshot_drop_prom_total = possnap_drop_total;
    // control event（ChannelReady/Lost）丢弃：must_not_drop 后仅物理满触发。丢 ChannelReady
    // 无自愈（worker td_channel_up_ 直到下次断连边沿不 re-arm），单独 counter + critical alert。
    const uint64_t ctrl_drop_total =
        signals.td_drop_control_cnt.load(std::memory_order_relaxed);
    const uint64_t ctrl_drop_delta =
        ctrl_drop_total >= st.last_td_control_event_drop_prom_total
            ? (ctrl_drop_total - st.last_td_control_event_drop_prom_total)
            : ctrl_drop_total;
    reg.inc_td_control_event_drop_total(venue, ctrl_drop_delta);
    st.last_td_control_event_drop_prom_total = ctrl_drop_total;
    // other 桶（position/control 之外的 exec/account + 非边沿 control 事件）：非 must_not_drop，
    // 软水位 shedding / 物理满 / loop 未运行拒绝皆计入 —— 背压诊断可见性（warning alert），
    // 非 readiness-wedge 信号（分桶动机见 XtHftChannels::classify_td_drop）。
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
    // 同 emit_td_drop_metrics：单调累计 → delta，回退（理论不发生，防御）时取全量。
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
    // equity 更新 + baseline init
    live_tracker.on_equity(frame.snapshot.equity);
    maybe_init_reporter_baseline(ctx, live_tracker, frame, st);

    // 多合约 per-sec snapshot (持仓 + 行情), 节流到每秒边界.
    maybe_flush_per_symbol_snapshots(ctx, live_tracker, frame, st);
    maybe_flush_account_truth_snapshot(ctx, cfg, live_tracker, frame, st);

    // fills 写入
    const double interval_turnover = emit_live_fills_and_get_turnover(live_tracker, frame);

    // KPI flush (build_kpi_ratios + build_kpi_latency)
    xt_report::OrderRatios ratios;
    xt_report::LatencyStats latency;
    build_kpi_ratios(frame.snapshot, ratios);
    build_kpi_latency(frame.snapshot, latency);
    // md_gap_ms_p99 源 = per-frame drain 直方图 p99 的 kpi 窗口 max（跨 frame 无法合并
    // 直方图，取 max = 保守上界；非真跨窗 p99）。emit 侧披露口径，gauge/help 同步。
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
            // Prometheus 不再接收派生 `_p50/_p90/_p99` gauge；panel/Grafana 通过
            // hot path histogram 的 histogram_quantile() 计算分位数。emit_latency
            // 的 p50/p90/p99 字段保留给 kpi_1s.csv。
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
            // xt_risk_rejects_total counter：与 gauge 同源 total_rejects()，cumulative→delta
            // emit（series 由 runner 启动预注册 + 每帧 eager Add）→
            // venue:xt_risk_block_rate_5m 真数据源。
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
        // shm MD reader attach 可观测（T1.141①）：仅 multibook shm reader 网关有该指针。
        // gauge Set（非 cumulative→delta）——attach_failing 是当前 0/1 态、fail_count 是进程内
        // atomic 直设（单调），每 1Hz 帧 emit → 序列非惰性、免 T1.145 series-birth 盲区。
        if (ctx.shm_md_gateway) {
            const auto& ring = ctx.shm_md_gateway->ring_name();
            reg.set_shm_md_attach_failing(ring, ctx.shm_md_gateway->attach_failing());
            reg.set_shm_md_attach_fail_count(ring, ctx.shm_md_gateway->attach_fail_count());
        }
        // 孤儿回收撤单数：engine cumulative counter → delta emit（同 md_ticks 模式）。
        const uint64_t reap_total = ctx.strategy_event_loop.engine_orphan_reap_cancel_count();
        const uint64_t reap_delta = reap_total >= st.last_orphan_reap_prom_total
            ? (reap_total - st.last_orphan_reap_prom_total)
            : reap_total;
        reg.inc_recovery_orphan_reaped_total(venue, reap_delta);
        st.last_orphan_reap_prom_total = reap_total;
        // TD 丢弃三桶（position/control/other）cumulative→delta emit（独立 helper 便于单测）。
        emit_td_drop_metrics(venue, ctx.runtime_shared_state, st);
        // unmatched 丢弃 reason 拆分 + engine 自愈计数 cumulative→delta emit。
        emit_unmatched_drop_metrics(ctx.runtime_shared_state, st);
        // worker guard 异常计数 emit。不配独立 alert：body 随 fail-stop 由 liveness/up 告警
        // 接手；normalize/record 是 swallow 类诊断，counter 可查即可。
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
        // 本地 book alive 在途单数 gauge：逼近 venue per-symbol 上限（binance 200）预警。
        reg.set_open_orders(venue, ctx.strategy_event_loop.engine_open_order_count());
        maybe_emit_strategy_enabled_metrics(ctx, cfg);
    }
#endif
    if (kpi_flushed) {
        st.kpi_window_tick_lag_us_peak = 0;
        st.kpi_window_tick_lag_us_p99 = 0;
    }

    // Account-level session PnL；venue label 或 baseline 未就绪时跳过。
#ifdef XT_HAVE_METRICS
    if (!cfg.venue_label.empty() && live_tracker.equity_initialized()) {
        ct::metrics::XtMetricRegistry::instance().set_pnl_net(
            cfg.venue_label,
            cfg.account_label.empty() ? "_default" : cfg.account_label,
            live_tracker.session_pnl_net());
    }
    // per-strategy PnL matrix.
    //   xt_today_pnl_net_v2{strategy,book,venue,symbol} gauge — HFT only.
    //   pnl = realized - fees, 与 account-level pnl_net 定义对齐.
    //   sid 空 (single-strategy config) 用 "_default" sentinel.
    if (ctx.strategy_pnl_aggr && !cfg.venue_label.empty()) {
        const auto rows = ctx.strategy_pnl_aggr->snapshot();
        xt_live::control::emit_strategy_pnl_metrics(
            cfg, ctx.strategy_pnl_baseline, ctx.declared_registry,
            rows, st.last_baseline_day_id);

        // per-strategy risk limits emit.
        //   Source: ctx.live_order_gateway.risk_config()；snapshot 在 loop 外构造，避免每个 row 重复 copy。
        //   XtRiskCfg 实字段 2 个 (max_order_qty + max_pos_qty), 其余 3 字段
        //   (max_drawdown_pct / max_position_value_usd / max_orders_per_min) 不在 cfg 中,
        //   stay 0.0 (panel 看 0 则判"未启用"). per_strategy_max_pos_qty_by_sid_string
        //   查到 sid 用 override, 否则 fallback 全局 max_pos_qty.
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
                sample.max_drawdown_pct        = 0.0;  // not in XtRiskCfg
                sample.max_position_value_usd  = 0.0;  // not in XtRiskCfg
                sample.max_orders_per_min      = 0.0;  // not in XtRiskCfg
                sample.allow_short             = risk_cfg_snap.allow_short;
                ct::metrics::XtMetricRegistry::instance().set_strategy_limits(
                    sid, sid /* book = sid */, cfg.venue_label, sample);
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
    // live_tracker 保留在签名中以匹配控制环 emit 调用形态；策略指标只写日志与 Prom custom gauge。
    // 1Hz 冷路径，不触 strategy hot path 的 Family::Add 锁。
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

}  // namespace xt_live::control
