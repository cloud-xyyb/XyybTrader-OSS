#include "xt_live/hft/telemetry/HftRunnerControlStrategyPnlEmit.h"

#include <algorithm>
#include <string>
#include <utility>

#include "XtDeclaredSymbolRegistry.h"
#include "xt_metrics/XtMetricRegistry.h"
#include "xt_metrics/XtStrategyPnlBaseline.h"

namespace xt_live::control {
namespace {

using ct::metrics::XtStrategyPnlAggregator;
using ct::metrics::XtStrategyPnlBaseline;

struct EmitTarget {
    std::string sid;
    std::string code;
};

std::string normalize_sid(const std::string& sid) {
    return sid.empty() ? std::string("_default") : sid;
}

void add_target(std::vector<EmitTarget>& out, std::string sid, std::string code) {
    sid = normalize_sid(sid);
    if (sid.empty() || code.empty()) return;
    const auto exists = std::any_of(out.begin(), out.end(), [&](const EmitTarget& target) {
        return target.sid == sid && target.code == code;
    });
    if (!exists) out.push_back({std::move(sid), std::move(code)});
}

std::vector<EmitTarget> collect_emit_targets(
    const XtRunnerControlConfig& cfg,
    const xt_strategy_engine::XtDeclaredSymbolRegistry* declared_registry,
    const std::vector<XtStrategyPnlAggregator::Row>& rows) {
    std::vector<EmitTarget> targets;
    targets.reserve(cfg.strategy_pnl_emit_targets.size() + rows.size() + 2);

    for (const auto& target : cfg.strategy_pnl_emit_targets) {
        add_target(targets, target.strategy_id, target.symbol);
    }
    if (cfg.strategy_pnl_emit_targets.empty()
            && !cfg.unified_strategy_id.empty()
            && !cfg.unified_symbol.empty()) {
        add_target(targets, cfg.unified_strategy_id, cfg.unified_symbol);
    }
    if (declared_registry) {
        for (const auto& sid : declared_registry->registered_sids()) {
            for (const auto& code : declared_registry->declared_symbols(sid)) {
                add_target(targets, sid, code);
            }
        }
    }
    for (const auto& row : rows) {
        add_target(targets, row.sid, row.code);
    }
    return targets;
}

void rotate_baseline_if_needed(const XtRunnerControlConfig& cfg,
                               XtStrategyPnlBaseline* baseline,
                               const std::vector<XtStrategyPnlAggregator::Row>& rows,
                               int& last_baseline_day_id) {
    if (!baseline) return;

    const int current_day = XtStrategyPnlBaseline::compute_trading_day_id(
        cfg.trading_day_tz_offset_h);
    if (last_baseline_day_id == 0) {
        last_baseline_day_id = current_day;
        return;
    }
    if (current_day == last_baseline_day_id) return;

    std::vector<XtStrategyPnlBaseline::BaselineRow> base_rows;
    base_rows.reserve(rows.size());
    for (const auto& r : rows) {
        XtStrategyPnlBaseline::BaselineRow br;
        br.sid      = r.sid;
        br.code     = r.code;
        br.realized = r.realized;
        br.fees     = r.fees;
        br.fills    = r.fills;
        base_rows.push_back(std::move(br));
    }
    baseline->rotate_day(base_rows);
    baseline->save(state_root(), current_day);
    ct::metrics::XtMetricRegistry::instance().inc_baseline_rotated_total(
        cfg.venue_label);
    last_baseline_day_id = current_day;
}

}

StrategyPnlEmitResult emit_strategy_pnl_metrics(
    const XtRunnerControlConfig& cfg,
    XtStrategyPnlBaseline* baseline,
    const xt_strategy_engine::XtDeclaredSymbolRegistry* declared_registry,
    const std::vector<XtStrategyPnlAggregator::Row>& rows,
    int& last_baseline_day_id) {
    StrategyPnlEmitResult result;
    result.row_count = rows.size();
    if (cfg.venue_label.empty()) return result;

    rotate_baseline_if_needed(cfg, baseline, rows, last_baseline_day_id);
    result.baseline_ready = baseline && baseline->ready();

    for (const auto& r : rows) {
        const std::string sid = normalize_sid(r.sid);
        const double cumulative_pnl = r.realized - r.fees;
        if (result.baseline_ready) {
            bool is_new = false;
            const auto base = baseline->get(sid, r.code, &is_new);
            if (is_new) {
                ct::metrics::XtMetricRegistry::instance().inc_strategy_baseline_new_total(
                    sid, cfg.venue_label);
            }
            const double today_pnl = cumulative_pnl - (base.realized - base.fees);
            ct::metrics::XtMetricRegistry::instance().set_today_pnl_net_v2(
                 sid,  sid, cfg.venue_label,  r.code, today_pnl);

            const int64_t today_realized_fp =
                static_cast<int64_t>((r.realized - base.realized) * 1'000'000.0);
            const int64_t today_fees_fp =
                static_cast<int64_t>((r.fees - base.fees) * 1'000'000.0);
            const uint64_t today_fills = r.fills >= base.fills ? (r.fills - base.fills) : 0;
            ct::metrics::XtMetricRegistry::instance().set_strategy_ledger_today(
                 sid,  sid, cfg.venue_label,  r.code,
                today_realized_fp, today_fees_fp, today_fills);
            ct::metrics::XtMetricRegistry::instance().set_today_fills_total_v2(
                 sid, cfg.venue_label,  r.code, today_fills);
        }
    }

    if (rows.empty()) {
        const auto targets = collect_emit_targets(cfg, declared_registry, rows);
        result.zero_target_count = targets.size();
        for (const auto& target : targets) {
            ct::metrics::XtMetricRegistry::instance().set_today_pnl_net_v2(
                 target.sid,  target.sid,
                cfg.venue_label,  target.code, 0.0);
            ct::metrics::XtMetricRegistry::instance().set_today_fills_total_v2(
                 target.sid, cfg.venue_label,  target.code, 0);
        }
    }

    return result;
}

}
