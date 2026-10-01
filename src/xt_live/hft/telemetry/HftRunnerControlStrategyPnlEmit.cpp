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

// Prometheus label 不写空 strategy；空 sid 统一落到 `_default`，保证 row 与 zero-heartbeat 同口径。
std::string normalize_sid(const std::string& sid) {
    return sid.empty() ? std::string("_default") : sid;
}

// target 来源可能重叠；这里统一规范 sid 并按 (sid, code) 去重，避免同一 gauge label 重复写入。
void add_target(std::vector<EmitTarget>& out, std::string sid, std::string code) {
    sid = normalize_sid(sid);
    if (sid.empty() || code.empty()) return;
    const auto exists = std::any_of(out.begin(), out.end(), [&](const EmitTarget& target) {
        return target.sid == sid && target.code == code;
    });
    if (!exists) out.push_back({std::move(sid), std::move(code)});
}

// 计算 zero-heartbeat 覆盖面：先收显式 target；无显式 target 时用 unified 配置兜底，
// 再补 declared symbols 与本轮真实 rows，确保无成交策略也能持续暴露 0 值。
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

// trading-day rollover 时用当前 cumulative rows 作为新日初基准；baseline 缺失或同日直接跳过。
// 本 helper 只在 control 线程调用，baseline state 目录沿用 runner 现有持久化约定。
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
        br.fills    = r.fills;  // fills 也进入日初基准；today_fills 与 realized/fees 一样按当日增量上报。
        base_rows.push_back(std::move(br));
    }
    baseline->rotate_day(base_rows);
    baseline->save(state_root(), current_day);
    ct::metrics::XtMetricRegistry::instance().inc_baseline_rotated_total(
        cfg.venue_label);
    last_baseline_day_id = current_day;
}

}  // namespace

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
            // D9：缺 baseline 的 row 每帧 is_new=true（get 为 const 不写 baseline_），改为
            // 每 (sid,code) 每 epoch 计一次（mark_new_reported 去重，rotate/load 清空）。
            if (is_new && baseline->mark_new_reported(sid, r.code)) {
                ct::metrics::XtMetricRegistry::instance().inc_strategy_baseline_new_total(
                    sid, cfg.venue_label);
            }
            const double today_pnl = cumulative_pnl - (base.realized - base.fees);
            ct::metrics::XtMetricRegistry::instance().set_today_pnl_net_v2(
                /*strategy=*/sid, /*book=*/sid, cfg.venue_label, /*symbol=*/r.code, today_pnl);

            // strategy ledger gauge 复用同一 baseline，避免 PnL 与 fills 的 today 口径漂移。
            const int64_t today_realized_fp =
                static_cast<int64_t>((r.realized - base.realized) * 1'000'000.0);
            const int64_t today_fees_fp =
                static_cast<int64_t>((r.fees - base.fees) * 1'000'000.0);
            const uint64_t today_fills = r.fills >= base.fills ? (r.fills - base.fills) : 0;
            ct::metrics::XtMetricRegistry::instance().set_strategy_ledger_today(
                /*strategy=*/sid, /*book=*/sid, cfg.venue_label, /*symbol=*/r.code,
                today_realized_fp, today_fees_fp, today_fills);
            ct::metrics::XtMetricRegistry::instance().set_today_fills_total_v2(
                /*strategy=*/sid, cfg.venue_label, /*symbol=*/r.code, today_fills);
        }
    }

    if (rows.empty()) {
        const auto targets = collect_emit_targets(cfg, declared_registry, rows);
        result.zero_target_count = targets.size();
        for (const auto& target : targets) {
            ct::metrics::XtMetricRegistry::instance().set_today_pnl_net_v2(
                /*strategy=*/target.sid, /*book=*/target.sid,
                cfg.venue_label, /*symbol=*/target.code, 0.0);
            ct::metrics::XtMetricRegistry::instance().set_today_fills_total_v2(
                /*strategy=*/target.sid, cfg.venue_label, /*symbol=*/target.code, 0);
        }
    }

    return result;
}

}  // namespace xt_live::control
