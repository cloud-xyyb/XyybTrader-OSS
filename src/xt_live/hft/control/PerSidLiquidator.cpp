#include "xt_live/hft/control/PerSidLiquidator.h"

#include <cstdint>
#include <string>

#include "xt_live/hft/control/HftRunnerControl.h"
#include "PanelHelpers.h"
#include "XtLogger.h"
#include "XtDeclaredSymbolRegistry.h"
#include "xt_risk/engine/XtRiskMonitorState.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_risk/live/XtPerSidTodayPnl.h"
#include "xt_account/api/AccountSnapshot.h"
#include "xt_account/state/AccountStateStore.h"
#include "xt_metrics/XtMetricRegistry.h"
#include "xt_metrics/XtStrategyPnlAggregator.h"
#include "xt_metrics/XtStrategyPnlBaseline.h"

namespace xt_live::control {

void PerSidLiquidator::step(XtHftRunnerControlContext& ctx) {
    if (!cfg_.enabled) return;

    xt_risk::XtRiskMonitorState* mon = ctx.risk_monitor_state;
    const xt_account::AccountStateStore* store = ctx.account_store;
    const xt_strategy_engine::XtDeclaredSymbolRegistry* dreg = ctx.declared_registry;
    if (!mon || !store || !dreg) return;

    auto* sid_reg = xt_identity::StrategySidRegistry::global();
    if (!sid_reg) return;

    const int64_t starting_balance_fp = store->starting_balance_fp();
    const auto* aggr     = ctx.strategy_pnl_aggr;
    const auto* baseline = ctx.strategy_pnl_baseline;

    XtPanelContext panel{};
    panel.td_gateway           = ctx.td_gateway;
    panel.order_book           = ctx.order_book;
    panel.account_store        = ctx.account_store;
    panel.live_order_gateway   = &ctx.live_order_gateway;
    panel.runtime_shared_state = &ctx.runtime_shared_state;
    panel.param_domain         = ctx.param_domain;
    panel.catalog              = ctx.catalog;
    panel.force_flat_fence     = ctx.force_flat_fence;

    auto& reg = ct::metrics::XtMetricRegistry::instance();

    for (const std::string& sid_str : dreg->registered_sids()) {
        const uint32_t sid_id = sid_reg->lookup(sid_str);
        if (sid_id == xt_identity::StrategySidRegistry::kNoSidId) continue;

        const auto codes = dreg->declared_symbols(sid_str);
        if (codes.size() != 1) {
            if (warned_1to1_.insert(sid_str).second) {
                XtLogger::warn_m("control",
                    "auto_liquidation 1:1 violation sid={} declared_symbols={} → skip sid",
                    sid_str, codes.size());
                reg.inc_strategy_auto_liquidation_1to1_violation_total(sid_str);
            }
            continue;
        }
        const std::string& code = *codes.begin();

        const auto pos = store->snapshot_position(code);

        int64_t today_realized_fp = 0;
        if (aggr) {
            today_realized_fp =
                xt_risk::live::per_sid_today_realized_fp(aggr, baseline, sid_str);
        }
        const int64_t trigger_pnl_fp = today_realized_fp + pos.unrealized_pnl;
        reg.set_strategy_auto_liquidation_trigger_pnl_fp(sid_str, trigger_pnl_fp);

        if (!mon->per_strategy.is_auto_liquidating(sid_id)) {
            LiquidateTriggerInput in;
            in.killed              = mon->per_strategy.is_strategy_killed(sid_id);
            in.starting_balance_fp = starting_balance_fp;
            in.mark_price_fp       = pos.mark_price;
            in.unrealized_fp       = pos.unrealized_pnl;
            const auto it = cfg_.liquidate_pct_fp.find(sid_id);
            in.liquidate_pct_fp = (it != cfg_.liquidate_pct_fp.end()) ? it->second : 0;

            in.today_realized_fp = (in.liquidate_pct_fp > 0) ? today_realized_fp : 0;
            if (liquidate_breach(in)) {
                const auto src = in.killed ? xt_risk::XtAutoLiquidateSource::KillSwitch
                                           : xt_risk::XtAutoLiquidateSource::DailyLoss;
                mon->per_strategy.set_auto_liquidating(sid_id, true, src);

                if (src == xt_risk::XtAutoLiquidateSource::DailyLoss) {
                    mon->per_strategy.set_strategy_killed(sid_id, true);
                }
                reg.inc_strategy_auto_liquidation_breach_total(sid_str);
                XtLogger::warn_m("control",
                    "auto_liquidation ARM sid={} code={} source={} dry_run={}",
                    sid_str, code,
                    src == xt_risk::XtAutoLiquidateSource::KillSwitch ? "kill_switch" : "daily_loss",
                    cfg_.dry_run ? 1 : 0);
            }
        }

        const bool latched = mon->per_strategy.is_auto_liquidating(sid_id);
        reg.set_strategy_auto_liquidating(sid_str, latched);

        if (latched) {
            if (cfg_.dry_run) continue;
            bool hedge_warn = false;
            uint64_t oid = 0;

            const ForceExitOutcome out =
                force_exit_panel_request(panel, code, sid_str, hedge_warn, &oid);
            switch (out) {
                case ForceExitOutcome::ForceClosed:
                    reg.inc_strategy_auto_liquidation_submit_total(sid_str);
                    break;
                case ForceExitOutcome::NoTd:
                case ForceExitOutcome::NoMarkPrice:
                case ForceExitOutcome::SubmitFailed:
                case ForceExitOutcome::CancelledOnly:
                    reg.inc_strategy_auto_liquidation_deferred_total(sid_str);
                    break;
                default:
                    break;
            }
        }
    }
}

}
