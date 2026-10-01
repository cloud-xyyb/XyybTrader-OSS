#include "xt_live/hft/control/PerSidLiquidator.h"

#include <cstdint>
#include <string>

#include "xt_live/hft/control/HftRunnerControl.h"   // XtHftRunnerControlContext
#include "PanelHelpers.h"       // force_exit_panel_request, XtPanelContext, ForceExitOutcome
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

// control frame 内的 per-sid auto-liquidation 执行入口，串行完成四步：
// 1. 读取 risk/account/declared-symbol/PnL 观测源，缺关键依赖时 fail-safe no-op；
// 2. 按 sid→single-symbol 约束计算 trigger PnL，并刷新观测 gauge；
// 3. 对未 latch 的 sid 做 rising-edge 判定，DailyLoss 源同步设置 per-sid kill_switch；
// 4. 对已 latch 的 sid 复用 panel force_exit，执行 cancel-first + flatten，失败类结果留到下帧重试。
void PerSidLiquidator::step(XtHftRunnerControlContext& ctx) {
    if (!cfg_.enabled) return;

    xt_risk::XtRiskMonitorState* mon = ctx.risk_monitor_state;
    const xt_account::AccountStateStore* store = ctx.account_store;
    const xt_strategy_engine::XtDeclaredSymbolRegistry* dreg = ctx.declared_registry;
    if (!mon || !store || !dreg) return;  // fail-safe bypass（任一缺失 → no-op）

    auto* sid_reg = xt_identity::StrategySidRegistry::global();
    if (!sid_reg) return;

    const auto* aggr     = ctx.strategy_pnl_aggr;
    const auto* baseline = ctx.strategy_pnl_baseline;

    // force_exit 复用所需的 panel 依赖视图（control ctx 字段直映；单帧构造）。
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

        // 1:1 sid:symbol 命脉：!=1 时不平 + warn-once（绝不取非确定元素 / 不 crash）。
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

        // trigger PnL（today_realized + unrealized）：emit 观测 gauge，并复用为下方
        // daily-loss 突破判定的 today_realized 输入（pct<=0 时 breach 早返，值不参与）。
        int64_t today_realized_fp = 0;
        if (aggr) {
            today_realized_fp =
                xt_risk::live::per_sid_today_realized_fp(aggr, baseline, sid_str);
        }
        const int64_t trigger_pnl_fp = today_realized_fp + pos.unrealized_pnl;
        reg.set_strategy_auto_liquidation_trigger_pnl_fp(sid_str, trigger_pnl_fp);

        // 评估突破（rising edge，仅未 latch 时）。**不被 flat 挡住**：
        //   - killed（operator 或 daily-loss）→ liquidate_breach 直返 true（killed first），
        //     无仓也 arm，使下方 actuation 去 cancel kill 期残留挂单。
        //   - daily-loss：mark<=0 skip；flat 时 unrealized=0，realized-only 突破由 reject
        //     层兜（mark>0 时此处仍会 arm+kill，belt-and-suspenders）。
        // 输入组装（含 mark 的 venue-only 口径）见 make_liquidate_trigger_input 契约。
        if (!mon->per_strategy.is_auto_liquidating(sid_id)) {
            const auto it = cfg_.liquidate_pct_fp.find(sid_id);
            const LiquidateTriggerInput in = make_liquidate_trigger_input(
                *store, code,
                /*killed=*/mon->per_strategy.is_strategy_killed(sid_id),
                today_realized_fp, pos.unrealized_pnl,
                /*liquidate_pct_fp=*/(it != cfg_.liquidate_pct_fp.end()) ? it->second : 0);
            if (liquidate_breach(in)) {
                const auto src = in.killed ? xt_risk::XtAutoLiquidateSource::KillSwitch
                                           : xt_risk::XtAutoLiquidateSource::DailyLoss;
                mon->per_strategy.set_auto_liquidating(sid_id, true, src);
                // daily-loss 源：同步 set kill_switch 让 re-open block 不依赖 realized 是否也突破。
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

        // latch 状态 gauge（post-arm，含本帧 rising edge）。
        const bool latched = mon->per_strategy.is_auto_liquidating(sid_id);
        reg.set_strategy_auto_liquidating(sid_str, latched);

        // actuation：latched → force_exit（cancel-first 残单 + flatten），**不论 has_pos**。
        //   flat 也跑：cancel-first 撤掉 kill 期残留挂单，防其成交后在 kill 期重新建仓；
        //   flatten 在 flat 时 no-op；force_exit 内部 genuine-force-flat 去重防重发，flat-无单
        //   时即快速 no-op。latch **不在 flat 时清**——留到 session_begin（DailyLoss）/ operator
        //   resume（KillSwitch），否则 DailyLoss source 丢失 → 次日 reset 找不到 → per-sid kill 永锁。
        //   dry-run 只 latch+log（0 订单 / 0 book 变更）。
        if (latched) {
            if (cfg_.dry_run) continue;
            bool hedge_warn = false;
            uint64_t oid = 0;
            // NoTd/NoMarkPrice/SubmitFailed → 保 latch 下帧重试，绝不升级 persistent kill latch。
            const ForceExitOutcome out =
                force_exit_panel_request(panel, code, sid_str, hedge_warn, &oid);
            switch (out) {
                case ForceExitOutcome::ForceClosed:
                    reg.inc_strategy_auto_liquidation_submit_total(sid_str);
                    break;
                case ForceExitOutcome::NoTd:
                case ForceExitOutcome::NoMarkPrice:
                case ForceExitOutcome::SubmitFailed:
                case ForceExitOutcome::CancelledOnly:  // 已撤 working order，本帧未 submit close，下帧重试
                    reg.inc_strategy_auto_liquidation_deferred_total(sid_str);
                    break;
                default:  // AlreadyForceFlat（前序 force-flat 仍在途）/ NoPosition（flat）：无需 submit 也无需重试，benign。
                    break;
            }
        }
    }
}

}  // namespace xt_live::control
