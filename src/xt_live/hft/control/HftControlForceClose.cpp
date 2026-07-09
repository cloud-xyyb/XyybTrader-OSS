
#include "xt_live/hft/control/HftControlStages.h"

#include <algorithm>
#include <string>

#include "XtLogger.h"
#include "api/XtRunnerSignals.h"
#include "xt_live/hft/control/HftCloseoutGate.h"

namespace xt_live::control::detail {

void run_force_close_once(XtHftRunnerControlContext& ctx,
                          const XtRunnerControlConfig& cfg,
                          ControlLoopState& st) {
    const auto result = try_runner_force_close(ctx, st.closeout_last_try_ms, st.closeout_force_active_since_ms);
    const auto result_label = [](ForceCloseResult r) -> const char* {
        switch (r) {
        case ForceCloseResult::kGatewayDown: return "GatewayDown";
        case ForceCloseResult::kThrottled: return "Throttled";
        case ForceCloseResult::kRejectedAmbiguousScope: return "RejectedAmbiguousScope";
        case ForceCloseResult::kNotNeeded: return "NotNeeded";
        case ForceCloseResult::kSessionGated: return "SessionGated";
        case ForceCloseResult::kCancelledFirst: return "CancelledFirst";
        case ForceCloseResult::kExecuted: return "Executed";
        }
        return "Unknown";
    };

    switch (result) {
    case ForceCloseResult::kNotNeeded:
    case ForceCloseResult::kSessionGated:

        if (st.closeout_blocked) {
            st.closeout_blocked = false;
            ctx.runtime_shared_state.refresh_trading_ready();
            XtLogger::warn_m("RT",
                             "closeout block released: not in force-close window "
                             "(streak_was={})",
                             st.closeout_fail_streak);
        }
        st.closeout_fail_streak = 0;
        return;

    case ForceCloseResult::kExecuted:
    case ForceCloseResult::kCancelledFirst:

        st.closeout_fail_streak = 0;
        return;

    case ForceCloseResult::kGatewayDown:
    case ForceCloseResult::kThrottled:
    case ForceCloseResult::kRejectedAmbiguousScope:

        break;
    }

    ++st.closeout_fail_streak;

    const auto warn_streak = static_cast<uint64_t>(std::max(cfg.closeout_warn_streak, 1));
    const auto block_streak = static_cast<uint64_t>(std::max(cfg.closeout_block_streak, 1));

    if (st.closeout_fail_streak == warn_streak ||
        (st.closeout_fail_streak > warn_streak &&
         (st.closeout_fail_streak % warn_streak) == 0)) {
        XtLogger::error_m("RT",
                          "closeout blocked: result={} streak={} "
                          "thresholds(warn/block)={}/{}",
                          result_label(result),
                          st.closeout_fail_streak,
                          warn_streak,
                          block_streak);
    }

    if (st.closeout_fail_streak >= block_streak && !st.closeout_blocked) {
        ctx.runtime_shared_state.trading_ready.store(false, std::memory_order_release);
        st.closeout_blocked = true;
        if (closeout_result_trips_kill_switch(result)) {
            const std::string reason = std::string("closeout-block result=") + result_label(result)
                                     + " streak=" + std::to_string(st.closeout_fail_streak);
            ctx.runtime_shared_state.kill_switch.store(true, std::memory_order_release);
            ctx.runtime_shared_state.kill_switch_source.store(
                XtRunnerSignals::kKillSwitchSourceAutoCloseoutBlock, std::memory_order_release);
            emit_kill_switch_transition(ctx, cfg,  true, "system:closeout-block", reason);
            XtLogger::error_m("RT",
                              "closeout block -> trading_ready=false + kill_switch=true (auto-latch) "
                              "result={} streak={} block_threshold={}",
                              result_label(result), st.closeout_fail_streak, block_streak);
        } else {
            XtLogger::error_m("RT",
                              "closeout blocked through threshold -> trading_ready=false "
                              "(NO kill_switch; self-heals on TD reconnect) result={} streak={} block_threshold={}",
                              result_label(result), st.closeout_fail_streak, block_streak);
        }
    }
}

}
