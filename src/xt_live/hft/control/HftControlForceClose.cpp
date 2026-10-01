// ── 收盘强平协调 stage ──────────────────────────────────────────────────
//
// 每帧调用 try_runner_force_close，按结果驱动连续失败升级：warn → block
// trading（连通性类随 TD 重连自愈）；仅 kRejectedAmbiguousScope 升级持久化
// kill_switch。跨 TU 入口：run_force_close_once。

#include "xt_live/hft/control/HftControlStages.h"

#include <algorithm>
#include <string>

#include "XtLogger.h"
#include "api/XtRunnerSignals.h"
#include "xt_live/hft/control/HftCloseoutGate.h"  // try_runner_force_close / closeout_result_trips_kill_switch

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
        // 不在强平窗口 — 正常跳过，重置 streak
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
        // 正在执行强平流程 — 重置 streak
        st.closeout_fail_streak = 0;
        return;

    case ForceCloseResult::kGatewayDown:
    case ForceCloseResult::kThrottled:
    case ForceCloseResult::kRejectedAmbiguousScope:
        // 需要强平但被阻塞 — 升级告警
        // TD 通道不可用时只进入本地 streak 升级；外部告警推送和更细的
        // deadline kill policy 属于 Closeout FSM 后续重构范围。
        break;
    }

    ++st.closeout_fail_streak;

    const auto warn_streak = static_cast<uint64_t>(std::max(cfg.closeout_warn_streak, 1));
    const auto block_streak = static_cast<uint64_t>(std::max(cfg.closeout_block_streak, 1));

    // 首次达到 warn 阈值立即告警，后续按 warn_streak 周期告警
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

    // 升级到 block：禁止新订单提交（trading_ready 可随 MD/TD 重连自愈）。
    // kill_switch（持久化 emergency latch，需人工复位）只在通道在线却拒绝平仓时升级，
    // 连通性类不升级；契约见 closeout_result_trips_kill_switch 声明。
    if (st.closeout_fail_streak >= block_streak && !st.closeout_blocked) {
        ctx.runtime_shared_state.trading_ready.store(false, std::memory_order_release);
        st.closeout_blocked = true;
        if (closeout_result_trips_kill_switch(result)) {
            const std::string reason = std::string("closeout-block result=") + result_label(result)
                                     + " streak=" + std::to_string(st.closeout_fail_streak);
            ctx.runtime_shared_state.kill_switch.store(true, std::memory_order_release);
            ctx.runtime_shared_state.kill_switch_source.store(
                XtRunnerSignals::kKillSwitchSourceAutoCloseoutBlock, std::memory_order_release);
            emit_kill_switch_transition(ctx, cfg, /*active=*/true, "system:closeout-block", reason);
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

}  // namespace xt_live::control::detail
