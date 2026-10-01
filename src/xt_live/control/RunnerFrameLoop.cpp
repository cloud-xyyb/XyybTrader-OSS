#include "RunnerFrameLoop.h"

#include <chrono>

#include "AdvanceRiskFrame.h"
#include "XtLogger.h"
#include "xt_util/runtime/XtShutdownSignal.h"

namespace xt_live::control {

void run_runner_frame_loop(const RunnerFrameLoopCtx& ctx) {
    XtLogger::info_m(ctx.module_tag, "running, waiting for shutdown signal");
    int risk_frame_tick = 0;
    RiskFrameState risk_state;  // function-local，run_*_live_with_config 重入安全
    RiskFrameDeps deps;
    deps.risk_monitor  = ctx.risk_monitor;
    deps.account_store = ctx.account_store;
    deps.signals       = ctx.signals;
    deps.order_gateway = ctx.order_gateway;
    deps.sid_registry  = ctx.sid_registry;
    deps.md            = ctx.md;
    deps.td            = ctx.td;
    deps.venue_label   = ctx.venue_label;
    deps.post_publish_request = ctx.post_publish_request;
    // session-begin 有界等待共用主循环的停机信号：停机时立即退出等待。
    deps.shutdown_flag = ctx.shutdown_flag;
    while (!ctx.shutdown_flag->load(std::memory_order_acquire)) {
        // shutdown 触发后 ≤50ms 退出
        if (xt_util::XtShutdownSignal::wait_for(
                *ctx.shutdown_flag, std::chrono::milliseconds(100))) {
            break;
        }
        if (++risk_frame_tick >= 10) {  // ≈ 1000ms 节拍
            risk_frame_tick = 0;
            advance_risk_frame(deps, risk_state);
            if (ctx.frame_tail_hook) ctx.frame_tail_hook();
        }
    }
}

}  // namespace xt_live::control
