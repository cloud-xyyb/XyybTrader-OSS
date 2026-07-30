#include "xt_live/hft/control/PanelForceExitLatch.h"

#include <cstdint>

#include "xt_live/hft/control/HftRunnerControl.h"

namespace xt_live {
namespace control {

void PanelForceExitLatch::step(XtHftRunnerControlContext& ctx, int64_t now_ms) {

    XtPanelContext panel{};
    panel.td_gateway           = ctx.td_gateway;
    panel.order_book           = ctx.order_book;
    panel.account_store        = ctx.account_store;
    panel.live_order_gateway   = &ctx.live_order_gateway;
    panel.runtime_shared_state = &ctx.runtime_shared_state;
    panel.param_domain         = ctx.param_domain;
    panel.catalog              = ctx.catalog;
    panel.force_flat_fence     = ctx.force_flat_fence;
    step_panel(panel, now_ms);
}

void PanelForceExitLatch::step_panel(XtPanelContext& panel, int64_t now_ms) {

    if (!panel.td_gateway || !panel.order_book || !panel.account_store
        || !panel.runtime_shared_state) {
        return;
    }

    std::lock_guard<std::mutex> lk(mu_);
    for (auto it = entries_.begin(); it != entries_.end();) {
        Entry& e = *it;
        bool hedge_warn = false;
        uint64_t oid = 0;

        const ForceExitOutcome out =
            force_exit_panel_request(panel, e.symbol, e.strategy_id, hedge_warn, &oid);
        ++e.frames;
        e.last_outcome = static_cast<int64_t>(out);
        switch (panel_force_exit_latch_step(out, now_ms - e.armed_ms, ttl_ms_)) {
            case LatchStep::Done:
                XtLogger::info_m("control",
                    "panel force_exit latch DONE symbol={} scope={} frames={}",
                    e.symbol, e.strategy_id.empty() ? "_global" : e.strategy_id,
                    e.frames);
                it = entries_.erase(it);
                break;
            case LatchStep::Expired:
                XtLogger::warn_m("control",
                    "panel force_exit latch EXPIRED symbol={} scope={} last_outcome={} "
                    "frames={} ttl_ms={} (仍未 flat, operator 需重新触发 ForceExit)",
                    e.symbol, e.strategy_id.empty() ? "_global" : e.strategy_id,
                    e.last_outcome, e.frames, ttl_ms_);
                it = entries_.erase(it);
                break;
            case LatchStep::Keep:
            default:
                ++it;
                break;
        }
    }
}

}
}
