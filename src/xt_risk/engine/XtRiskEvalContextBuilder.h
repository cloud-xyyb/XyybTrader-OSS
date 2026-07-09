#pragma once

#include "xt_risk/engine/XtRiskEngine.h"
#include "xt_core/order/XtOrderRequest.h"

#include <cstdint>

/// @file
/// @brief 从订单请求构造 `XtRiskEvalContext` 的轻量 adapter。

namespace xt_risk {

inline XtRiskEvalContext make_eval_context(const xt_core::XtOrderRequest& req,
                                           int64_t now_ms_fallback,
                                           bool run_input_guards) noexcept {
    XtRiskEvalContext ctx{};
    ctx.now_ms           = (req.now_ms != 0) ? req.now_ms : now_ms_fallback;
    ctx.run_input_guards = run_input_guards;
    ctx.is_forced_close  = req.is_forced_close;
    ctx.dry_run          = req.dry_run;
    ctx.strategy_id      = req.sid_id;
    ctx.trace_id         = req.audit_trace_id;
    return ctx;
}

}
