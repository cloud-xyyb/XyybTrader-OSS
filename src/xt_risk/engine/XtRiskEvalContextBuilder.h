#pragma once

#include "xt_risk/engine/XtRiskEngine.h"
#include "xt_core/order/XtOrderRequest.h"

#include <cstdint>

/// @file
/// @brief 从订单请求构造 `XtRiskEvalContext` 的轻量 adapter。

namespace xt_risk {

/// @brief 将 XtOrderRequest metadata 显式桥接为 XtRiskEvalContext。
///
/// 字段只做直通或兜底：sid_id -> strategy_id，audit_trace_id -> trace_id，
/// dry_run/is_forced_close 原样复制，now_ms 为 0 时使用 fallback。
///
/// @param req 已填充 submit metadata 的订单请求。
/// @param now_ms_fallback req.now_ms 为 0 时使用的评估时间戳，单位 ms。
/// @param run_input_guards caller 控制的 input guard 开关；常用于切片首单/后续单区分。
/// @warning 不从 req.source 推导 is_forced_close；强平语义必须由 caller 显式设置。
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

} // namespace xt_risk
