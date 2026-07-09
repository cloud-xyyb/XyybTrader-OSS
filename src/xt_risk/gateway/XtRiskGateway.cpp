#include "xt_risk/gateway/XtRiskGateway.h"

#include "XtFixedPoint.h"

void XtRiskGateway::bind(XtRiskEngine& engine, const XtRiskPolicy& policy) {
    engine_ = &engine;
    policy_ = &policy;
}

XtRiskResult XtRiskGateway::evaluate(const xt_core::XtOrder& req,
                                     const IRiskGuardContext& guard_ctx,
                                     const XtRiskEvalContext& eval_ctx) {
    if (!engine_ || !policy_) {
        XtRiskResult out;
        out.passed = false;
        out.reason_code = XtRiskReasonCode::TradingNotReady;
        out.stage = XtRiskStage::InputGuard;
        out.reason_text = "risk gateway unbound";
        return out;
    }

    const XtRiskResult input = guard_ctx.precheck_input(req, eval_ctx);
    if (!input.passed) return input;

    const XtRiskResult capability = guard_ctx.precheck_capability(req, eval_ctx);
    if (!capability.passed) return capability;

    const XtRiskSnapshot snap = guard_ctx.make_snapshot(req, eval_ctx);
    XtRiskPolicy effective_policy = *policy_;
    if (eval_ctx.is_forced_close) {

        effective_policy.enforce_daily_loss  = false;
        effective_policy.enforce_annual_loss = false;
    }
    XtRiskResult core = engine_->evaluate(req, snap, effective_policy, eval_ctx);
    if (!core.passed) {
        return core;
    }

    XtRiskResult pre = guard_ctx.guard_before_submit(req, eval_ctx);
    if (!pre.passed) return pre;

    if (pre.required_margin_fp == 0) {
        pre.required_margin_fp  = core.required_margin_fp;
        pre.per_unit_margin_fp  = core.per_unit_margin_fp;
    }
    return pre;
}
