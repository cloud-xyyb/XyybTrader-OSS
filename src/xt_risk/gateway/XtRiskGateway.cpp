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

    // 输入守卫先处理绝对门禁和消耗型限速；失败时不读取后续 provider。
    const XtRiskResult input = guard_ctx.precheck_input(req, eval_ctx);
    if (!input.passed) return input;

    // 通道能力先于核心规则，避免把 venue 不支持的请求归因到 policy 限制。
    const XtRiskResult capability = guard_ctx.precheck_capability(req, eval_ctx);
    if (!capability.passed) return capability;

    // make_snapshot 依赖 eval_ctx.strategy_id 补 per-sid 字段，放在 input/capability 之后
    // 避免无效请求触发账户、PnL 或 monitor provider 读取。
    const XtRiskSnapshot snap = guard_ctx.make_snapshot(req, eval_ctx);
    XtRiskPolicy effective_policy = *policy_;
    if (eval_ctx.is_forced_close) {
        // 强制平仓必须能穿透亏损 latch；copy-on-write 保持共享 policy_ 只读。
        effective_policy.enforce_daily_loss  = false;
        effective_policy.enforce_annual_loss = false;
    }
    XtRiskResult core = engine_->evaluate(req, snap, effective_policy, eval_ctx);
    if (!core.passed) {
        return core;
    }

    // 提交前资源门禁最后执行，使用最新账户、可平量和在途订单视图。
    XtRiskResult pre = guard_ctx.guard_before_submit(req, eval_ctx);
    if (!pre.passed) return pre;
    // 某些 guard context 只做最终资源校验，通过结果不重填 margin 字段。
    // 保留 core 估算，确保 accept-side accounting 仍能 reserve frozen_cash。
    if (pre.required_margin_fp == 0) {
        pre.required_margin_fp  = core.required_margin_fp;
        pre.per_unit_margin_fp  = core.per_unit_margin_fp;
    }
    return pre;
}
