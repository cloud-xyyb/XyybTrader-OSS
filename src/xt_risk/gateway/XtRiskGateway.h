#pragma once

#include "xt_risk/engine/XtRiskEngine.h"

/// @brief 下单前风控编排层（input -> capability -> core -> submit-guard）。
///
/// 固定执行顺序：
/// 1. precheck_input：硬断路器、kill switch、限频/冷却、基础运行态。
/// 2. precheck_capability：TD/MD 能力与 venue capability。
/// 3. make_snapshot + XtRiskEngine::evaluate：核心规则与 per-strategy 规则。
/// 4. guard_before_submit：可平量、保证金/现金等提交前资源门禁。
///
/// @note 本类只返回 XtRiskResult，不负责调用 IRejectSink 或写日志。
class XtRiskGateway {
public:
    XtRiskGateway() = default;

    void bind(XtRiskEngine& engine, const XtRiskPolicy& policy);

    XtRiskResult evaluate(const xt_core::XtOrder& req,
                          const IRiskGuardContext& guard_ctx,
                          const XtRiskEvalContext& eval_ctx);

private:
    XtRiskEngine* engine_ = nullptr;
    const XtRiskPolicy* policy_ = nullptr;
};
