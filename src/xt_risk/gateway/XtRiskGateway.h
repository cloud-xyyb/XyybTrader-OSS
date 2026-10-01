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

    /// @brief 绑定非拥有的核心规则引擎与基础 policy。
    /// @warning 调用方必须保证 engine/policy 生命周期覆盖 gateway 使用期。
    void bind(XtRiskEngine& engine, const XtRiskPolicy& policy);

    /// @brief 执行完整风控链，返回首个失败阶段或最终通过结果。
    /// @param req 待评估订单；非法字段由 input/core 阶段转成拒单结果。
    /// @param guard_ctx 运行态/venue 守卫实现，提供 precheck、snapshot 与提交前门禁。
    /// @param eval_ctx 单次评估 metadata；is_forced_close 会在局部 policy 中关闭 daily/annual loss。
    /// @note 未 bind 时返回 TradingNotReady/InputGuard；不抛异常。
    /// @note guard_before_submit 未填 margin 字段时，透传 core 的 stock-mode 资金占用估算
    ///       （期货 margin 由 guard 自填，core 对期货恒返 0）。
    XtRiskResult evaluate(const xt_core::XtOrder& req,
                          const IRiskGuardContext& guard_ctx,
                          const XtRiskEvalContext& eval_ctx);

private:
    XtRiskEngine* engine_ = nullptr;       ///< non-owning core rule engine。
    const XtRiskPolicy* policy_ = nullptr; ///< non-owning base policy。
};
