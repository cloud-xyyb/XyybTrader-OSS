#pragma once

// ---------------------------------------------------------------------------
// api/IRiskGuardContext.h — 风控守卫接口契约 (Risk guard contract port)
// ---------------------------------------------------------------------------

#include "api/XtRiskSnapshot.h"   // XtRiskSnapshot (POD)
#include "api/XtRiskTypes.h"      // XtRiskResult / XtRiskEvalContext / XtRiskReasonCode / XtRiskStage

#include "xt_core/order/XtOrder.h"      // xt_core::XtOrder (method 入参 by-const-ref)

/** @brief 风控守卫接口（input/capability/precheck + snapshot view）。 */
class IRiskGuardContext {
public:
    virtual ~IRiskGuardContext() = default;
    /// @brief 构建风控决策快照 (per-submit, 含 per-strategy 字段)。
    /// @param req 当前 order request
    /// @param eval_ctx 单次评估上下文 — `strategy_id` 用于 per-strategy 字段 fill
    ///   (per_strategy_today_pnl_fp ← tracker.today_pnl_fp(sid));其余 ctx 字段
    ///   实现层未必读取, 但接口契约通过 ctx 让 caller 一次传齐。
    [[nodiscard]] virtual XtRiskSnapshot make_snapshot(const xt_core::XtOrder& req,
                                                       const XtRiskEvalContext& eval_ctx) const = 0;
    /** @brief 输入守卫：限频/冷却/参数与运行态就绪检查。 */
    [[nodiscard]] virtual XtRiskResult precheck_input(const xt_core::XtOrder& req,
                                        const XtRiskEvalContext& eval_ctx) const = 0;
    /** @brief 能力守卫：柜台/行情能力是否支持该请求。 */
    [[nodiscard]] virtual XtRiskResult precheck_capability(const xt_core::XtOrder& req,
                                             const XtRiskEvalContext& eval_ctx) const = 0;
    /** @brief 提交前最终守卫：可平量、保证金等资源检查。 */
    [[nodiscard]] virtual XtRiskResult guard_before_submit(const xt_core::XtOrder& req,
                                             const XtRiskEvalContext& eval_ctx) const = 0;
};
