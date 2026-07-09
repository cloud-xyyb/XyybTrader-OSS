#pragma once

#include "api/XtRiskSnapshot.h"
#include "api/XtRiskTypes.h"

#include "xt_core/order/XtOrder.h"

/** @brief 风控守卫接口（input/capability/precheck + snapshot view）。 */
class IRiskGuardContext {
public:
    virtual ~IRiskGuardContext() = default;

    [[nodiscard]] virtual XtRiskSnapshot make_snapshot(const xt_core::XtOrder& req,
                                                       const XtRiskEvalContext& eval_ctx) const = 0;

    [[nodiscard]] virtual XtRiskResult precheck_input(const xt_core::XtOrder& req,
                                        const XtRiskEvalContext& eval_ctx) const = 0;

    [[nodiscard]] virtual XtRiskResult precheck_capability(const xt_core::XtOrder& req,
                                             const XtRiskEvalContext& eval_ctx) const = 0;

    [[nodiscard]] virtual XtRiskResult guard_before_submit(const xt_core::XtOrder& req,
                                             const XtRiskEvalContext& eval_ctx) const = 0;
};
