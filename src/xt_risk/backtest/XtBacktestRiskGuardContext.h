#pragma once

#include "state/AccountStateStore.h"
#include "xt_risk/engine/XtRiskEngine.h"

namespace xt_runtime { namespace order { class XtOrderRuntimeBook; } }
class XtCommodityCatalog;
class ISTPChecker;

/// @brief 回测侧 `IRiskGuardContext` 实现。
///
/// XtBacktestRiskGuardContext 把账户状态、在途订单簿、涨跌停、STP 和当日 PnL 组织成
/// `XtRiskEngine` 可消费的 snapshot 与提交前资源门禁。
class XtBacktestRiskGuardContext final : public IRiskGuardContext {
public:

    XtBacktestRiskGuardContext(const xt_account::AccountStateStore& account,
                               const xt_runtime::order::XtOrderRuntimeBook* order_book = nullptr,
                               const XtCommodityCatalog* catalog = nullptr,
                               int64_t limit_up_fp = 0,
                               int64_t limit_down_fp = 0,
                               const ISTPChecker* stp = nullptr,
                               int64_t today_pnl_fp = 0);

    XtRiskSnapshot make_snapshot(const xt_core::XtOrder& req,
                                 const XtRiskEvalContext& eval_ctx) const override;

    XtRiskResult precheck_input(const xt_core::XtOrder& req,
                                  const XtRiskEvalContext& eval_ctx) const override;

    XtRiskResult precheck_capability(const xt_core::XtOrder& req,
                                       const XtRiskEvalContext& eval_ctx) const override;

    XtRiskResult guard_before_submit(const xt_core::XtOrder& req,
                                       const XtRiskEvalContext& eval_ctx) const override;

private:
    const xt_account::AccountStateStore& account_;
    const xt_runtime::order::XtOrderRuntimeBook* order_book_ = nullptr;
    const XtCommodityCatalog* catalog_ = nullptr;
    int64_t limit_up_fp_ = 0;
    int64_t limit_down_fp_ = 0;
    const ISTPChecker* stp_ = nullptr;
    int64_t today_pnl_fp_ = 0;
};
