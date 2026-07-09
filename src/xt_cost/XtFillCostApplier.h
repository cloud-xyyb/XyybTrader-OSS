#pragma once

#include <vector>

#include "XtTradeCostConfig.h"
#include "api/IFeeRuleProvider.h"

class XtCommodityCatalog;
struct XtTradeRecord;

namespace xt_fill_cost {

/// @brief 为成交记录补齐执行成本字段。
///
/// 调用方先用 `init()` 注入滑点配置、费率规则和合约规格；之后通过 `apply()`
/// 对单笔或批量 `XtTradeRecord` 原地写入 `exec_price`、`fee` 和 `slippage`。
/// 费率与合约规格来源为 non-owning，可为空；为空或未命中时按零手续费 / `vol_scale=1.0` 处理。
class FillCostApplier {
public:
    FillCostApplier() = default;

    void init(XtTradeCostConfig cost, const IFeeRuleProvider* fees, const XtCommodityCatalog* commodities);

    void apply(std::vector<XtTradeRecord>& trades) const;

    void apply(XtTradeRecord& trade) const;

private:
    XtTradeCostConfig cost_;
    const IFeeRuleProvider* fees_ = nullptr;
    const XtCommodityCatalog* commodities_ = nullptr;
};

}
