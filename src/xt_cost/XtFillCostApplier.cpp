#include "XtFillCostApplier.h"

#include "XtFillCostModel.h"
#include "xt_config/catalog/XtCommodityCatalog.h"
#include "xt_core/order/XtTradeRecord.h"

namespace xt_fill_cost {

void FillCostApplier::init(XtTradeCostConfig cost,
                           const IFeeRuleProvider* fees,
                           const XtCommodityCatalog* commodities) {
    cost_ = std::move(cost);
    fees_ = fees;
    commodities_ = commodities;
}

void FillCostApplier::apply(XtTradeRecord& trade) const {
    const XtCommodityInfo* comm = commodities_ ? commodities_->find(trade.code.c_str()) : nullptr;

    const double base_price = (trade.exec_price > 0.0) ? trade.exec_price : trade.price;
    const XtAppliedTradeCost cost_applied =
        xt_fill_cost::apply(base_price, trade, cost_, fees_, comm, true);
    trade.exec_price = cost_applied.exec_price;
    trade.fee        = cost_applied.fee;
    trade.slippage   = cost_applied.slippage;
}

void FillCostApplier::apply(std::vector<XtTradeRecord>& trades) const {
    for (auto& tr : trades) apply(tr);
}

}
