#pragma once

/// @file XtFillCostModel.h
/// @brief 成交成本模型的无状态计算入口。
///
/// 声明 fill-time fee/slippage 计算、live 成交成本归一化和 pre-trade fee 估算helper。
/// 调用方提供成交记录、滑点配置、手续费规则和合约规格；本文件不持有状态。

#include "api/IFeeRuleProvider.h"

struct XtCommodityInfo;
struct XtTradeCostConfig;
struct XtTradeRecord;
class XtCommodityCatalog;

namespace xt_fill_cost {

/// @brief 单笔成交成本计算结果。
struct XtAppliedTradeCost {
    double exec_price = 0.0;
    double fee = 0.0;
    double slippage = 0.0;
};

XtAppliedTradeCost apply(double base_price,
                         const XtTradeRecord& tr,
                         const XtTradeCostConfig& cost,
                         const IFeeRuleProvider* fees,
                         const XtCommodityInfo* comm,
                         bool sim_slippage);

XtTradeRecord normalize_trade_cost(const XtTradeRecord& tr,
                                   const XtTradeCostConfig* live_cost,
                                   const IFeeRuleProvider* live_fees,
                                   const XtCommodityCatalog* catalog);

double estimate_fee(const XtFeeRule& rule,
                    double price,
                    double qty,
                    bool is_open,
                    bool is_today,
                    bool is_buy,
                    const XtCommodityInfo* comm);

}
