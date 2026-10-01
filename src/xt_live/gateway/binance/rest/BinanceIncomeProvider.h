#pragma once

#include "xt_account/venue/binance/BinanceAccountTruthSource.h"

struct BinanceTdCfg;

namespace binance_u {

/// @brief 将 Binance signed income REST 查询适配为 AccountTruthSource 的 IncomeProvider。
/// @note make_binance_income_provider 拥有 REST client、认证配置、分页与超时预算；
///       AccountTruthSource 只决定何时按交易日窗口调用它，并把返回的 income rows
///       过滤/汇总为 `external_cashflow_today_fp`。
BinanceAccountTruthSource::IncomeProvider make_binance_income_provider(const BinanceTdCfg& cfg);

}  // namespace binance_u
