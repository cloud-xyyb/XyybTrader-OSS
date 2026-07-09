#pragma once

/// @file
/// @brief 以 AccountStateStore 快照组装 AccountTruth 的 store-side provider。
///
/// 纯 account 域快照读取：账户资金 + trading day 推导 + cashflow gating +
/// 可选 per-symbol store-mirror。venue-specific REST/polling 细节留在
/// gateway 或 venue truth source 适配器。

#include "xt_account/api/IAccountTruthSource.h"
#include "xt_core/session/XtTradingDay.h"

struct XtRunnerSignals;

namespace xt_account {

class AccountStateStore;

AccountTruth read_store_account_truth(const AccountStateStore& account_store,
                                      XtRunnerSignals& signals,
                                      xt_core::TimezonePolicy trading_day_policy,
                                      bool fill_store_per_symbol);

}
