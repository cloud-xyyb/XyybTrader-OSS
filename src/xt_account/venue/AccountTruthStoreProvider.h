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

/// @brief 从 AccountStateStore 读取一份 AccountTruth 快照。
/// @param account_store 运行期账本快照来源。
/// @param signals 共享信号板；提供 last_tick_date / external cashflow 上下文。
/// @param trading_day_policy 用于 last_tick_date 缺失时从 wall clock 推导交易日。
/// @param fill_store_per_symbol true 时以 account_store position 填 per_symbol store mirror。
/// @return 按值返回的 AccountTruth；缺失字段保持 0。
AccountTruth read_store_account_truth(const AccountStateStore& account_store,
                                      XtRunnerSignals& signals,
                                      xt_core::TimezonePolicy trading_day_policy,
                                      bool fill_store_per_symbol);

}  // namespace xt_account
