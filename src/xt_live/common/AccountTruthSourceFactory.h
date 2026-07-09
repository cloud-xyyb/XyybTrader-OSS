#pragma once

/// @file
/// @brief 为 live runner 创建账户真值源（IAccountTruthSource）。
///
/// make_runner_account_truth_source() 根据 live config 的 `td_provider` 选择 Binance、
/// CTP 或 store-backed source，并把 runner 持有的 AccountStateStore 接成基础快照来源。
///
/// Binance income REST provider 由调用方注入，避免 common factory 依赖 gateway 实现。

#include <memory>

#include "xt_account/api/IAccountTruthSource.h"
#include "xt_account/venue/binance/BinanceAccountTruthSource.h"

struct XtLiveConfig;
struct XtRunnerSignals;

namespace xt_account {
class AccountStateStore;
}

std::unique_ptr<IAccountTruthSource> make_runner_account_truth_source(
    const xt_account::AccountStateStore& account_store,
    XtRunnerSignals& signals,
    const XtLiveConfig& cfg,
    BinanceAccountTruthSource::IncomeProvider income_provider = {});
