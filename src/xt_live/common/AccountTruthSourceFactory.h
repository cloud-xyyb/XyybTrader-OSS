#pragma once

/// @file
/// @brief 为 live runner 创建账户真值源（IAccountTruthSource）。
///
/// make_runner_account_truth_source() 根据 live config 的 `td_provider` 选择 Binance、
/// CTP 或 store-backed source，并把 runner 持有的 AccountStateStore 接成基础快照来源。
///
/// Binance income REST provider 由调用方注入，避免 common factory 依赖 gateway 实现。

#include <memory>

#include "xt_account/api/IAccountTruthSink.h"
#include "xt_account/api/IAccountTruthSource.h"
#include "xt_account/venue/binance/BinanceAccountTruthSource.h"

struct XtLiveConfig;
struct XtRunnerSignals;

namespace xt_account {
class AccountStateStore;
}

/// @brief 一个 truth 对象的两个 port 视图；读侧同时持有所有权。
/// @note `sink` 指向 `source` 同一对象，随 `source` 一起析构；venue 分发在 factory
///       内完成，装配点因此不需要 dynamic_cast 就能拿到写侧句柄。
struct RunnerAccountTruth {
    std::unique_ptr<IAccountTruthSource> source;
    IAccountTruthSink* sink = nullptr;
};

RunnerAccountTruth make_runner_account_truth_source(
    const xt_account::AccountStateStore& account_store,
    XtRunnerSignals& signals,
    const XtLiveConfig& cfg,
    BinanceAccountTruthSource::IncomeProvider income_provider = {});
