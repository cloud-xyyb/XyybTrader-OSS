#pragma once

#include <cstdint>
#include <string>
#include <vector>

/// @brief 单 symbol 的 broker / venue 持仓真值行。
///
/// 数量、价格、盈亏、保证金字段均为 fixed-point(1e6)。venue REST/TD 回报填充，
/// 供 report 与 Prometheus per-symbol gauge；不要求所有 venue 都提供。
struct PerSymbolTruth {
    std::string symbol;
    int64_t qty_fp = 0;
    int64_t avg_price_fp = 0;
    int64_t mark_price_fp = 0;
    int64_t unrealized_pnl_fp = 0;
    int64_t margin_fp = 0;
};

/// @brief gateway 向 read-side 推送的 broker venue-truth overlay（venue-neutral）。
///
/// gateway 在收到 venue 账户/持仓查询回报后构造并经 `ITradeGateway::Callbacks::
/// on_broker_account_truth` emit；runner 层 bridge 转发进写侧 `IAccountTruthSink::
/// update_broker_overlay`（读侧 IAccountTruthSource 只出快照，不收灌入）。
/// **不**经 account_store reducer（与 fill-derived 状态分离，
/// 供 store-vs-broker divergence 交叉校验）。
///
/// 金额字段 fixed-point(1e6)；venue 不提供的字段保持 0。CTP 仅填 available；
/// binance 填全字段并置 per_symbol_is_broker_truth=true。
struct BrokerVenueTruth {
    int64_t available_balance_fp = 0;
    int64_t maint_margin_fp = 0;
    int64_t total_margin_balance_fp = 0;
    std::vector<PerSymbolTruth> per_symbol;
    bool per_symbol_is_broker_truth = false;
};
