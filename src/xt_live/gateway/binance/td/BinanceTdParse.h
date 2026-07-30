#pragma once

/// @file BinanceTdParse.h
/// @brief 解析 Binance TD REST 返回体的 helper。
///
/// BinanceTdParse 把 Binance REST JSON body 转成 gateway 使用的内部结构
///
/// 本文件只做字符串解析

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "api/BrokerVenueTruth.h"
#include "api/ITradeGateway.h"
#include "api/StrategySid.h"

namespace binance_u {

/// @brief 账户真值详情：account 顶层资金字段 + positions[] per-symbol 字段。
struct AccountTruthDetail {
    ITradeGateway::AccountSnapshot acct;
    int64_t maint_margin_fp = 0;
    int64_t available_balance_fp = 0;
    int64_t total_margin_balance_fp = 0;
    std::vector<PerSymbolTruth> per_symbol;
};

/// @brief gap-fill userTrades 反查本地订单归属的结果。
struct GapFillOrderAttribution {
    uint64_t local_id = 0;
    bool is_close = false;
    StrategySid strategy_sid;
    std::string strategy_id;
};

[[nodiscard]] std::optional<AccountTruthDetail>
    parse_account_detail_body(const std::string& body);

[[nodiscard]] std::unordered_map<std::string, int64_t>
    parse_position_marks_body(const std::string& body);

[[nodiscard]] std::optional<double> parse_wallet_balance_body(const std::string& body);

[[nodiscard]] std::optional<GapFillOrderAttribution>
    parse_gap_fill_order_attribution_body(const std::string& body);

void populate_gap_fill_strategy_identity(GapFillOrderAttribution& attr);

}
