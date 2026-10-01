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

#include "api/BrokerVenueTruth.h"  // AccountTruthDetail::per_symbol 行类型。
#include "api/ITradeGateway.h"
#include "api/StrategySid.h"

namespace binance_u {

/// @brief 账户真值详情：account 顶层资金字段 + positions[] per-symbol 字段。
struct AccountTruthDetail {
    ITradeGateway::AccountSnapshot acct;
    int64_t maint_margin_fp = 0;         ///< account.totalMaintMargin，scale 1e6。
    int64_t available_balance_fp = 0;    ///< account.availableBalance，scale 1e6。
    int64_t total_margin_balance_fp = 0; ///< account.totalMarginBalance，scale 1e6。
    std::vector<PerSymbolTruth> per_symbol; ///< positions[] 展平结果；直接搬进 BrokerVenueTruth。
};

/// @brief gap-fill userTrades 反查本地订单归属的结果。
struct GapFillOrderAttribution {
    uint64_t local_id = 0;        ///< 本进程 local_id；0 表示无法归因。
    bool is_close = false;        ///< 原始订单是否 reduceOnly/close。
    StrategySid strategy_sid;     ///< local_id 关联的策略 sid。
    std::string strategy_id;      ///< sid registry 可解析时的策略名。
};

/// @brief 解析 `/fapi/v2/account` body；顶层资金字段失败即整包失败。
/// @return REST 或顶层资金字段解析失败返回 std::nullopt；per-symbol 行按 best-effort 过滤。
[[nodiscard]] std::optional<AccountTruthDetail>
    parse_account_detail_body(const std::string& body);

/// @brief 解析 positionRisk/markPrice 数组中的 symbol -> markPrice fixed-point。
/// @return 非 array 或解析失败返回空 map，调用方保留 mark price 为 0。
[[nodiscard]] std::unordered_map<std::string, int64_t>
    parse_position_marks_body(const std::string& body);

/// @brief 解析 `/fapi/v2/account` body 的 totalWalletBalance（停机对账 fresh 直比用）。
/// @note wallet 口径不含未实现盈亏，与 cash 账本全部锚源（UDS wb + REST）同口径；
///       totalMarginBalance（含 uPnL）仅供 truth overlay，两口径刻意分开。
/// @return 字段缺失或解析失败返回 std::nullopt。
[[nodiscard]] std::optional<double> parse_wallet_balance_body(const std::string& body);

/// @brief 从 `/fapi/v1/order` body 解析 clientOrderId/reduceOnly 归属字段。
/// @return clientOrderId 无法反解为本进程 local_id 时返回 std::nullopt。
[[nodiscard]] std::optional<GapFillOrderAttribution>
    parse_gap_fill_order_attribution_body(const std::string& body);

/// @brief 用 local_id 补 strategy sid/name；解析失败时保留默认空 identity。
void populate_gap_fill_strategy_identity(GapFillOrderAttribution& attr);

}  // namespace binance_u
