#pragma once

/// @file
/// @brief Binance 交易网关的 wire 字段转换 helper。
///
/// Binance TD 接口里很多东西是字符串，比如 newClientOrderId、价格、数量、订单状态
/// 系统内部用的是 local_id、fixed-point 整数、XtOrderStatus enum。
/// BinanceTdWireUtil.h 就是两边之间的小翻译层
///
/// 本文件只处理 wire 字段格式，不发 REST/WS 请求，也不维护订单状态。

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "xt_core/order/XtOrderTypes.h"

namespace binance_u {

std::string base36_encode(uint64_t value);

std::optional<uint64_t> base36_decode(std::string_view s);

std::string make_client_order_id(uint64_t local_id, uint64_t nonce);

uint64_t parse_local_id(std::string_view client_order_id);

std::string fp_to_str(int64_t fp);

std::optional<XtOrderStatus> parse_binance_status_opt(std::string_view s);

XtOrderStatus parse_binance_status(std::string_view s);

}
