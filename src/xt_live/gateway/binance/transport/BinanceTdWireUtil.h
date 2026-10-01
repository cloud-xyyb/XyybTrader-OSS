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

/// @brief 将整数编码为小写 base36；0 编码为 `"0"`。
std::string base36_encode(uint64_t value);

/// @brief 解析小写 base36；非法字符或 uint64 溢出时返回 std::nullopt。
std::optional<uint64_t> base36_decode(std::string_view s);

/// @brief 生成 Binance `newClientOrderId`。
/// @return 当前格式为 `ct_<local_id_base36>_<nonce_base36>`。
std::string make_client_order_id(uint64_t local_id, uint64_t nonce);

/// @brief 从本系统生成的 Binance client order id 中解析 local_id。
/// @return 解析失败或非本系统格式时返回 0。
uint64_t parse_local_id(std::string_view client_order_id);

/// @brief 将订单定点数值（scale 1e6）格式化为 Binance REST/WS 参数字符串。
std::string fp_to_str(int64_t fp);

/// @brief 已知 Binance 订单状态串 → XtOrderStatus 的单一映射；未知串返回 std::nullopt。
/// @note 已知状态表的 SSOT；调用方据自身语义决定未知串兜底（ack 路径 reject / 流式
///       路径 alive / ghost 路径 keep-alive），见 parse_binance_status 与各 caller。
std::optional<XtOrderStatus> parse_binance_status_opt(std::string_view s);

/// @brief 将 Binance 订单状态字符串映射为内部 XtOrderStatus；未知串兜底 Rejected。
XtOrderStatus parse_binance_status(std::string_view s);

}  // namespace binance_u
