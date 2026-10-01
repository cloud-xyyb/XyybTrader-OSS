#pragma once

/// @file
/// @brief Binance signed endpoint 的 query 构造和签名 helper。
///
/// BinanceSign 提供 URL encode、稳定 query 拼接、HMAC-SHA256 签名和
/// api key 指纹计算。调用方负责把 timestamp / recvWindow 等业务参数放入
/// params；本文件只按给定参数生成 Binance REST/WS signed endpoint 需要的
/// query 和 signature。

#include <cstdint>
#include <map>
#include <string>
#include <string_view>

namespace binance_u {

namespace sign {

/// @brief HMAC-SHA256 hex digest（64 字符小写十六进制）。
/// @param secret API secret，不做 URL encode。
/// @param payload 已按最终提交顺序拼好的 query/body 字符串。
/// @throws std::runtime_error OpenSSL HMAC 失败时抛出；不得发出 unsigned signed endpoint 请求。
std::string hmac_sha256_hex(std::string_view secret, std::string_view payload);

/// @brief URL-encode 单个字符串（百分号编码，RFC 3986 unreserved 外全部 %XX）。
std::string url_encode(std::string_view s);

/// @brief 拼 query string；`std::map` 使 key 按字典序稳定输出。
/// @note value 会做 URL encode，key 不做 encode；签名 payload 与实际提交 payload 必须一致。
std::string build_query(const std::map<std::string, std::string>& params);

/// @brief 追加 `signature=<hmac_hex>` 后返回完整 signed query。
/// @param params 必须已包含 Binance signed endpoint 需要的 timestamp/recvWindow 等参数。
std::string sign_query(std::string_view secret,
                       const std::map<std::string, std::string>& params);

/// @brief 对 params 构造出的 query 计算 HMAC-SHA256 hex signature，不追加到 query。
std::string sign_params(std::string_view secret,
                        const std::map<std::string, std::string>& params);

/// @brief sha256(api_key) 前 8 hex（小写）指纹；用于启动期 identity 自证
///        （runtime.td.expected_api_key_sha8），单向不可逆、日志可打印。
/// @throws std::runtime_error OpenSSL digest 失败时抛出。
std::string api_key_sha8(std::string_view api_key);

/// @brief 当前 UTC 毫秒时间戳（`chrono::system_clock::now()` 取 epoch ms）。
uint64_t now_ms();

}  // namespace sign
}  // namespace binance_u
