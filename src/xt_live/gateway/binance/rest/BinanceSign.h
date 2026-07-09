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

std::string hmac_sha256_hex(std::string_view secret, std::string_view payload);

std::string url_encode(std::string_view s);

std::string build_query(const std::map<std::string, std::string>& params);

std::string sign_query(std::string_view secret,
                       const std::map<std::string, std::string>& params);

std::string sign_params(std::string_view secret,
                        const std::map<std::string, std::string>& params);

std::string api_key_sha8(std::string_view api_key);

uint64_t now_ms();

}
}
