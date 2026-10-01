#pragma once

#include <string>
#include <string_view>

namespace binance_u {

/// @brief Binance base URL 解析结果。
struct BinanceWsUrl {
    std::string host;      ///< 不含 scheme 与端口的 host。
    std::string port;      ///< URL 显式端口；无端口时使用调用方提供的 default_port。
    std::string path_root; ///< URL path 部分；无 path 时为空串。
};

/// @brief 解析 `wss://host[:port][/path]` 或 `https://host[:port][/path]` 形式的 base URL。
/// @note 只做轻量拆分，不校验 scheme 类型；调用方负责确保配置语义正确。
BinanceWsUrl parse_binance_ws_url(std::string_view url, std::string_view default_port);

}  // namespace binance_u
