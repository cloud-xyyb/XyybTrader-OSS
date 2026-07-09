#pragma once

#include <string>
#include <string_view>

namespace binance_u {

/// @brief Binance base URL 解析结果。
struct BinanceWsUrl {
    std::string host;
    std::string port;
    std::string path_root;
};

BinanceWsUrl parse_binance_ws_url(std::string_view url, std::string_view default_port);

}
