#pragma once

#include <string>

/// @brief Binance U 本位永续行情网关配置。
///
/// - BinanceMdCfg 只放 Binance 行情网关的配置。通用的订阅列表、账号、密码这些还是放在 IMdGateway::Config。
/// - 行情 WS 和 depth snapshot REST 必须连同一套 Binance 环境 （futures mainnet 或都连 testnet）
struct BinanceMdCfg {

    enum class TickSource {
        TradeAndQuote,
        TradeOnly,
    };

    std::string base_ws_url;
    std::string base_rest_url;
    bool        testnet = true;
    TickSource  tick_source = TickSource::TradeAndQuote;
};
