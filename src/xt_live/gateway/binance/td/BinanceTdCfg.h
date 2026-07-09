#pragma once

#include <string>

/// @brief Binance U 本位永续 TD 专属配置。
///
/// 只保存 Binance REST/UDS/WS trade 端点、认证和 transport 行为开关；
/// provider-neutral 字段仍由 `ITradeGateway::Config` 承载。
struct BinanceTdCfg {

    enum class Transport { kRest, kWs };

    std::string base_rest_url;
    std::string base_ws_url;
    std::string api_key;
    std::string api_secret;
    uint32_t listen_key_refresh_ms = 30 * 60 * 1000;
    bool     testnet = true;
    int      recv_window_ms = 5000;
    uint32_t time_sync_interval_ms = 60 * 1000;

    Transport transport = Transport::kRest;
    std::string base_ws_trade_url;
    uint32_t ws_submit_timeout_ms = 6000;
    uint32_t ws_stale_drop_ms = 1500;
    uint32_t ws_send_queue_cap = 256;
    uint32_t ws_reconnect_min_ms = 500;
    uint32_t ws_reconnect_max_ms = 30000;

    bool     uds_first_effective_ack = false;

    bool     close_reject_plain_fallback = false;

};
