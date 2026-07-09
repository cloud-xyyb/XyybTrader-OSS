#pragma once

#include <atomic>
#include <string>
#include <string_view>
#include <vector>

#include "BinanceAggGap.h"

namespace binance_u {

bool parse_agg_trades_response(const std::string& body,
                               std::vector<AggTrade>& out,
                               std::string* err);

/// @brief Binance public aggTrades 拉取器（`GET /fapi/v1/aggTrades`，无签名）。
///
/// 用于 reconnect-anchored gap 回补：按 fromId 翻页拉缺失的聚合成交。
///
/// WebSocket 漏了成交时，去 Binance REST 把漏掉的成交拉回来
/// 给它 symbol 和 fromId，它去 Binance 拉一页 aggTrades，然后把 JSON 解析成 AggTrade 列表
///
/// @warning `fetch_sync` 同步阻塞，必须放到独立线程，勿阻塞 WS io_context。
class BinanceAggTradesFetcher {
public:
    struct Config {
        std::string host;
        std::string port = "443";
        int timeout_sec = 10;
    };

    explicit BinanceAggTradesFetcher(Config cfg) : cfg_(std::move(cfg)) {}

    bool fetch_sync(std::string_view symbol, int64_t from_id, int limit,
                    std::atomic<bool>* cancel,
                    std::vector<AggTrade>* out, std::string* err);

private:
    Config cfg_;
};

}
