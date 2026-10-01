#pragma once

#include <atomic>
#include <string>
#include <string_view>
#include <vector>

#include "BinanceAggGap.h"  // AggTrade

namespace binance_u {

/// @brief 解析 binance `GET /fapi/v1/aggTrades` JSON 数组为 AggTrade 列表。
/// @return true 成功（out 已填）；false 解析失败（err 已填）。
/// @note 纯函数，可单测；只取 a/q/p，忽略 f/l/T/m/nq/st 等字段。
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
        std::string host;          ///< REST host（与 depth snapshot 同数据面）。
        std::string port = "443";  ///< HTTPS 端口。
        int timeout_sec = 10;      ///< CURL 总超时。
    };

    explicit BinanceAggTradesFetcher(Config cfg) : cfg_(std::move(cfg)) {}

    /// @brief 同步拉一页 aggTrades（fromId 含界，limit ≤ 1000）。
    /// @param from_id  aggregate trade id 起点（INCLUSIVE）。
    /// @param limit    单页上限（binance max 1000）。
    /// @param cancel   可选 shutdown 标志（curl progress abort）。
    /// @param out      成功时填解析出的 AggTrade（升序，binance 默认）。
    /// @param err      失败原因输出（可为 nullptr）。
    /// @return true 表示 HTTP 2xx + 解析成功；false 表示 transport/HTTP/解析失败。
    bool fetch_sync(std::string_view symbol, int64_t from_id, int limit,
                    std::atomic<bool>* cancel,
                    std::vector<AggTrade>* out, std::string* err);

private:
    Config cfg_;
};

}  // namespace binance_u
