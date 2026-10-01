#pragma once

#include <string>

/// @brief Binance U 本位永续行情网关配置。
///
/// - BinanceMdCfg 只放 Binance 行情网关的配置。通用的订阅列表、账号、密码这些还是放在 IMdGateway::Config。
/// - 行情 WS 和 depth snapshot REST 必须连同一套 Binance 环境 （futures mainnet 或都连 testnet）
struct BinanceMdCfg {
    /// @brief tick 驱动模型：哪些上游流触发一笔 XtRawTick push。
    ///
    /// aggTrade 永远推（trade-driven 基线）；depth 永远静默维护 L2..N；
    /// bookTicker 永远订阅 + 静默维护鲜 L1（供 tick 的 L1）。TickSource 仅控制
    /// bookTicker 是否**额外再推一笔** quote tick。
    enum class TickSource {
        TradeAndQuote,  ///< 默认：aggTrade 与 bookTicker 都推（≈现行为 + L1 修复）。
        TradeOnly,      ///< 仅 aggTrade 推（trade-time 采样，tick=成交，无重复）；book-ready 后推一帧 quote 作 mark seed。
    };

    std::string base_ws_url;     ///< Binance public MD WS 基础 URL，如 `wss://fstream.binance.com`。
    std::string base_rest_url;   ///< Binance public REST 基础 URL；仅用于 depth snapshot。
    bool        testnet = true;  ///< 是否连接 testnet；仅用于日志/诊断，不改变 URL。
    TickSource  tick_source = TickSource::TradeAndQuote;  ///< bookTicker 是否额外推 tick；yaml `md.binance.tick_source`。
};
