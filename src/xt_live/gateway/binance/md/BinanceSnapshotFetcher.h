#pragma once

#include <atomic>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace binance_u {

/// @brief Binance `GET /fapi/v1/depth` REST snapshot 响应（已解析）。
struct DepthSnapshot {
    uint64_t last_update_id = 0;                        ///< binance `lastUpdateId` 字段
    std::vector<std::pair<double, double>> bids;        ///< `<price, qty>`，snapshot 时 qty 必 > 0
    std::vector<std::pair<double, double>> asks;        ///< 同上
};

/// @brief Binance public depth snapshot 拉取器
///
/// - MD book bootstrap 专用 fetcher：`fetch_sync()` 每次创建短生命周期 CURL handle，返回已解析的 `DepthSnapshot`
/// - WS diff 合并由 BinanceOrderBook 完成，线程/重试/重连策略由 MdBinanceUGateway 驱动。
///       
/// @warning - `fetch_sync()` 是 libcurl 同步阻塞调用，调用方必须放到独立线程，避免阻塞 WS `io_context`。
//           - `cancel` 通过 curl progress callback 尽快退出；
///            若 progress callback 未及时运行，则由 `timeout_sec` 兜底。
class BinanceSnapshotFetcher {
public:
    struct Config {
        std::string host;         ///< REST host，如 `fapi.binance.com` 或 testnet host。
        std::string port = "443"; ///< HTTPS 端口。
        int timeout_sec = 10;     ///< TCP connect + TLS handshake + read 总超时。
    };

    explicit BinanceSnapshotFetcher(Config cfg);

    /// @brief 同步拉取 depth snapshot；阻塞直至完成、失败或超时。
    /// @warning 必须在独立线程调用；`cancel` 只能请求尽快中止，最终仍受
    ///          `timeout_sec` 兜底约束。
    /// @param symbol  合约代码（传入时会被大写化后拼接到 URL，e.g. "BTCUSDT"）
    /// @param limit   档位数（binance 支持 5/10/20/50/100/500/1000；full book 用 1000）
    /// @param err     失败原因输出（可为 nullptr）
    /// @param cancel  可选 shutdown 标志；非 nullptr 且 load()==true 时 curl
    ///                progress callback 会返回非零让 curl_easy_perform 立即
    ///                CURLE_ABORTED_BY_CALLBACK 退出（让 snapshot_thread.join
    ///                在 stop() 时 ~ms 级返回，避免阻塞到 10s curl timeout）。
    /// @return        成功 std::optional<DepthSnapshot>；失败 std::nullopt
    std::optional<DepthSnapshot> fetch_sync(std::string_view symbol,
                                            int limit,
                                            std::string* err,
                                            std::atomic<bool>* cancel = nullptr);

private:
    Config cfg_; ///< 构造期配置，fetch_sync 只读。
};

}  // namespace binance_u
