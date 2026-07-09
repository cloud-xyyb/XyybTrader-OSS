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
    uint64_t last_update_id = 0;
    std::vector<std::pair<double, double>> bids;
    std::vector<std::pair<double, double>> asks;
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
        std::string host;
        std::string port = "443";
        int timeout_sec = 10;
    };

    explicit BinanceSnapshotFetcher(Config cfg);

    std::optional<DepthSnapshot> fetch_sync(std::string_view symbol,
                                            int limit,
                                            std::string* err,
                                            std::atomic<bool>* cancel = nullptr);

private:
    Config cfg_;
};

}
