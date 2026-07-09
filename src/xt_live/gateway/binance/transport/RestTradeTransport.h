#pragma once

#include "ITradeTransport.h"

#include <cstdint>
#include <memory>

namespace binance_u {

class BinanceRestClient;

/// @brief REST cancel 对 venue 限频/封禁类失败的有界重试配置。
/// @note 仅作用于 cancel：submit 失败有 kRejected ack 收敛路径；cancel 失败
///       若不重试，委托会一直挂在交易所，堆到 per-symbol max-open-order 上限
///       后所有新单（含平仓）被 -2025 拒。
struct RestCancelRetryCfg {
    int      max_tries          = 4;
    uint64_t base_backoff_ms    = 500;
    uint64_t max_backoff_ms     = 5000;
    uint64_t max_retry_after_ms = 60000;
};

bool rest_cancel_error_retryable(long http_status, int err_code);

uint64_t rest_cancel_backoff_ms(int tries_done, long retry_after_sec,
                                const RestCancelRetryCfg& cfg);

/// @brief Binance TD 的 REST 下单发送器，实现 ITradeTransport
///
/// - TraderBinanceUGateway 把报单/撤单交给RestTradeTransport
/// - 把任务先入本地队列，然后由一个 REST worker 线程串行调用 Binance signed REST 接口处理
class RestTradeTransport : public ITradeTransport {
public:

    explicit RestTradeTransport(BinanceRestClient& rest,
                                RestCancelRetryCfg cancel_retry = {});

    ~RestTradeTransport() override;

    bool start() override;

    void stop() override;

    void submit(TransportSubmitReq&& req) override;

    void cancel(TransportCancelReq&& req) override;

    void bind_ack_handler(AckHandler handler) override;

    Health health() const override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
