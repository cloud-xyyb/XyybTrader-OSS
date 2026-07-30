#pragma once

#include "ITradeTransport.h"
#include "WsTradeCodec.h"

#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace binance_u {

class BinanceRestClient;

/// @brief Binance TD 的 WebSocket 下单发送器，实现 ITradeTransport
///
/// - 用一条 Binance ws-fapi WebSocket 连接发送报单/撤单，不走 REST
/// - TraderBinanceUGateway 把 request 交给 WsTradeTransport, 封装后异步发送
class WsTradeTransport : public ITradeTransport {
public:
    /// @brief WebSocket TD transport 的连接与队列参数。
    struct Config {
        std::string host;
        std::string port = "443";
        std::string target = "/ws-fapi/v1";
        uint32_t submit_timeout_ms = 6000;
        uint32_t stale_drop_ms = 1500;
        uint32_t send_queue_cap = 256;
        uint32_t reconnect_min_ms = 500;
        uint32_t reconnect_max_ms = 30000;
    };

    WsTradeTransport(Config cfg,
                     BinanceRestClient& rest_for_time_offset,
                     WsTradeCredentials cred);
    ~WsTradeTransport() override;

    bool start() override;

    void stop() override;

    void submit(TransportSubmitReq&& req) override;

    void cancel(TransportCancelReq&& req) override;

    void bind_ack_handler(AckHandler handler) override;

    Health health() const override;

    uint64_t submit_exec_unknown_count() const;

    void inject_test_message(std::string_view payload);

    void inject_test_connect();

    void inject_test_disconnect();

    void inject_test_advance_clock(uint64_t delta_us);

    std::vector<std::string> inject_test_take_sent_payloads();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
