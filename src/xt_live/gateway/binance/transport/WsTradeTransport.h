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
        std::string host;       ///< Binance WS host，如 `ws-fapi.binance.com` 或 testnet host。
        std::string port = "443";       ///< TLS 连接端口。
        std::string target = "/ws-fapi/v1"; ///< ws-fapi target path，必须以 `/` 开头。
        uint32_t submit_timeout_ms = 6000;  ///< 单笔 submit/cancel 总超时，需覆盖 recvWindow。
        uint32_t stale_drop_ms = 1500;      ///< 请求入队后超过该时间未发送则直接 stale。
        uint32_t send_queue_cap = 256;      ///< 未连接或背压时本地发送队列上限。
        uint32_t reconnect_min_ms = 500;    ///< 底层 WS 最小重连退避。
        uint32_t reconnect_max_ms = 30000;  ///< 底层 WS 最大重连退避。
    };

    /// @brief 构造 WS 交易 transport。
    /// @param cfg WS endpoint、超时、队列容量与重连退避配置；按值接收并由 transport 持有。
    /// @param rest_for_time_offset 已完成 server time sync 的 REST client 引用；
    ///        transport 只读取 server-local 时间偏移，不拥有该 client。
    /// @param cred ws-fapi 下单认证信息；由 transport 持有，用于构造签名 submit/cancel 请求。
    WsTradeTransport(Config cfg,
                     BinanceRestClient& rest_for_time_offset,
                     WsTradeCredentials cred);
    ~WsTradeTransport() override;

    /// @copydoc ITradeTransport::start
    bool start() override;
    /// @copydoc ITradeTransport::stop
    void stop() override;
    /// @copydoc ITradeTransport::submit
    void submit(TransportSubmitReq&& req) override;
    /// @copydoc ITradeTransport::cancel
    void cancel(TransportCancelReq&& req) override;
    /// @copydoc ITradeTransport::bind_ack_handler
    void bind_ack_handler(AckHandler handler) override;
    /// @copydoc ITradeTransport::health
    Health health() const override;

    /// @brief 进程内累计的 WS submit 回包 execution-unknown 事件数（codec 把 5xx /
    ///        -1007 等分类为保留在途、交 reconcile 收敛的次数）。观测用，无 prom
    ///        metric；跨线程原子读。
    uint64_t submit_exec_unknown_count() const;

    /// @brief 测试注入：模拟收到一条 WS payload，不需要真实 socket。
    void inject_test_message(std::string_view payload);

    /// @brief 测试注入：模拟底层 WS on_connect，触发连接态切换并 drain 队列。
    void inject_test_connect();

    /// @brief 测试注入：模拟底层 WS on_disconnect，清空 inflight 并进入重连态。
    void inject_test_disconnect();

    /// @brief 测试注入：推进内部测试时钟，用于确定性超时测试。
    void inject_test_advance_clock(uint64_t delta_us);

    /// @brief 测试注入：取出已发送 payload 并清空测试缓存。
    std::vector<std::string> inject_test_take_sent_payloads();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace binance_u
