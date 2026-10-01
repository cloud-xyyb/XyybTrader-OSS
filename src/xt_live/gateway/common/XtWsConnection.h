#pragma once

#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl/context.hpp>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace xt_live::gateway {

/// @brief 通用 WebSocket 客户端连接封装
///
/// 负责 WS 连接的完整生命周期（TLS 握手、读循环、重连退避），
/// 各 venue 的行情流、回报流和 WS 下单的底层基础。
/// 
/// @warning 不拥有也不 join io_context 线程；
class XtWsConnection {
public:
    /// @brief WS 连接参数与重连/保活阈值。
    struct Config {
        std::string host;          ///< WS host，如 `fstream.binance.com`。
        std::string port = "443";  ///< TLS 端口。
        std::string target;        ///< WS target path，如 `/stream?streams=btcusdt@depth`。
        std::string log_tag = "WS"; ///< 日志标签，由 owner 注入（如 `BinanceMD`/`BinanceUDS`/`BinanceWSTrade`）。
        uint32_t reconnect_min_ms = 500;    ///< 主动重连或首轮重试的最小退避。
        uint32_t reconnect_max_ms = 30000;  ///< 连续失败时的最大退避。
        uint32_t stale_threshold_ms = 60000; ///< 超过该时间无消息时由外部健康检查判 stale。
        /// HTTP CONNECT 代理（dev 场景，如 WSL 直连数据面被代理 TUN 掐断）。
        /// 两者均非空才启用：TCP 先连代理，CONNECT 2xx 后在同一 socket 上继续
        /// 既有 TLS/WS 握手（SNI/hostname 校验/Host 头均对真实 host，TLS 端点
        /// 不是代理）。默认空 = 直连，路径与历史逐字节相同。ctor 会在两者为空时
        /// 读 env `XT_WS_PROXY` 填充；只配一半按直连处理并 warn。
        std::string proxy_host;  ///< 代理主机；空 = 不走代理。
        std::string proxy_port;  ///< 代理端口；与 proxy_host 成对。
    };

    using OnMessage = std::function<void(std::string_view)>;
    using OnConnect = std::function<void()>;
    using OnDisconnect = std::function<void()>;

    XtWsConnection(boost::asio::io_context& ioc,
                   boost::asio::ssl::context& ssl_ctx,
                   Config cfg);
    ~XtWsConnection();

    XtWsConnection(const XtWsConnection&) = delete;
    XtWsConnection& operator=(const XtWsConnection&) = delete;

    /// @brief 绑定连接回调；所有回调在传入的 io_context 线程上触发。
    void bind_callbacks(OnMessage on_msg, OnConnect on_conn, OnDisconnect on_disc);

    /// @brief 异步启动连接；立即返回，断线后按退避策略自动重连。
    void start();

    /// 异步 stop；可重复调用，只停止内部 session/state。
    /// @warning 不会停止 `io_context`，也不会 join owner 创建的线程。
    void stop();

    /// @brief async_write 成功完成后触发的可选回调。
    /// @note 调用方用它记录真实 client-side write done 时刻（如延迟切片 t0c）。
    using WriteCompleteCb = std::function<void()>;

    /// @brief 发送 text frame；线程安全，内部 post 到 io_context。
    /// @param payload JSON / WS frame body，调用后移入内部队列。
    /// @param on_write_done 可选成功回调；仅在 async_write 无错误完成后触发一次。
    ///        连接未建立、消息被丢弃或 write 失败时不会触发，调用方应依赖自身超时机制。
    void send_text(std::string payload, WriteCompleteCb on_write_done = {});

    /// @brief 主动重连：关闭当前 session，并按最小退避调度下一次连接。
    void request_reconnect();

    /// @brief 返回最近连接状态快照；跨线程原子读取。
    bool is_connected() const;

    /// @brief 外部健康检查入口；超过 stale_threshold_ms 未收到消息时返回 false。
    /// @note 本函数只判定 stale，不主动重连；调用方决定是否调用 request_reconnect()。
    bool check_stale(uint32_t now_ms) const;

private:
    class Impl;  ///< 隐藏 resolver/socket/timer 等实现细节，避免调用方依赖底层 IO 类型。
    std::unique_ptr<Impl> impl_;  ///< 内部状态唯一 owner；析构在 .cpp 中看到完整 Impl。
};

/// @brief 解析 `XT_WS_PROXY` 形态的代理地址 → {host, port}。
/// 接受 `http://host:port`（可带尾 `/`）与裸 `host:port`；不支持认证（含 `@`）、
/// IPv6 字面量（含 `[` 或 host 内含 `:`）、缺端口/端口越界（1-65535）、其它
/// scheme——解析失败返回空 host，调用方按未配置处理。
inline std::pair<std::string, std::string> parse_ws_proxy_url(std::string_view url) {
    constexpr std::string_view kScheme = "http://";
    if (url.substr(0, kScheme.size()) == kScheme) url.remove_prefix(kScheme.size());
    while (!url.empty() && url.back() == '/') url.remove_suffix(1);
    if (url.empty() || url.find('/') != std::string_view::npos ||
        url.find('@') != std::string_view::npos ||
        url.find('[') != std::string_view::npos ||
        url.find("://") != std::string_view::npos) {
        return {};
    }
    const auto colon = url.rfind(':');
    if (colon == std::string_view::npos || colon == 0 || colon + 1 == url.size()) return {};
    const std::string_view host = url.substr(0, colon);
    // 裸 IPv6（host 内还有 ':'）会被 rfind 误切成垃圾 host/port——显式拒绝。
    if (host.find(':') != std::string_view::npos) return {};
    const std::string_view port = url.substr(colon + 1);
    if (port.size() > 5) return {};
    uint32_t port_num = 0;
    for (char c : port) {
        if (c < '0' || c > '9') return {};
        port_num = port_num * 10 + static_cast<uint32_t>(c - '0');
    }
    if (port_num == 0 || port_num > 65535) return {};
    return {std::string(host), std::string(port)};
}

}  // namespace xt_live::gateway
