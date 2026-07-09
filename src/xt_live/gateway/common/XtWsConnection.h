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
        std::string host;
        std::string port = "443";
        std::string target;
        std::string log_tag = "WS";
        uint32_t reconnect_min_ms = 500;
        uint32_t reconnect_max_ms = 30000;
        uint32_t stale_threshold_ms = 60000;

        std::string proxy_host;
        std::string proxy_port;
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

    void bind_callbacks(OnMessage on_msg, OnConnect on_conn, OnDisconnect on_disc);

    void start();

    void stop();

    using WriteCompleteCb = std::function<void()>;

    void send_text(std::string payload, WriteCompleteCb on_write_done = {});

    void request_reconnect();

    bool is_connected() const;

    bool check_stale(uint32_t now_ms) const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

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

}
