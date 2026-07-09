#include "XtWsConnection.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <utility>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/version.hpp>

#include "xt_logger/XtLogger.h"
#include "xt_logger/XtCallbackGuard.h"
#include "xt_live/gateway/common/WriteSerializer.h"
#include "xt_util/time/XtSteadyClock.h"

namespace xt_live::gateway {

namespace {

std::atomic<uint64_t> g_ws_exc_total_{0};
}

namespace beast     = boost::beast;
namespace websocket = beast::websocket;
namespace net       = boost::asio;
namespace ssl       = net::ssl;
using tcp           = net::ip::tcp;

namespace {

uint32_t now_ms() {
    return static_cast<uint32_t>(XtSteadyClock::steady_now_ms());
}

/// @brief 单次底层 WS session；负责 resolve/connect/handshake/read/write。
///
/// 每个实例只服务一轮连接：`State::connect_now()` 创建，失败或主动重连后由
/// 外层 `State` 丢弃并重建。session 只上报失败，不决定重连退避或 stale 策略。
///
/// resolve 回调与 WS stream 回调分别由各自 strand 串行调度；写路径统一 post 到
/// `ws_.get_executor()`，避免为 Beast stream 再加 mutex。
/// 生命周期由 shared_ptr 捕获在异步 handler 中，直到该轮 pending IO 自然完成。
class ConnectionSession : public std::enable_shared_from_this<ConnectionSession> {
public:
    using OnMessage = std::function<void(std::string_view)>;
    using OnConnect = std::function<void()>;
    using OnFailure = std::function<void(const char*, std::string)>;
    using OnActivity = std::function<void()>;

    ConnectionSession(net::io_context& ioc, ssl::context& ssl_ctx,
                      XtWsConnection::Config cfg,
                      OnMessage on_msg, OnConnect on_conn, OnFailure on_fail)
        : resolver_(net::make_strand(ioc))
        , ws_(net::make_strand(ioc), ssl_ctx)
        , cfg_(std::move(cfg))
        , on_message_(std::move(on_msg))
        , on_connect_(std::move(on_conn))
        , on_failure_(std::move(on_fail)) {}

    void set_on_activity(OnActivity cb) { on_activity_ = std::move(cb); }

    void run() {
        if (!SSL_set_tlsext_host_name(ws_.next_layer().native_handle(), cfg_.host.c_str())) {
            beast::error_code ec{static_cast<int>(::ERR_get_error()),
                                 net::error::get_ssl_category()};
            fail(ec, "SSL_set_tlsext_host_name");
            return;
        }

        if (!SSL_set1_host(ws_.next_layer().native_handle(), cfg_.host.c_str())) {
            beast::error_code ec{static_cast<int>(::ERR_get_error()),
                                 net::error::get_ssl_category()};
            fail(ec, "SSL_set1_host");
            return;
        }
        const bool via_proxy = !cfg_.proxy_host.empty() && !cfg_.proxy_port.empty();
        resolver_.async_resolve(
            via_proxy ? cfg_.proxy_host : cfg_.host,
            via_proxy ? cfg_.proxy_port : cfg_.port,
            beast::bind_front_handler(&ConnectionSession::on_resolve, shared_from_this()));
    }

    void close() {
        closing_ = true;
        net::post(ws_.get_executor(),
                  beast::bind_front_handler(&ConnectionSession::do_close, shared_from_this()));
    }

    void force_close() {
        auto self = shared_from_this();
        net::post(ws_.get_executor(), [self] {
            beast::error_code ec;
            auto& sock = beast::get_lowest_layer(self->ws_).socket();
            sock.cancel(ec);
            sock.close(ec);
        });
    }

    void send_text(std::string payload, XtWsConnection::WriteCompleteCb on_done) {

        net::post(ws_.get_executor(),
                  [self = shared_from_this(), payload = std::move(payload),
                   cb = std::move(on_done)]() mutable {
            if (!self->connected_ || !self->ws_.is_open()) {
                XtLogger::warn_m(self->cfg_.log_tag.c_str(),
                                 "ws send dropped: not connected");
                return;
            }
            auto data = std::make_shared<std::string>(std::move(payload));
            self->write_serializer_.enqueue(std::move(data), std::move(cb));
            self->drive_writer();
        });
    }

private:

    void drive_writer() {
        auto pending = write_serializer_.begin_next();
        if (!pending) return;
        ws_.async_write(net::buffer(*pending->data),
            [self = shared_from_this(),
             data = pending->data](beast::error_code ec, std::size_t  ) {
                if (ec) {
                    XtLogger::warn_m(self->cfg_.log_tag.c_str(),
                                     "ws async_write failed: {}", ec.message());
                }
                self->write_serializer_.on_done(ec);
                self->drive_writer();
            });
    }

    void fail(beast::error_code ec, const char* where) {
        connected_ = false;
        if (closing_) return;
        if (on_failure_) on_failure_(where, ec.message());
    }

    void on_resolve(beast::error_code ec, tcp::resolver::results_type results) {
        if (ec) return fail(ec, "resolve");
        beast::get_lowest_layer(ws_).expires_after(std::chrono::seconds(10));
        beast::get_lowest_layer(ws_).async_connect(
            results,
            beast::bind_front_handler(&ConnectionSession::on_tcp_connect, shared_from_this()));
    }

    void on_tcp_connect(beast::error_code ec, tcp::resolver::results_type::endpoint_type) {
        if (ec) return fail(ec, "connect");

        beast::error_code nodelay_ec;
        beast::get_lowest_layer(ws_).socket().set_option(tcp::no_delay(true), nodelay_ec);
        if (nodelay_ec) {
            XtLogger::warn_m(cfg_.log_tag.c_str(),
                             "set TCP_NODELAY failed on {}{}: {}",
                             cfg_.host, cfg_.target, nodelay_ec.message());
        }
        beast::get_lowest_layer(ws_).expires_after(std::chrono::seconds(10));
        if (!cfg_.proxy_host.empty() && !cfg_.proxy_port.empty()) {
            do_proxy_connect();
            return;
        }
        do_ssl_handshake();
    }

    void do_ssl_handshake() {
        ws_.next_layer().async_handshake(
            ssl::stream_base::client,
            beast::bind_front_handler(&ConnectionSession::on_ssl_handshake, shared_from_this()));
    }

    void do_proxy_connect() {
        const std::string authority = cfg_.host + ":" + cfg_.port;
        auto req = std::make_shared<beast::http::request<beast::http::empty_body>>(
            beast::http::verb::connect, authority, 11);
        req->set(beast::http::field::host, authority);
        beast::http::async_write(
            beast::get_lowest_layer(ws_), *req,
            [self = shared_from_this(), req](beast::error_code ec, std::size_t) {
                if (ec) return self->fail(ec, "proxy_connect_write");
                self->read_proxy_response();
            });
    }

    void read_proxy_response() {
        proxy_parser_ = std::make_shared<beast::http::response_parser<beast::http::empty_body>>();

        proxy_parser_->skip(true);
        beast::http::async_read_header(
            beast::get_lowest_layer(ws_), proxy_buffer_, *proxy_parser_,
            [self = shared_from_this()](beast::error_code ec, std::size_t) {
                if (ec) return self->fail(ec, "proxy_connect_read");
                const auto status = self->proxy_parser_->get().result_int();
                if (status < 200 || status >= 300) {
                    XtLogger::warn_m(self->cfg_.log_tag.c_str(),
                                     "proxy CONNECT rejected: status={}", status);
                    return self->fail(net::error::make_error_code(net::error::connection_refused),
                                      "proxy_connect_status");
                }
                if (self->proxy_buffer_.size() != 0) {

                    XtLogger::warn_m(self->cfg_.log_tag.c_str(),
                                     "proxy CONNECT leftover {} bytes after header",
                                     self->proxy_buffer_.size());
                    return self->fail(net::error::make_error_code(net::error::connection_refused),
                                      "proxy_connect_leftover");
                }
                self->do_ssl_handshake();
            });
    }

    void on_ssl_handshake(beast::error_code ec) {
        if (ec) return fail(ec, "ssl_handshake");
        beast::get_lowest_layer(ws_).expires_never();

        ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));

        ws_.control_callback([this](websocket::frame_type kind, beast::string_view) {
            if (kind == websocket::frame_type::ping || kind == websocket::frame_type::pong) {
                if (on_activity_) on_activity_();
            }
        });

        ws_.set_option(websocket::stream_base::decorator([](websocket::request_type& req) {
            req.set(beast::http::field::user_agent,
                    "XyybTrader/" BOOST_BEAST_VERSION_STRING);
        }));
        ws_.async_handshake(
            cfg_.host, cfg_.target,
            beast::bind_front_handler(&ConnectionSession::on_ws_handshake, shared_from_this()));
    }

    void on_ws_handshake(beast::error_code ec) {
        if (ec) return fail(ec, "ws_handshake");
        connected_ = true;

        if (on_connect_) on_connect_();
        do_read();
    }

    void do_read() {
        if (closing_) return;
        ws_.async_read(
            buffer_,
            beast::bind_front_handler(&ConnectionSession::on_read, shared_from_this()));
    }

    void on_read(beast::error_code ec, std::size_t  ) {
        if (ec) return fail(ec, "read");
        const auto data = buffer_.cdata();
        std::string_view sv(static_cast<const char*>(data.data()), data.size());

        if (on_message_) on_message_(sv);
        buffer_.consume(buffer_.size());
        do_read();
    }

    void do_close() {
        if (!ws_.is_open()) return;
        ws_.async_close(websocket::close_code::normal,
                        beast::bind_front_handler(&ConnectionSession::on_close,
                                                  shared_from_this()));
    }

    void on_close(beast::error_code  ) { connected_ = false; }

    tcp::resolver resolver_;
    websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws_;
    beast::flat_buffer buffer_;
    beast::flat_buffer proxy_buffer_;
    std::shared_ptr<beast::http::response_parser<beast::http::empty_body>>
        proxy_parser_;

    XtWsConnection::Config cfg_;
    OnMessage on_message_;
    OnConnect on_connect_;
    OnFailure on_failure_;
    OnActivity on_activity_;

    bool closing_ = false;
    bool connected_ = false;
    xt_live::gateway::WriteSerializer write_serializer_;
};

}

/// @brief XtWsConnection 的 PImpl 实体，隐藏 .cpp 内部状态机与 Boost IO 类型。
///
/// 本层只持有 `State` 并转发 public API；长期运行状态、重连退避和当前
/// ConnectionSession 都归 `State` 管理。析构时调用 stop()，但不停止外部 io_context。
class XtWsConnection::Impl {
public:

    Impl(net::io_context& ioc, ssl::context& ssl_ctx, Config cfg)
        : state_(std::make_shared<State>(ioc, ssl_ctx, std::move(cfg))) {}

    ~Impl() { stop(); }

    void bind_callbacks(OnMessage on_msg, OnConnect on_conn, OnDisconnect on_disc) {
        state_->bind_callbacks(std::move(on_msg), std::move(on_conn), std::move(on_disc));
    }

    void start() { state_->start(); }
    void stop() { state_->stop(); }
    void send_text(std::string payload, WriteCompleteCb on_done) {
        state_->send_text(std::move(payload), std::move(on_done));
    }
    void request_reconnect() { state_->request_reconnect(); }
    bool is_connected() const { return state_->is_connected(); }
    bool check_stale(uint32_t now_ms_in) const { return state_->check_stale(now_ms_in); }

private:
    /// @brief 长期连接状态机，跨多轮 ConnectionSession 保存重连与健康状态。
    ///
    /// public 入口通过 post 切到 ioc 线程；异步 lambda 捕获 shared_ptr<State>，
    /// 避免外层 XtWsConnection 析构后已投递任务访问悬空对象。
    struct State : public std::enable_shared_from_this<State> {

        State(net::io_context& ioc_in, ssl::context& ssl_ctx_in, Config cfg_in)
            : ioc(ioc_in)
            , ssl_ctx(ssl_ctx_in)
            , cfg(std::move(cfg_in))
            , reconnect_timer(ioc) {}

        void bind_callbacks(OnMessage on_msg, OnConnect on_conn, OnDisconnect on_disc) {
            on_message = std::move(on_msg);
            on_connect = std::move(on_conn);
            on_disconnect = std::move(on_disc);
        }

        void start() {
            net::post(ioc, [self = shared_from_this()] {
                if (self->running) return;
                self->running = true;
                self->stopping = false;
                self->connect_now();
            });
        }

        void stop() {
            net::post(ioc, [self = shared_from_this()] {
                self->running = false;
                self->stopping = true;
                self->connected.store(false, std::memory_order_relaxed);
                self->reconnect_timer.cancel();
                if (self->session) self->session->close();
            });
        }

        void send_text(std::string payload, XtWsConnection::WriteCompleteCb on_done) {
            net::post(ioc, [self = shared_from_this(),
                            payload = std::move(payload),
                            on_done = std::move(on_done)]() mutable {
                if (!self->session || !self->connected.load(std::memory_order_relaxed)) {
                    XtLogger::warn_m(self->log_tag(), "ws send dropped: not connected");

                    return;
                }
                self->session->send_text(std::move(payload), std::move(on_done));
            });
        }

        void request_reconnect() {
            net::post(ioc, [self = shared_from_this()] {
                if (!self->running || self->stopping) return;
                self->connected.store(false, std::memory_order_relaxed);
                if (self->session) self->session->force_close();
                self->schedule_reconnect(true);
            });
        }

        bool is_connected() const { return connected.load(std::memory_order_relaxed); }

        bool check_stale(uint32_t now_ms_in) const {
            if (cfg.stale_threshold_ms == 0) return true;
            const uint32_t last = last_msg_ms.load(std::memory_order_relaxed);
            if (last == 0) return true;
            return now_ms_in - last <= cfg.stale_threshold_ms;
        }

        const char* log_tag() const { return cfg.log_tag.c_str(); }

        void connect_now() {
            if (!running || stopping) return;

            reconnect_timer.cancel();
            auto weak = weak_from_this();
            Config session_cfg = cfg;
            session = std::make_shared<ConnectionSession>(
                ioc, ssl_ctx, std::move(session_cfg),
                [weak](std::string_view sv) {
                    if (auto self = weak.lock()) self->handle_message(sv);
                },
                [weak] {
                    if (auto self = weak.lock()) self->handle_connect();
                },
                [weak](const char* where, std::string msg) {
                    if (auto self = weak.lock()) self->handle_failure(where, std::move(msg));
                });
            session->set_on_activity([weak] {
                if (auto self = weak.lock()) self->note_activity();
            });
            session->run();
        }

        void handle_connect() {
            connected.store(true, std::memory_order_relaxed);
            consecutive_fail = 0;
            last_msg_ms.store(now_ms(), std::memory_order_relaxed);
            if (on_connect) on_connect();
        }

        void note_activity() {
            last_msg_ms.store(now_ms(), std::memory_order_relaxed);
        }

        void handle_message(std::string_view sv) {
            last_msg_ms.store(now_ms(), std::memory_order_relaxed);
            consecutive_fail = 0;

            xt_log::run_guarded(log_tag(), "ws.on_message", g_ws_exc_total_,
                                [&] { if (on_message) on_message(sv); });
        }

        void handle_failure(const char* where, std::string msg) {
            connected.store(false, std::memory_order_relaxed);
            if (!running || stopping) return;
            XtLogger::warn_m(log_tag(), "ws {}: {}", where, msg);
            if (on_disconnect) on_disconnect();
            schedule_reconnect(false);
        }

        void schedule_reconnect(bool min_backoff) {
            if (!running || stopping || reconnect_pending) return;
            reconnect_pending = true;
            const uint32_t fail = consecutive_fail++;
            const uint32_t formula_ms =
                std::min<uint32_t>(cfg.reconnect_max_ms,
                                   (1 << std::min<uint32_t>(fail, 5)) * 1000);
            const uint32_t backoff_ms = min_backoff ? cfg.reconnect_min_ms : formula_ms;

            XtLogger::warn_m(log_tag(), "reconnect in {}ms (fail={})",
                              backoff_ms, consecutive_fail);
            reconnect_timer.expires_after(std::chrono::milliseconds(backoff_ms));
            reconnect_timer.async_wait([self = shared_from_this()](beast::error_code ec) {
                self->reconnect_pending = false;
                if (ec || !self->running || self->stopping) return;
                self->session.reset();
                self->connect_now();
            });
        }

        net::io_context& ioc;
        ssl::context& ssl_ctx;
        Config cfg;

        net::steady_timer reconnect_timer;
        std::shared_ptr<ConnectionSession> session;

        OnMessage on_message;
        OnConnect on_connect;
        OnDisconnect on_disconnect;

        std::atomic<bool> connected{false};
        std::atomic<uint32_t> last_msg_ms{0};

        bool running = false;
        bool stopping = false;
        bool reconnect_pending = false;
        uint32_t consecutive_fail = 0;
    };

    std::shared_ptr<State> state_;
};

namespace {

XtWsConnection::Config apply_ws_proxy_env(XtWsConnection::Config cfg) {
    if (cfg.proxy_host.empty() != cfg.proxy_port.empty()) {
        XtLogger::warn_m(cfg.log_tag.c_str(),
                         "proxy_host/proxy_port 只配置了一半，按直连处理");
        return cfg;
    }
    if (!cfg.proxy_host.empty()) return cfg;
    const char* env = std::getenv("XT_WS_PROXY");
    if (!env || !*env) return cfg;
    auto [host, port] = parse_ws_proxy_url(env);
    if (host.empty()) {

        XtLogger::warn_m(cfg.log_tag.c_str(),
                         "XT_WS_PROXY 无法解析（期望 http://host:port 或 host:port；"
                         "不支持认证/IPv6 字面量），已忽略");
        return cfg;
    }
    cfg.proxy_host = std::move(host);
    cfg.proxy_port = std::move(port);
    XtLogger::info_m(cfg.log_tag.c_str(), "ws via HTTP CONNECT proxy {}:{} (XT_WS_PROXY)",
                     cfg.proxy_host, cfg.proxy_port);
    return cfg;
}
}

XtWsConnection::XtWsConnection(boost::asio::io_context& ioc,
                                         boost::asio::ssl::context& ssl_ctx,
                                         Config cfg)
    : impl_(std::make_unique<Impl>(ioc, ssl_ctx, apply_ws_proxy_env(std::move(cfg)))) {}

XtWsConnection::~XtWsConnection() = default;

void XtWsConnection::bind_callbacks(OnMessage on_msg, OnConnect on_conn,
                                         OnDisconnect on_disc) {
    impl_->bind_callbacks(std::move(on_msg), std::move(on_conn), std::move(on_disc));
}

void XtWsConnection::start() { impl_->start(); }
void XtWsConnection::stop() { impl_->stop(); }
void XtWsConnection::send_text(std::string payload, WriteCompleteCb on_done) {
    impl_->send_text(std::move(payload), std::move(on_done));
}
void XtWsConnection::request_reconnect() { impl_->request_reconnect(); }
bool XtWsConnection::is_connected() const { return impl_->is_connected(); }
bool XtWsConnection::check_stale(uint32_t now_ms) const {
    return impl_->check_stale(now_ms);
}

}
