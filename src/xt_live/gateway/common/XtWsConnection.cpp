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
// WS 回调守卫吞掉的异常累计；防止业务解析异常逃入 asio/beast IO 线程。
std::atomic<uint64_t> g_ws_exc_total_{0};
}  // namespace

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

    /// @param ioc 非 owning；owner 必须保证该 io_context 活到本轮 session 所有 handler 完成。
    /// @param ssl_ctx 非 owning；用于构造 TLS stream，生命周期要求同 ioc。
    /// @param cfg 本轮 session 的 host/port/target 快照，构造后只读。
    /// @param on_msg 收到完整 WS message 时触发；传入的 string_view 只在回调栈内有效。
    /// @param on_conn WS handshake 成功后、首轮 async_read 前触发。
    /// @param on_fail 非主动关闭的 IO 失败回调；参数为失败阶段与错误消息。
    ConnectionSession(net::io_context& ioc, ssl::context& ssl_ctx,
                      XtWsConnection::Config cfg,
                      OnMessage on_msg, OnConnect on_conn, OnFailure on_fail)
        : resolver_(net::make_strand(ioc))
        , ws_(net::make_strand(ioc), ssl_ctx)
        , cfg_(std::move(cfg))
        , on_message_(std::move(on_msg))
        , on_connect_(std::move(on_conn))
        , on_failure_(std::move(on_fail)) {}

    /// @brief 绑定控制帧（ping/pong）活跃回调；把 ws 层 keepalive 计入 liveness。
    void set_on_activity(OnActivity cb) { on_activity_ = std::move(cb); }

    /// @brief 启动该轮连接状态机；必须在 `shared_ptr` 管理后调用。
    /// @note `cfg_.host` 同时用于 TLS SNI、证书 hostname 校验与（直连时）DNS
    ///       resolve；配置了代理时 resolve/TCP 连的是代理，真实 host 经 CONNECT
    ///       隧道到达——TLS 身份始终锚定真实 host，代理无法用他域有效证书冒充。
    void run() {
        if (!SSL_set_tlsext_host_name(ws_.next_layer().native_handle(), cfg_.host.c_str())) {
            beast::error_code ec{static_cast<int>(::ERR_get_error()),
                                 net::error::get_ssl_category()};
            fail(ec, "SSL_set_tlsext_host_name");
            return;
        }
        // 证书 hostname 绑定（X509_check_host 进入 verify）：ctx 的 verify_peer 只验
        // 链不绑 host，任意域名的有效证书都能通过——与 REST 腿（libcurl 默认校验
        // hostname）对齐补齐。
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

    /// @brief 发起正常 WebSocket close；不停止 owner 的 io_context。
    void close() {
        closing_ = true;
        net::post(ws_.get_executor(),
                  beast::bind_front_handler(&ConnectionSession::do_close, shared_from_this()));
    }

    /// @brief 打断底层 socket，用于主动重连或停止时唤醒挂起的 connect/read/write。
    /// @note completion handler 会收到 operation_aborted/eof 并走既有失败清理路径。
    void force_close() {
        auto self = shared_from_this();
        net::post(ws_.get_executor(), [self] {
            beast::error_code ec;
            auto& sock = beast::get_lowest_layer(self->ws_).socket();
            sock.cancel(ec);
            sock.close(ec);
        });
    }

    /// @brief 发送 text frame；调用方可跨线程调用，实际写入在 WS strand 内串行化。
    /// @note `on_done` 只在 async_write 成功完成后触发；未连接、drop 或写失败不触发。
    void send_text(std::string payload, XtWsConnection::WriteCompleteCb on_done) {
        // WriteSerializer 保证同一 websocket stream 任一时刻最多一个 async_write 在途。
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
    /// @brief 推进 WriteSerializer；调用方必须位于 ws_.get_executor() strand。
    /// @note data 由 lambda 捕获，确保 async_write 的 net::buffer 视图在写完成前有效。
    void drive_writer() {
        auto pending = write_serializer_.begin_next();
        if (!pending) return;
        ws_.async_write(net::buffer(*pending->data),
            [self = shared_from_this(),
             data = pending->data](beast::error_code ec, std::size_t /*n*/) {
                if (ec) {
                    XtLogger::warn_m(self->cfg_.log_tag.c_str(),
                                     "ws async_write failed: {}", ec.message());
                }
                self->write_serializer_.on_done(ec);
                self->drive_writer();
            });
    }

    /// @brief 结束本轮 session 并把失败原因交给外层 State。
    /// @note close/stop 主动关闭时不触发 on_failure，避免误调度重连。
    void fail(beast::error_code ec, const char* where) {
        connected_ = false;
        if (closing_) return;
        if (on_failure_) on_failure_(where, ec.message());
    }

    /// @brief DNS resolve completion；成功后用 endpoint 列表发起 TCP connect。
    /// @param ec resolve 结果；非空时结束本轮 session 并上报 "resolve"。
    /// @param results resolver 返回的候选 TCP endpoint，交给 Beast 逐个尝试连接。
    void on_resolve(beast::error_code ec, tcp::resolver::results_type results) {
        if (ec) return fail(ec, "resolve");
        beast::get_lowest_layer(ws_).expires_after(std::chrono::seconds(10));
        beast::get_lowest_layer(ws_).async_connect(
            results,
            beast::bind_front_handler(&ConnectionSession::on_tcp_connect, shared_from_this()));
    }

    /// @brief TCP connect completion；成功后设置 socket 选项并进入 TLS handshake。
    /// @param ec connect 结果；非空时结束本轮 session 并上报 "connect"。
    /// @param endpoint Beast 成功连接的 endpoint；当前只需要连接成功信号，不读该值。
    void on_tcp_connect(beast::error_code ec, tcp::resolver::results_type::endpoint_type) {
        if (ec) return fail(ec, "connect");
        // 小 WS frame 对 Nagle 延迟敏感；TCP_NODELAY 设置失败只影响尾延迟，不影响协议正确性。
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

    /// @brief 发起既有 TLS handshake（直连路径与代理隧道建立后共用）。
    void do_ssl_handshake() {
        ws_.next_layer().async_handshake(
            ssl::stream_base::client,
            beast::bind_front_handler(&ConnectionSession::on_ssl_handshake, shared_from_this()));
    }

    /// @brief 对代理明文发 HTTP CONNECT，建立到真实 host:port 的隧道。
    /// @note 2xx 后同一 socket 上继续 TLS；TLS 端点是真实 host（SNI/hostname
    ///       校验见 run()），代理只转发密文字节流。
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

    /// @brief 读 CONNECT 响应头；2xx → 继续 TLS，其它状态码/残留字节按连接失败处理。
    void read_proxy_response() {
        proxy_parser_ = std::make_shared<beast::http::response_parser<beast::http::empty_body>>();
        // CONNECT 2xx 响应无 body；skip 防 parser 把隧道内的 TLS 字节当 body 消费。
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
                    // 响应头后残留字节 = 代理在隧道建立前注入数据；TLS 层直接读 socket
                    // 看不到这些字节，握手必乱——按失败重连处理。
                    XtLogger::warn_m(self->cfg_.log_tag.c_str(),
                                     "proxy CONNECT leftover {} bytes after header",
                                     self->proxy_buffer_.size());
                    return self->fail(net::error::make_error_code(net::error::connection_refused),
                                      "proxy_connect_leftover");
                }
                self->do_ssl_handshake();
            });
    }

    /// @brief TLS handshake completion；成功后配置 WebSocket 层并发起 WS handshake。
    /// @param ec TLS handshake 结果；非空时结束本轮 session 并上报 "ssl_handshake"。
    void on_ssl_handshake(beast::error_code ec) {
        if (ec) return fail(ec, "ssl_handshake");
        beast::get_lowest_layer(ws_).expires_never();
        // 给这个 WebSocket client 使用 Beast 推荐的一组超时设置
        ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
        // 业务消息空闲不代表连接失活；ping/pong control frame 也刷新 State 的活跃时间。
        ws_.control_callback([this](websocket::frame_type kind, beast::string_view) {
            if (kind == websocket::frame_type::ping || kind == websocket::frame_type::pong) {
                if (on_activity_) on_activity_();
            }
        });
        // decorator 给 WebSocket handshake 请求加 User-Agent 头，用于标识客户端
        ws_.set_option(websocket::stream_base::decorator([](websocket::request_type& req) {
            req.set(beast::http::field::user_agent,
                    "XyybTrader/" BOOST_BEAST_VERSION_STRING);
        }));
        ws_.async_handshake(
            cfg_.host, cfg_.target,
            beast::bind_front_handler(&ConnectionSession::on_ws_handshake, shared_from_this()));
    }

    /// @brief WebSocket handshake completion；成功后发布 connected 并启动 read loop。
    /// @param ec WS handshake 结果；非空时结束本轮 session 并上报 "ws_handshake"。
    void on_ws_handshake(beast::error_code ec) {
        if (ec) return fail(ec, "ws_handshake");
        connected_ = true;
        // 对外 connected 回调必须早于首轮 read；调用方会在这里 drain 待发送队列。
        if (on_connect_) on_connect_();
        do_read();
    }

    /// @brief 提交下一次 async_read；read completion 统一回到 on_read。
    void do_read() {
        if (closing_) return;
        ws_.async_read(
            buffer_,
            beast::bind_front_handler(&ConnectionSession::on_read, shared_from_this()));
    }

    /// @brief WebSocket read completion；成功时同步派发 message 并继续下一轮 read。
    /// @param ec read 结果；非空时结束本轮 session 并上报 "read"。
    /// @param bytes_transferred Beast 已把数据放入 buffer_，这里用 buffer_ 大小构造视图。
    void on_read(beast::error_code ec, std::size_t /*bytes_transferred*/) {
        if (ec) return fail(ec, "read");
        const auto data = buffer_.cdata();
        std::string_view sv(static_cast<const char*>(data.data()), data.size());
        // string_view 只在本回调栈内有效；业务层若需跨异步边界保存必须自行拷贝。
        if (on_message_) on_message_(sv);
        buffer_.consume(buffer_.size());
        do_read();
    }

    /// @brief 提交正常 WS close；completion 只更新本 session 的内部连接态。
    void do_close() {
        if (!ws_.is_open()) return;
        ws_.async_close(websocket::close_code::normal,
                        beast::bind_front_handler(&ConnectionSession::on_close,
                                                  shared_from_this()));
    }

    /// @brief WebSocket close completion；close 失败不再上报外层重连。
    void on_close(beast::error_code /*ec*/) { connected_ = false; }

    // ── 单 session IO 资源（strand 绑定）─────────────────────────────
    tcp::resolver resolver_;  ///< 异步 DNS 解析器；async_resolve 回调由 resolver strand 调度。
    websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws_;  ///< 本轮 TLS WebSocket stream。
    beast::flat_buffer buffer_;  ///< read buffer；派发给 on_message_ 的 string_view 指向这里。
    beast::flat_buffer proxy_buffer_;  ///< CONNECT 响应头 read buffer；隧道建立后不再使用。
    std::shared_ptr<beast::http::response_parser<beast::http::empty_body>>
        proxy_parser_;  ///< CONNECT 响应 parser；生命周期须覆盖 async_read_header。

    // ── session 配置与回调 ─────────────────────────────────────────
    XtWsConnection::Config cfg_;  ///< 按值保存，避免外层重连更新影响本轮 session。
    OnMessage on_message_;  ///< WS message 回调；在 strand 内同步触发。
    OnConnect on_connect_;  ///< WS handshake 成功回调；早于首轮 async_read。
    OnFailure on_failure_;  ///< 只上报失败阶段与错误文本；不在 session 内做重连决策。
    OnActivity on_activity_;  ///< ping/pong 活跃回调；可由 set_on_activity 注入。

    // ── session 状态（strand 内访问）────────────────────────────────
    bool closing_ = false;    ///< true 时 fail() 不调用 on_failure_，避免主动关闭触发重连。
    bool connected_ = false;  ///< 本 session 内部连接状态；外部观测使用 State::connected。
    xt_live::gateway::WriteSerializer write_serializer_;  ///< 排队待发送 payload，保证同一 ws_ 只有一个 async_write 在途。
};

}  // namespace

/// @brief XtWsConnection 的 PImpl 实体，隐藏 .cpp 内部状态机与 Boost IO 类型。
///
/// 本层只持有 `State` 并转发 public API；长期运行状态、重连退避和当前
/// ConnectionSession 都归 `State` 管理。析构时调用 stop()，但不停止外部 io_context。
class XtWsConnection::Impl {
public:
    /// @param ioc 非 owning；所有 State 操作都会 post 到该 io_context。
    /// @param ssl_ctx 非 owning；传给每轮 ConnectionSession 构造 TLS stream。
    /// @param cfg 连接参数快照，由 State 持有并用于后续重连。
    Impl(net::io_context& ioc, ssl::context& ssl_ctx, Config cfg)
        : state_(std::make_shared<State>(ioc, ssl_ctx, std::move(cfg))) {}

    /// @brief 请求内部状态停止；不阻塞等待已 post 的 handler 完成。
    ~Impl() { stop(); }

    /// @brief 安装外部回调；实际触发线程由 State/ConnectionSession 的 io_context 决定。
    void bind_callbacks(OnMessage on_msg, OnConnect on_conn, OnDisconnect on_disc) {
        state_->bind_callbacks(std::move(on_msg), std::move(on_conn), std::move(on_disc));
    }

    /// @brief 以下 public API 只转发到 State，保持 PImpl 层不持有业务状态。
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
        /// @param ioc_in 非 owning；驱动 timer、session 创建与外部 API 投递。
        /// @param ssl_ctx_in 非 owning；每轮新建 ConnectionSession 时复用。
        /// @param cfg_in 连接参数快照；重连时复制给新的 session。
        State(net::io_context& ioc_in, ssl::context& ssl_ctx_in, Config cfg_in)
            : ioc(ioc_in)
            , ssl_ctx(ssl_ctx_in)
            , cfg(std::move(cfg_in))
            , reconnect_timer(ioc) {}

        /// @brief 替换业务回调；回调对象由 State 持有，session 只持 weak 转发闭包。
        void bind_callbacks(OnMessage on_msg, OnConnect on_conn, OnDisconnect on_disc) {
            on_message = std::move(on_msg);
            on_connect = std::move(on_conn);
            on_disconnect = std::move(on_disc);
        }

        /// @brief 异步启动连接；重复 start 不会创建第二条 session。
        void start() {
            net::post(ioc, [self = shared_from_this()] {
                if (self->running) return;
                self->running = true;
                self->stopping = false;
                self->connect_now();
            });
        }

        /// @brief 异步停止：取消重连 timer，并让当前 session 走正常 close。
        /// @note 不停止 ioc，也不等待 close completion；线程 join 由外层 owner 负责。
        void stop() {
            net::post(ioc, [self = shared_from_this()] {
                self->running = false;
                self->stopping = true;
                self->connected.store(false, std::memory_order_relaxed);
                self->reconnect_timer.cancel();
                if (self->session) self->session->close();
            });
        }

        /// @brief 线程安全发送入口；未连接时直接 drop，成功写入语义由 on_done 表达。
        void send_text(std::string payload, XtWsConnection::WriteCompleteCb on_done) {
            net::post(ioc, [self = shared_from_this(),
                            payload = std::move(payload),
                            on_done = std::move(on_done)]() mutable {
                if (!self->session || !self->connected.load(std::memory_order_relaxed)) {
                    XtLogger::warn_m(self->log_tag(), "ws send dropped: not connected");
                    // 与 ConnectionSession::send_text 一致：drop 时不调成功回调。
                    return;
                }
                self->session->send_text(std::move(payload), std::move(on_done));
            });
        }

        /// @brief 主动重连入口；关闭当前 session，并按最小退避安排新连接。
        void request_reconnect() {
            net::post(ioc, [self = shared_from_this()] {
                if (!self->running || self->stopping) return;
                self->connected.store(false, std::memory_order_relaxed);
                if (self->session) self->session->force_close();
                self->schedule_reconnect(true);
            });
        }

        /// @brief 对外连接快照；跨线程读取，不保证与 session 内部状态同一时刻一致。
        bool is_connected() const { return connected.load(std::memory_order_relaxed); }

        /// @brief 健康检查只读入口；超过 stale_threshold_ms 未见消息或 ping/pong 返回 false。
        bool check_stale(uint32_t now_ms_in) const {
            if (cfg.stale_threshold_ms == 0) return true;
            const uint32_t last = last_msg_ms.load(std::memory_order_relaxed);
            if (last == 0) return true;
            return now_ms_in - last <= cfg.stale_threshold_ms;
        }

        /// @brief 返回 owner 注入的日志标签。
        const char* log_tag() const { return cfg.log_tag.c_str(); }

        /// @brief 创建并启动新一轮 ConnectionSession。
        /// @note session 回调用 weak_ptr 回到 State；State 已释放时回调直接丢弃。
        void connect_now() {
            if (!running || stopping) return;
            // 清空可能存在的重连任务
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

        /// @brief session 已完成 WS handshake；发布连接态并通知业务层 drain 队列。
        void handle_connect() {
            // stop() 已 post 而握手后到：不得复活连接（stop 已置 connected=false，
            // 这里翻回 true 且 on_connect 会驱动业务层 drain 待发队列）。
            if (!running || stopping) return;
            connected.store(true, std::memory_order_relaxed);
            consecutive_fail = 0;
            last_msg_ms.store(now_ms(), std::memory_order_relaxed);
            if (on_connect) on_connect();
        }

        /// @brief ping/pong 活跃信号；只刷新 liveness，不影响失败退避计数。
        void note_activity() {
            last_msg_ms.store(now_ms(), std::memory_order_relaxed);
        }

        /// @brief WS message 入口；刷新健康时间后调用业务解析回调。
        void handle_message(std::string_view sv) {
            last_msg_ms.store(now_ms(), std::memory_order_relaxed);
            consecutive_fail = 0;
            // on_message 是 MD/UserData 解析总入口；业务异常在这里计数并吞掉，保护 IO 线程。
            xt_log::run_guarded(log_tag(), "ws.on_message", g_ws_exc_total_,
                                [&] { if (on_message) on_message(sv); });
        }

        /// @brief 异常断线入口；通知业务层断开，并安排退避重连。
        void handle_failure(const char* where, std::string msg) {
            connected.store(false, std::memory_order_relaxed);
            if (!running || stopping) return;
            XtLogger::warn_m(log_tag(), "ws {}: {}", where, msg);
            if (on_disconnect) on_disconnect();
            schedule_reconnect(false);
        }

        /// @brief 安排下一轮 connect_now；同一时刻最多一个 pending reconnect timer。
        void schedule_reconnect(bool min_backoff) {
            if (!running || stopping || reconnect_pending) return;
            reconnect_pending = true;
            const uint32_t fail = consecutive_fail++;
            const uint32_t formula_ms =
                std::min<uint32_t>(cfg.reconnect_max_ms,
                                   (1 << std::min<uint32_t>(fail, 5)) * 1000);
            const uint32_t backoff_ms = min_backoff ? cfg.reconnect_min_ms : formula_ms;
            // 主动重连使用最小退避；异常断线按连续失败次数指数退避并封顶。
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

        // ── 外部资源（不拥有线程）───────────────────────────────────
        net::io_context& ioc;      ///< owner 提供并负责 run/stop/join。
        ssl::context& ssl_ctx;     ///< owner 提供；每轮 session 只借用，不修改 ownership。
        Config cfg;                ///< 长期连接配置；每轮 session 从这里复制快照。

        // ── 重连与当前 session（io_context 线程访问）────────────────
        net::steady_timer reconnect_timer;          ///< 退避重连 timer，绑定 ioc。
        std::shared_ptr<ConnectionSession> session; ///< 当前单轮 WS session；重连时替换。

        // ── 外部回调（io_context 线程触发）──────────────────────────
        OnMessage on_message;          ///< 业务 message 解析入口；由 handle_message 保护调用。
        OnConnect on_connect;          ///< 连接成功通知；在连接态发布后触发。
        OnDisconnect on_disconnect;    ///< 异常断线通知；主动 stop 不触发。

        // ── 跨线程健康观测 ─────────────────────────────────────────
        std::atomic<bool> connected{false};       ///< 对外连接快照，供 is_connected 跨线程读取。
        std::atomic<uint32_t> last_msg_ms{0};     ///< 最近 message/ping/pong steady ms；0 表示未连接成功。

        // ── 状态机（io_context 线程访问）───────────────────────────
        bool running = false;           ///< start 后为 true；stop 会清除。
        bool stopping = false;          ///< stop 进行中；屏蔽新连接与重连。
        bool reconnect_pending = false; ///< 已有 reconnect_timer 在途。
        uint32_t consecutive_fail = 0;  ///< 异常断线连续失败次数，用于指数退避。
    };

    std::shared_ptr<State> state_; ///< shared ownership 让已 post 到 ioc 的任务安全完成。
};

namespace {
/// @brief cfg 未显式配置代理时读 env `XT_WS_PROXY`（显式 opt-in，非 ambient
///        https_proxy——防生产 host 环境变量泄漏时静默改变 money-path 传输）。
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
        // 不回显 env 原文：形如 http://user:pw@host 的值会把凭证落进日志。
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
}  // namespace

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

}  // namespace xt_live::gateway
