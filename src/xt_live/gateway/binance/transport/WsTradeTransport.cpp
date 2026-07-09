#include "WsTradeTransport.h"

#include "BinanceIntentTrace.h"
#include "rest/BinanceRestClient.h"
#include "rest/BinanceSign.h"
#include "xt_live/gateway/common/IoContextThread.h"
#include "xt_live/gateway/common/XtWsConnection.h"

#include <atomic>
#include <deque>
#include <exception>
#include <future>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/system/error_code.hpp>

#include "xt_logger/XtLogger.h"
#include "xt_util/time/XtSteadyClock.h"

namespace binance_u {

namespace net = boost::asio;
namespace ssl = net::ssl;

namespace {

uint64_t steady_now_us() noexcept {
    return XtSteadyClock::steady_now_us();
}

TransportAck make_transport_ack(uint64_t local_id,
                                TransportAck::Kind kind,
                                TransportAck::ReqKind req_kind,
                                std::string msg = {}) {
    TransportAck ack;
    ack.kind = kind;
    ack.req_kind = req_kind;
    ack.local_id = local_id;
    ack.err_msg = std::move(msg);
    return ack;
}

const char* kind_msg(TransportAck::Kind kind) {
    switch (kind) {
    case TransportAck::Kind::kTransportError_Disconnect:
        return "transport disconnect";
    case TransportAck::Kind::kTransportError_TimeoutBeforeSend:
        return "timeout before send";
    case TransportAck::Kind::kTransportError_TimeoutAfterSend:
        return "timeout after send";
    case TransportAck::Kind::kTransportError_Stale:
        return "stale before send";
    case TransportAck::Kind::kTransportError_QueueOverflow:
        return "send queue overflow";
    case TransportAck::Kind::kTransportError_Internal:
        return "transport internal error";
    default:
        break;
    }
    return "";
}

}

class WsTradeTransport::Impl {
public:
    Impl(Config cfg_in, BinanceRestClient& rest_in, WsTradeCredentials cred_in)
        : cfg(std::move(cfg_in))
        , rest(rest_in)
        , cred(std::move(cred_in))
        , timeout_timer(io_ctx_.ioc()) {}

    ~Impl() { stop(); }

    bool start() {
        if (!validate_config()) return false;
        int expected = kStopped;
        if (!state.compare_exchange_strong(expected, kStarting)) return true;
        stopped.store(false, std::memory_order_relaxed);

        io_ctx_.start([this] { start_on_io(); });
        return true;
    }

    void stop() {
        if (state.load(std::memory_order_relaxed) == kStopped) return;

        io_ctx_.stop([this] { stop_on_io(); });
    }

    void submit(TransportSubmitReq&& req) {
        QueuedReq item;
        item.kind = QueuedReq::Kind::kSubmit;
        item.submit = std::move(req);
        item.trace.local_id = item.submit.local_id;
        item.trace.t0_us = item.submit.t0_us;
        enqueue_or_overflow(std::move(item));
    }

    void cancel(TransportCancelReq&& req) {
        QueuedReq item;
        item.kind = QueuedReq::Kind::kCancel;
        item.cancel = std::move(req);
        item.trace.local_id = item.cancel.local_id;
        item.trace.t0_us = item.cancel.t0_us;
        enqueue_or_overflow(std::move(item));
    }

    void bind_ack_handler(AckHandler handler) { ack_handler = std::move(handler); }

    Health health() const {
        Health h;
        h.connected = connected.load(std::memory_order_relaxed);
        h.inflight_count = inflight_count.load(std::memory_order_relaxed);
        h.last_ack_us = last_ack_us.load(std::memory_order_relaxed);
        return h;
    }

    void inject_test_message(std::string_view payload) {
        std::string copy(payload);
        net::post(io_ctx_.ioc(), [this, copy = std::move(copy)] { handle_response(copy); });
    }

    void inject_test_connect() {
        net::post(io_ctx_.ioc(), [this] { handle_connected(); });
    }

    void inject_test_disconnect() {
        net::post(io_ctx_.ioc(), [this] { handle_disconnected(); });
    }

    void inject_test_advance_clock(uint64_t delta_us) {
        net::post(io_ctx_.ioc(), [this, delta_us] {
            test_clock_offset_us += delta_us;
            handle_timeout_tick();
        });
    }

    std::vector<std::string> inject_test_take_sent_payloads() {
        auto done = std::make_shared<std::promise<std::vector<std::string>>>();
        auto fut = done->get_future();
        net::post(io_ctx_.ioc(), [this, done] {
            std::vector<std::string> out;
            out.swap(test_sent_payloads);
            done->set_value(std::move(out));
        });
        return fut.get();
    }

private:

    enum State { kStopped = 0, kStarting = 1, kConnected = 2,
                 kReconnecting = 3, kStopping = 4 };

    /// @brief 已进入 WS transport 的待发送请求；仅 submit 或 cancel 分支有效。
    struct QueuedReq {
        enum class Kind { kSubmit, kCancel };
        Kind kind = Kind::kSubmit;
        TransportSubmitReq submit;
        TransportCancelReq cancel;
        TransportLatencyTrace trace;
    };

    /// @brief 已发出但尚未收到 WS 响应的请求。
    struct InflightEntry {
        enum class Kind { kSubmit, kCancel };
        Kind kind = Kind::kSubmit;
        uint64_t local_id = 0;
        TransportLatencyTrace trace;
        uint64_t enq_us = 0;
        uint64_t t0c_deadline_us = 0;
        TransportSubmitReq submit_req;
        BinanceSubmitWireValues wire_values;
        bool has_submit_wire = false;
        bool wire_submit_emitted = false;
    };

    static TransportAck::ReqKind req_kind_of(QueuedReq::Kind k) noexcept {
        return k == QueuedReq::Kind::kCancel ? TransportAck::ReqKind::kCancel
                                             : TransportAck::ReqKind::kSubmit;
    }
    static TransportAck::ReqKind req_kind_of(InflightEntry::Kind k) noexcept {
        return k == InflightEntry::Kind::kCancel ? TransportAck::ReqKind::kCancel
                                                 : TransportAck::ReqKind::kSubmit;
    }

    bool validate_config() const {
        if (cfg.host.empty() || cfg.target.empty() || cfg.target.front() != '/') {
            XtLogger::error_m("BinanceWSTrade", "invalid ws config host='{}' target='{}'",
                              cfg.host, cfg.target);
            return false;
        }
        if (cfg.submit_timeout_ms < 1000 || cfg.stale_drop_ms >= cfg.submit_timeout_ms ||
            cfg.send_queue_cap == 0) {
            XtLogger::error_m("BinanceWSTrade",
                              "invalid timeout/cap timeout={} stale={} cap={}",
                              cfg.submit_timeout_ms, cfg.stale_drop_ms, cfg.send_queue_cap);
            return false;
        }
        return true;
    }

    bool test_mode() const { return cfg.host == "__test__"; }

    uint64_t now_us() const noexcept {
        return steady_now_us() + test_clock_offset_us;
    }

    int64_t wall_clock_ms() const {
        return static_cast<int64_t>(sign::now_ms())
             + static_cast<int64_t>(test_clock_offset_us / 1000);
    }

    void start_on_io() {
        connected.store(false, std::memory_order_relaxed);
        schedule_timeout_tick();
        if (test_mode()) return;
        xt_live::gateway::XtWsConnection::Config wcfg;
        wcfg.host = cfg.host;
        wcfg.port = cfg.port;
        wcfg.target = cfg.target;
        wcfg.log_tag = "BinanceWSTrade";
        wcfg.reconnect_min_ms = cfg.reconnect_min_ms;
        wcfg.reconnect_max_ms = cfg.reconnect_max_ms;
        conn = std::make_unique<xt_live::gateway::XtWsConnection>(
            io_ctx_.ioc(), io_ctx_.ssl_ctx(), std::move(wcfg));
        conn->bind_callbacks(
            [this](std::string_view payload) { handle_response(payload); },
            [this] { handle_connected(); },
            [this] { handle_disconnected(); });
        conn->start();
    }

    void stop_on_io() {

        state.store(kStopping, std::memory_order_relaxed);
        connected.store(false, std::memory_order_relaxed);
        timeout_timer.cancel();
        if (conn) conn->stop();
        fail_all_inflight(TransportAck::Kind::kTransportError_Disconnect);
        drain_queue_as_disconnect();
        state.store(kStopped, std::memory_order_relaxed);
        stopped.store(true, std::memory_order_relaxed);
    }

    void enqueue_or_overflow(QueuedReq&& item) {
        item.trace.transport = "ws";
        item.trace.enq_us = now_us();
        const int cur = state.load(std::memory_order_relaxed);
        if (cur == kStopped || cur == kStopping) {

            emit_pre_send_failure(item,
                                  TransportAck::Kind::kTransportError_QueueOverflow,
                                  "WsQueueOverflow",
                                  "send queue overflow");
            return;
        }
        bool should_drain = false;
        {
            std::lock_guard<std::mutex> lk(queue_mu);
            if (ws_send_q.size() >= cfg.send_queue_cap) {
                post_pre_send_error_ack(std::move(item),
                                        TransportAck::Kind::kTransportError_QueueOverflow,
                                        "WsQueueOverflow",
                                        "send queue overflow");
                return;
            }
            ws_send_q.push_back(std::move(item));
            should_drain = (cur == kConnected);
        }
        if (should_drain) net::post(io_ctx_.ioc(), [this] { do_drain(); });
    }

    using AckBatch = std::vector<std::pair<TransportAck, TransportLatencyTrace>>;

    void emit_pre_send_failure(const QueuedReq& item,
                               TransportAck::Kind kind,
                               const char* reject_code,
                               const char* wire_reason,
                               const char* ack_reason = nullptr) {
        if (item.kind == QueuedReq::Kind::kSubmit) {
            emit_wire_reject_trace(item.submit, reject_code, wire_reason);
        }
        dispatch(make_transport_ack(item.trace.local_id, kind, req_kind_of(item.kind),
                                    ack_reason ? ack_reason : wire_reason),
                 item.trace);
    }

    void collect_pre_send_failure(const QueuedReq& item,
                                  TransportAck::Kind kind,
                                  const char* reject_code,
                                  const char* reason,
                                  AckBatch& out) {
        if (item.kind == QueuedReq::Kind::kSubmit) {
            emit_wire_reject_trace(item.submit, reject_code, reason);
        }
        out.push_back({make_transport_ack(item.trace.local_id, kind, req_kind_of(item.kind), reason),
                       item.trace});
    }

    void collect_pre_send_failure(const InflightEntry& entry,
                                  TransportAck::Kind kind,
                                  const char* reject_code,
                                  const char* reason,
                                  AckBatch& out) {
        if (entry.kind == InflightEntry::Kind::kSubmit && !entry.wire_submit_emitted) {
            emit_wire_reject_trace(entry.submit_req, reject_code, reason);
        }
        out.push_back({make_transport_ack(entry.local_id, kind, req_kind_of(entry.kind), reason),
                       entry.trace});
    }

    void collect_inflight_error(const InflightEntry& entry,
                                TransportAck::Kind kind,
                                const char* reason,
                                AckBatch& out) {
        out.push_back({make_transport_ack(entry.local_id, kind, req_kind_of(entry.kind), reason),
                       entry.trace});
    }

    static void stamp_ack_req_kind(TransportAck& ack, const InflightEntry& entry) {
        ack.req_kind = req_kind_of(entry.kind);
    }

    void post_pre_send_error_ack(QueuedReq&& item,
                                 TransportAck::Kind kind,
                                 const char* reject_code,
                                 const char* reason) {
        net::post(io_ctx_.ioc(), [this,
                        item = std::move(item),
                        kind,
                        reject_code,
                        reason]() mutable {
            emit_pre_send_failure(item, kind, reject_code, reason);
        });
    }

    void handle_connected() {
        const int cur = state.load(std::memory_order_relaxed);
        if (cur != kStarting && cur != kReconnecting && cur != kConnected) return;
        state.store(kConnected, std::memory_order_relaxed);
        connected.store(true, std::memory_order_relaxed);
        do_drain();
    }

    void handle_disconnected() {
        if (state.load(std::memory_order_relaxed) == kStopped) return;
        connected.store(false, std::memory_order_relaxed);
        state.store(kReconnecting, std::memory_order_relaxed);

        fail_all_inflight(TransportAck::Kind::kTransportError_Disconnect);
    }

    void do_drain() {
        if (state.load(std::memory_order_relaxed) != kConnected) return;
        for (;;) {
            QueuedReq item;
            {
                std::lock_guard<std::mutex> lk(queue_mu);
                if (ws_send_q.empty()) break;
                item = std::move(ws_send_q.front());
                ws_send_q.pop_front();
            }
            drain_one(std::move(item));
        }
    }

    void drain_one(QueuedReq&& item) {
        const uint64_t t0a = now_us();
        if (t0a - item.trace.enq_us > static_cast<uint64_t>(cfg.stale_drop_ms) * 1000) {

            emit_pre_send_failure(item, TransportAck::Kind::kTransportError_Stale,
                                  "WsStaleBeforeSend", "stale before send");
            return;
        }
        item.trace.t0a_us = t0a;
        const std::string req_id = std::to_string(next_req_id++);
        const int64_t ts_ms = wall_clock_ms() + rest.server_time_offset_ms();
        BinanceSubmitWireValues wire_values;
        std::string payload = build_payload(item, req_id, ts_ms, &wire_values);
        item.trace.t0b_us = now_us();

        InflightEntry entry;
        entry.kind = (item.kind == QueuedReq::Kind::kSubmit)
                         ? InflightEntry::Kind::kSubmit : InflightEntry::Kind::kCancel;
        entry.local_id = item.trace.local_id;
        entry.trace = item.trace;
        entry.enq_us = item.trace.enq_us;
        if (item.kind == QueuedReq::Kind::kSubmit) {
            entry.submit_req = item.submit;
            entry.wire_values = wire_values;
            entry.has_submit_wire = item.submit.trace_id != 0;
        }
        inflight.emplace(req_id, std::move(entry));
        inflight_count.store(static_cast<uint32_t>(inflight.size()), std::memory_order_relaxed);

        send_payload(req_id, std::move(payload));
    }

	    std::string build_payload(const QueuedReq& item,
	                              const std::string& req_id,
	                              int64_t ts_ms,
	                              BinanceSubmitWireValues* wire_out) {
	        if (item.kind == QueuedReq::Kind::kSubmit) {
	            return build_submit_request(item.submit, req_id, ts_ms, cred, wire_out);
	        }
	        return build_cancel_request(item.cancel, req_id, ts_ms, cred);
	    }

    void send_payload(const std::string& req_id, std::string payload) {
        if (test_mode()) {

            test_sent_payloads.push_back(payload);
            net::post(io_ctx_.ioc(), [this, req_id] { handle_write_complete(req_id); });
            return;
        }
        if (!conn) return;

        conn->send_text(std::move(payload),
                        [this, req_id] {
                            net::post(io_ctx_.ioc(), [this, req_id] { handle_write_complete(req_id); });
                        });
    }

    void handle_write_complete(const std::string& req_id) {
        auto it = inflight.find(req_id);
        if (it == inflight.end()) return;
        const uint64_t now = now_us();
        it->second.trace.t0c_us = now;
        it->second.t0c_deadline_us =
            now + static_cast<uint64_t>(cfg.submit_timeout_ms) * 1000;
        if (it->second.has_submit_wire && !it->second.wire_submit_emitted) {
            emit_wire_submit_trace(it->second.submit_req, it->second.wire_values);
            it->second.wire_submit_emitted = true;
        }
    }

    void handle_response(std::string_view payload) {
        TransportAck ack;
        std::string req_id;
        try {
            req_id = parse_response(payload, ack);
        } catch (const std::exception& e) {
            XtLogger::warn_m("BinanceWSTrade", "parse_response exception: {}", e.what());
            return;
        } catch (...) {
            XtLogger::warn_m("BinanceWSTrade", "parse_response unknown exception");
            return;
        }
        if (req_id.empty()) return;
        auto it = inflight.find(req_id);
        if (it == inflight.end()) {
            XtLogger::warn_m("BinanceWSTrade", "stale_ack req_id={}", req_id);
            return;
        }
        InflightEntry entry = std::move(it->second);
        inflight.erase(it);
        inflight_count.store(static_cast<uint32_t>(inflight.size()), std::memory_order_relaxed);
        entry.trace.t1_us = now_us();
        ack.local_id = entry.local_id;

        stamp_ack_req_kind(ack, entry);
        dispatch(ack, entry.trace);
    }

    void handle_timeout_tick() {
        const uint64_t now = now_us();
        AckBatch expired;
        for (auto it = inflight.begin(); it != inflight.end();) {
            auto& e = it->second;
            if (e.t0c_deadline_us != 0 && e.t0c_deadline_us < now) {

                collect_inflight_error(e, TransportAck::Kind::kTransportError_TimeoutAfterSend,
                                       "timeout after send", expired);
                it = inflight.erase(it);
            } else if (e.t0c_deadline_us == 0 &&
                       e.enq_us + static_cast<uint64_t>(cfg.submit_timeout_ms) * 1000 < now) {

                collect_pre_send_failure(e, TransportAck::Kind::kTransportError_TimeoutBeforeSend,
                                         "WsTimeoutBeforeSend", "timeout before send", expired);
                it = inflight.erase(it);
            } else {
                ++it;
            }
        }
        expire_queued_before_write(now, expired);
        inflight_count.store(static_cast<uint32_t>(inflight.size()), std::memory_order_relaxed);
        for (auto& p : expired) dispatch(p.first, p.second);
    }

    void expire_queued_before_write(uint64_t now, AckBatch& out) {

        std::deque<QueuedReq> kept;
        std::vector<QueuedReq> expired_items;
        {
            std::lock_guard<std::mutex> lk(queue_mu);
            while (!ws_send_q.empty()) {
                QueuedReq item = std::move(ws_send_q.front());
                ws_send_q.pop_front();
                if (item.trace.enq_us + static_cast<uint64_t>(cfg.submit_timeout_ms) * 1000 < now) {
                    expired_items.push_back(std::move(item));
                } else {
                    kept.push_back(std::move(item));
                }
            }
            ws_send_q.swap(kept);
        }
        for (const auto& item : expired_items) {
            collect_pre_send_failure(item, TransportAck::Kind::kTransportError_TimeoutBeforeSend,
                                     "WsTimeoutBeforeSend", "timeout before send", out);
        }
    }

    void schedule_timeout_tick() {
        if (state.load(std::memory_order_relaxed) == kStopped) return;
        timeout_timer.expires_after(std::chrono::seconds(1));
        timeout_timer.async_wait([this](boost::system::error_code ec) {
            if (ec || state.load(std::memory_order_relaxed) == kStopped) return;
            handle_timeout_tick();
            schedule_timeout_tick();
        });
    }

    void fail_all_inflight(TransportAck::Kind kind) {

        AckBatch failed;
        failed.reserve(inflight.size());
        for (auto& kv : inflight) {
            auto& e = kv.second;

            if (kind == TransportAck::Kind::kTransportError_Disconnect &&
                e.t0c_deadline_us == 0) {
                collect_pre_send_failure(e, kind, "WsDisconnectBeforeSend",
                                         kind_msg(kind), failed);
            } else {
                collect_inflight_error(e, kind, kind_msg(kind), failed);
            }
        }
        inflight.clear();
        inflight_count.store(0, std::memory_order_relaxed);
        for (auto& p : failed) dispatch(p.first, p.second);
    }

    void drain_queue_as_disconnect() {
        std::deque<QueuedReq> q;
        {
            std::lock_guard<std::mutex> lk(queue_mu);
            q.swap(ws_send_q);
        }
        while (!q.empty()) {
            auto item = std::move(q.front());
            q.pop_front();

            emit_pre_send_failure(item, TransportAck::Kind::kTransportError_Disconnect,
                                  "WsTransportStopped", "transport stopped before send",
                                  "transport stopped");
        }
    }

    void dispatch(const TransportAck& ack, const TransportLatencyTrace& trace) {
        last_ack_us.store(now_us(), std::memory_order_relaxed);
        if (ack_handler) ack_handler(ack, trace);
    }

    Config cfg;
    BinanceRestClient& rest;
    WsTradeCredentials cred;
    AckHandler ack_handler;

    xt_live::gateway::IoContextThread io_ctx_;
    net::steady_timer timeout_timer;
    std::unique_ptr<xt_live::gateway::XtWsConnection> conn;

    mutable std::mutex queue_mu;
    std::deque<QueuedReq> ws_send_q;

    std::unordered_map<std::string, InflightEntry> inflight;
    std::vector<std::string> test_sent_payloads;

    std::atomic<int> state{kStopped};
    std::atomic<bool> connected{false};
    std::atomic<bool> stopped{true};
    std::atomic<uint32_t> inflight_count{0};
    std::atomic<uint64_t> last_ack_us{0};

    uint64_t next_req_id = 1;
    uint64_t test_clock_offset_us = 0;
};

WsTradeTransport::WsTradeTransport(Config cfg,
                                   BinanceRestClient& rest_for_time_offset,
                                   WsTradeCredentials cred)
    : impl_(std::make_unique<Impl>(std::move(cfg), rest_for_time_offset, std::move(cred))) {}

WsTradeTransport::~WsTradeTransport() = default;

bool WsTradeTransport::start() { return impl_->start(); }
void WsTradeTransport::stop() { impl_->stop(); }
void WsTradeTransport::submit(TransportSubmitReq&& req) { impl_->submit(std::move(req)); }
void WsTradeTransport::cancel(TransportCancelReq&& req) { impl_->cancel(std::move(req)); }
void WsTradeTransport::bind_ack_handler(AckHandler handler) {
    impl_->bind_ack_handler(std::move(handler));
}
ITradeTransport::Health WsTradeTransport::health() const { return impl_->health(); }
void WsTradeTransport::inject_test_message(std::string_view payload) {
    impl_->inject_test_message(payload);
}
void WsTradeTransport::inject_test_connect() { impl_->inject_test_connect(); }
void WsTradeTransport::inject_test_disconnect() { impl_->inject_test_disconnect(); }
void WsTradeTransport::inject_test_advance_clock(uint64_t delta_us) {
    impl_->inject_test_advance_clock(delta_us);
}
std::vector<std::string> WsTradeTransport::inject_test_take_sent_payloads() {
    return impl_->inject_test_take_sent_payloads();
}

}
