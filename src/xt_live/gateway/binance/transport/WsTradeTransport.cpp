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

/// @brief execution-unknown 事件的限频告警窗口（steady µs）；对齐 REST 侧节奏。
constexpr uint64_t kExecUnknownLogIntervalUs = 5'000'000ULL;

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

}  // namespace

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
        // io 线程 lifecycle 由 IoContextThread 持有；start_on_io（含 schedule_timeout_tick
        // 与 conn->start）在 ioc.run() 前 post，非阻塞建连。
        io_ctx_.start([this] { start_on_io(); });
        return true;
    }

    void stop() {
        if (state.load(std::memory_order_relaxed) == kStopped) return;
        // stop_on_io 把未决请求全部派发为 Disconnect（见其注释）；控制线程等其在 io
        // 线程跑完再停 ioc + join（IoContextThread wait_for_teardown 语义）。
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
    // state 由外部线程读写启动/停止入口，由 IO 线程推进连接态；原子值用于跨线程快速判定。
    enum State { kStopped = 0, kStarting = 1, kConnected = 2,
                 kReconnecting = 3, kStopping = 4 };

    /// @brief 已进入 WS transport 的待发送请求；仅 submit 或 cancel 分支有效。
    struct QueuedReq {
        enum class Kind { kSubmit, kCancel };
        Kind kind = Kind::kSubmit;         ///< 请求类型。
        TransportSubmitReq submit;         ///< kind == kSubmit 时有效。
        TransportCancelReq cancel;         ///< kind == kCancel 时有效。
        TransportLatencyTrace trace;       ///< 入队到 ack 的 latency 切片。
    };

    /// @brief 已发出但尚未收到 WS 响应的请求。
    struct InflightEntry {
        enum class Kind { kSubmit, kCancel };
        Kind kind = Kind::kSubmit;         ///< 原始请求类型；ack.req_kind 归属与 pre-send WIRE_REJECT 分支据此判定。
        uint64_t local_id = 0;             ///< 本地委托编号，收到 req_id 后回填到 ack。
        TransportLatencyTrace trace;       ///< 已记录到 write/ack 阶段的 latency 切片。
        uint64_t enq_us = 0;               ///< 入队时间，timeout-before-send 的检测基准。
        uint64_t t0c_deadline_us = 0;      ///< write 完成后的 ack 截止时间；0 表示尚未写完。
        TransportSubmitReq submit_req;     ///< submit 分支的原始请求，write 完成后用于 INTENT_TRACE。
        BinanceSubmitWireValues wire_values; ///< 发送前已截断/格式化的 wire 值。
        bool has_submit_wire = false;      ///< true 表示本 entry 需要在 write done 后 emit WIRE_SUBMIT。
        bool wire_submit_emitted = false;  ///< 防御重复 write completion 导致重复 trace。
    };

    /// QueuedReq/InflightEntry 的请求类型 → ack 归属标记（req_kind）。
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
        // stop 需要把所有未决请求显式派发为 Disconnect，避免上层订单状态机永久等待 ack。
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
            // transport 已停止时没有可用连接语义；这里复用 QueueOverflow 表达本地未接收成功。
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

    // ── ack 派发收敛（req_kind 一律在 helper 内从 item.kind 派生，callsite 不再人肉传）──
    // 与 REST transport 同构：helper 名字焊死请求归属。
    //   · emit_pre_send_failure：单请求 pre-send 失败——WIRE_REJECT(仅 submit) +
    //     make_transport_ack + dispatch 一次成型，立即派发。
    //   · collect_pre_send_failure：批量失败路径（timeout tick / fail_all）——落 WIRE_REJECT
    //     并把 ack 追加进 out 队列，dispatch 由 caller 在 inflight 变更后统一执行
    //     （保「先复制失败事件再 dispatch，ack_handler 反入不观察半清空 inflight」契约）。
    //   · collect_inflight_error：已 write 出的 inflight 的 transport-error ack（无 WIRE_REJECT，
    //     订单可能已达交易所，上层走 reconcile）；同样只入队不派发。
    //   · stamp_ack_req_kind：codec 解析出的响应 ack，归属以 inflight 登记为准。
    using AckBatch = std::vector<std::pair<TransportAck, TransportLatencyTrace>>;

    // wire_reason 供 WIRE_REJECT trace，ack_reason 供 ack.err_msg；多数路径两者同串。
    // transport-stopped drain 路径 wire_reason 与 ack_reason 刻意使用不同串
    // （wire_reason trace 区分 pre-send 位置，ack_reason 表达停机）。
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
        // 已写出但未确认的请求可能已经到达交易所；上层收到 Disconnect 后会走 reconcile。
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
            // 入队等待过久的请求不再发送，防止使用过期时间戳或过期行情意图下单。
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
            // test_mode 不走真实 socket；立即补写 t0c，只验证 req_id/inflight/parse 路径。
            test_sent_payloads.push_back(payload);
            net::post(io_ctx_.ioc(), [this, req_id] { handle_write_complete(req_id); });
            return;
        }
        if (!conn) return;
        // t0c 必须等 async_write 完成后记录，表示 client-side write done。
        // write 失败时没有 t0c，timeout_tick 会按 TimeoutBeforeSend 收敛。
        // 回调再 post 回 ioc，使 inflight 的所有访问保持同一任务序列。
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
        // codec 无法从 response payload 区分 submit/cancel；归属以 inflight 登记为准。
        stamp_ack_req_kind(ack, entry);
        // codec 把 5xx / -1007 等 execution-unknown 回包分类为 TimeoutAfterSend
        //（保留在途交 reconcile）；submit 侧计数 + 限频 warn，对齐 REST BIN-D14。
        if (ack.req_kind == TransportAck::ReqKind::kSubmit &&
            ack.kind == TransportAck::Kind::kTransportError_TimeoutAfterSend) {
            note_submit_exec_unknown(ack);
        }
        dispatch(ack, entry.trace);
    }

    /// @brief 记录一次 WS submit 回包 execution-unknown：累计 counter + 限频 warn。
    /// @note 仅 io 线程调用（handle_response）；log 节流状态为该线程私有。
    void note_submit_exec_unknown(const TransportAck& ack) {
        const uint64_t total =
            submit_exec_unknown_count_.fetch_add(1, std::memory_order_relaxed) + 1;
        const uint64_t now = now_us();
        if (last_exec_unknown_log_us_ == 0 ||
            now - last_exec_unknown_log_us_ >= kExecUnknownLogIntervalUs) {
            last_exec_unknown_log_us_ = now;
            XtLogger::warn_m("BinanceWSTrade",
                "submit execution-unknown (kept inflight for reconcile): local_id={} "
                "err_code={} err='{}' total={} suppressed_since_last={}",
                ack.local_id, ack.err_code, ack.err_msg, total,
                exec_unknown_suppressed_count_);
            exec_unknown_suppressed_count_ = 0;
        } else {
            ++exec_unknown_suppressed_count_;
        }
    }

    void handle_timeout_tick() {
        const uint64_t now = now_us();
        AckBatch expired;
        for (auto it = inflight.begin(); it != inflight.end();) {
            auto& e = it->second;
            if (e.t0c_deadline_us != 0 && e.t0c_deadline_us < now) {
                // 已完成本地 write，交易所可能已收到；上层需要 reconcile。
                collect_inflight_error(e, TransportAck::Kind::kTransportError_TimeoutAfterSend,
                                       "timeout after send", expired);
                it = inflight.erase(it);
            } else if (e.t0c_deadline_us == 0 &&
                       e.enq_us + static_cast<uint64_t>(cfg.submit_timeout_ms) * 1000 < now) {
                // 长时间没有 write completion，按未送达处理。
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
        // 未连接或背压期间留在 send_q 的请求也要走总超时，不能只检查 inflight。
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
        // 先复制失败事件再 dispatch，避免 ack_handler 反入时观察到半清空的 inflight。
        AckBatch failed;
        failed.reserve(inflight.size());
        for (auto& kv : inflight) {
            auto& e = kv.second;
            // 未 write 完成（t0c_deadline_us==0）的断连是 pre-send 失败，落 WIRE_REJECT；
            // 已 write 出的走 collect_inflight_error（订单可能已达交易所，不落 reject）。
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
            // q 已从 ws_send_q swap 出，为本地队列，逐项立即派发不暴露半清空共享结构。
            emit_pre_send_failure(item, TransportAck::Kind::kTransportError_Disconnect,
                                  "WsTransportStopped", "transport stopped before send",
                                  "transport stopped");
        }
    }

    void dispatch(const TransportAck& ack, const TransportLatencyTrace& trace) {
        last_ack_us.store(now_us(), std::memory_order_relaxed);
        if (ack_handler) ack_handler(ack, trace);
    }

    // ── 构造期依赖与只读配置 ───────────────────────────────────────
    Config cfg;
    BinanceRestClient& rest;  ///< 仅读取 server_time_offset_ms，不拥有。
    WsTradeCredentials cred;
    AckHandler ack_handler;

    // ── IO 线程资源 ───────────────────────────────────────────────
    // io_context + ssl_ctx + work_guard + WS 独占线程 由 IoContextThread 持有
    // （lifecycle 样板）；须先于 timeout_timer 声明——后者构造依赖 io_ctx_.ioc()。
    xt_live::gateway::IoContextThread io_ctx_;
    net::steady_timer timeout_timer;
    std::unique_ptr<xt_live::gateway::XtWsConnection> conn;

    // ── 跨线程发送队列（queue_mu 保护）────────────────────────────
    mutable std::mutex queue_mu;
    std::deque<QueuedReq> ws_send_q;

    // ── IO 线程独占状态 ───────────────────────────────────────────
    std::unordered_map<std::string, InflightEntry> inflight;
    std::vector<std::string> test_sent_payloads;

    // ── 跨线程观测状态 ───────────────────────────────────────────
    std::atomic<int> state{kStopped};
    std::atomic<bool> connected{false};
    std::atomic<bool> stopped{true};
    std::atomic<uint32_t> inflight_count{0};
    std::atomic<uint64_t> last_ack_us{0};

    // ── IO 线程计数/测试时钟 ─────────────────────────────────────
    uint64_t next_req_id = 1;
    uint64_t test_clock_offset_us = 0;

public:
    uint64_t submit_exec_unknown_count() const {
        return submit_exec_unknown_count_.load(std::memory_order_relaxed);
    }

private:
    // ── execution-unknown 观测（WS 侧对齐 REST BIN-D14）──────────
    std::atomic<uint64_t> submit_exec_unknown_count_{0};  ///< 累计 exec-unknown 事件数（跨线程读）。
    uint64_t last_exec_unknown_log_us_ = 0;               ///< io-thread-only：上次限频 warn 时刻。
    uint64_t exec_unknown_suppressed_count_ = 0;          ///< io-thread-only：窗口内被抑制的日志条数。
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
uint64_t WsTradeTransport::submit_exec_unknown_count() const {
    return impl_->submit_exec_unknown_count();
}
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

}  // namespace binance_u
