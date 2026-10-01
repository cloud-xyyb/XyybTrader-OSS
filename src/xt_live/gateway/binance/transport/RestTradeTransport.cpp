#include "RestTradeTransport.h"

#include "BinanceIntentTrace.h"
#include "BinanceTdWireUtil.h"
#include "BinanceOrderParams.h"
#include "rest/BinanceRestClient.h"
#include "refdata/BinanceExchangeInfo.h"

#include <algorithm>
#include <atomic>
#include <cmath>  // std::isfinite（P15 executedQty 防线）
#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <variant>

#include <nlohmann/json.hpp>

#include "xt_core/order/XtOffset.h"
#include "xt_logger/XtLogger.h"
#include "xt_util/time/XtSteadyClock.h"

namespace binance_u {
namespace {

inline uint64_t lat_now_us() noexcept {
    return XtSteadyClock::steady_now_us();
}

/// @brief 从 Binance 错误响应体解析 `code` 字段；缺失 / 非整数 / 非法 JSON → 0。
int parse_rest_error_code(const std::string& body) noexcept {
    auto j = nlohmann::json::parse(body, nullptr, false);
    if (j.is_discarded() || !j.is_object()) return 0;
    auto it = j.find("code");
    if (it == j.end() || !it->is_number_integer()) return 0;
    return it->get<int>();
}

/// @brief execution-unknown 事件的限频告警窗口（steady µs）；对齐 cancel-fail 日志节奏。
constexpr uint64_t kExecUnknownLogIntervalUs = 5'000'000ULL;

struct ReqSubmit {
    TransportSubmitReq req;          ///< 已移交给 REST worker 的 submit 请求。
    TransportLatencyTrace trace;     ///< submit 从策略入口到 REST ack 的 latency 切片。
};

struct ReqCancel {
    TransportCancelReq req;          ///< 已移交给 REST worker 的 cancel 请求。
    TransportLatencyTrace trace;     ///< cancel 从策略入口到 REST ack 的 latency 切片。
    int tries_done = 0;              ///< 已完成的 REST 尝试次数（限频重试调度用）。
};

/// @brief 等待退避到期的 cancel 重试项；due_us 为 steady µs。
struct DelayedCancel {
    ReqCancel item;
    uint64_t  due_us = 0;
};

using Req = std::variant<ReqSubmit, ReqCancel>;

}  // anonymous namespace

bool rest_cancel_error_retryable(long http_status, int err_code) {
    if (http_status == 429 || http_status == 418) return true;
    return err_code == -1015 || err_code == -1003;
}

RestSubmitFailureClass classify_rest_submit_failure(long http_status,
                                                    int err_code) noexcept {
    // http==0：curl transport 失败（DNS / 连接 / 读超时），请求可能已抵达 venue。
    if (http_status == 0) return RestSubmitFailureClass::kExecutionUnknown;
    // 3xx：LB / 反代重定向（Binance fapi 实际不发）；请求是否被处理不可判定，保守当未知。
    if (http_status >= 300 && http_status < 400) return RestSubmitFailureClass::kExecutionUnknown;
    // 5xx：Binance 官方明示服务端内部错误，execution status unknown（可能已成交）。
    if (http_status >= 500) return RestSubmitFailureClass::kExecutionUnknown;
    // 执行状态未知的业务码（可能以 4xx 到达，不能当业务拒终态化）：
    //   -1007 TIMEOUT           后端响应超时，发送/执行状态均未知。
    //   -1006 UNEXPECTED_RESP   Binance 官方明示 "execution status unknown"（可能已成交）。
    //   -1001 DISCONNECTED      内部错误 "unable to process your request"，是否被处理不可判定。
    // 保守方向：真拒被当未知，只多一次 reconcile 查 not-exist 终态化（安全）；反之漏判
    // （未知当业务拒终态化）会本地销账而委托仍挂/成交 → 漏成交 / 幽灵仓（危险）。
    // 刻意不纳入 -1008 SERVER_BUSY：过载拒绝，委托明确未挂出（限流业务拒，见
    // BinanceOrderLedger），storm 期纳入未知反而累积幽灵在途并延迟拒因信号。
    if (err_code == -1007 || err_code == -1006 || err_code == -1001)
        return RestSubmitFailureClass::kExecutionUnknown;
    // 其余（4xx 业务拒 / 限流 429/418 / -1003 / -1008）：订单未挂出，可安全终态化为 Rejected。
    return RestSubmitFailureClass::kBusinessReject;
}

uint64_t rest_cancel_backoff_ms(int tries_done, long retry_after_sec,
                                const RestCancelRetryCfg& cfg) {
    if (retry_after_sec > 0) {
        // venue 已明示等待窗口（429→418 封禁升级语义），优先于指数退避。
        const uint64_t ra_ms = static_cast<uint64_t>(retry_after_sec) * 1000ULL;
        return std::min(ra_ms, cfg.max_retry_after_ms);
    }
    const int shift = std::min(tries_done > 0 ? tries_done - 1 : 0, 20);
    return std::min(cfg.base_backoff_ms << shift, cfg.max_backoff_ms);
}

class RestTradeTransport::Impl {
public:
    Impl(BinanceRestClient& rest_in, RestCancelRetryCfg cancel_retry)
        : rest(rest_in), retry_cfg(cancel_retry) {}

    ~Impl() { stop(); }

    bool start() {
        if (running.exchange(true)) return true;
        worker_thread = std::thread([this] { worker_loop(); });
        return true;
    }

    void stop() {
        if (!running.exchange(false)) return;
        auto [pending, dropped_retries] = take_pending_before_stop();
        log_dropped_cancel_retries(dropped_retries);
        queue_cv.notify_all();
        drain_pending_before_stop(std::move(pending));
        if (worker_thread.joinable()) worker_thread.join();
    }

    void submit(TransportSubmitReq&& req) {
        ReqSubmit item;
        item.trace.local_id = req.local_id;
        item.trace.transport = "rest";
        item.trace.t0_us = req.t0_us;
        item.trace.enq_us = lat_now_us();
        item.req = std::move(req);
        enqueue_or_reject_on_stop(std::move(item));
    }

    void cancel(TransportCancelReq&& req) {
        ReqCancel item;
        item.trace.local_id = req.local_id;
        item.trace.transport = "rest";
        item.trace.t0_us = req.t0_us;
        item.trace.enq_us = lat_now_us();
        item.req = std::move(req);
        enqueue_or_reject_on_stop(std::move(item));
    }

    /// @brief submit/cancel 共享的入队路径：running 双检 → 入队 + notify，任一 gate
    ///        失败即按 stop-before-send 拒绝（拒绝动作由 reject_before_wire_on_stop
    ///        的 ReqSubmit/ReqCancel 重载分别决定，与 worker stop drain 同口径）。
    template <typename Item>
    void enqueue_or_reject_on_stop(Item&& item) {
        if (!running.load(std::memory_order_relaxed)) {
            reject_before_wire_on_stop(item);
            return;
        }
        bool queued = false;
        {
            std::lock_guard<std::mutex> lk(queue_mu);
            if (running.load(std::memory_order_relaxed)) {
                queue.push_back(Req{std::move(item)});
                queued = true;
            }
        }
        if (queued) {
            queue_cv.notify_one();
        } else {
            reject_before_wire_on_stop(item);
        }
    }

    void bind_ack_handler(AckHandler handler) {
        ack_handler = std::move(handler);
    }

    Health health() const {
        Health h;
        h.connected = true;
        h.inflight_count = inflight_count.load(std::memory_order_relaxed);
        h.last_ack_us = last_ack_us.load(std::memory_order_relaxed);
        return h;
    }

    bool is_running() const { return running.load(std::memory_order_acquire); }

private:
    void worker_loop() {
        // submit/cancel 共用单 worker：保证 REST 调用顺序与队列顺序一致，
        // 同时把 HTTP 阻塞从策略线程移出。到期的 cancel 重试与主队列同 worker
        // 串行执行；未到期的重试只决定等待超时，不阻塞新请求。
        while (true) {
            Req req{ReqSubmit{}};
            {
                std::unique_lock<std::mutex> lk(queue_mu);
                for (;;) {
                    if (!running.load()) {
                        std::deque<Req> pending;
                        pending.swap(queue);
                        const size_t dropped_retries = retry_queue.size();
                        retry_queue.clear();
                        lk.unlock();
                        log_dropped_cancel_retries(dropped_retries);
                        drain_pending_before_stop(std::move(pending));
                        return;
                    }
                    if (!queue.empty()) {
                        req = std::move(queue.front());
                        queue.pop_front();
                        break;
                    }
                    const uint64_t now_us = lat_now_us();
                    if (!retry_queue.empty() && retry_queue.front().due_us <= now_us) {
                        req = Req{std::move(retry_queue.front().item)};
                        retry_queue.pop_front();
                        break;
                    }
                    if (retry_queue.empty()) {
                        queue_cv.wait(lk);
                    } else {
                        queue_cv.wait_for(lk, std::chrono::microseconds(
                                                  retry_queue.front().due_us - now_us));
                    }
                }
            }
            std::visit([this](auto& r) { execute(r); }, req);
        }
    }

    std::pair<std::deque<Req>, size_t> take_pending_before_stop() {
        std::lock_guard<std::mutex> lk(queue_mu);
        std::deque<Req> pending;
        pending.swap(queue);
        const size_t dropped_retries = retry_queue.size();
        retry_queue.clear();
        return {std::move(pending), dropped_retries};
    }

    void log_dropped_cancel_retries(size_t dropped_retries) const {
        if (dropped_retries == 0) return;
        XtLogger::warn_m("BinanceTD",
                         "rest transport stop: dropping {} pending cancel retries",
                         dropped_retries);
    }

    void drain_pending_before_stop(std::deque<Req>&& pending) {
        for (auto& req : pending) {
            std::visit([this](auto& r) { reject_before_wire_on_stop(r); }, req);
        }
    }

    void reject_before_wire_on_stop(ReqSubmit& r) {
        reject_submit_before_wire(r, "RestTransportStopped",
                                  "rest transport stopped before send",
                                  TransportAck::Kind::kTransportError_Disconnect);
    }

    void reject_before_wire_on_stop(ReqCancel& r) {
        r.trace.t1_us = lat_now_us();
        dispatch(make_cancel_ack(r.req.local_id, TransportAck::Kind::kTransportError_Disconnect,
                                 TransportAck::TimingOrigin::kLocalFailure),
                 r.trace);
    }

    void execute(ReqSubmit& r) {
        r.trace.t0a_us = lat_now_us();
        execute_submit(r);
    }

    void execute(ReqCancel& r) {
        r.trace.t0a_us = lat_now_us();
        execute_cancel(r);
    }

    BinanceSubmitOrderParams build_submit_params(const TransportSubmitReq& r) {
        // Binance REST 签名按参数 map 的稳定顺序生成；数量/价格在发送边界统一截断。
        return build_submit_order_params(r);
    }

    void execute_submit(ReqSubmit& r) {
        auto built = build_submit_params(r.req);
        r.trace.t0b_us = lat_now_us();
        inflight_count.fetch_add(1, std::memory_order_relaxed);
        auto resp = rest.post_signed("/fapi/v1/order", std::move(built.params));
        inflight_count.fetch_sub(1, std::memory_order_relaxed);
        r.trace.t0c_us = lat_now_us();
        r.trace.t1_us = lat_now_us();
        const bool has_http_response = resp.http_status != 0;
        if (has_http_response) {
            emit_wire_submit_trace(r.req, built.wire);
        }

        XtLogger::info_m("BinanceLat",
                         "kind=t1 local_id={} ts_us={} http={} curl_err='{}'",
                         r.req.local_id, r.trace.t1_us,
                         resp.http_status,
                         resp.curl_err);

        if (!resp.ok()) {
            const int err_code = parse_rest_error_code(resp.body);
            const std::string reject_reason =
                resp.curl_err.empty() ? resp.body : resp.curl_err;

            // BIN-D14：5xx / 网络层失败 / -1007 下委托可能已挂出或成交，终态化会让
            // 本地销账而单仍 live 在 venue（漏成交 / 幽灵仓）。保留在途，派发
            // kTransportError_TimeoutAfterSend（needs_reconcile），由 reconcile /
            // openOrders / userTrades 补查真实终态；ledger 对该 kind 保持 inflight。
            if (classify_rest_submit_failure(resp.http_status, err_code) ==
                RestSubmitFailureClass::kExecutionUnknown) {
                note_submit_exec_unknown(r.req.local_id, resp.http_status, err_code,
                                         reject_reason);
                emit_wire_reject_trace(r.req, "RestSubmitExecUnknown",
                                       reject_reason.c_str());
                // R2-P1-1：execution-unknown 双义——收到真实 HTTP 响应（5xx/-1007，
                // http!=0）是 venue 响应可采端点；http==0 是本地 transport 失败不可采。
                dispatch(make_submit_transport_error(
                             r.req.local_id,
                             TransportAck::Kind::kTransportError_TimeoutAfterSend,
                             has_http_response ? TransportAck::TimingOrigin::kVenueResponse
                                               : TransportAck::TimingOrigin::kLocalFailure,
                             reject_reason),
                         r.trace);
                return;
            }

            // 明确业务拒绝（4xx / 限流）：订单未挂出，按 kRejected 终态化。
            // 此分支 http_status 必非 0（http==0 已归 execution-unknown），故必有 HTTP 响应。
            XtLogger::warn_m("BinanceTD",
                             "submit REST rejected: http={} code={} body={} curl_err={}",
                             resp.http_status, err_code, resp.body, resp.curl_err);
            emit_wire_reject_trace(r.req, "RestSubmitFailed", reject_reason.c_str());
            dispatch(make_submit_reject(r.req.local_id, resp.body, 0), r.trace);
            return;
        }

        auto j = nlohmann::json::parse(resp.body, nullptr, false);
        if (j.is_discarded()) {
            XtLogger::warn_m("BinanceTD", "submit response invalid json: {}", resp.body);
            emit_wire_reject_trace(r.req, "RestInvalidJson", resp.body.c_str());
            dispatch(make_submit_reject(r.req.local_id, resp.body, 0), r.trace);
            return;
        }

        TransportAck ack;
        ack.kind = TransportAck::Kind::kAccepted;
        ack.timing_origin = TransportAck::TimingOrigin::kVenueResponse;  // REST 2xx = venue 响应
        ack.local_id = r.req.local_id;
        ack.raw_status = j.value("status", std::string("NEW"));
        if (j.contains("orderId")) ack.sys_id = std::to_string(j.value("orderId", 0LL));
        if (j.contains("executedQty")) {
            try {
                const double eq = std::stod(j["executedQty"].get<std::string>());
                // P15：stod("nan") 不抛；非有限不进 fp（保持 filled_fp=0，同字段缺失语义）。
                if (std::isfinite(eq)) ack.filled_fp = order_exec_to_fp(eq);
            } catch (...) {}
        }
        dispatch(ack, r.trace);
    }

    void execute_cancel(ReqCancel& r) {
        std::map<std::string, std::string> params;
        params["symbol"] = r.req.symbol;
        params["origClientOrderId"] = r.req.client_order_id;
        r.trace.t0b_us = lat_now_us();
        inflight_count.fetch_add(1, std::memory_order_relaxed);
        auto resp = rest.delete_signed("/fapi/v1/order", std::move(params));
        inflight_count.fetch_sub(1, std::memory_order_relaxed);
        r.trace.t0c_us = lat_now_us();
        r.trace.t1_us = lat_now_us();

        if (!resp.ok()) {
            auto j = nlohmann::json::parse(resp.body, nullptr, false);
            const int code = (!j.is_discarded() && j.contains("code"))
                                  ? j["code"].get<int>() : 0;
            if (code == -2011) {
                // -2011 "Unknown order" 在本地撤单语义中等价于目标委托已不可撤；
                // 上层订单状态机按 cancel noop 收敛，避免误报业务拒绝。
                dispatch(make_cancel_ack(r.req.local_id, TransportAck::Kind::kCancelNoop,
                                         TransportAck::TimingOrigin::kVenueResponse),
                         r.trace);
                return;
            }
            if (rest_cancel_error_retryable(resp.http_status, code)) {
                schedule_cancel_retry(std::move(r), resp.http_status, code,
                                      resp.retry_after_sec);
                return;
            }
            XtLogger::warn_m("BinanceTD",
                             "cancel REST failed: http={} body={} curl_err={}",
                             resp.http_status, resp.body, resp.curl_err);
            return;
        }
        dispatch(make_cancel_ack(r.req.local_id, TransportAck::Kind::kCanceled,
                                 TransportAck::TimingOrigin::kVenueResponse), r.trace);
    }

    /// @brief 限频类 cancel 失败的有界重试调度；超出 max_tries 弃单并升级 error。
    /// @note 弃重试后委托可能仍在交易所挂着，占用 per-symbol max-open-order 限额
    ///       （-2025 根因），收敛依赖 reconcile / UDS 终态推送。
    void schedule_cancel_retry(ReqCancel&& r, long http_status, int err_code,
                               long retry_after_sec) {
        r.tries_done += 1;
        if (r.tries_done >= retry_cfg.max_tries) {
            XtLogger::error_m("BinanceTD",
                              "cancel abandoned after {} tries: local_id={} symbol={} "
                              "coid={} http={} code={}",
                              r.tries_done, r.req.local_id, r.req.symbol,
                              r.req.client_order_id, http_status, err_code);
            return;
        }
        const uint64_t backoff_ms =
            rest_cancel_backoff_ms(r.tries_done, retry_after_sec, retry_cfg);
        XtLogger::warn_m("BinanceTD",
                         "cancel rate-limited (http={} code={}), retry {}/{} in {}ms: local_id={}",
                         http_status, err_code, r.tries_done + 1, retry_cfg.max_tries,
                         backoff_ms, r.req.local_id);
        const uint64_t due_us = lat_now_us() + backoff_ms * 1000;
        std::lock_guard<std::mutex> lk(queue_mu);
        const auto it = std::upper_bound(
            retry_queue.begin(), retry_queue.end(), due_us,
            [](uint64_t due, const DelayedCancel& e) { return due < e.due_us; });
        retry_queue.insert(it, DelayedCancel{std::move(r), due_us});
    }

    static TransportAck make_submit_reject(uint64_t local_id,
                                           std::string err_msg,
                                           int err_code) {
        TransportAck ack;
        ack.kind = TransportAck::Kind::kRejected;
        // 业务拒绝 / 2xx 坏 JSON 均在 http!=0 分支构造 → venue 响应，可采端点。
        ack.timing_origin = TransportAck::TimingOrigin::kVenueResponse;
        ack.local_id = local_id;
        ack.err_msg = std::move(err_msg);
        ack.err_code = err_code;
        return ack;
    }

    static TransportAck make_submit_transport_error(uint64_t local_id,
                                                    TransportAck::Kind kind,
                                                    TransportAck::TimingOrigin origin,
                                                    std::string err_msg) {
        TransportAck ack;
        ack.kind = kind;
        ack.req_kind = TransportAck::ReqKind::kSubmit;
        ack.timing_origin = origin;
        ack.local_id = local_id;
        ack.err_msg = std::move(err_msg);
        return ack;
    }

    static TransportAck make_cancel_ack(uint64_t local_id, TransportAck::Kind kind,
                                        TransportAck::TimingOrigin origin) {
        TransportAck ack;
        ack.kind = kind;
        ack.req_kind = TransportAck::ReqKind::kCancel;
        ack.timing_origin = origin;
        ack.local_id = local_id;
        return ack;
    }

    void dispatch(const TransportAck& ack, const TransportLatencyTrace& trace) {
        last_ack_us.store(trace.t1_us, std::memory_order_relaxed);
        if (ack_handler) ack_handler(ack, trace);
    }

    void reject_submit_before_wire(ReqSubmit& r,
                                   const char* reject_code,
                                   const char* reason,
                                   TransportAck::Kind kind) {
        emit_wire_reject_trace(r.req, reject_code, reason);
        r.trace.t1_us = lat_now_us();
        // pre-wire 拒绝（stop-before-send）从未触达 venue → 本地失败，不可采。
        dispatch(make_submit_transport_error(r.req.local_id, kind,
                                             TransportAck::TimingOrigin::kLocalFailure, reason),
                 r.trace);
    }

    /// @brief 记录一次 submit execution-unknown：累计 counter + 限频 warn。
    /// @note 仅 worker 线程调用；last_exec_unknown_log_us_ 为该线程私有。
    void note_submit_exec_unknown(uint64_t local_id, long http_status, int err_code,
                                  const std::string& reason) {
        const uint64_t total =
            submit_exec_unknown_count_.fetch_add(1, std::memory_order_relaxed) + 1;
        const uint64_t now_us = lat_now_us();
        if (last_exec_unknown_log_us_ == 0 ||
            now_us - last_exec_unknown_log_us_ >= kExecUnknownLogIntervalUs) {
            last_exec_unknown_log_us_ = now_us;
            XtLogger::warn_m("BinanceTD",
                "submit execution-unknown (kept inflight for reconcile): local_id={} "
                "http={} code={} reason='{}' total={} suppressed_since_last={}",
                local_id, http_status, err_code, reason, total,
                exec_unknown_suppressed_count_);
            exec_unknown_suppressed_count_ = 0;
        } else {
            ++exec_unknown_suppressed_count_;
        }
    }

public:
    uint64_t submit_exec_unknown_count() const {
        return submit_exec_unknown_count_.load(std::memory_order_relaxed);
    }

private:
    // ── 外部依赖（不拥有）────────────────────────────────────────────
    BinanceRestClient& rest;
    AckHandler ack_handler;

    // ── cancel 限频重试配置（构造期注入，只读）──────────────────────
    const RestCancelRetryCfg retry_cfg;

    // ── worker 生命周期 ─────────────────────────────────────────────
    std::atomic<bool> running{false};
    std::thread worker_thread;

    // ── 请求队列（queue_mu 保护）────────────────────────────────────
    mutable std::mutex queue_mu;
    std::condition_variable queue_cv;
    std::deque<Req> queue;
    std::deque<DelayedCancel> retry_queue;   ///< 按 due_us 升序；worker 到点取出重发。

    // ── 健康指标（跨线程原子观测）────────────────────────────────────
    std::atomic<uint32_t> inflight_count{0};
    std::atomic<uint64_t> last_ack_us{0};

    // ── execution-unknown 观测（BIN-D14）──────────────────────────────
    std::atomic<uint64_t> submit_exec_unknown_count_{0};  ///< 累计 exec-unknown 事件数（跨线程读）。
    uint64_t last_exec_unknown_log_us_ = 0;               ///< worker-only：上次限频 warn 时刻。
    uint64_t exec_unknown_suppressed_count_ = 0;          ///< worker-only：窗口内被抑制的日志条数。
};

RestTradeTransport::RestTradeTransport(BinanceRestClient& rest,
                                       RestCancelRetryCfg cancel_retry)
    : impl_(std::make_unique<Impl>(rest, cancel_retry)) {}

RestTradeTransport::~RestTradeTransport() = default;

bool RestTradeTransport::start() { return impl_->start(); }
void RestTradeTransport::stop() { impl_->stop(); }

void RestTradeTransport::submit(TransportSubmitReq&& req) {
    impl_->submit(std::move(req));
}

void RestTradeTransport::cancel(TransportCancelReq&& req) {
    impl_->cancel(std::move(req));
}

void RestTradeTransport::bind_ack_handler(AckHandler handler) {
    impl_->bind_ack_handler(std::move(handler));
}

ITradeTransport::Health RestTradeTransport::health() const {
    return impl_->health();
}

bool RestTradeTransport::is_running() const {
    return impl_->is_running();
}

uint64_t RestTradeTransport::submit_exec_unknown_count() const {
    return impl_->submit_exec_unknown_count();
}

}  // namespace binance_u
