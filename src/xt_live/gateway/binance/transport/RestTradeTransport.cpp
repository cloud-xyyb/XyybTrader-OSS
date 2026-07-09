#include "RestTradeTransport.h"

#include "BinanceIntentTrace.h"
#include "BinanceTdWireUtil.h"
#include "BinanceOrderParams.h"
#include "rest/BinanceRestClient.h"
#include "refdata/BinanceExchangeInfo.h"

#include <algorithm>
#include <atomic>
#include <cmath>
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

struct ReqSubmit {
    TransportSubmitReq req;
    TransportLatencyTrace trace;
};

struct ReqCancel {
    TransportCancelReq req;
    TransportLatencyTrace trace;
    int tries_done = 0;
};

/// @brief 等待退避到期的 cancel 重试项；due_us 为 steady µs。
struct DelayedCancel {
    ReqCancel item;
    uint64_t  due_us = 0;
};

using Req = std::variant<ReqSubmit, ReqCancel>;

}

bool rest_cancel_error_retryable(long http_status, int err_code) {
    if (http_status == 429 || http_status == 418) return true;
    return err_code == -1015 || err_code == -1003;
}

uint64_t rest_cancel_backoff_ms(int tries_done, long retry_after_sec,
                                const RestCancelRetryCfg& cfg) {
    if (retry_after_sec > 0) {

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

private:
    void worker_loop() {

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
        dispatch(make_cancel_ack(r.req.local_id, TransportAck::Kind::kTransportError_Disconnect),
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
            XtLogger::warn_m("BinanceTD",
                             "submit REST failed: http={} body={} curl_err={}",
                             resp.http_status, resp.body, resp.curl_err);
            const std::string reject_reason =
                resp.curl_err.empty() ? resp.body : resp.curl_err;
            const char* reject_code =
                has_http_response ? "RestSubmitFailed" : "RestTransportError";
            emit_wire_reject_trace(r.req, reject_code, reject_reason.c_str());
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
        ack.local_id = r.req.local_id;
        ack.raw_status = j.value("status", std::string("NEW"));
        if (j.contains("orderId")) ack.sys_id = std::to_string(j.value("orderId", 0LL));
        if (j.contains("executedQty")) {
            try {
                const double eq = std::stod(j["executedQty"].get<std::string>());

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

                dispatch(make_cancel_ack(r.req.local_id, TransportAck::Kind::kCancelNoop),
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
        dispatch(make_cancel_ack(r.req.local_id, TransportAck::Kind::kCanceled), r.trace);
    }

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
        ack.local_id = local_id;
        ack.err_msg = std::move(err_msg);
        ack.err_code = err_code;
        return ack;
    }

    static TransportAck make_submit_transport_error(uint64_t local_id,
                                                    TransportAck::Kind kind,
                                                    std::string err_msg) {
        TransportAck ack;
        ack.kind = kind;
        ack.req_kind = TransportAck::ReqKind::kSubmit;
        ack.local_id = local_id;
        ack.err_msg = std::move(err_msg);
        return ack;
    }

    static TransportAck make_cancel_ack(uint64_t local_id, TransportAck::Kind kind) {
        TransportAck ack;
        ack.kind = kind;
        ack.req_kind = TransportAck::ReqKind::kCancel;
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
        dispatch(make_submit_transport_error(r.req.local_id, kind, reason), r.trace);
    }

    BinanceRestClient& rest;
    AckHandler ack_handler;

    const RestCancelRetryCfg retry_cfg;

    std::atomic<bool> running{false};
    std::thread worker_thread;

    mutable std::mutex queue_mu;
    std::condition_variable queue_cv;
    std::deque<Req> queue;
    std::deque<DelayedCancel> retry_queue;

    std::atomic<uint32_t> inflight_count{0};
    std::atomic<uint64_t> last_ack_us{0};
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

}
