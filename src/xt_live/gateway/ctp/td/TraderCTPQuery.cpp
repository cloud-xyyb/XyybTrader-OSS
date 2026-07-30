// CTP query path：统一发起 ReqQry*，维护 per-kind inflight/dirty 与跨 kind flow-control。
// recovery query、ready 后 refresh query 和 timeout retry 都通过 query_once() 进入本分片。
// request_query() 在 api_mu_ shared 下发送，避免 stop()/check_health() Release/rebuild 并发释放 api_。
// retry worker 串行扫描 pending，防止多个 QueryKind 同轮打满 CTP session 级查询限流。

#include "ctp/td/TraderCTP.h"

#include "XtLogger.h"
#include "common/XtLiveRecoveryError.h"
#include "xt_util/time/XtSteadyClock.h"
#include "xt_util/string/XtCStringUtil.h"
#include "xt_util/runtime/ThreadUtil.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <thread>

namespace {

const char* rc(XtLiveRecoveryError code) { return live_recovery_error_str(code); }

}

int TraderCTP::request_query(QueryKind kind) {

    std::shared_lock<std::shared_mutex> api_lk(api_mu_);
    if (!api_) return -1;
    auto prepare_req = [this](auto& req) {
        std::memset(&req, 0, sizeof(req));
        XtCStringUtil::copy(req.BrokerID, broker_.c_str());
        XtCStringUtil::copy(req.InvestorID, user_.c_str());
    };

    const int rid = next_req_id();
    qry_[qry_idx(kind)].active_request_id.store(rid, std::memory_order_release);
    switch (kind) {
        case QueryKind::Position: {
            CThostFtdcQryInvestorPositionField req;
            prepare_req(req);

            recovery_.begin_position_query(rid);
            return api_->ReqQryInvestorPosition(&req, rid);
        }
        case QueryKind::AccountState: {
            CThostFtdcQryTradingAccountField req;
            prepare_req(req);
            return api_->ReqQryTradingAccount(&req, rid);
        }
        case QueryKind::XtOrder: {
            CThostFtdcQryOrderField req;
            prepare_req(req);
            return api_->ReqQryOrder(&req, rid);
        }
        case QueryKind::Trade: {
            CThostFtdcQryTradeField req;
            prepare_req(req);
            return api_->ReqQryTrade(&req, rid);
        }
    }
    return -1;
}

TraderCTP::QueryMeta TraderCTP::query_meta(QueryKind kind) {
    switch (kind) {
        case QueryKind::Position:     return {"position", "ReqQryInvestorPosition"};
        case QueryKind::AccountState: return {"account",  "ReqQryTradingAccount"};
        case QueryKind::XtOrder:      return {"order",    "ReqQryOrder"};
        case QueryKind::Trade:        return {"trade",    "ReqQryTrade"};
    }
    return {};
}

bool TraderCTP::can_run_refresh_query() const {

    if (stopping_.load(std::memory_order_acquire)) return false;
    if (!connected_.load(std::memory_order_acquire) || !api_) return false;
    if (recovery_.is_recovering()) return false;
    return session_phase_.load(std::memory_order_acquire) == SessionPhase::Ready;
}

void TraderCTP::clear_query_retry_states() {
    for (auto& q : qry_) {
        q.retry_pending.store(false, std::memory_order_release);
    }
}

void TraderCTP::reset_query_control_states() {
    for (auto& q : qry_) {
        q.inflight.store(false, std::memory_order_release);
        q.dirty.store(false, std::memory_order_release);

        q.active_request_id.store(0, std::memory_order_release);
        q.next_allowed_ms.store(0, std::memory_order_release);
        q.backoff_level.store(0, std::memory_order_release);
        q.throttled_count.store(0, std::memory_order_release);
        q.inflight_skip_count.store(0, std::memory_order_release);
        q.dirty_merge_count.store(0, std::memory_order_release);

        q.refresh_timeout_token.fetch_add(1, std::memory_order_acq_rel);
        q.refresh_timeout_count.store(0, std::memory_order_release);
    }
}

void TraderCTP::on_query_response_done(QueryKind kind, bool success) {
    auto& q = qry_[qry_idx(kind)];
    q.inflight.store(false, std::memory_order_release);

    q.refresh_timeout_token.fetch_add(1, std::memory_order_acq_rel);
    q.refresh_timeout_count.store(0, std::memory_order_release);
    if (success) {
        q.backoff_level.store(0, std::memory_order_release);
        q.next_allowed_ms.store(0, std::memory_order_release);
    }
    if (q.dirty.exchange(false, std::memory_order_acq_rel)) {
        schedule_query_retry(kind);
    }
}

void TraderCTP::query_once(QueryKind kind) {

    if (!api_) return;
    const QueryMeta meta = query_meta(kind);
    auto& q = qry_[qry_idx(kind)];

    bool expected = false;
    if (!q.inflight.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        q.inflight_skip_count.fetch_add(1, std::memory_order_relaxed);
        if (!q.dirty.exchange(true, std::memory_order_acq_rel)) {
            q.dirty_merge_count.fetch_add(1, std::memory_order_relaxed);
        }

        schedule_query_retry(kind);
        if (!q.inflight.load(std::memory_order_acquire)) {
            schedule_query_retry(kind);
        }
        return;
    }

    const int64_t now_ms = XtSteadyClock::steady_now_ms();
    if (q.next_allowed_ms.load(std::memory_order_acquire) > now_ms) {
        q.inflight.store(false, std::memory_order_release);
        schedule_query_retry(kind);
        return;
    }

    constexpr int64_t kGlobalQueryGapMs = 1100;
    if (next_global_query_allowed_ms_.load(std::memory_order_acquire) > now_ms) {
        q.inflight.store(false, std::memory_order_release);
        schedule_query_retry(kind);
        return;
    }
    q.dirty.store(false, std::memory_order_release);

    const int ret = request_query(kind);

    next_global_query_allowed_ms_.store(now_ms + kGlobalQueryGapMs, std::memory_order_release);

    if (ret == 0) {

        if (recovery_.is_recovering())
            XtLogger::info_m("TD", "query {} start", meta.name);
        else
            XtLogger::debug_m("TD", "query {} start", meta.name);
        q.backoff_level.store(0, std::memory_order_release);
        q.next_allowed_ms.store(0, std::memory_order_release);

        arm_recovery_query_timeout(kind);
        arm_refresh_query_timeout(kind);
        return;
    }

    q.inflight.store(false, std::memory_order_release);
    XtLogger::error_m("TD", "{} failed: {} reason_code={}",
                      meta.req_name, ret, rc(XtLiveRecoveryError::TdQueryFailed));

    if (ret == -2) {
        handle_query_throttled(kind, q, meta, now_ms);
        return;
    }

    if (recovery_.is_recovering()) {
        arm_recovery_query_timeout(kind);
    }
}

void TraderCTP::handle_query_throttled(QueryKind kind,
                                       QueryCtrl& q,
                                       const QueryMeta& meta,
                                       int64_t now_ms) {

    constexpr int kBackoffBaseMs   = 300;
    constexpr int kBackoffMaxMs    = 5000;
    constexpr int kBackoffMaxShift = 4;
    constexpr int kBackoffJitterMs = 120;

    q.throttled_count.fetch_add(1, std::memory_order_relaxed);
    const int level      = q.backoff_level.fetch_add(1, std::memory_order_acq_rel) + 1;
    const int shift      = std::max(0, std::min(level - 1, kBackoffMaxShift));
    const int backoff_ms = std::min(kBackoffBaseMs << shift, kBackoffMaxMs);
    const int jitter_ms  = static_cast<int>(
        XtSteadyClock::steady_now_us() % static_cast<uint64_t>(kBackoffJitterMs + 1));
    q.next_allowed_ms.store(now_ms + backoff_ms + jitter_ms, std::memory_order_release);

    XtLogger::warn_m("TD",
                     "query {} limited(-2), backoff={}ms jitter={}ms reason_code={}",
                     meta.name, backoff_ms, jitter_ms,
                     rc(XtLiveRecoveryError::TdQueryThrottled));
    schedule_query_retry(kind);
}

void TraderCTP::schedule_query_retry(QueryKind kind) {
    const QueryMeta meta = query_meta(kind);
    auto& q = qry_[qry_idx(kind)];
    if (!q.retry_pending.exchange(true, std::memory_order_acq_rel)) {

        if (recovery_.is_recovering())
            XtLogger::info_m("TD", "query {} retry scheduled", meta.name);
        else
            XtLogger::debug_m("TD", "query {} retry scheduled", meta.name);
    }
    run_query_retry_worker();
}

bool TraderCTP::process_query_retry(QueryKind kind) {
    auto& q = qry_[qry_idx(kind)];
    if (!q.retry_pending.load(std::memory_order_acquire)) return false;
    if (q.inflight.load(std::memory_order_acquire)) return true;
    const int64_t now_ms = XtSteadyClock::steady_now_ms();
    if (q.next_allowed_ms.load(std::memory_order_acquire) > now_ms) return true;
    q.retry_pending.store(false, std::memory_order_release);
    query_once(kind);
    return q.retry_pending.load(std::memory_order_acquire);
}

void TraderCTP::run_query_retry_worker() {
    bool expected = false;
    if (!qry_retry_worker_running_.compare_exchange_strong(expected, true,
                                                           std::memory_order_acq_rel)) {
        return;
    }
    ThreadUtil::join_or_detach(qry_retry_thread_);
    qry_retry_thread_ = std::thread([this]() {
        constexpr QueryKind kProcessOrder[] = {
            QueryKind::Position, QueryKind::AccountState,
            QueryKind::XtOrder,  QueryKind::Trade
        };
        while (!stopping_.load(std::memory_order_acquire)) {

            bool has_pending = false;
            for (const QueryKind kind : kProcessOrder) {
                if (qry_[qry_idx(kind)].retry_pending.load(std::memory_order_acquire)) {
                    has_pending = true;
                    break;
                }
            }
            if (!has_pending) break;

            if (!connected_.load(std::memory_order_acquire) || !api_) {

                std::unique_lock<std::mutex> wl(wait_mu_);
                wait_cv_.wait_for(wl, std::chrono::milliseconds(100),
                                  [this]() { return stopping_.load(std::memory_order_acquire); });
                continue;
            }
            for (const QueryKind kind : kProcessOrder) {
                if (qry_[qry_idx(kind)].retry_pending.load(std::memory_order_acquire)) {
                    (void)process_query_retry(kind);
                }
            }
            std::unique_lock<std::mutex> wl(wait_mu_);
            wait_cv_.wait_for(wl, std::chrono::milliseconds(100),
                              [this]() { return stopping_.load(std::memory_order_acquire); });
        }
        qry_retry_worker_running_.store(false, std::memory_order_release);
    });
}
