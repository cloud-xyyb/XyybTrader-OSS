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

}  // anonymous namespace

// ── 查询发送 ──────────────────────────────────

int TraderCTP::request_query(QueryKind kind) {
    // 持 shared 锁到 ReqQry* 返回；stop()/check_health 的 Release/rebuild 必须取
    // unique 锁，避免查询线程解引用已释放或半重建的 api_。
    std::shared_lock<std::shared_mutex> api_lk(api_mu_);
    if (!api_) return -1;
    auto prepare_req = [this](auto& req) {
        std::memset(&req, 0, sizeof(req));
        XtCStringUtil::copy(req.BrokerID, broker_.c_str());
        XtCStringUtil::copy(req.InvestorID, user_.c_str());
    };
    // bind-before-send：本轮 request_id 先绑定该 kind 的 generation fence 归属，
    // 令四类回包按 nRequestID 过滤过期轮（超时 rearm 重发后旧代迟到行/bIsLast 丢弃）。
    // recovery pipeline 与 Ready 稳态 refresh 共用此发送入口，两阶段同受栅栏保护。
    const int rid = next_req_id();
    qry_[qry_idx(kind)].active_request_id.store(rid, std::memory_order_release);
    switch (kind) {
        case QueryKind::Position: {
            CThostFtdcQryInvestorPositionField req;
            prepare_req(req);
            // 持仓聚合缓存归属绑定 + 清残留半截行（accumulator 内层栅栏，独立于
            // TraderCTP 层 fence——开关关闭构型下仍防混轮叠加）。
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
    // ready 后刷新只做粗粒度 gate；发送路径必须在 request_query() 内重新确认 api_。
    // recovery 查询不走此 gate，否则登录后的恢复 pipeline 会被 Recovering 阶段挡住。
    if (stopping_.load(std::memory_order_acquire)) return false;
    if (!connected_.load(std::memory_order_acquire) || !api_) return false;
    if (recovery_.is_recovering()) return false;
    return session_phase_.load(std::memory_order_acquire) == SessionPhase::Ready;
}

// ── 查询重试与退避控制 ──────────────────────────

void TraderCTP::clear_query_retry_states() {
    for (auto& q : qry_) {
        q.retry_pending.store(false, std::memory_order_release);
    }
}

void TraderCTP::reset_query_control_states() {
    for (auto& q : qry_) {
        q.inflight.store(false, std::memory_order_release);
        q.dirty.store(false, std::memory_order_release);
        // 新会话解除 fence 绑定（0=未绑定 fail-open）：旧 session 回包随 front 断开
        // 不会再到达，首次发送重新绑定。
        q.active_request_id.store(0, std::memory_order_release);
        q.next_allowed_ms.store(0, std::memory_order_release);
        q.backoff_level.store(0, std::memory_order_release);
        q.throttled_count.store(0, std::memory_order_release);
        q.inflight_skip_count.store(0, std::memory_order_release);
        q.dirty_merge_count.store(0, std::memory_order_release);
        // 新会话不继承上轮 Ready 稳态 refresh 超时计数：bump token 失效在挂 timer + 清连续超时计数。
        q.refresh_timeout_token.fetch_add(1, std::memory_order_acq_rel);
        q.refresh_timeout_count.store(0, std::memory_order_release);
    }
}

void TraderCTP::on_query_response_done(QueryKind kind, bool success) {
    auto& q = qry_[qry_idx(kind)];
    q.inflight.store(false, std::memory_order_release);
    // 收到回包（无论成败）即证明该 kind 未 wedge：失效在挂的 refresh 超时 timer
    // （bump token）+ 清连续超时计数。recovery 阶段无 refresh timer，此处 no-op。
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
    // 快速过滤明显未初始化窗口；发送前仍由 request_query() 在 api_mu_ shared 下重读 api_。
    if (!api_) return;
    const QueryMeta meta = query_meta(kind);
    auto& q = qry_[qry_idx(kind)];

    // CTP 不接受同类查询并发；在途期间的新触发折叠为一次 dirty retry。
    bool expected = false;
    if (!q.inflight.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        q.inflight_skip_count.fetch_add(1, std::memory_order_relaxed);
        if (!q.dirty.exchange(true, std::memory_order_acq_rel)) {
            q.dirty_merge_count.fetch_add(1, std::memory_order_relaxed);
        }
        // CAS 失败后发布 dirty，再兜底调度 retry。dirty_merge_count 只统计
        // false->true 的补发窗口创建次数；重复触发仍计入 inflight_skip_count。
        // 若响应线程夹在 CAS 失败和 dirty exchange 之间完成，on_query_response_done
        // 可能已读过 dirty=false；下方 inflight=false 检测会再补一次。
        // retry_pending 保证重复调度安全。
        schedule_query_retry(kind);
        if (!q.inflight.load(std::memory_order_acquire)) {
            schedule_query_retry(kind);
        }
        return;
    }

    // per-kind 最早发送时间未到；本次不下发 ReqQry*。
    // 释放刚才 CAS 占用的 inflight，否则 retry worker 会被同类在途标记挡住。
    const int64_t now_ms = XtSteadyClock::steady_now_ms();
    if (q.next_allowed_ms.load(std::memory_order_acquire) > now_ms) {
        q.inflight.store(false, std::memory_order_release);
        schedule_query_retry(kind);
        return;
    }
    // CTP 对同一 TD session 的所有 ReqQry* 共用限流，不是每个 QueryKind 各有额度。
    // Position 刚发完后，Account/Order/Trade 也必须等待全局间隔，否则会被柜台 -2 拒绝。
    constexpr int64_t kGlobalQueryGapMs = 1100;  // ~1/s 限流，额外留 100ms 余量。
    if (next_global_query_allowed_ms_.load(std::memory_order_acquire) > now_ms) {
        q.inflight.store(false, std::memory_order_release);
        schedule_query_retry(kind);
        return;
    }
    q.dirty.store(false, std::memory_order_release);

    const int ret = request_query(kind);
    // 发起到 SDK 的 qry* 都按已消耗发送机会处理；即使 ret=-2，也推进全局窗口，
    // 避免 retry 立即二次打满柜台。
    next_global_query_allowed_ms_.store(now_ms + kGlobalQueryGapMs, std::memory_order_release);

    // ret=0 进入 SDK 异步回包生命周期；ret=-2 走指数退避；其它错误释放 inflight，
    // recovery 阶段再由 timeout/ready gate 兜底。
    if (ret == 0) {
        // 周期 refresh 查询稳态降噪：recovery/bootstrap 期保 INFO（进度可见），稳态 DEBUG。
        if (recovery_.is_recovering())
            XtLogger::info_m("TD", "query {} start", meta.name);
        else
            XtLogger::debug_m("TD", "query {} start", meta.name);
        q.backoff_level.store(0, std::memory_order_release);
        q.next_allowed_ms.store(0, std::memory_order_release);
        // recovery 阶段挂 recovery 超时守卫；Ready 稳态挂 refresh 超时自愈守卫。
        // 两者按 phase/recovering gate 互斥（各自内部 no-op 条件不重叠），恰挂一个。
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
    // -2 是柜台 flow-control 拒绝。这里只设置下一次 per-kind 允许发送时间；
    // 全局 ReqQry* 间隔已在 request_query() 返回后统一推进。
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
        // 稳态周期 refresh 在 global-query-gap deferral 下每 ~100ms re-arm（retry_pending
        // 每轮被消费再 arm）→ recovery/bootstrap 期保 INFO（进度可见），稳态降 DEBUG 防刷屏。
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
            // retry worker 单线程按固定顺序扫描 pending，避免多个 QueryKind 同轮并发
            // 占用 CTP 查询队列。
            bool has_pending = false;
            for (const QueryKind kind : kProcessOrder) {
                if (qry_[qry_idx(kind)].retry_pending.load(std::memory_order_acquire)) {
                    has_pending = true;
                    break;
                }
            }
            if (!has_pending) break;

            if (!connected_.load(std::memory_order_acquire) || !api_) {
                // 断线或 API rebuild 窗口内保留 retry_pending，等待下一轮检查。
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
