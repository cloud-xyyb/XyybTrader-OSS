// CTP ready 后周期刷新子系统：ready 后一次性刷新 + 周期账户/持仓刷新。
// 不直接重建 api_；stuck-disconnect self-heal 由 TraderCTP.cpp::check_health() 执行。
// refresh 查询只在 SessionPhase::Ready 后通过 can_run_refresh_query() 放行，recovery 查询不走该 gate。
// wait_cv_ 用于可中断 sleep，stop() 唤醒周期刷新循环以缩短 shutdown latency。

#include "ctp/td/TraderCTP.h"

#include "ctp/td/CtpRefreshTimeout.h"
#include "XtLogger.h"
#include "xt_util/runtime/ThreadUtil.h"

#include <chrono>
#include <thread>

// ── ready 后周期刷新 ──────────────────────────────

void TraderCTP::refresh_once_after_ready() {
    if (account_refresh_ms_ <= 0 && position_refresh_ms_ <= 0) return;
    bool expected = false;
    if (!refresh_once_running_.compare_exchange_strong(expected, true,
                                                       std::memory_order_acq_rel)) {
        return;
    }
    ThreadUtil::join_or_detach(refresh_once_thread_);
    refresh_once_thread_ = std::thread([this]() {
        struct ResetFlag {
            std::atomic<bool>* flag = nullptr;
            ~ResetFlag() { if (flag) flag->store(false, std::memory_order_release); }
        } reset{&refresh_once_running_};

        if (!can_run_refresh_query()) return;
        if (account_refresh_ms_ > 0) query_once(QueryKind::AccountState);
        if (position_refresh_ms_ > 0) {
            // 200ms 间隔避免两个查询背靠背触发 CTP 流控
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            if (can_run_refresh_query()) query_once(QueryKind::Position);
        }
    });
}

void TraderCTP::start_refresh_loop() {
    const int acc_ms = account_refresh_ms_;
    const int pos_ms = position_refresh_ms_;
    if (acc_ms <= 0 && pos_ms <= 0) return;
    if (refresh_thread_.joinable()) {
        if (refresh_running_.load(std::memory_order_acquire)) return;
        refresh_thread_.join();
    }
    bool expected = false;
    if (!refresh_running_.compare_exchange_strong(expected, true)) return;
    refresh_thread_ = std::thread([this, acc_ms, pos_ms]() {
        using clock = std::chrono::steady_clock;
        const auto start = clock::now();
        auto next_acc = acc_ms > 0 ? start + std::chrono::milliseconds(acc_ms)
                                   : clock::time_point::max();
        auto next_pos = pos_ms > 0 ? start + std::chrono::milliseconds(pos_ms)
                                   : clock::time_point::max();
        while (!stopping_.load(std::memory_order_acquire)) {
            // 同轮到期时先资金后持仓，并插入 200ms 间隔降低柜台 burst
            auto next = next_acc < next_pos ? next_acc : next_pos;
            if (next == clock::time_point::max()) break;
            auto now = clock::now();
            if (now < next) {
                // 可中断等待到下一个到期点；stop() notify 后即时唤醒，避免 join 阻塞整段 interval。
                std::unique_lock<std::mutex> wl(wait_mu_);
                wait_cv_.wait_until(wl, next, [this]() {
                    return stopping_.load(std::memory_order_acquire);
                });
            }
            if (stopping_.load(std::memory_order_acquire)) break;
            if (!can_run_refresh_query()) {
                std::unique_lock<std::mutex> wl(wait_mu_);
                wait_cv_.wait_for(wl, std::chrono::milliseconds(200), [this]() {
                    return stopping_.load(std::memory_order_acquire);
                });
                continue;
            }
            now = clock::now();
            const bool due_acc = (acc_ms > 0 && now >= next_acc);
            const bool due_pos = (pos_ms > 0 && now >= next_pos);
            if (due_acc) {
                query_once(QueryKind::AccountState);
                next_acc = now + std::chrono::milliseconds(acc_ms);
            }
            if (due_pos) {
                if (due_acc) std::this_thread::sleep_for(std::chrono::milliseconds(200));
                query_once(QueryKind::Position);
                next_pos = now + std::chrono::milliseconds(pos_ms);
            }
        }
        refresh_running_.store(false, std::memory_order_release);
    });
}

// ── Ready 稳态 refresh 查询超时自愈 ──────────────────

void TraderCTP::arm_refresh_query_timeout(QueryKind kind) {
    if (refresh_query_timeout_ms_ <= 0) return;                        // 关闭自愈（回滚开关）
    if (recovery_.is_recovering()) return;                            // recovery 有独立超时守卫，不重复挂
    if (session_phase_.load(std::memory_order_acquire) != SessionPhase::Ready) return;

    auto&          q          = qry_[qry_idx(kind)];
    const uint64_t token      = q.refresh_timeout_token.fetch_add(1, std::memory_order_acq_rel) + 1;
    const int      timeout_ms = refresh_query_timeout_ms_;

    schedule_after_ms(timeout_ms, [this, kind, token, timeout_ms]() {
        if (!is_session_alive()) return;
        if (recovery_.is_recovering()) return;                        // 已回到 recovery，交给 recovery 守卫
        if (session_phase_.load(std::memory_order_acquire) != SessionPhase::Ready) return;
        auto& q = qry_[qry_idx(kind)];
        if (q.refresh_timeout_token.load(std::memory_order_acquire) != token) return;  // 已被新查询 supersede
        if (!q.inflight.load(std::memory_order_acquire)) return;                        // 已正常回包完成

        // 稳态 refresh 查询发出后超阈仍无 bIsLast 回包 → 视为该 kind 查询失败（丢包/柜台无回）。
        // query_once 的 inflight CAS 在 stuck 状态下恒失败 → 后续周期 refresh 全 no-op（永久 wedge）。
        // 解开 stuck inflight 后重发，并按连续超时轮数升级 refresh wedged 观测状态（page-then-continue）。
        const int consecutive =
            q.refresh_timeout_count.fetch_add(1, std::memory_order_acq_rel) + 1;
        refresh_query_timeout_total_.fetch_add(1, std::memory_order_acq_rel);
        const auto d = ctp_refresh::refresh_timeout_decision(consecutive, refresh_query_wedge_rounds_);
        const QueryMeta meta = query_meta(kind);
        // 每 kind 每 timeout_ms 至多触发一次 → 日志天然限频。
        if (d.raise_wedge) {
            XtLogger::error_m("TD",
                "event=refresh_query_wedged stage={} timeout_ms={} consecutive={}/{} re-issuing",
                meta.name, timeout_ms, consecutive, refresh_query_wedge_rounds_);
        } else {
            XtLogger::warn_m("TD",
                "event=refresh_query_timeout stage={} timeout_ms={} consecutive={} re-issuing",
                meta.name, timeout_ms, consecutive);
        }
        if (!d.reissue) return;
        // 解 stuck inflight（qry_ 全 atomic，从 timer 线程触碰安全；不碰 state_）。
        // query_once 重发若 ret==0 会再挂一次 refresh timeout；若被 throttle 跳过则
        // 走 retry worker 兜底，且 inflight 已被 query_once 释放，不再永久 wedge。
        q.inflight.store(false, std::memory_order_release);
        q.dirty.store(false, std::memory_order_release);
        q.next_allowed_ms.store(0, std::memory_order_release);
        q.backoff_level.store(0, std::memory_order_release);
        query_once(kind);
    });
}
