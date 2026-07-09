// CTP ready 后周期刷新子系统：ready 后一次性刷新 + 周期账户/持仓刷新。
// 不直接重建 api_；stuck-disconnect self-heal 由 TraderCTP.cpp::check_health() 执行。
// refresh 查询只在 SessionPhase::Ready 后通过 can_run_refresh_query() 放行，recovery 查询不走该 gate。
// wait_cv_ 用于可中断 sleep，stop() 唤醒周期刷新循环以缩短 shutdown latency。

#include "ctp/td/TraderCTP.h"

#include "XtLogger.h"
#include "xt_util/runtime/ThreadUtil.h"

#include <chrono>
#include <thread>

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

            auto next = next_acc < next_pos ? next_acc : next_pos;
            if (next == clock::time_point::max()) break;
            auto now = clock::now();
            if (now < next) {

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
