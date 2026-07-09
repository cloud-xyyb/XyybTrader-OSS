#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <thread>
#include <utility>

namespace xt_live::gateway {

/// @brief DeferredTimerQueue是拥有自己 worker 线程的延时回调队列。
///
/// 需要 “过 N 毫秒再执行一个回调” 时把 action 丢进来，到点在 worker 线程上串行执行。
/// 它存在的意义是替代 detached 计时器线程：stop()/析构会 join worker，
/// 保证返回后没有任何 action 还在跑或将要跑，析构后不会再被回调。
///
/// 任意线程可并发 schedule()；不支持取消单个任务，stop() 整体丢弃未触发任务。
/// @warning 不得在 action 内调用本队列的 stop() 或触发其析构（worker 自 join 死锁）。
class DeferredTimerQueue {
public:
    using Clock = std::chrono::steady_clock;

    DeferredTimerQueue() = default;
    ~DeferredTimerQueue() { stop(); }

    DeferredTimerQueue(const DeferredTimerQueue&) = delete;
    DeferredTimerQueue& operator=(const DeferredTimerQueue&) = delete;

    void schedule(int delay_ms, std::function<void()> action) {
        if (!action) return;
        std::lock_guard<std::mutex> lk(mu_);
        if (stopping_) return;
        if (!worker_.joinable()) {
            worker_ = std::thread([this] { run_(); });
        }
        const auto delay = std::chrono::milliseconds(delay_ms > 0 ? delay_ms : 0);
        pending_.emplace(Clock::now() + delay, std::move(action));
        cv_.notify_all();
    }

    void stop() {
        std::thread worker;
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (stopping_) return;
            stopping_ = true;
            pending_.clear();
            cv_.notify_all();
            worker = std::move(worker_);
        }
        if (worker.joinable()) worker.join();
    }

private:
    void run_() {
        std::unique_lock<std::mutex> lk(mu_);
        while (!stopping_) {
            if (pending_.empty()) {
                cv_.wait(lk);
                continue;
            }
            const auto due = pending_.begin()->first;
            if (Clock::now() < due) {

                cv_.wait_until(lk, due);
                continue;
            }
            std::function<void()> action = std::move(pending_.begin()->second);
            pending_.erase(pending_.begin());
            lk.unlock();
            action();
            lk.lock();
        }
    }

    std::mutex                                              mu_;
    std::condition_variable                                 cv_;
    std::multimap<Clock::time_point, std::function<void()>> pending_;
    std::thread                                             worker_;
    bool                                                    stopping_ = false;
};

}
