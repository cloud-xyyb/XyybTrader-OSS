#pragma once

#include <atomic>
#include <chrono>
#include <thread>

namespace xt_util {

/// @brief 支持 shutdown 提前返回的 sleep helper。
///
/// XtShutdownSignal 在等待期间按短间隔检查调用方持有的 shutdown flag；flag 变为
/// true 时提前返回，timeout 到期时返回 false。
///
class XtShutdownSignal {
public:

    template <typename Rep1, typename Period1, typename Rep2, typename Period2>
    static bool wait_for(const std::atomic<bool>& flag,
                         std::chrono::duration<Rep1, Period1> total,
                         std::chrono::duration<Rep2, Period2> poll) {
        const auto start = std::chrono::steady_clock::now();
        const auto deadline = start + total;
        while (true) {
            if (flag.load(std::memory_order_acquire)) return true;
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) return false;
            const auto remaining = deadline - now;
            const auto step = (remaining < poll) ? remaining : poll;
            std::this_thread::sleep_for(step);
        }
    }

    template <typename Rep, typename Period>
    static bool wait_for(const std::atomic<bool>& flag,
                         std::chrono::duration<Rep, Period> total) {
        return wait_for(flag, total, std::chrono::milliseconds(50));
    }
};

}
