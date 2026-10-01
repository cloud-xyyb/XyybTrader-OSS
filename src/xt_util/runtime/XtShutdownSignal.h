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
    /// @brief 短间隔 polling sleep；flag 变为 true 时提前返回。
    ///
    /// @param flag       shutdown atomic flag (caller-owned, 通常是 g_shutdown)。
    /// @param total      最大睡眠时长。
    /// @param poll       检查间隔 (默认 50ms)。设小则 shutdown latency 短 + CPU 多;
    ///                   设大则相反。50ms 是 1000ms frame 的 1/20,够细。
    /// @return true 表示 shutdown 触发提前退出;false 表示 timeout 完整睡到。
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

    /// @brief 默认 poll=50ms 的便捷重载。
    template <typename Rep, typename Period>
    static bool wait_for(const std::atomic<bool>& flag,
                         std::chrono::duration<Rep, Period> total) {
        return wait_for(flag, total, std::chrono::milliseconds(50));
    }
};

}  // namespace xt_util
