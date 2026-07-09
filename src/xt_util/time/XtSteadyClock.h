#pragma once

/// @file XtSteadyClock.h
/// @brief 单调时钟 helper
///
/// 仅用于超时、延迟和节流等相对时间计算
/// 底层为 `std::chrono::steady_clock`；返回值是任意单调计数，只能相减使用。

#include <chrono>
#include <cstdint>

namespace XtSteadyClock {

inline uint64_t steady_now_ns() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

inline uint64_t steady_now_us() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

inline int64_t steady_now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

}
