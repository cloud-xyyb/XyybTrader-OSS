#pragma once

#include <atomic>
#include <cstdint>

/// @brief 固定时间窗口 lock-free 限流器。
///
/// 用来限制报单/撤单/日志这类操作在每个时间窗口内的通过次数
struct AtomicWindowLimiter {
    int64_t window_ms = 1000;
    std::atomic<uint64_t> state{0};

    inline bool allow(int limit, int64_t now_ms) {
        if (limit <= 0) return true;
        const int64_t w = window_ms > 0 ? window_ms : 1000;
        const uint32_t bucket = static_cast<uint32_t>(now_ms / w);
        const uint32_t cap = static_cast<uint32_t>(limit);
        for (;;) {
            uint64_t cur = state.load(std::memory_order_relaxed);
            const uint32_t cur_bucket = static_cast<uint32_t>(cur >> 32);
            const uint32_t cur_count = static_cast<uint32_t>(cur & 0xffffffffu);
            uint64_t next = 0;
            if (cur_bucket != bucket) {

                next = (static_cast<uint64_t>(bucket) << 32) | 1u;
            } else {
                if (cur_count >= cap) return false;
                next = (static_cast<uint64_t>(bucket) << 32) | static_cast<uint64_t>(cur_count + 1);
            }
            if (state.compare_exchange_weak(
                    cur,
                    next,
                    std::memory_order_acq_rel,
                    std::memory_order_relaxed)) {
                return true;
            }
        }
    }
};
