#pragma once

#include <array>
#include <atomic>
#include <cstdint>

namespace xt_live {

/// @brief 事件循环队列滞留延迟直方图（log2 分桶，窗口分位）。
///
/// 写端为事件循环 worker 线程（每事件一次 relaxed fetch_add）；读端为 1Hz
/// control loop `drain()`（逐桶 exchange 清零并计算窗口 p50/p99）。分位取桶
/// 上界，报告值 >= 真分位（保守）。
///
/// @note 与 record 并发安全；窗口边界上的样本落本窗或下窗，1Hz 观测口径可接受。
class XtLagHistogram {
public:
    static constexpr size_t kBuckets = 64;

    struct Stats {
        uint64_t count  = 0;
        uint64_t p50_us = 0;
        uint64_t p99_us = 0;
    };

    void record(uint64_t lag_us) noexcept {
        buckets_[bucket_index(lag_us)].fetch_add(1, std::memory_order_relaxed);
    }

    Stats drain() noexcept {
        std::array<uint64_t, kBuckets> snap{};
        uint64_t total = 0;
        for (size_t i = 0; i < kBuckets; ++i) {
            snap[i] = buckets_[i].exchange(0, std::memory_order_relaxed);
            total += snap[i];
        }
        Stats s;
        s.count = total;
        if (total == 0) return s;
        s.p50_us = percentile_from_counts(snap.data(), kBuckets, total, 50);
        s.p99_us = percentile_from_counts(snap.data(), kBuckets, total, 99);
        return s;
    }

    static constexpr size_t bucket_index(uint64_t lag_us) noexcept {
        if (lag_us == 0) return 0;
        const size_t idx = static_cast<size_t>(64 - __builtin_clzll(lag_us));
        return idx < kBuckets ? idx : kBuckets - 1;
    }

    static constexpr uint64_t bucket_upper_us(size_t i) noexcept {
        return i >= kBuckets - 1 ? (1ULL << (kBuckets - 1)) : (1ULL << i);
    }

    static uint64_t percentile_from_counts(const uint64_t* counts, size_t n,
                                           uint64_t total, unsigned pct) noexcept {
        if (total == 0 || n == 0) return 0;
        const uint64_t rank = (total * pct + 99) / 100;
        uint64_t cum = 0;
        for (size_t i = 0; i < n; ++i) {
            cum += counts[i];
            if (cum >= rank) return bucket_upper_us(i);
        }
        return bucket_upper_us(n - 1);
    }

private:
    std::array<std::atomic<uint64_t>, kBuckets> buckets_{};
};

}
