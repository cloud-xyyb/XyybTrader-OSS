#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

/// @file XtHistogramUtil.h
/// @brief 延迟采样共用的 log2 bucket 直方图与滚动窗口直方图。
///
/// 固定 log2 bucket + 线性插值分位数 + `max_us` 收紧尾 bucket，避免各延迟
/// 采样器重复维护分位数算法。

namespace xt_util {

/// @brief 固定 N 个 log2 桶的延迟直方图。
///
/// `bucket[0] = [0, 1]us`，`bucket[i] = (2^(i-1), 2^i]us`；最后一桶吸收
/// 所有更大的样本。
template <size_t NBuckets>
struct LatencyHistogram {
    static_assert(NBuckets >= 2, "LatencyHistogram 至少需要 2 个 bucket");
    static_assert(NBuckets <= 53,
                  "LatencyHistogram: NBuckets<=53 锁模板契约——保 hist_bucket_upper 的 "
                  "1<<(N-1) 不达 UB shift(>=64) 且 percentile 插值乘积留安全余量（#7 10b R1）");
    uint64_t samples = 0;
    uint64_t sum_us = 0;
    uint64_t max_us = 0;
    std::array<uint64_t, NBuckets> buckets{};
};

inline size_t hist_bucket(uint64_t us, size_t n_buckets) {
    if (us <= 1) return 0;
#if defined(__clang__) || defined(__GNUC__)
    const size_t idx = 64u - static_cast<size_t>(__builtin_clzll(us - 1));
#else
    size_t idx = 0;
    uint64_t t = us - 1;
    while (t != 0) { ++idx; t >>= 1u; }
#endif
    return idx < n_buckets - 1 ? idx : n_buckets - 1;
}

inline uint64_t hist_bucket_upper(size_t bucket_idx, size_t n_buckets) {
    if (bucket_idx == 0) return 1;
    if (bucket_idx >= n_buckets - 1) {
        return uint64_t(1) << (n_buckets - 1);
    }
    return uint64_t(1) << bucket_idx;
}

template <size_t NBuckets>
inline void hist_add(LatencyHistogram<NBuckets>& h, uint64_t us) {
    ++h.samples;
    h.sum_us += us;
    if (us > h.max_us) h.max_us = us;
    ++h.buckets[hist_bucket(us, NBuckets)];
}

inline uint64_t clamp_upper_with_max_us(
        uint64_t upper, uint64_t lower, uint64_t max_us,
        uint64_t cum_acc, uint64_t total_samples) {
    if (max_us > lower && max_us < upper && cum_acc == total_samples) {
        return max_us;
    }
    return upper;
}

template <size_t NBuckets>
inline uint64_t hist_percentile_ratio(const LatencyHistogram<NBuckets>& h,
                                      uint64_t num, uint64_t den) {
    if (h.samples == 0 || num == 0 || den == 0) return 0;
    const uint64_t target = (h.samples * num + den - 1) / den;
    uint64_t acc = 0;
    for (size_t i = 0; i < NBuckets; ++i) {
        const uint64_t in_bucket = h.buckets[i];
        if (acc + in_bucket < target) {
            acc += in_bucket;
            continue;
        }
        const uint64_t lower = (i == 0) ? 0 : hist_bucket_upper(i - 1, NBuckets);
        const uint64_t upper = clamp_upper_with_max_us(
            hist_bucket_upper(i, NBuckets), lower, h.max_us,
            acc + in_bucket, h.samples);
        const uint64_t rank = target - acc;
        return lower + (upper - lower) * rank / in_bucket;
    }

    return h.max_us > 0 ? h.max_us : hist_bucket_upper(NBuckets - 1, NBuckets);
}

template <size_t NBuckets>
inline uint64_t hist_percentile(const LatencyHistogram<NBuckets>& h, uint32_t p) {
    return hist_percentile_ratio(h, p, 100);
}

/// @brief 分位数比例定义。
struct QRatio {
    uint64_t num;
    uint64_t den;
};

template <size_t NBuckets, size_t NQ>
inline void hist_percentile_multi(const LatencyHistogram<NBuckets>& h,
                                  const std::array<QRatio, NQ>& queries,
                                  std::array<uint64_t, NQ>& out) {

    if (h.samples == 0) {
        for (size_t k = 0; k < NQ; ++k) out[k] = 0;
        return;
    }

    std::array<uint64_t, NQ> targets{};
    std::array<bool, NQ> filled{};
    for (size_t k = 0; k < NQ; ++k) {
        const auto& q = queries[k];
        if (q.num == 0 || q.den == 0) {
            out[k] = 0;
            filled[k] = true;
        } else {
            targets[k] = (h.samples * q.num + q.den - 1) / q.den;
            filled[k] = false;
        }
    }

    uint64_t acc = 0;
    for (size_t i = 0; i < NBuckets; ++i) {
        const uint64_t in_bucket = h.buckets[i];
        if (in_bucket == 0) continue;
        const uint64_t new_acc = acc + in_bucket;
        const uint64_t lower = (i == 0) ? 0 : hist_bucket_upper(i - 1, NBuckets);
        const uint64_t upper = clamp_upper_with_max_us(
            hist_bucket_upper(i, NBuckets), lower, h.max_us, new_acc, h.samples);

        bool all_filled = true;
        for (size_t k = 0; k < NQ; ++k) {
            if (filled[k]) continue;
            if (targets[k] <= new_acc) {
                const uint64_t rank = targets[k] - acc;
                out[k] = lower + (upper - lower) * rank / in_bucket;
                filled[k] = true;
            } else {
                all_filled = false;
            }
        }
        if (all_filled) return;
        acc = new_acc;
    }

    for (size_t k = 0; k < NQ; ++k) {
        if (!filled[k]) {
            out[k] = h.max_us > 0
                ? h.max_us : hist_bucket_upper(NBuckets - 1, NBuckets);
        }
    }
}

/// @brief 滚动窗口直方图。
///
/// 累计 histogram 中单个长期 outlier 会持续污染 p99；滚动窗口让旧样本自然
/// 过期，使监控反映最近窗口。`add()` 时按时间 lazy rotate，snapshot 时 merge
/// ring + current 后复用普通 percentile 算法。
///
/// 线程安全：本类型不含 atomic，调用方必须持锁或保证单线程访问。

template <size_t NBuckets, size_t NSlots>
struct RollingHistogram {
    static_assert(NSlots >= 2, "RollingHistogram 至少需要 2 slot");
    LatencyHistogram<NBuckets> current;
    std::array<LatencyHistogram<NBuckets>, NSlots> ring{};
    size_t head = 0;
    uint64_t current_start_us = 0;
};

template <size_t NBuckets, size_t NSlots>
inline void rolling_rotate_if_needed(RollingHistogram<NBuckets, NSlots>& h,
                                     uint64_t now_us, uint64_t slot_dur_us) {
    if (slot_dur_us == 0) return;
    if (h.current_start_us == 0) {
        h.current_start_us = now_us;
        return;
    }
    const uint64_t elapsed = now_us >= h.current_start_us
                                 ? now_us - h.current_start_us
                                 : 0;
    size_t full_slots = static_cast<size_t>(elapsed / slot_dur_us);
    if (full_slots == 0) return;

    if (full_slots >= NSlots) {
        h.current = {};
        h.ring = {};
        h.head = 0;
        h.current_start_us = now_us;
        return;
    }
    for (size_t i = 0; i < full_slots; ++i) {
        h.ring[h.head] = h.current;
        h.head = (h.head + 1) % NSlots;
        h.current = {};
    }
    h.current_start_us += static_cast<uint64_t>(full_slots) * slot_dur_us;
}

template <size_t NBuckets, size_t NSlots>
inline void rolling_add(RollingHistogram<NBuckets, NSlots>& h,
                        uint64_t us, uint64_t now_us, uint64_t slot_dur_us) {
    rolling_rotate_if_needed(h, now_us, slot_dur_us);
    hist_add(h.current, us);
}

template <size_t NBuckets, size_t NSlots>
inline LatencyHistogram<NBuckets> rolling_merge(
        const RollingHistogram<NBuckets, NSlots>& h) {
    LatencyHistogram<NBuckets> out;
    auto merge_one = [&out](const LatencyHistogram<NBuckets>& s) {
        out.samples += s.samples;
        out.sum_us += s.sum_us;
        if (s.max_us > out.max_us) out.max_us = s.max_us;
        for (size_t b = 0; b < NBuckets; ++b) {
            out.buckets[b] += s.buckets[b];
        }
    };
    for (size_t i = 0; i < NSlots; ++i) merge_one(h.ring[i]);
    merge_one(h.current);
    return out;
}

}
