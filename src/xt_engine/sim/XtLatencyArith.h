#pragma once
#include <cstdint>
#include <stdexcept>
#include <string>

namespace xt_latency {

inline constexpr int64_t kMaxLatencyNs =
    static_cast<int64_t>(UINT32_MAX) * 1'000'000LL;

inline int64_t validate_latency_ns(int64_t ns, const char* what) {
    if (ns < 0 || ns > kMaxLatencyNs) {
        throw std::runtime_error(
            std::string("latency 越界 [0, kMaxLatencyNs]: ") + what +
            " = " + std::to_string(ns) + " ns");
    }
    return ns;
}

inline int64_t checked_us_to_ns(int64_t us, const char* what) {
    const __int128 ns = static_cast<__int128>(us) * 1000;
    if (ns > INT64_MAX || ns < INT64_MIN) {
        throw std::runtime_error(
            std::string("us→ns 乘法溢出 int64: ") + what +
            " = " + std::to_string(us) + " us");
    }
    return validate_latency_ns(static_cast<int64_t>(ns), what);
}

inline int64_t checked_add_event_ns(int64_t base_ns, int64_t dur_ns) {
    const __int128 sum = static_cast<__int128>(base_ns) + dur_ns;
    if (sum > INT64_MAX || sum < INT64_MIN) {
        throw std::runtime_error(
            "event deadline 溢出 int64: base=" + std::to_string(base_ns) +
            " + dur=" + std::to_string(dur_ns));
    }
    return static_cast<int64_t>(sum);
}

}
