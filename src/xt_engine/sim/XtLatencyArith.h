#pragma once
#include <cstdint>
#include <stdexcept>
#include <string>

/// @brief latency 数值域单一契约：上界、消费点校验与绝对时刻 checked 加法。
///
/// 模型构造输出与消费点双重校验（防 test/custom model 绕过构造 gate）；
/// 越界一律 throw 穿透到 BT 顶层失败，禁止 wrap 成过去时刻或静默 clamp。
namespace xt_latency {

/// 单段延迟上界：UINT32_MAX ms 折 ns（≈49.7 天），远超一切合理延迟建模。
inline constexpr int64_t kMaxLatencyNs =
    static_cast<int64_t>(UINT32_MAX) * 1'000'000LL;

/// @brief 校验单段延迟取值域 [0, kMaxLatencyNs]；越界抛 std::runtime_error。
inline int64_t validate_latency_ns(int64_t ns, const char* what) {
    if (ns < 0 || ns > kMaxLatencyNs) {
        throw std::runtime_error(
            std::string("latency 越界 [0, kMaxLatencyNs]: ") + what +
            " = " + std::to_string(ns) + " ns");
    }
    return ns;
}

/// @brief us→ns 的 checked 乘法（__int128 中间量验 int64），再交 validate_latency_ns
///        校验取值域。裸 `us * 1000` 在 us 极大时溢出 int64 会 wrap 成小正值绕过
///        validate；本 helper 先验乘法溢出。clean 配置（us 合理）与裸乘法逐位等价。
inline int64_t checked_us_to_ns(int64_t us, const char* what) {
    const __int128 ns = static_cast<__int128>(us) * 1000;
    if (ns > INT64_MAX || ns < INT64_MIN) {
        throw std::runtime_error(
            std::string("us→ns 乘法溢出 int64: ") + what +
            " = " + std::to_string(us) + " us");
    }
    return validate_latency_ns(static_cast<int64_t>(ns), what);
}

/// @brief 绝对 event 时刻 + 时长的 checked 加法（__int128 中间量验 int64）。
inline int64_t checked_add_event_ns(int64_t base_ns, int64_t dur_ns) {
    const __int128 sum = static_cast<__int128>(base_ns) + dur_ns;
    if (sum > INT64_MAX || sum < INT64_MIN) {
        throw std::runtime_error(
            "event deadline 溢出 int64: base=" + std::to_string(base_ns) +
            " + dur=" + std::to_string(dur_ns));
    }
    return static_cast<int64_t>(sum);
}

}  // namespace xt_latency
