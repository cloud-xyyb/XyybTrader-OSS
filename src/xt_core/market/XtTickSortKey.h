#pragma once

#include <cstdint>

/// @file
/// @brief tick 全局排序键的单一口径。
///
/// 本文件负责把 `TradingDay + HHMMSSmmm action_time` 编成可比较 key。
/// CSV 读取、fbs event-store 归并和 memory-mode sort 必须共用这里的夜盘旋转规则。
/// 函数不校验交易日或时间合法性；调用方负责提供交易所口径的 `TradingDay`。

constexpr uint64_t kActionTimeDaySpan = 240'000'000ULL;

constexpr uint64_t night_shift_ms_for_origin(uint32_t origin_hhmm) {
    const uint64_t origin_h   = origin_hhmm / 100;
    const uint64_t origin_m   = origin_hhmm % 100;
    const uint64_t origin_min = origin_h * 60 + origin_m;
    const uint64_t span_min   = 24ULL * 60ULL - origin_min;

    return span_min * 100'000ULL;
}

constexpr uint32_t kDefaultNightOriginHHMM = 1800;

constexpr uint64_t kDefaultNightShiftMs = night_shift_ms_for_origin(kDefaultNightOriginHHMM);

inline uint64_t make_tick_sort_key(uint32_t trading_day, uint32_t action_time) {
    if (trading_day == 0 && action_time == 0) return 0;
    const uint64_t shifted =
        (static_cast<uint64_t>(action_time) + kDefaultNightShiftMs) % kActionTimeDaySpan;
    return static_cast<uint64_t>(trading_day) * kActionTimeDaySpan + shifted;
}
