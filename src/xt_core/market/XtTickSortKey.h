#pragma once

#include <cstdint>

/// @file
/// @brief tick 全局排序键的单一口径。
///
/// 本文件负责把 `TradingDay + HHMMSSmmm action_time` 编成可比较 key。
/// CSV 读取、fbs event-store 归并和 memory-mode sort 必须共用这里的夜盘旋转规则。
/// 函数不校验交易日或时间合法性；调用方负责提供交易所口径的 `TradingDay`。

/// @brief HHMMSSmmm 十进制编码下一天的排序跨度。
constexpr uint64_t kActionTimeDaySpan = 240'000'000ULL;

/// @brief 将夜盘起点 HHMM 转为 HHMMSSmmm sort-key 旋转 slot。
///
/// @param origin_hhmm 夜盘起点，格式为 HHMM；调用方保证取值合法。
/// @return 可直接加到 `action_time` 上的 HHMMSSmmm 十进制偏移量。
/// @note 返回值不是 epoch ms，而是与 `action_time` 同编码域的排序 slot。
constexpr uint64_t night_shift_ms_for_origin(uint32_t origin_hhmm) {
    const uint64_t origin_h   = origin_hhmm / 100;
    const uint64_t origin_m   = origin_hhmm % 100;
    const uint64_t origin_min = origin_h * 60 + origin_m;
    const uint64_t span_min   = 24ULL * 60ULL - origin_min;
    // HHMMSSmmm 编码中 1 分钟 = 100000 slot。
    return span_min * 100'000ULL;
}

/// @brief 中国期货默认夜盘排序起点。
constexpr uint32_t kDefaultNightOriginHHMM = 1800;

/// @brief 默认夜盘起点对应的 HHMMSSmmm 排序偏移。
constexpr uint64_t kDefaultNightShiftMs = night_shift_ms_for_origin(kDefaultNightOriginHHMM);

/// @brief 构造 tick 全局排序键。
///
/// @param trading_day 交易所口径 `TradingDay`，通常为 YYYYMMDD。
/// @param action_time HHMMSSmmm 十进制时间，来自行情源的 UpdateTime/UpdateMillisec。
/// @return `trading_day * day_span + rotated_action_time`；`0/0` 保留为缺失时间 sentinel。
/// @note 对同一 `TradingDay`，默认 18:00 origin 会把夜盘时间排在日盘之前。
inline uint64_t make_tick_sort_key(uint32_t trading_day, uint32_t action_time) {
    if (trading_day == 0 && action_time == 0) return 0;
    const uint64_t shifted =
        (static_cast<uint64_t>(action_time) + kDefaultNightShiftMs) % kActionTimeDaySpan;
    return static_cast<uint64_t>(trading_day) * kActionTimeDaySpan + shifted;
}
