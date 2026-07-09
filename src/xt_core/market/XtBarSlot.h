#pragma once
#include <cstdint>

/// @file XtBarSlot.h
/// @brief CTA/live/backtest 共用的 bar 时间槽计算。
///
/// `action_time` 使用 HHMMSSmmm 编码。分钟级 slot 以 action_date 分组并按
/// period 向下取整；日线及以上以 trading_date 分组，避免夜盘跨自然日断裂。

namespace xt_bar_slot {

inline uint32_t minute_slot(uint32_t action_time, int period_min) {
    const uint32_t t = action_time / 1000;
    const uint32_t total_min = (t / 10000) * 60 + ((t / 100) % 100);
    return total_min / static_cast<uint32_t>(period_min)
           * static_cast<uint32_t>(period_min);
}

inline uint32_t slot_to_action_time(uint32_t slot_min) {
    return (slot_min / 60) * 10000000u + (slot_min % 60) * 100000u;
}

inline uint64_t compute_slot_id(uint32_t action_date, uint32_t action_time,
                                uint32_t trading_date, int period_min) {
    if (period_min >= 1440)
        return trading_date != 0 ? trading_date : action_date;
    const uint32_t total_min = minute_slot(action_time, 1);
    return static_cast<uint64_t>(action_date) * 10000ULL
           + total_min / static_cast<uint32_t>(period_min);
}

}
