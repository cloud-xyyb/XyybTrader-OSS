#pragma once
#include <cstdint>

/// @file XtBarSlot.h
/// @brief bar 时间槽（bucket）计算的单一口径，CTA/live/backtest 共用。
///
/// K 线要把连续 tick 归进「分钟桶 / 日桶」再聚合成一根 OHLCV；只要 live 聚合和 backtest
/// 回放用的桶边界不一致，同一段行情就会算出不同的 bar。本文件把归桶规则收成一处：分钟级
/// slot 以 action_date（自然日）分组并按 period 向下取整，日线及以上以 trading_date 分组，
/// 让夜盘不会因为跨过自然日而被切成两根。`action_time` 用 HHMMSSmmm 编码。

namespace xt_bar_slot {

/// @brief 计算分钟级 bar 的槽边界（minute boundary floor）。
/// @param action_time  HHMMSSMMM 格式（如 93015500 = 09:30:15.500）
/// @param period_min   周期分钟数（如 1, 5, 15, 60）
/// @return 槽编号 = total_minutes / period_min × period_min（分钟）
inline uint32_t minute_slot(uint32_t action_time, int period_min) {
    const uint32_t t = action_time / 1000;  // HHMMSSMMM → HHMMSS
    const uint32_t total_min = (t / 10000) * 60 + ((t / 100) % 100);
    return total_min / static_cast<uint32_t>(period_min)
           * static_cast<uint32_t>(period_min);
}

/// @brief 将分钟槽号转回 action_time 格式（HH × 10000000 + MM × 100000）。
inline uint32_t slot_to_action_time(uint32_t slot_min) {
    return (slot_min / 60) * 10000000u + (slot_min % 60) * 100000u;
}

/// @brief 返回完整 slot ID；分钟级含 action_date，日线及以上用 trading_date。
inline uint64_t compute_slot_id(uint32_t action_date, uint32_t action_time,
                                uint32_t trading_date, int period_min) {
    if (period_min >= 1440)
        return trading_date != 0 ? trading_date : action_date;
    const uint32_t total_min = minute_slot(action_time, 1);
    return static_cast<uint64_t>(action_date) * 10000ULL
           + total_min / static_cast<uint32_t>(period_min);
}

}  // namespace xt_bar_slot
