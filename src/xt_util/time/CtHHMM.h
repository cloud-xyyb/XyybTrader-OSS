#pragma once
#include <cstdint>

/// @file CtHHMM.h
/// @brief HHMM 分钟算术与区间判断 helper
///
/// HHMM 使用 4 位时钟编码（2059 = 20:59, 100 = 01:00）。这些 helper 用于
/// session 边界、auction tick 判定和 bar 对齐；非法 HHMM 输入按保守策略返回
/// false，分钟加减接口假设调用方传入合法值。

namespace xt_hhmm {

/// @brief HHMM 下一分钟，处理 59 → 00 进位（23:59 → 00:00 循环）。
/// @param hhmm  HHMM 格式 uint32_t（合法范围 0..2359）
/// @return      下一分钟的 HHMM；2359 → 0
inline uint32_t next_minute(uint32_t hhmm) noexcept {
    const uint32_t mm = hhmm % 100;
    if (mm >= 59) {
        const uint32_t next_hh = (hhmm / 100 + 1) % 24;
        return next_hh * 100;
    }
    return hhmm + 1;
}

/// @brief HHMM 上一分钟，处理 00 → 59 借位（00:00 → 23:59 循环）。
/// @param hhmm  HHMM 格式 uint32_t（合法范围 0..2359）
/// @return      上一分钟的 HHMM；0 → 2359
inline uint32_t prev_minute(uint32_t hhmm) noexcept {
    const uint32_t mm = hhmm % 100;
    if (mm == 0) {
        const uint32_t hh = hhmm / 100;
        const uint32_t prev_hh = (hh == 0) ? 23 : (hh - 1);
        return prev_hh * 100 + 59;
    }
    return hhmm - 1;
}

/// @brief 判断 b 是否是 a 的下一分钟（严格向前，不跨多分钟）。
inline bool is_next_minute(uint32_t a, uint32_t b) noexcept {
    return next_minute(a) == b;
}

/// @name HHMM 校验与区间判断
/// 半开区间用于 session 交易段；闭区间用于 closeout/no-open 窗口。
/// @{

/// @brief HHMM 合法性校验。
/// @param hhmm  待校验值（int 形态，允许负数表示"未设置"）。
/// @return      `true` 当 0 <= hhmm <= 2359 且 mm 部分（hhmm%100）< 60。
inline bool valid_hhmm(int hhmm) noexcept {
    if (hhmm < 0 || hhmm > 2359) return false;
    return hhmm % 100 < 60;
}

/// @brief HHMM 半开区间判断 [start, end)，支持跨午夜（end < start）。
///
/// 与 xt_core::in_section_halfopen 语义一致：用于 session 段、active sections
/// 等"end 时段已收盘"场景。
///
/// 参数任一非法时返回 `false`（保守策略：拒绝可疑配置而非误判命中）。
inline bool hhmm_in_section(int hhmm, int start_hhmm, int end_hhmm) noexcept {
    if (!valid_hhmm(hhmm) || !valid_hhmm(start_hhmm) || !valid_hhmm(end_hhmm)) return false;
    const uint32_t h = static_cast<uint32_t>(hhmm);
    const uint32_t s = static_cast<uint32_t>(start_hhmm);
    const uint32_t e = static_cast<uint32_t>(end_hhmm);
    if (e >= s) {
        return h >= s && h < e;
    }
    // 跨午夜（如 [2101, 230]）拆两半。
    return h >= s || h < e;
}

/// @brief HHMM 闭区间判断 [start, end]，支持跨午夜（end < start）。
///
/// 用于 closeout no-open 窗口等需要保留 end 分钟的场景，端点匹配视为命中。
/// 参数非法返回 `false`。
inline bool hhmm_in_range(int hhmm, int start_hhmm, int end_hhmm) noexcept {
    if (!valid_hhmm(hhmm) || !valid_hhmm(start_hhmm) || !valid_hhmm(end_hhmm)) return false;
    if (start_hhmm <= end_hhmm) {
        return hhmm >= start_hhmm && hhmm <= end_hhmm;
    }
    // 跨午夜：要么 >= start，要么 <= end。
    return hhmm >= start_hhmm || hhmm <= end_hhmm;
}

/// @}

}  // namespace xt_hhmm
