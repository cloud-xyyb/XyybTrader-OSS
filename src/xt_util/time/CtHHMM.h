#pragma once
#include <cstdint>

/// @file CtHHMM.h
/// @brief HHMM 分钟算术与区间判断 helper
///
/// HHMM 使用 4 位时钟编码（2059 = 20:59, 100 = 01:00）。这些 helper 用于
/// session 边界、auction tick 判定和 bar 对齐；非法 HHMM 输入按保守策略返回
/// false，分钟加减接口假设调用方传入合法值。

namespace xt_hhmm {

inline uint32_t next_minute(uint32_t hhmm) noexcept {
    const uint32_t mm = hhmm % 100;
    if (mm >= 59) {
        const uint32_t next_hh = (hhmm / 100 + 1) % 24;
        return next_hh * 100;
    }
    return hhmm + 1;
}

inline uint32_t prev_minute(uint32_t hhmm) noexcept {
    const uint32_t mm = hhmm % 100;
    if (mm == 0) {
        const uint32_t hh = hhmm / 100;
        const uint32_t prev_hh = (hh == 0) ? 23 : (hh - 1);
        return prev_hh * 100 + 59;
    }
    return hhmm - 1;
}

inline bool is_next_minute(uint32_t a, uint32_t b) noexcept {
    return next_minute(a) == b;
}

inline bool valid_hhmm(int hhmm) noexcept {
    if (hhmm < 0 || hhmm > 2359) return false;
    return hhmm % 100 < 60;
}

inline bool hhmm_in_section(int hhmm, int start_hhmm, int end_hhmm) noexcept {
    if (!valid_hhmm(hhmm) || !valid_hhmm(start_hhmm) || !valid_hhmm(end_hhmm)) return false;
    const uint32_t h = static_cast<uint32_t>(hhmm);
    const uint32_t s = static_cast<uint32_t>(start_hhmm);
    const uint32_t e = static_cast<uint32_t>(end_hhmm);
    if (e >= s) {
        return h >= s && h < e;
    }

    return h >= s || h < e;
}

inline bool hhmm_in_range(int hhmm, int start_hhmm, int end_hhmm) noexcept {
    if (!valid_hhmm(hhmm) || !valid_hhmm(start_hhmm) || !valid_hhmm(end_hhmm)) return false;
    if (start_hhmm <= end_hhmm) {
        return hhmm >= start_hhmm && hhmm <= end_hhmm;
    }

    return hhmm >= start_hhmm || hhmm <= end_hhmm;
}

}
