#pragma once

/// @file
/// @brief 回测风控时钟编码 helper。
///
/// 将交易日和 `HHMMSSmmm` 打包时刻编码为单一可比较 `int64_t`，供
/// `XtRiskEvalContext::now_ms` 使用。HFT/CTA 两侧回测 gateway 共用同一编码，
/// 保证 reject 与 normal 路径时间戳同源可比。

#include <cstdint>

inline int64_t bt_risk_now_ms(uint32_t date, uint32_t time_ms) noexcept {
    const uint32_t hh = time_ms / 10000000;
    const uint32_t mm = (time_ms / 100000) % 100;
    const uint32_t ss = (time_ms / 1000) % 100;
    const uint32_t ms = time_ms % 1000;
    const int64_t linear_ms =
        ((static_cast<int64_t>(hh) * 60 + mm) * 60 + ss) * 1000 + ms;
    return static_cast<int64_t>(date) * 100'000'000LL + linear_ms;
}
