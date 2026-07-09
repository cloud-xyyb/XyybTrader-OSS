#pragma once

#include <cstdint>

/// @file
/// @brief 开平语义枚举与净仓推导 helper。

enum class XtOffset : uint8_t {
    Open = 0,
    Close = 1,
    CloseToday = 2,
    CloseYesterday = 3,
    Auto = 4,
};

inline XtOffset infer_offset_from_pos(int64_t pos_fp, bool is_buy) noexcept {
    const bool is_open =
        (is_buy && pos_fp >= 0) || (!is_buy && pos_fp <= 0);
    return is_open ? XtOffset::Open : XtOffset::Close;
}
