#pragma once

#include <cstdint>

/// @file
/// @brief 开平（open/close offset）语义枚举与净仓推导 helper。
///
/// 期货报单除了买卖方向，还要声明这一单是开仓还是平仓；SHFE/INE 等交易所更要求平仓时区分
/// 平今、平昨。本文件定义这套开平枚举，并提供由净持仓和买卖方向反推 Open/Close 的 helper
/// （今昨的进一步拆分交给路由层）。

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
