#pragma once

#include <cstdint>

/// @file
/// @brief 开平（open/close offset）语义枚举与净仓推导 helper。
///
/// 期货报单除了买卖方向，还要声明这一单是开仓还是平仓；SHFE/INE 等交易所更要求平仓时区分
/// 平今、平昨。本文件定义这套开平枚举，并提供由净持仓和买卖方向反推 Open/Close 的 helper
/// （今昨的进一步拆分交给路由层）。

/**
 * @brief 持仓开平语义。
 *
 * `Open` 表示开仓；`Close` 表示平仓意图，是否拆平今/平昨由路由层决定。
 * `CloseToday` / `CloseYesterday` 为已解析的强指定平仓方向。
 * `Auto` 只允许作为策略 submit-time sentinel；提交入口必须解析成 `Open` 或拒单，
 * 不应进入风控、venue wire 或执行回报。
 */
enum class XtOffset : uint8_t {
    Open = 0,
    Close = 1,
    CloseToday = 2,
    CloseYesterday = 3,
    Auto = 4,
};

/// @brief 根据净仓位与报单方向推导 Open/Close。
///
/// `pos_fp` 为 long_qty - short_qty。买入在净空头时视为平仓，卖出在净多头时
/// 视为平仓；`pos_fp=0` 或同向加仓视为开仓。
///
/// @return 只返回 Open/Close，不拆 CloseToday/CloseYesterday。
inline XtOffset infer_offset_from_pos(int64_t pos_fp, bool is_buy) noexcept {
    const bool is_open =
        (is_buy && pos_fp >= 0) || (!is_buy && pos_fp <= 0);
    return is_open ? XtOffset::Open : XtOffset::Close;
}
