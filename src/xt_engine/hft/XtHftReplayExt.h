#pragma once

#include <cstdint>
#include <functional>

namespace xt_engine::hft {

/// @brief 回放语境下的交易日历与夜盘交易日推断扩展（replay extension）。
///
/// 由组合根在 engine 构造前填好；engine 只读，不感知数据来源是历史回放还是实时行情
/// （live 组合根留空 = 全部路径 inactive）。
struct XtHftReplayExt {
    /// @brief 交易日过滤；非空时返回 false 的 trading_date 整日丢弃（计 calendar drop）。
    /// 典型接入：XtBacktestTradingCalendar::is_trading_day。
    std::function<bool(uint32_t)> calendar_filter;

    /// @brief 夜盘偏移分钟数；> 0 时启用 trading_date 推断（300 = 中国期货 21:00 起算次日）。
    /// 数据源直接提供 trading_date 时无需启用。
    int session_offset_minutes = 0;

    /// @brief 跨假期下一交易日回调；为空时推断器回退到内置 next_weekday（只跳周末）。
    std::function<uint32_t(uint32_t)> next_trading_day;
};

}  // namespace xt_engine::hft
