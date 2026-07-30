#pragma once

#include <cstdint>
#include <functional>

namespace xt_engine::hft {

/// @brief 回放语境下的交易日历与夜盘交易日推断扩展（replay extension）。
///
/// 由组合根在 engine 构造前填好；engine 只读，不感知数据来源是历史回放还是实时行情
/// （live 组合根留空 = 全部路径 inactive）。
struct XtHftReplayExt {

    std::function<bool(uint32_t)> calendar_filter;

    int session_offset_minutes = 0;

    std::function<uint32_t(uint32_t)> next_trading_day;
};

}
