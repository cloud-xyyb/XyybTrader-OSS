#pragma once
#include <cstdint>

/// @brief K 线数据结构（bar / candlestick）。
///
/// 表示单根 OHLCV 快照，供策略历史行情读取与 bar 模式回调使用。价格字段使用
/// double；bar 是聚合统计值，不参与订单价格精确比较。
struct XtBar {
    char code[32] = {0};
    uint32_t action_date   = 0;
    uint32_t action_time   = 0;
    uint32_t trading_date  = 0;
    double open = 0.0;
    double high = 0.0;
    double low = 0.0;
    double close = 0.0;
    double volume = 0.0;
    double turnover = 0.0;
    double open_interest = 0.0;
};

namespace xt_tick_convert {
inline uint32_t trading_date(const XtBar& bar) {
    return bar.trading_date ? bar.trading_date : bar.action_date;
}
}
