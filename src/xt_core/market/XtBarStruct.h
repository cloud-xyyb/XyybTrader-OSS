#pragma once
#include <cstdint>

/// @brief 一根 K 线（bar）。
///
/// 一根 bar 是一段时间（分钟 / 日…）内的 tick 聚合出的开高低收 + 成交量快照，供 bar 级
/// 策略读历史行情和 on_bar 回调使用。价格用 double：bar 是统计聚合值，只喂指标计算，不进
/// 撮合/账务这类要求精确的订单价格比较（那条路径用 fixed-point 的 XtTick）。
struct XtBar {
    char code[32] = {0};           ///< 合约代码（instrument code）
    uint32_t action_date   = 0;    ///< 自然日 yyyymmdd（calendar date）
    uint32_t action_time   = 0;    ///< 时间戳 hhmmssmmm（action time）
    uint32_t trading_date  = 0;    ///< 交易日 yyyymmdd（trading date）；夜盘时 != action_date，0 表示未设置回退到 action_date
    double open = 0.0;
    double high = 0.0;
    double low = 0.0;
    double close = 0.0;
    double volume = 0.0;
    double turnover = 0.0;
    double open_interest = 0.0;
};

/// @brief 与 XtTick/XtRawTick 的 trading_date helper 保持同 namespace。
namespace xt_tick_convert {
inline uint32_t trading_date(const XtBar& bar) {
    return bar.trading_date ? bar.trading_date : bar.action_date;
}
}  // namespace xt_tick_convert
