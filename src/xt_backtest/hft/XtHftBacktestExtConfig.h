#pragma once

#include <cstdint>
#include <functional>

namespace xt_backtest::hft {

/// @brief Backtest-only 引擎扩展配置。
///
/// 由 XtHftBacktestRunner（或测试 helper HftTestEnv）在 engine 构造后通过
/// `engine.configure_backtest_ext(ext)` 注入。Live 路径无需调用。
struct XtHftBacktestExtConfig {

    std::function<bool(uint32_t)> calendar_filter;

    int session_offset_minutes = 0;

    std::function<uint32_t(uint32_t)> next_trading_day;
};

}
