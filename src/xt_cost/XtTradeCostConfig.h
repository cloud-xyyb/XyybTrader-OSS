#pragma once

/// @brief 全局成交滑点配置。
struct XtTradeCostConfig {
    double slippage = 0.0;   ///< 绝对价格滑点；买单加、卖单减
};
