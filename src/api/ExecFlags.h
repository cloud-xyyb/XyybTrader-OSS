#pragma once

#include <cstdint>

/**
 * @brief 执行提示正交标志（orthogonal execution hints）。
 *
 * @details
 * - 与 `XtOffset`（期货开平今昨语义）正交，表示对撮合/挂单行为的额外约束。
 * - 各交易所支持度不同：CTP 期货通常忽略所有 flag；binance perp 支持
 *   `ReduceOnly`（reduceOnly=true）和 `PostOnly`（timeInForce=GTX）。
 * - 用法：`submit_order(..., with_flag(exec_none(), ExecFlag::ReduceOnly))`。
 */
enum class ExecFlag : uint32_t {
    None       = 0,
    ReduceOnly = 1u << 0,  ///< 仅减仓：触发关闭但不反向建仓（binance perp reduceOnly）
    PostOnly   = 1u << 1,  ///< 不吃对手方：否则拒单（binance timeInForce=GTX）
    Hidden     = 1u << 2,  ///< 暗单（预留；当前无交易所直接支持）
    Market     = 1u << 3,  ///< 市价意图：在 XtStrategyContext 解析为激进限价 IOC，tag 随单 travel 供 analyzer / 未来 native opt-in
};

/// 位掩码组合；`has_flag` / `with_flag` 辅助操作。
using ExecFlags = uint32_t;

constexpr ExecFlags exec_none() { return 0; }

constexpr bool has_flag(ExecFlags f, ExecFlag bit) {
    return (f & static_cast<uint32_t>(bit)) != 0;
}

constexpr ExecFlags with_flag(ExecFlags f, ExecFlag bit) {
    return f | static_cast<uint32_t>(bit);
}
