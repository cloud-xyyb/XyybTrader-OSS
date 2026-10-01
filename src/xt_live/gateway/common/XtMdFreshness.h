#pragma once

/// @file
/// @brief 行情连接的状态判断 helper
///
/// MD gateway 定期问它一句：“现在行情是不是太久没动了，要不要重连？”

#include <algorithm>

namespace xt_live::gateway::detail {

enum class MdFreshnessAction {
    /// anchor 仍新鲜，或 cold start 阶段尚未出现任何 anchor；调用方不动作。
    healthy,
    /// anchor 已 stale，但 throttle 窗口内已经触发过 reconnect；调用方跳过本次。
    throttled,
    /// anchor 已 stale 且 throttle 已过；调用方应触发 WS reconnect 并更新 stale_last_close_ms。
    should_reconnect,
};

/// @brief 基于真实 tick 与 book-ready 两个 witness 评估行情新鲜度。
/// @note 所有 `*_ms` 参数均为 steady_clock milliseconds。入参使用 uint64_t，避免减法溢出。
/// @param now_ms 当前 steady_clock ms。
/// @param last_tick_ms 最近真实 tick 的 steady_clock ms；0 表示从未收到。
/// @param last_connect_ready_ms 最近 book-ready 的 steady_clock ms；0 表示从未 ready。
/// @param stale_last_close_ms 最近一次 stale reconnect 触发时刻；0 表示从未触发。
/// @param stale_reconnect_ms stale 阈值；0 表示禁用 watchdog。
/// @param throttle_ms stale reconnect 的最小触发间隔。
inline MdFreshnessAction evaluate_md_freshness(uint64_t now_ms,
                                               uint64_t last_tick_ms,
                                               uint64_t last_connect_ready_ms,
                                               uint64_t stale_last_close_ms,
                                               uint32_t stale_reconnect_ms,
                                               uint32_t throttle_ms) {
    if (stale_reconnect_ms == 0) return MdFreshnessAction::healthy;
    const uint64_t anchor = std::max(last_tick_ms, last_connect_ready_ms);
    if (anchor == 0) return MdFreshnessAction::healthy;  // cold start 由 start/reconnect 流程负责。
    if (now_ms < anchor) return MdFreshnessAction::healthy;  // steady 时钟异常时保守处理。
    const uint64_t delta = now_ms - anchor;
    if (delta <= stale_reconnect_ms) return MdFreshnessAction::healthy;
    if (stale_last_close_ms != 0 &&
        now_ms >= stale_last_close_ms &&
        (now_ms - stale_last_close_ms) < throttle_ms) {
        return MdFreshnessAction::throttled;
    }
    return MdFreshnessAction::should_reconnect;
}

}  // namespace xt_live::gateway::detail
