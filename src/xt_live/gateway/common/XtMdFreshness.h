#pragma once

/// @file
/// @brief 行情连接的状态判断 helper
///
/// MD gateway 定期问它一句：“现在行情是不是太久没动了，要不要重连？”

#include <algorithm>

namespace xt_live::gateway::detail {

enum class MdFreshnessAction {

    healthy,

    throttled,

    should_reconnect,
};

inline MdFreshnessAction evaluate_md_freshness(uint64_t now_ms,
                                               uint64_t last_tick_ms,
                                               uint64_t last_connect_ready_ms,
                                               uint64_t stale_last_close_ms,
                                               uint32_t stale_reconnect_ms,
                                               uint32_t throttle_ms) {
    if (stale_reconnect_ms == 0) return MdFreshnessAction::healthy;
    const uint64_t anchor = std::max(last_tick_ms, last_connect_ready_ms);
    if (anchor == 0) return MdFreshnessAction::healthy;
    if (now_ms < anchor) return MdFreshnessAction::healthy;
    const uint64_t delta = now_ms - anchor;
    if (delta <= stale_reconnect_ms) return MdFreshnessAction::healthy;
    if (stale_last_close_ms != 0 &&
        now_ms >= stale_last_close_ms &&
        (now_ms - stale_last_close_ms) < throttle_ms) {
        return MdFreshnessAction::throttled;
    }
    return MdFreshnessAction::should_reconnect;
}

}
