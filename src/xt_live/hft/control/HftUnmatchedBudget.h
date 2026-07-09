#pragma once

/// @file
/// @brief 回报归因失败率的预算判定；超限自动 trip kill switch。
///
/// 多策略模式下，一笔成交/回报找不到归属策略会被 engine 丢弃。偶发丢弃有
/// 快照对账兜底；持续丢弃说明归因链结构性损坏，继续交易会造成持续的记账偏差。
/// 本文件把丢弃速率装进滚动窗口预算：窗口内同时超过绝对阈值和比例阈值
/// 就 trip 全局 kill switch。纯函数判定，便于单测。

#include <cstdint>

#include "api/XtRunnerSignals.h"

namespace xt_live::control {

/// @brief tumbling window 基线；生命周期同 ControlLoopState（跨帧累积）。
struct UnmatchedBudgetWindow {
    int64_t window_start_ms = 0;
    uint64_t drops_at_start = 0;
    uint64_t dispatch_at_start = 0;
};

[[nodiscard]] inline bool unmatched_budget_decision(uint64_t window_drops,
                                                    uint64_t window_dispatch,
                                                    int max_drops,
                                                    double max_ratio) noexcept {
    if (window_drops == 0) return false;
    if (max_drops <= 0 && max_ratio <= 0.0) return false;
    const bool abs_breach =
        max_drops <= 0 || window_drops >= static_cast<uint64_t>(max_drops);
    const bool ratio_breach =
        max_ratio <= 0.0 ||
        static_cast<double>(window_drops) >=
            max_ratio * static_cast<double>(window_dispatch);
    return abs_breach && ratio_breach;
}

[[nodiscard]] inline bool unmatched_budget_step(UnmatchedBudgetWindow& w,
                                                int64_t now_ms,
                                                int window_sec,
                                                uint64_t drops_total,
                                                uint64_t dispatch_total,
                                                int max_drops,
                                                double max_ratio) noexcept {
    if (window_sec <= 0) return false;
    const bool counters_rewound =
        drops_total < w.drops_at_start || dispatch_total < w.dispatch_at_start;
    if (w.window_start_ms == 0 || counters_rewound) {
        w.window_start_ms = now_ms;
        w.drops_at_start = drops_total;
        w.dispatch_at_start = dispatch_total;
        return false;
    }
    const uint64_t window_drops = drops_total - w.drops_at_start;
    const uint64_t window_dispatch = dispatch_total - w.dispatch_at_start;
    const bool trip =
        unmatched_budget_decision(window_drops, window_dispatch, max_drops, max_ratio);
    const bool expired =
        now_ms - w.window_start_ms >= static_cast<int64_t>(window_sec) * 1000;
    if (trip || expired) {
        w.window_start_ms = now_ms;
        w.drops_at_start = drops_total;
        w.dispatch_at_start = dispatch_total;
    }
    return trip;
}

/// @brief 评估结果；tripped 仅在本帧真正把 kill_switch 从 false 翻到 true 时为 true。
struct UnmatchedBudgetOutcome {
    bool tripped = false;
    uint64_t window_drops = 0;
    uint64_t window_dispatch = 0;
};

[[nodiscard]] inline UnmatchedBudgetOutcome evaluate_unmatched_budget(
    XtRunnerSignals& signals,
    UnmatchedBudgetWindow& w,
    int64_t now_ms,
    int window_sec,
    int max_drops,
    double max_ratio) noexcept {
    UnmatchedBudgetOutcome out;
    const uint64_t drops_total =
        signals.multi_strategy_dropped_fill_cnt.load(std::memory_order_relaxed) +
        signals.multi_strategy_dropped_order_update_cnt.load(std::memory_order_relaxed);
    const uint64_t dispatch_total =
        signals.engine_exec_dispatch_cnt.load(std::memory_order_relaxed);

    const uint64_t wd = drops_total >= w.drops_at_start && w.window_start_ms != 0
                            ? drops_total - w.drops_at_start : 0;
    const uint64_t wq = dispatch_total >= w.dispatch_at_start && w.window_start_ms != 0
                            ? dispatch_total - w.dispatch_at_start : 0;
    const bool trip = unmatched_budget_step(w, now_ms, window_sec, drops_total,
                                            dispatch_total, max_drops, max_ratio);
    if (!trip) return out;

    if (signals.kill_switch.load(std::memory_order_acquire)) return out;
    signals.kill_switch.store(true, std::memory_order_release);
    signals.kill_switch_source.store(XtRunnerSignals::kKillSwitchSourceUnmatchedBudget,
                                     std::memory_order_release);
    out.tripped = true;
    out.window_drops = wd;
    out.window_dispatch = wq;
    return out;
}

}
