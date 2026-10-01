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
    int64_t window_start_ms = 0;    ///< 0 = 未初始化（首帧只落基线不判定）
    uint64_t drops_at_start = 0;    ///< 窗口起点的 drop 累计值
    uint64_t dispatch_at_start = 0; ///< 窗口起点的派发累计值
};

/// @brief 预算判定（AND 语义）：窗口增量同时满足绝对与比例条件才 trip。
///
/// AND 的动机：孤立 cancel/fill race 会造成偶发良性 unmatched drop，高吞吐下
/// 纯绝对阈值会误杀（3 drops / 10k events 是噪声）；纯比例阈值在低吞吐下失灵
/// （1 drop / 2 events = 50%）。两者相与后：低吞吐由绝对阈值主导，高吞吐由
/// 比例阈值豁免背景噪声。
///
/// @param window_drops    窗口内 unmatched drop 增量（fill + order update）
/// @param window_dispatch 窗口内派发尝试增量（分母；engine 在 drop 之前计数
///                        → dispatch >= drops 恒成立，比例 <= 1 良定义。该序关系
///                        由部署面 x86/TSO 保证（全链 relaxed）；非 TSO 架构下
///                        偶发倒序的失败方向保守，比例虚高仍需绝对阈值同时越限）
/// @param max_drops       绝对阈值；<=0 时该条件恒真（纯比例模式）
/// @param max_ratio       比例阈值 drops/dispatch；<=0 时该条件恒真（纯绝对模式）
/// @return true = 预算超限应 trip。两个阈值都 <=0 时恒 false（等效关闭）。
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

/// @brief 窗口步进 + 判定：每帧调用，窗口内每帧评估（不是只在窗口边界评估，
///        避免边界帧到达的 drop 被下一窗口基线吞掉而逃过判定）。
///
/// trip 或窗口到期后 rebase 基线：trip 后 rebase 保证人工 resume 不会被同一批
/// 旧 drop 立即重复 trip（重新 trip 需要新窗口内的新增 drop）。计数器回退
/// （进程内单调，防御性处理）同样 rebase。
///
/// @pre now_ms > 0（steady clock ms；window_start_ms==0 是未初始化哨兵，
///      now_ms=0 会使首帧基线永远重初始化）。
/// @return true = 本帧预算超限（caller 负责置 kill_switch + emit 转换事件）。
[[nodiscard]] inline bool unmatched_budget_step(UnmatchedBudgetWindow& w,
                                                int64_t now_ms,
                                                int window_sec,
                                                uint64_t drops_total,
                                                uint64_t dispatch_total,
                                                int max_drops,
                                                double max_ratio) noexcept {
    if (window_sec <= 0) return false;  // 总闸：window_sec<=0 关闭整个预算判定
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
    uint64_t window_drops = 0;     ///< trip 时的窗口 drop 增量（日志/reason 用）
    uint64_t window_dispatch = 0;  ///< trip 时的窗口派发增量
};

/// @brief signals 面评估器：读 1Hz wire 过来的 drop/dispatch 累计值，步进窗口，
///        超限且当前未 latch 时置 kill_switch + source=UnmatchedBudget。
///        emit_kill_switch_transition（需 ctx/metrics）由 caller 在 tripped 时执行。
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
    // 先快照增量再步进（步进可能 rebase 掉窗口）。
    const uint64_t wd = drops_total >= w.drops_at_start && w.window_start_ms != 0
                            ? drops_total - w.drops_at_start : 0;
    const uint64_t wq = dispatch_total >= w.dispatch_at_start && w.window_start_ms != 0
                            ? dispatch_total - w.dispatch_at_start : 0;
    const bool trip = unmatched_budget_step(w, now_ms, window_sec, drops_total,
                                            dispatch_total, max_drops, max_ratio);
    if (!trip) return out;
    // 已 latch（手动或其它自动源）不重复 trip，也不覆盖既有 source。
    if (signals.kill_switch.load(std::memory_order_acquire)) return out;
    signals.kill_switch.store(true, std::memory_order_release);
    signals.kill_switch_source.store(XtRunnerSignals::kKillSwitchSourceUnmatchedBudget,
                                     std::memory_order_release);
    out.tripped = true;
    out.window_drops = wd;
    out.window_dispatch = wq;
    return out;
}

}  // namespace xt_live::control
