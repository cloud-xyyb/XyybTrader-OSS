#pragma once

#include <atomic>
#include <cstdint>
#include <utility>
#include <vector>

#include "xt_core/session/XtSessionFilter.h"
#include "xt_core/session/XtSessionWindow.h"
#include "xt_util/time/XtTime.h"

namespace xt_live {

/// @brief per-tick 提交门禁 flag 更新（HFT/CTA event loop 共用）。
///
/// 按 tick.action_time 计算 active_sections 半开命中与 closeout 闭区间命中，
/// 写入 XtRunnerSignals 的两个 atomic — XtStrategyContext::can_submit() 的读端。
/// update_session_gate_flags() 使用 `xt_core::in_hhmm_sections_halfopen()` 与
/// `xt_core::in_closeout_no_open_window_closed()`，与 risk guard 读侧共用同一 predicate。
///
/// @pre 调用必须先于本 tick 的策略 dispatch（同 tick 一致性：策略 can_submit
///      读到的 gate 与 risk 对同一 tick 的判定一致）。
/// @note 仅变化才 store，避免每 tick 无谓的 release 写。
inline void update_session_gate_flags(
        uint32_t action_time,
        const std::vector<std::pair<int, int>>* sections,
        const std::vector<XtSessionWindow>* closeout_windows,
        std::atomic<bool>* in_trading_section,
        std::atomic<bool>* closeout_no_open) {
    if (!action_time) return;
    const int hhmm = static_cast<int>(xt_time::action_time_to_hhmm(action_time));

    if (in_trading_section && sections && !sections->empty()) {
        const bool in = xt_core::in_hhmm_sections_halfopen(hhmm, *sections);
        if (in != in_trading_section->load(std::memory_order_relaxed)) {
            in_trading_section->store(in, std::memory_order_release);
        }
    }

    if (closeout_no_open && closeout_windows && !closeout_windows->empty()) {
        const bool in_closeout =
            xt_core::in_closeout_no_open_window_closed(hhmm, *closeout_windows);
        if (in_closeout != closeout_no_open->load(std::memory_order_relaxed)) {
            closeout_no_open->store(in_closeout, std::memory_order_release);
        }
    }
}

}  // namespace xt_live
