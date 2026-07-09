#pragma once

#include <atomic>
#include <cstdint>
#include <utility>
#include <vector>

#include "xt_core/session/XtSessionFilter.h"
#include "xt_core/session/XtSessionWindow.h"
#include "xt_util/time/XtTime.h"

namespace xt_live {

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

}
