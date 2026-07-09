#pragma once

#include <cstdint>

/// @brief unmatched routing 与 recovery 修复的诊断计数。
///
/// 1Hz control loop 可 relaxed 读取本结构；字段之间不提供跨字段原子性，
/// 用于 telemetry 和 unmatched budget 判定。
struct XtHftUnmatchedStats {
    uint64_t fill_empty_sid = 0;
    uint64_t fill_unknown_sid = 0;
    uint64_t order_update_empty_sid = 0;
    uint64_t order_update_unknown_sid = 0;
    uint64_t exec_dispatch = 0;
    uint64_t sid_fallback_recovered = 0;
    uint64_t lost_terminal_reconciled = 0;
};
