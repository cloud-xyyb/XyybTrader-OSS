#pragma once

/// @file
/// @brief 控制环每轮各阶段（采样 / 策略链 / 收盘强平 / 持久化）的函数入口。
///
/// `HftRunnerControl::run()` 按固定顺序调用本文件声明的 stage 函数；
/// 各 stage 的实现按职责分布在同目录 HftControl*.cpp。

#include <atomic>
#include <cstdint>
#include <string>

#include "xt_live/hft/control/HftRunnerControlState.h"

namespace xt_risk::state {
struct SafetyState;
}

namespace xt_live::control::detail {

LoopFrame collect_loop_frame(const XtRunnerControlConfig& cfg,
                             XtHftRunnerControlContext& ctx,
                             ControlLoopState& st);

struct ConsistencyThresholds {
    uint64_t warn_loops = 1;
    uint64_t block_loops = 1;
    uint64_t kill_loops = 1;
};

ConsistencyThresholds make_consistency_thresholds(const XtRunnerControlConfig& cfg);

void log_control_stats(const ControlSnapshot& s, uint64_t latency_sla_breach_total);

bool apply_policy_chain(std::atomic<bool>& shutdown,
                        const XtRunnerControlConfig& cfg,
                        XtHftRunnerControlContext& ctx,
                        const ConsistencyThresholds& thresholds,
                        const LoopFrame& frame,
                        ControlLoopState& st);

void run_force_close_once(XtHftRunnerControlContext& ctx,
                          const XtRunnerControlConfig& cfg,
                          ControlLoopState& st);

void emit_kill_switch_transition(XtHftRunnerControlContext& ctx,
                                 const XtRunnerControlConfig& cfg,
                                 bool active, const char* actor, const std::string& reason);

xt_risk::state::SafetyState collect_safety_state(const XtHftRunnerControlContext& ctx);

void bootstrap_safety_state(XtHftRunnerControlContext& ctx, const std::string& path);

void save_safety_state_if_due(const std::string& path,
                              const XtHftRunnerControlContext& ctx,
                              xt_risk::state::SafetyState& last_state,
                              int64_t& last_save_ms,
                              int64_t now_ms,
                              int interval_ms,
                              const char* reason);

void maybe_auto_release_closeout_latch(XtHftRunnerControlContext& ctx,
                                       const XtRunnerControlConfig& cfg,
                                       ControlLoopState& st);

}
