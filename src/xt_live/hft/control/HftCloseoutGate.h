#pragma once

/// @file
/// @brief 收盘强平（closeout）的判定与执行入口。
///
/// 到了收盘窗口，runner 把剩余持仓强制平掉，避免带仓过夜。
/// decide_closeout_gate() 是可单测的纯判定（开没开、在不在窗口、通道可不可用、
/// 节流到没到）；try_runner_force_close() 是控制环每轮调用的执行入口，
/// 具体撤单/下单阶段在实现文件内。

#include <optional>
#include <utility>
#include <vector>

#include <cstdint>

#include "xt_live/control/SafetyStateWire.h"

// XtHftRunnerControlContext 完整定义在 HftRunnerControl.h；本头仅 forward decl。
struct XtHftRunnerControlContext;

enum class ForceCloseResult : uint8_t {
    kNotNeeded,
    kSessionGated,
    kGatewayDown,
    kThrottled,
    kRejectedAmbiguousScope,
    kCancelledFirst,
    kExecuted,
};

/// @brief closeout gate 纯函数输入。
///
/// 时间字段使用 HHMM 和 steady ms；调用方负责在同一控制帧内构造，避免本地时钟、
/// tick 时钟和节流时间戳跨帧混用。
struct CloseoutGateInput {
    bool cfg_enabled = false;
    bool has_windows = false;
    bool session_gate_enabled = false;
    const std::vector<std::pair<int, int>>* session_gate_sections = nullptr;
    const std::vector<int>* force_hhmm_list = nullptr;
    int  local_hhmm = -1;
    int  tick_hhmm  = -1;
    bool td_available = false;
    int64_t tnow_ms = 0;
    int64_t closeout_last_try_ms = 0;
    int64_t closeout_retry_ms = 1000;
};

ForceCloseResult try_runner_force_close(XtHftRunnerControlContext& ctx,
                                        int64_t& closeout_last_try_ms,
                                        int64_t& closeout_force_active_since_ms);

std::optional<ForceCloseResult> decide_closeout_gate(const CloseoutGateInput& in);

[[nodiscard]] bool closeout_should_escalate(int64_t escalate_after_ms,
                                            int64_t force_active_since_ms,
                                            int64_t now_ms) noexcept;

[[nodiscard]] bool closeout_result_trips_kill_switch(ForceCloseResult r) noexcept;

[[nodiscard]] bool kill_switch_source_auto_releasable(uint8_t source) noexcept;

[[nodiscard]] bool closeout_latch_auto_release_decision(bool kill_switch_active,
                                                        uint8_t source,
                                                        bool account_hydrated,
                                                        bool account_flat) noexcept;

[[nodiscard]] bool closeout_boundary_trap(const std::vector<int>& force_hhmm_list,
                                          const std::vector<std::pair<int, int>>& sections);
