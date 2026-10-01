#pragma once

/// @file
/// @brief 控制环每轮各阶段（采样 / 策略链 / 收盘强平 / 持久化）的函数入口。
///
/// `HftRunnerControl::run()` 按固定顺序调用本文件声明的 stage 函数；
/// 各 stage 的实现按职责分布在同目录 HftControl*.cpp。

#include <atomic>
#include <cstdint>
#include <string>

#include "xt_live/hft/control/HftRunnerControlState.h"  // LoopFrame / ControlSnapshot / ControlLoopState + XtHftRunnerControlContext / XtRunnerControlConfig

namespace xt_risk::state {
struct SafetyState;
}

namespace xt_live::control::detail {

// ── 采样与推进（HftControlFrameSample.cpp） ─────────────────────────────

/// @brief 采样单轮控制帧；同一帧内复用同一个 now/snapshot 口径。
/// @note 副作用推进（risk frame / health check）先于采样执行；采样函数保持只读。
LoopFrame collect_loop_frame(const XtRunnerControlConfig& cfg,
                             XtHftRunnerControlContext& ctx,
                             ControlLoopState& st);

// ── 策略链（HftControlPolicyChain.cpp） ─────────────────────────────────

struct ConsistencyThresholds {
    uint64_t warn_loops = 1;
    uint64_t block_loops = 1;
    uint64_t kill_loops = 1;
};

/// @brief 归一化一致性阈值（warn/block/kill）。
/// @note 每个阈值至少为 1（配置 0 会使策略链立即触发）；并保证 block >= warn、
///       kill >= block，维持「告警→阻断→停机」的单调升级顺序。
ConsistencyThresholds make_consistency_thresholds(const XtRunnerControlConfig& cfg);

/// @brief 写单轮 runtime 统计摘要日志（log_stats 关闭时 no-op）。
void log_control_stats(const ControlSnapshot& s, uint64_t latency_sla_breach_total);

/// @brief 执行 heartbeat / consistency / failure / latency SLA 策略链。
/// @return false = 策略链已请求 shutdown，调用方应退出主循环。
bool apply_policy_chain(std::atomic<bool>& shutdown,
                        const XtRunnerControlConfig& cfg,
                        XtHftRunnerControlContext& ctx,
                        const ConsistencyThresholds& thresholds,
                        const LoopFrame& frame,
                        ControlLoopState& st);

// ── 收盘强平协调（HftControlForceClose.cpp） ────────────────────────────

/// @brief 每轮尝试收盘强平，根据返回结果驱动连续失败升级（warn → block trading）。
/// @note closeout 采用逐轮尝试 + streak 兜底；独立 Closeout FSM 可避免窗口边界
///       下的状态丢失，但会改变执行状态机，本轮只保留为重构建议。
void run_force_close_once(XtHftRunnerControlContext& ctx,
                          const XtRunnerControlConfig& cfg,
                          ControlLoopState& st);

// ── safety latch 持久化 / kill-switch 可观测（HftControlSafetyState.cpp） ──

/// @brief kill_switch auto trip / release 统一可观测性：panel 因果链 hook +
///        prometheus counter/gauge。
void emit_kill_switch_transition(XtHftRunnerControlContext& ctx,
                                 const XtRunnerControlConfig& cfg,
                                 bool active, const char* actor, const std::string& reason);

/// @brief 从 ctx 当前 atomic 状态采集 SafetyState（read-only，不修改任何状态）。
xt_risk::state::SafetyState collect_safety_state(const XtHftRunnerControlContext& ctx);

/// @brief 启动期一次性 load + apply。失败/缺失走 default。
void bootstrap_safety_state(XtHftRunnerControlContext& ctx, const std::string& path);

/// @brief 周期写盘：state changed since last save OR 间隔超过 cfg。
///  - last_state in/out：调用方持有上一次落盘的快照；本函数更新；
///  - last_save_ms in/out：上次落盘的 monotonic ms；本函数更新。
/// @param reason "bootstrap"/"frame"/"shutdown"；其中 "bootstrap"/"shutdown"
///        强制落盘（绕过 changed/间隔判定），并进入 log。
void save_safety_state_if_due(const std::string& path,
                              const XtHftRunnerControlContext& ctx,
                              xt_risk::state::SafetyState& last_state,
                              int64_t& last_save_ms,
                              int64_t now_ms,
                              int interval_ms,
                              const char* reason);

/// @brief 冷启 hydrate 完成后一次性自动释放 closeout-block 自动 trip 的 kill_switch。
/// @note bootstrap 时 account_store 尚未 hydrate，早判会假阳性 flat 误释放，故移到
///       hydrate 后判定；manual / 旧文件 latch (source != AutoCloseoutBlock) 一律保留，
///       需人工复位。
void maybe_auto_release_closeout_latch(XtHftRunnerControlContext& ctx,
                                       const XtRunnerControlConfig& cfg,
                                       ControlLoopState& st);

}  // namespace xt_live::control::detail
