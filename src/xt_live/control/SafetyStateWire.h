#pragma once

/// @file
/// @brief latched safety state（safety.bin）持久化的跨引擎共享 wire。
///
/// collect / apply / bootstrap / save 四步由各 runner composition root 编排：
/// HFT 在 HftRunnerControl 控制线程（经 detail:: 薄 wrapper 从
/// XtHftRunnerControlContext 构 deps）；CTA/SEL 在 runner main 进 1Hz 帧循环前
/// bootstrap restore，周期落盘经 frame_tail_hook（RunnerFrameLoop 单注入点契约
/// 不破），loop 退出后 shutdown 强制落盘。

#include <cstdint>
#include <string>

class XtLiveOrderGateway;
struct XtRunnerSignals;

namespace xt_account {
class AccountStateStore;
}
namespace xt_risk {
class XtRiskMonitor;
class XtRiskMonitorState;
}
namespace xt_risk::state {
struct SafetyState;
}

/// @brief kill_switch_source 是否跨重启回灌 kill latch（bootstrap restore 单源谓词）。
///
/// ReconcileDivergence 是自动条件：重启走 exchange-as-truth recovery 重建账本后，
/// 由新进程 reconcile loop 重评（分歧仍在会在 kill_after_n 周期内重新拉起），
/// 不做持久化单向闩锁。manual(None) / AutoCloseoutBlock / UnmatchedBudget 与
/// 未知来源照常回灌（fail-safe 保留保护；后两者由冷启 flat 自动释放接管）。
[[nodiscard]] bool kill_switch_source_latches_across_restart(uint8_t source) noexcept;

namespace xt_live::control {

/// @brief safety state 持久化的非拥有依赖；全部指向 runner composition root 持有对象。
///
/// @pre `signals` 与 `order_gateway` 非空；其余指针可空（对应恢复/采集段跳过）。
struct SafetyStateWireDeps {
    XtRunnerSignals*             signals = nullptr;            ///< kill_switch + source atomics
    xt_risk::XtRiskMonitor*      risk_monitor = nullptr;       ///< restore_latches + broadcast_state（可空）
    xt_risk::XtRiskMonitorState* risk_monitor_state = nullptr; ///< per-sid kill / auto-liq latch（可空）
    XtLiveOrderGateway*          order_gateway = nullptr;      ///< annual latch snapshot/restore
    const xt_account::AccountStateStore* account_store = nullptr;  ///< frozen_cash + risk_fund（可空）
    /// per-sid auto-liquidation latch 是否随 bootstrap 回灌。仅 HFT 为 true：
    /// disarm owner（PerSidLiquidator 的 confirmed-flat gate）只存在于 HFT 控制环，
    /// CTA/SEL 回灌会产生无人能解除的「清算中」僵死态。collect 侧不受此开关影响
    /// （按 in-memory 状态落盘）；注意：skip-restore 的引擎首次 forced save 即以
    /// in-memory 全零覆写磁盘 latch——同 instance_id 跨引擎复用不属支持形态
    /// （topology SSOT 下不会发生），per-sid kill 回灌不受影响。
    bool restore_auto_liquidation = false;
    const char* module_tag = "RT";                             ///< 日志 tag（"RT"/"CTA"/"SEL"）
};

/// @brief 从 deps 当前 atomic 状态采集 SafetyState（read-only，不修改任何状态）。
xt_risk::state::SafetyState collect_safety_state(const SafetyStateWireDeps& deps);

/// @brief 启动期恢复 SafetyState：latch 字段只从 false→true，peak_equity 取 max。
void apply_safety_state_on_bootstrap(const SafetyStateWireDeps& deps,
                                     const xt_risk::state::SafetyState& s);

/// @brief 启动期一次性 load + apply。
///
/// - 文件缺失 → 冷启动默认（行为不变）；
/// - 文件存在但校验失败（损坏）→ fail-closed：先 quarantine 原件（保留证据，先于任何落盘），
///   再以 synthetic kill_switch+open_halted 带闩启动（source=manual，不被 auto-release 解除），
///   并递增 quarantine 计数；
/// - 文件有效 → 正常 restore 全部 latch。
void bootstrap_safety_state(const SafetyStateWireDeps& deps, const std::string& path);

/// @brief safety.bin quarantine 事件累计数（进程内 atomic，非 prometheus）。
///
/// bootstrap 检测到文件存在但校验失败并 quarantine 时递增；供测试与诊断读取。
[[nodiscard]] uint64_t safety_state_quarantine_count() noexcept;

/// @brief 周期写盘：state changed since last save OR 间隔超过 interval_ms。
///  - last_state in/out：调用方持有上一次落盘的快照；本函数更新；
///  - last_save_ms in/out：上次落盘的 monotonic ms；本函数更新。
/// @param reason "bootstrap"/"frame"/"shutdown"；其中 "bootstrap"/"shutdown"
///        强制落盘（绕过 changed/间隔判定），并进入 log。
void save_safety_state_if_due(const std::string& path,
                              const SafetyStateWireDeps& deps,
                              xt_risk::state::SafetyState& last_state,
                              int64_t& last_save_ms,
                              int64_t now_ms,
                              int interval_ms,
                              const char* reason);

}  // namespace xt_live::control
