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

[[nodiscard]] bool kill_switch_source_latches_across_restart(uint8_t source) noexcept;

namespace xt_live::control {

/// @brief safety state 持久化的非拥有依赖；全部指向 runner composition root 持有对象。
///
/// @pre `signals` 与 `order_gateway` 非空；其余指针可空（对应恢复/采集段跳过）。
struct SafetyStateWireDeps {
    XtRunnerSignals*             signals = nullptr;
    xt_risk::XtRiskMonitor*      risk_monitor = nullptr;
    xt_risk::XtRiskMonitorState* risk_monitor_state = nullptr;
    XtLiveOrderGateway*          order_gateway = nullptr;
    const xt_account::AccountStateStore* account_store = nullptr;

    bool restore_auto_liquidation = false;
    const char* module_tag = "RT";
};

xt_risk::state::SafetyState collect_safety_state(const SafetyStateWireDeps& deps);

void apply_safety_state_on_bootstrap(const SafetyStateWireDeps& deps,
                                     const xt_risk::state::SafetyState& s);

void bootstrap_safety_state(const SafetyStateWireDeps& deps, const std::string& path);

[[nodiscard]] uint64_t safety_state_quarantine_count() noexcept;

void save_safety_state_if_due(const std::string& path,
                              const SafetyStateWireDeps& deps,
                              xt_risk::state::SafetyState& last_state,
                              int64_t& last_save_ms,
                              int64_t now_ms,
                              int interval_ms,
                              const char* reason);

}
