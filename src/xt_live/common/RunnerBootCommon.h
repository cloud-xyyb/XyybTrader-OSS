#pragma once
/// @file
/// @brief HFT/CTA/SEL runner 共享的 boot 步骤（进程初始化 / 单实例锁 /
///        插件装载 / sid registry）。
///
/// 每个函数只接收被消费的窄参数，不接收 runner 运行时聚合体；
/// engine 特异的初始化留在各 runner composition root。

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "xt_strategy_engine/loader/StrategyExecCaps.h"
#include "xt_util/runtime/XtProcessUtil.h"

class XtStrategyContext;
class XtLiveParamDomain;

class XtPluginHolder;
struct XtLiveConfig;
namespace xt_risk {
class XtRiskMonitorState;
}
namespace xt_identity { class StrategySidRegistry; }

namespace xt_live::common {

bool load_live_config_or_report(int argc, char** argv,
                                const char* runner_name, XtLiveConfig& out);

void runner_process_init(const XtLiveConfig& cfg, const char* module_tag,
                         std::atomic<bool>& shutdown_flag,
                         std::atomic<int>& shutdown_sig);

int runner_fail_exit(std::atomic<bool>& shutdown_flag);

bool lock_single_instance(XtProcessUtil::SingleInstanceLock& lock,
                          const std::string& cfg_path,
                          const char* module_tag, const char* runner_name);

bool load_strategy_plugins(std::vector<std::unique_ptr<XtPluginHolder>>& out,
                           const XtLiveConfig& cfg, const char* module_tag);

inline bool exec_sink_gate_rejects(uint64_t caps, bool is_multi,
                                   bool gate_single_strategy) {
    if (!is_multi && !gate_single_strategy) return false;
    return xt_strategy_engine::exec_sink_caps_violation(caps);
}

bool validate_strategy_exec_sink_capabilities(
    const std::vector<std::unique_ptr<XtPluginHolder>>& plugins,
    const XtLiveConfig& cfg, const char* module_tag,
    bool gate_single_strategy, const char* deploy_desc);

bool wire_sid_registry(std::unique_ptr<xt_identity::StrategySidRegistry>& out,
                       XtLiveConfig& cfg);

void apply_initial_control_enabled(XtStrategyContext& ctx, XtLiveParamDomain& pd,
                                   const std::string& sid, const char* module_tag);

void finalize_sid_registry(xt_identity::StrategySidRegistry* registry);

void init_per_sid_risk_state(xt_identity::StrategySidRegistry* registry,
                             xt_risk::XtRiskMonitorState* risk_monitor_state);

}
