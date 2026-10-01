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

class XtStrategyContext;   // apply_initial_control_enabled 引用（impl 见 .cpp）
class XtLiveParamDomain;

class XtPluginHolder;
struct XtLiveConfig;
namespace xt_risk {
class XtRiskMonitorState;
}
namespace xt_identity { class StrategySidRegistry; }

namespace xt_live::common {

/// @brief 解析 + 校验 live config；失败把原因打到 stderr（XtLogger 尚未 init）。
/// @param runner_name stderr 前缀（如 "XtCtaRunner"）。
bool load_live_config_or_report(int argc, char** argv,
                                const char* runner_name, XtLiveConfig& out);

/// @brief 进程级初始化：XtLogger init + shutdown signal handler + runtime
///        tuning（mlockall）+ Prometheus exposer 启动（XT_HAVE_METRICS 下，
///        端口为 0 跳过）。
void runner_process_init(const XtLiveConfig& cfg, const char* module_tag,
                         std::atomic<bool>& shutdown_flag,
                         std::atomic<int>& shutdown_sig);

/// @brief bootstrap 失败统一退出：置 shutdown_flag + 停 exposer + XtLogger
///        shutdown。
/// @return 恒为 1（进程退出码）。
int runner_fail_exit(std::atomic<bool>& shutdown_flag);

/// @brief 单实例锁；失败时带 holder pid 报错。
/// @param runner_name 报错文案中的进程名（如 "XtCtaRunner"）。
bool lock_single_instance(XtProcessUtil::SingleInstanceLock& lock,
                          const std::string& cfg_path,
                          const char* module_tag, const char* runner_name);

/// @brief 按 cfg.parsed.strategies 逐个 dlopen 策略插件并调 init。
bool load_strategy_plugins(std::vector<std::unique_ptr<XtPluginHolder>>& out,
                           const XtLiveConfig& cfg, const char* module_tag);

/// @brief exec-sink gate 纯判定：部署形态无法兑现 exec sink 回调时拒载。
///
/// gate_single_strategy=false（CTA：单策略引擎真派发 on_order/on_fill）时
/// 单策略放行；=true（SEL：引擎无论单/多策略都不派发 venue lifecycle /
/// on_fill）时一律校验。
inline bool exec_sink_gate_rejects(uint64_t caps, bool is_multi,
                                   bool gate_single_strategy) {
    if (!is_multi && !gate_single_strategy) return false;
    return xt_strategy_engine::exec_sink_caps_violation(caps);
}

/// @brief 校验插件 capability：exec sink callback path 不可用的部署形态下，
///        声明 `kExecSinkRequiredCaps`（RequiresOnFill/RequiresOnOrder 掩码，
///        XtStrategyBase.h SSOT）的策略回调会静默失效，启动期 abort 让 ops
///        立即看到。
/// @param gate_single_strategy  false = 仅多策略校验（CTA：单策略 exec sink
///        可兑现）；true = 单/多策略一律校验（SEL：引擎无 exec sink 派发）。
/// @param deploy_desc  拒绝文案中的部署形态描述（如 "multi-strategy mode"）。
bool validate_strategy_exec_sink_capabilities(
    const std::vector<std::unique_ptr<XtPluginHolder>>& plugins,
    const XtLiveConfig& cfg, const char* module_tag,
    bool gate_single_strategy, const char* deploy_desc);

/// @brief 构造 sid registry 并把 yaml string-keyed per-strategy 风控字段翻译成
///        uint32-keyed 字段（writeback 到 cfg.bootstrap.order_gateway_cfg.risk）。
/// @note 调用时机：策略装载之后、try_build_*（gateway ctor 读 risk cfg）之前。
bool wire_sid_registry(std::unique_ptr<xt_identity::StrategySidRegistry>& out,
                       XtLiveConfig& cfg);

/// @brief 启动期读 `strategy.<sid>.enabled` 初始化 ctx 控制面 enable gate；
///        key 缺失时保持默认（enabled=true）。HFT/CTA/SEL 三 runner 共用。
void apply_initial_control_enabled(XtStrategyContext& ctx, XtLiveParamDomain& pd,
                                   const std::string& sid, const char* module_tag);

/// @brief sid registry 收尾：发布 process-global（EventLoop tap 反查）+ freeze
///        （run-time 新 sid fail-soft）。
void finalize_sid_registry(xt_identity::StrategySidRegistry* registry);

/// @brief 按 frozen registry 尺寸 init per-sid risk runtime state
///        （kill_switch/watchdog 槽位）。
/// @pre risk_monitor_state 已由 XtLiveBootstrap::create() 构造 —— 必须在
///      try_build_*（内含 base.create()）之后调用，否则槽位恒为空，
///      per-sid kill_switch/watchdog 永不生效（frame loop 的
///      sanity_check_per_sid_state 会按尺寸不一致告警）。
void init_per_sid_risk_state(xt_identity::StrategySidRegistry* registry,
                             xt_risk::XtRiskMonitorState* risk_monitor_state);

}  // namespace xt_live::common
