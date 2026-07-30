#pragma once
/// @file
/// @brief Live runner YAML 配置的解析结果、装配计划和运行计划。

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "XtLoggerCfg.h"
#include "xt_config/catalog/XtFeeRuleBook.h"
#include "xt_cost/XtTradeCostConfig.h"
#include "api/IMdGateway.h"
#include "api/ITradeGateway.h"
#include "api/XtStrategyRuntimeTypes.h"
#include "xt_live/gateway/GatewayFactory.h"
#include "latency/XtLiveLatencyTracker.h"
#include "recording/XtLiveRecorder.h"
#include "XtLiveOrderGatewayCfg.h"
#include "XtLiveParamDomain.h"
#include "XtLiveRuntimeStateCfg.h"
#include "XtRunnerControlCfg.h"
#include "xt_live/hft/runtime/XtHftEventLoopCfg.h"
#include "xt_live/hft/reconcile/XtHftReconcileLoopCfg.h"
#include "XtStrategyFactoryCfg.h"
#include "XtStrategyInitConfig.h"
#include "xt_risk/engine/XtRiskMonitor.h"
#include "xt_util/runtime/XtProcessUtil.h"
#include "xt_core/market/XtRollEngine.h"

/// @brief recorder 可选 FBS event-store sink 配置。
struct XtEventStoreCfg {
    bool        enabled   = false;
    std::string root_path;
};

/// @brief 单个 live 策略装配单元。
struct LiveStrategyEntry {
    XtStrategyFactoryCfg factory_cfg;
    XtStrategyInitConfig init_cfg;
};

/// @brief 单个 SEL live 调度任务（顶层 `selector.tasks[]` 解析结果）。
///
/// 字段与 BT `xt_bt_sel::SelTaskCfg` 对齐；`XtSelRunner` 启动期逐条转
/// `SelTaskInfo` 注册进 engine，task↔strategy 不匹配 fail-fast。
struct XtSelLiveTaskCfg {
    std::string strategy_id;
    TaskPeriod  period = TaskPeriod::None;
    uint32_t    day    = 1;
    uint32_t    time   = 1500;
    bool        strict_time = true;
    std::string trdtpl  = "CHINA";
    std::string session = "default";
    std::string name;
};

/// @brief 保存 live yaml 的直接解析结果。
///
/// 字段尽量贴近 YAML 结构和 provider-specific variant；下游 runner 不应在此层
/// 做运行期状态修改。
struct XtLiveParsedConfig {
    std::string cfg_path;
    XtLoggerCfg log_cfg;

    XtStrategyFactoryCfg strategy_cfg;
    IMdGateway::Config md_cfg;
    std::string md_provider;
    gateway_factory::MdProviderCfg md_provider_cfg;
    ITradeGateway::Config td_cfg;
    std::string td_provider;
    gateway_factory::TdProviderCfg td_provider_cfg;
    std::vector<std::pair<std::string, std::string>> param_seed_values;

    XtStrategyInitConfig strategy_init_cfg;

    std::vector<LiveStrategyEntry> strategies;

    std::vector<XtSelLiveTaskCfg> sel_tasks;

    bool portfolio_router = true;
};

/// @brief Runner bootstrap 阶段需要构造的公共组件配置。
struct XtLiveBootstrapPlan {
    XtLiveRuntimeStateCfg runtime_state_cfg;
    XtLiveOrderGatewayCfg order_gateway_cfg;
    XtTradeCostConfig live_cost;
    XtFeeRuleBook live_fees;
    XtProcessUtil::RuntimeTuningConfig process_tuning_cfg;
    XtHftEventLoopCfg event_loop_cfg;
    xt_live::control::XtHftReconcileLoopCfg reconcile_cfg;
    XtLiveParamDomain::BackendConfig param_backend_cfg;
    XtLiveLatencyTracker::Config latency_cfg;
    XtLiveRecorder::Config recording_cfg;
    XtEventStoreCfg        event_store_cfg;
    xt_risk::XtRiskMonitor::Config risk_monitor_cfg;
    XtRollEngine::Config roll_cfg;
    bool has_roll_cfg = false;
    int params_wait_timeout_ms = 1000;
    size_t strategy_history_ticks = 2048;
    size_t strategy_history_bars = 512;
    bool strategy_debug_signal_log = false;
    uint32_t strategy_debug_signal_log_every_n_ticks = 1;
    std::unordered_map<std::string, xt_runtime::XtStraContractInfo> strategy_contract_info;
};

/// @brief runner-side gRPC operator transport 配置。
///
/// 字段来自 YAML `runtime.control_grpc`；runner 装配层据此创建
/// `XtControlService` gRPC server。
struct XtLiveControlGrpcConfig {
    bool        enabled        = false;

    bool        enabled_explicit = false;
    std::string listen_addr    = "0.0.0.0:9095";
    std::string tls_mode       = "mtls";
    std::string tls_ca_path    = "/etc/xtrunner-control-tls/ca.crt";
    std::string tls_cert_path  = "/etc/xtrunner-control-tls/server.crt";
    std::string tls_key_path   = "/etc/xtrunner-control-tls/server.key";
    std::string audit_log_path = "/opt/xyybtrader/var/log/control_audit.jsonl";
};

void fill_control_grpc_cfg(const YAML::Node& runtime_node, XtLiveControlGrpcConfig& cfg);

/// @brief Runner 主循环运行期配置。
struct XtLiveRuntimePlan {
    XtRunnerControlConfig control_cfg;
    XtLiveControlGrpcConfig control_grpc_cfg;
};

/// @brief `load_live_config()` 输出的三层配置对象。
struct XtLiveConfig {
    XtLiveParsedConfig parsed;
    XtLiveBootstrapPlan bootstrap;
    XtLiveRuntimePlan runtime;
};

bool load_live_config(int argc,
                      char** argv,
                      XtLiveConfig& out,
                      std::string& fatal_error,
                      std::vector<std::string>& validation_errors);
