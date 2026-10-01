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
    bool        enabled   = false; ///< disabled 时 recorder 只写 JSONL
    std::string root_path;         ///< event-store 根目录；为空时不启用
};

/// @brief 单个 live 策略装配单元。
struct LiveStrategyEntry {
    XtStrategyFactoryCfg factory_cfg; ///< strategy id / plugin path
    XtStrategyInitConfig init_cfg;    ///< 展平后的 strategy init 参数
};

/// @brief 单个 SEL live 调度任务（顶层 `selector.tasks[]` 解析结果）。
///
/// 字段与 BT `xt_bt_sel::SelTaskCfg` 对齐；`XtSelRunner` 启动期逐条转
/// `SelTaskInfo` 注册进 engine，task↔strategy 不匹配 fail-fast。
struct XtSelLiveTaskCfg {
    std::string strategy_id;                 ///< 必填；须匹配 runtime.strategies[].id
    TaskPeriod  period = TaskPeriod::None;   ///< minute / daily / weekly / monthly / yearly
    uint32_t    day    = 1;                  ///< Weekly: 1-7；Monthly: 1-31；Yearly: MMDD
    uint32_t    time   = 1500;               ///< HHMM 触发时分
    bool        strict_time = true;          ///< false = ≥time 即触发（同日去重）
    std::string trdtpl  = "CHINA";           ///< 交易日历模板
    std::string session = "default";
    std::string name;                        ///< 空时注册期派生 `<strategy_id>_task`
};

/// @brief 保存 live yaml 的直接解析结果。
///
/// 字段尽量贴近 YAML 结构和 provider-specific variant；下游 runner 不应在此层
/// 做运行期状态修改。
struct XtLiveParsedConfig {
    std::string cfg_path;                ///< source config file path
    XtLoggerCfg log_cfg;
    /// 首个策略的加载配置；兼容仍读取 single-strategy 字段的 runner。
    XtStrategyFactoryCfg strategy_cfg;
    IMdGateway::Config md_cfg;           ///< provider-neutral MD 配置
    std::string md_provider;             ///< "ctp" / "binance_u" / "xtp"
    gateway_factory::MdProviderCfg md_provider_cfg;  ///< 行情 provider-specific 配置（variant）
    ITradeGateway::Config td_cfg;        ///< provider-neutral TD 配置
    std::string td_provider;             ///< "ctp" / "binance_u" / "xtp"
    gateway_factory::TdProviderCfg td_provider_cfg;  ///< 交易 provider-specific 配置（variant）
    std::vector<std::pair<std::string, std::string>> param_seed_values; ///< 策略参数初始种子（key-value seed params）
    /// 首个策略 init cfg；兼容 single-strategy 调用点。
    XtStrategyInitConfig strategy_init_cfg;
    /// `runtime.strategies` 全量列表；单策略时为 1-element vector。
    std::vector<LiveStrategyEntry> strategies;

    /// 顶层 `selector.tasks[]` 全量列表（SEL runner 消费；其他 runner 忽略）。
    std::vector<XtSelLiveTaskCfg> sel_tasks;

    /// true 时启用 sid-aware routing；false 仅用于单策略诊断。
    bool portfolio_router = true;
};

/// @brief Runner bootstrap 阶段需要构造的公共组件配置。
struct XtLiveBootstrapPlan {
    XtLiveRuntimeStateCfg runtime_state_cfg;
    XtLiveOrderGatewayCfg order_gateway_cfg;
    XtTradeCostConfig live_cost;
    XtFeeRuleBook live_fees;
    XtProcessUtil::RuntimeTuningConfig process_tuning_cfg; ///< 主线程 CPU affinity / priority / mlockall 配置
    XtHftEventLoopCfg event_loop_cfg;
    xt_live::control::XtHftReconcileLoopCfg reconcile_cfg; ///< TD reconcile loop 配置（runtime.td.reconcile.*）
    XtLiveParamDomain::BackendConfig param_backend_cfg; ///< 参数热更新后端配置
    XtLiveLatencyTracker::Config latency_cfg;      ///< 延迟追踪配置
    XtLiveRecorder::Config recording_cfg;          ///< 事件录制配置
    XtEventStoreCfg        event_store_cfg;        ///< fbs sink 配置
    xt_risk::XtRiskMonitor::Config risk_monitor_cfg; ///< Active risk monitor 配置（drawdown / stale 等）
    XtRollEngine::Config roll_cfg;                  ///< 换月引擎配置（roll engine config，可选）
    bool has_roll_cfg = false;                      ///< 是否配置了换月引擎
    int params_wait_timeout_ms = 1000;             ///< 参数加载等待超时 ms
    size_t strategy_history_ticks = 2048;          ///< 策略可见历史 tick 窗口大小
    size_t strategy_history_bars = 512;            ///< 策略可见历史 bar 窗口大小
    bool strategy_debug_signal_log = false;        ///< 策略信号调试日志开关
    uint32_t strategy_debug_signal_log_every_n_ticks = 1; ///< 每 N tick 输出一次信号日志
    std::unordered_map<std::string, xt_runtime::XtStraContractInfo> strategy_contract_info; ///< 合约信息表（instrument → contract info）
};

/// @brief runner-side gRPC operator transport 配置。
///
/// 字段来自 YAML `runtime.control_grpc`；runner 装配层据此创建
/// `XtControlService` gRPC server。
struct XtLiveControlGrpcConfig {
    bool        enabled        = false;
    /// @brief `enabled` 是否由 yaml 显式给出。OSS 版默认 false（无节即无控制面）；
    ///        grpc-OFF build 下未显式请求的控制面由 parser 降级为 disabled
    ///        （warn 不 fatal）；显式 enabled:true + grpc-OFF 仍 fail-closed。
    bool        enabled_explicit = false;
    std::string listen_addr    = "0.0.0.0:9095";
    std::string tls_mode       = "mtls";   ///< "mtls"(默认) | "dev_insecure"(仅 loopback, opt-in)
    std::string tls_ca_path    = "/etc/xtrunner-control-tls/ca.crt";
    std::string tls_cert_path  = "/etc/xtrunner-control-tls/server.crt";
    std::string tls_key_path   = "/etc/xtrunner-control-tls/server.key";
    std::string audit_log_path = "/opt/xyybtrader/var/log/control_audit.jsonl";
};

/// @brief 解析 `runtime.control_grpc` 节（含 grpc-OFF 非显式请求降级 disabled）。
///
/// 「控制面是否 requested」的单一真值源：runner 装配与
/// `validate_control_grpc_node` 都以本函数产出的 `cfg.enabled` 判定，
/// 防 parser/validator 双默认值分歧（节存在但无 `enabled` 键的格）。
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

/// @brief 加载并校验 live config。
///
/// 仅致命加载错误返回 false；非致命解析/校验错误追加到 `validation_errors`。
bool load_live_config(int argc,
                      char** argv,
                      XtLiveConfig& out,
                      std::string& fatal_error,
                      std::vector<std::string>& validation_errors);
