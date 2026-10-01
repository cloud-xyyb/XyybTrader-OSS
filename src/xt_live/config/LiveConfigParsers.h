#pragma once

/// @file
/// @brief Live 配置加载期的 leaf parser 声明。
///
/// 每个函数把一个 YAML 节点映射成对应 cfg struct。
/// schema allowlist / 跨字段校验由 validators 负责，parser 只保留本地 fail-fast 约束。

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <yaml-cpp/yaml.h>

#include "api/IMdGateway.h"
#include "api/ITradeGateway.h"
#include "api/XtStrategyRuntimeTypes.h"
#include "XtLiveOrderGatewayCfg.h"
#include "XtLiveRuntimeStateCfg.h"
#include "recording/XtLiveRecorder.h"
#include "xt_live/hft/runtime/XtHftEventLoop.h"
#include "XtRunnerControlCfg.h"
#include "xt_risk/engine/XtRiskMonitor.h"
#include "xt_util/runtime/XtProcessUtil.h"
#include "XtLiveConfig.h"
#include "xt_live/gateway/GatewayFactory.h"

// ── Gateway-common parsers ────────────────────────────────────────────────────
/// @brief 解析 provider-neutral MD 字段。
IMdGateway::Config parse_md_gateway_config(const YAML::Node& node);
/// @brief 解析 provider-neutral TD 字段。
ITradeGateway::Config parse_trade_gateway_config(const YAML::Node& node);
/// @brief 解析 `runtime.td.reconcile.*` 到 venue-neutral reconcile loop 配置；
/// 缺节/缺键走 XtHftReconcileLoopCfg 字段默认值（唯一默认源）。
xt_live::control::XtHftReconcileLoopCfg parse_reconcile_cfg(const YAML::Node& td_node);

// ── Provider-specific parsers ─────────────────────────────────────────────────
/// @brief 解析 CTP 行情接入字段，结果写入 GatewayFactory MD variant。
CTPMdCfg parse_ctp_md_cfg(const YAML::Node& node);
/// @brief 解析 CTP 交易接入字段，结果写入 GatewayFactory TD variant。
CTPTdCfg parse_ctp_td_cfg(const YAML::Node& node);
/// @brief 解析 Binance U 行情接入字段，结果写入 GatewayFactory MD variant。
BinanceMdCfg parse_binance_md_cfg(const YAML::Node& node);
/// @brief 解析 Binance U 交易接入字段，结果写入 GatewayFactory TD variant。
BinanceTdCfg parse_binance_td_cfg(const YAML::Node& node);
/// @brief 解析 XTP 行情接入字段，结果写入 GatewayFactory MD variant。
XTPMdCfg parse_xtp_md_cfg(const YAML::Node& node);
/// @brief 解析 XTP 交易接入字段，结果写入 GatewayFactory TD variant。
XTPTdCfg parse_xtp_td_cfg(const YAML::Node& node);

// ── Strategy / runtime state parsers ──────────────────────────────────────────
/// @brief 从 `commodities` 构建 strategy 可见的合约信息表。
std::unordered_map<std::string, xt_runtime::XtStraContractInfo>
parse_stra_contract_info(const YAML::Node& node);

/// @brief 解析 account / commodity / margin 相关 runtime state 配置。
XtLiveRuntimeStateCfg parse_live_runtime_state_config(const YAML::Node& runtime_node,
                                                      const YAML::Node& commodities_node);

// ── Session / order gateway / event loop parsers ──────────────────────────────
/// @brief 解析 HHMM session section 列表；校验由 validators 负责。
void parse_session_sections(const YAML::Node& sections_list_node,
                            std::vector<std::pair<int, int>>& out);

/// @brief 解析 live order gateway 的 submit / cancel / closeout 配置。
XtLiveOrderGatewayCfg parse_live_order_gateway_config(const YAML::Node& runtime_node);

/// @brief 解析 HFT event loop 队列和批处理配置。
XtHftEventLoop::Config parse_event_loop_cfg(const YAML::Node& queue_node,
                                            const YAML::Node& md_node);

// ── Runner-level parsers ──────────────────────────────────────────────────────
/// @brief 聚合控制环相关 YAML 节点为 HftRunnerControl 配置。
XtRunnerControlConfig parse_runner_control_config(const YAML::Node& md_node,
                                                  const YAML::Node& debug_node,
                                                  const YAML::Node& strategy_node,
                                                  const YAML::Node& instrument_node,
                                                  const YAML::Node& account_node);

/// @brief 解析主线程进程调优配置。
XtProcessUtil::RuntimeTuningConfig parse_runner_process_tuning_config(const YAML::Node& debug_node,
                                                                      const YAML::Node& md_node);

// ── Recording / event store / risk monitor parsers ────────────────────────────
/// @brief 解析 JSONL live recorder 配置。
XtLiveRecorder::Config parse_recording_cfg(const YAML::Node& runtime_node);

/// @brief 解析 recorder 的 FBS event-store sink 配置。
XtEventStoreCfg parse_event_store_cfg(const YAML::Node& runtime_node);

/// @brief 解析 Active risk monitor 配置。
xt_risk::XtRiskMonitor::Config parse_risk_monitor_cfg(const YAML::Node& runtime_node);
