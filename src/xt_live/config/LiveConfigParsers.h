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

IMdGateway::Config parse_md_gateway_config(const YAML::Node& node);

ITradeGateway::Config parse_trade_gateway_config(const YAML::Node& node);

xt_live::control::XtHftReconcileLoopCfg parse_reconcile_cfg(const YAML::Node& td_node);

CTPMdCfg parse_ctp_md_cfg(const YAML::Node& node);

CTPTdCfg parse_ctp_td_cfg(const YAML::Node& node);

BinanceMdCfg parse_binance_md_cfg(const YAML::Node& node);

BinanceTdCfg parse_binance_td_cfg(const YAML::Node& node);

XTPMdCfg parse_xtp_md_cfg(const YAML::Node& node);

XTPTdCfg parse_xtp_td_cfg(const YAML::Node& node);

std::unordered_map<std::string, xt_runtime::XtStraContractInfo>
parse_stra_contract_info(const YAML::Node& node);

XtLiveRuntimeStateCfg parse_live_runtime_state_config(const YAML::Node& runtime_node,
                                                      const YAML::Node& commodities_node);

void parse_session_sections(const YAML::Node& sections_list_node,
                            std::vector<std::pair<int, int>>& out);

XtLiveOrderGatewayCfg parse_live_order_gateway_config(const YAML::Node& runtime_node);

XtHftEventLoop::Config parse_event_loop_cfg(const YAML::Node& queue_node,
                                            const YAML::Node& md_node);

XtRunnerControlConfig parse_runner_control_config(const YAML::Node& md_node,
                                                  const YAML::Node& debug_node,
                                                  const YAML::Node& strategy_node,
                                                  const YAML::Node& instrument_node,
                                                  const YAML::Node& account_node);

XtProcessUtil::RuntimeTuningConfig parse_runner_process_tuning_config(const YAML::Node& debug_node,
                                                                      const YAML::Node& md_node);

XtLiveRecorder::Config parse_recording_cfg(const YAML::Node& runtime_node);

XtEventStoreCfg parse_event_store_cfg(const YAML::Node& runtime_node);

xt_risk::XtRiskMonitor::Config parse_risk_monitor_cfg(const YAML::Node& runtime_node);
