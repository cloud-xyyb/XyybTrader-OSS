#pragma once

/// @file
/// @brief live config 的 validator 与跨字段校验入口。
///
/// validator 负责校验解析前后的 yaml/cfg 组合约束，错误统一追加到 `errors` sink。
/// 细粒度 validate_* helper 留在 `.cpp` anonymous namespace。

#include <string>
#include <vector>
#include <yaml-cpp/yaml.h>

#include "api/IMdGateway.h"
#include "api/ITradeGateway.h"
#include "XtLoggerCfg.h"
#include "XtStrategyFactoryCfg.h"

void validate_live_config(const YAML::Node& runtime_node,
                          const YAML::Node& queue_node,
                          const YAML::Node& commodities_node,
                          const YAML::Node& fees_node,
                          const XtLoggerCfg* log_cfg,
                          const YAML::Node& stra_node,
                          const XtStrategyFactoryCfg* stra_cfg,
                          const YAML::Node& md_node,
                          const IMdGateway::Config* md_cfg,
                          const YAML::Node& td_node,
                          const ITradeGateway::Config* td_cfg,
                          std::vector<std::string>& errors);

bool validate_strategy_or_strategies_cfg(const YAML::Node& runtime_node,
                                         std::string& err);

bool live_selector_section_present(const YAML::Node& root);
