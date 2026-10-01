#pragma once

/// @file
/// @brief live config 的 validator 与跨字段校验入口。
///
/// validator 负责校验解析前后的 yaml/cfg 组合约束，错误统一追加到 `errors` sink。
/// 细粒度 validate_* helper 留在 `.cpp` anonymous namespace。

#include <string>
#include <vector>
#include <yaml-cpp/yaml.h>

#include "api/IMdGateway.h"               // IMdGateway::Config nested type
#include "api/ITradeGateway.h"            // ITradeGateway::Config nested type
#include "XtLoggerCfg.h"                  // XtLoggerCfg full definition
#include "XtStrategyFactoryCfg.h"         // XtStrategyFactoryCfg full definition (.validate)

/// @brief Live config 主校验入口；错误追加到 `errors`。
///
/// 同时校验 YAML 原始节点和已解析 cfg，覆盖类型、范围、废弃 key 和跨字段约束。
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

/// @brief 校验 `runtime.strategies` 的单策略/多策略形态。
bool validate_strategy_or_strategies_cfg(const YAML::Node& runtime_node,
                                         std::string& err);

/// @brief 校验 DeclaredUniverse 规模不超过 `xt_live::kMaxDeclaredSymbols`。
///
/// 独立于 `validate_live_config`：后者跑在 `runtime.md` 解析之后、declared
/// universe 装配之前，此时看到的 `md_cfg.subs` 还不是最终集合。由装配点在
/// universe 落地后调用。
///
/// @param declared `collect_declared_universe()` 的产出（已去重）。
void validate_declared_universe_size(const std::vector<std::string>& declared,
                                     std::vector<std::string>& errors);
