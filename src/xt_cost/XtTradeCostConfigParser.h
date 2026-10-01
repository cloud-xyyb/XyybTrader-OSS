#pragma once

#include "XtTradeCostConfig.h"

namespace YAML {
class Node;
}

namespace engine_config {

/// @brief 从 YAML 节点解析全局成交滑点配置。
/// 仅解析 `slippage`；手续费规则由 XtFeeRuleBook 解析。
XtTradeCostConfig parse_trade_cost_config(const YAML::Node& node);

} // namespace engine_config
