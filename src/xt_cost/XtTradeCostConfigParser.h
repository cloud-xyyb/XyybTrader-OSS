#pragma once

#include "XtTradeCostConfig.h"

namespace YAML {
class Node;
}

namespace engine_config {

XtTradeCostConfig parse_trade_cost_config(const YAML::Node& node);

}
