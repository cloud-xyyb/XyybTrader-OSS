#include "xt_cost/XtTradeCostConfigParser.h"

#include "xt_config/yaml/ConfigYamlUtil.h"

#include <yaml-cpp/yaml.h>

namespace engine_config {

XtTradeCostConfig parse_trade_cost_config(const YAML::Node& node) {
    XtTradeCostConfig cfg;
    cfg.slippage = XtConfigYamlUtil::as_or<double>(node, "slippage", cfg.slippage);
    return cfg;
}

}
