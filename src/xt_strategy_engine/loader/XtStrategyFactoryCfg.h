#pragma once

#include <string>
#include <vector>

/// @brief 策略工厂配置：策略 id 与插件路径。
struct XtStrategyFactoryCfg {
    std::string id      = "strategy";
    std::string so_path;

    void validate(std::vector<std::string>& errors) const;
};

namespace YAML { class Node; }

XtStrategyFactoryCfg parse_strategy_factory_config(const YAML::Node& node);
