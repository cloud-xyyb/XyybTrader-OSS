#pragma once

#include <string>
#include <vector>

/// @brief 策略工厂配置：策略 id 与插件路径。
struct XtStrategyFactoryCfg {
    std::string id      = "strategy";  ///< 策略唯一标识
    std::string so_path;               ///< 动态库路径；安全校验由 XtStraLoader 执行。

    /** @brief 校验配置有效性；发现错误时追加到 errors，不清空调用方已有内容。 */
    void validate(std::vector<std::string>& errors) const;
};

namespace YAML { class Node; }

/// @brief 从 YAML strategy 节点解析策略工厂配置（id / so_path）。
XtStrategyFactoryCfg parse_strategy_factory_config(const YAML::Node& node);
