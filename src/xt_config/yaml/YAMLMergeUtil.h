#pragma once

#include <string>

#include <yaml-cpp/yaml.h>

/// @file
/// @brief YAML overlay 合并与 profile 加载 helper。

namespace XtConfigYamlUtil {

/// @brief 深合并两棵 YAML 树。
///
/// map 节点按 key 递归合并；scalar、sequence 或类型不一致时 overlay 覆盖 base。
/// 返回节点不共享调用方传入的根节点，调用方可安全继续修改结果。
YAML::Node deep_merge(const YAML::Node& base, const YAML::Node& overlay);

/// @brief 加载 profile YAML，并在同目录存在 common 文件时先合并 common。
///
/// profile_path 必须存在；common 文件缺失时直接返回 profile。
/// 合并优先级为 common < profile。
bool load_profile_with_common(const std::string& profile_path,
                              const std::string& common_file_name,
                              YAML::Node& out,
                              std::string* err);

/// @brief 加载 profile、可选 common、必需 market，并把同目录 secrets.yaml 作为最终 overlay。
///
/// 合并优先级为 common < profile < market < secrets。
/// `market_file_name` 缺失时返回 false；`err` 非空时写入失败原因。
bool load_profile_with_common_and_market(const std::string& profile_path,
                                         const std::string& common_file_name,
                                         const std::string& market_file_name,
                                         YAML::Node& out,
                                         std::string* err);

}  // namespace XtConfigYamlUtil
