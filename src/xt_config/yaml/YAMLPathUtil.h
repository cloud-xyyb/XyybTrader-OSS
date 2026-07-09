#pragma once

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

/// @file
/// @brief YAML dot-path 查询与 flatten helper。
///
/// `get_path_node()` 读取点分 map 路径；flatten helpers 将 YAML 树展平成 map
/// 或有序 pair 列表。调用方负责加载 YAML 和后续类型转换。

namespace YAML {
class Node;
}

namespace XtConfigYamlUtil {

YAML::Node get_path_node(const YAML::Node& root, const std::string& path);

void flatten_yaml_node_to_map(const YAML::Node& node,
                              const std::string& prefix,
                              std::unordered_map<std::string, std::string>& out,
                              bool skip_empty_map_keys = false);

void flatten_yaml_node_to_pairs(const YAML::Node& node,
                                const std::string& prefix,
                                std::vector<std::pair<std::string, std::string>>& out,
                                bool skip_empty_map_keys = true);

}
