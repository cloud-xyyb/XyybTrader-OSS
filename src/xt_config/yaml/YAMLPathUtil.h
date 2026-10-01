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

/// @brief 按点分路径定位 YAML 子节点；路径无效时返回空节点。
/// @note 路径段按 map key 解释；此函数不解析 sequence 下标，空路径返回 root clone。
YAML::Node get_path_node(const YAML::Node& root, const std::string& path);

/**
 * @brief 将 YAML 树递归展平为 map。
 * @param prefix 键前缀，递归时用 "." 拼接。
 * @param skip_empty_map_keys 跳过空键的 map 条目。
 * @note sequence item 使用数字路径段；不能转换为 string 的叶子节点会被跳过。
 */
void flatten_yaml_node_to_map(const YAML::Node& node,
                              const std::string& prefix,
                              std::unordered_map<std::string, std::string>& out,
                              bool skip_empty_map_keys = false);

/**
 * @brief 将 YAML 树递归展平为有序 pair 列表。
 * @param prefix 键前缀，递归时用 "." 拼接。
 * @param skip_empty_map_keys 跳过空键的 map 条目。
 * @note 保留 DFS 遍历顺序；相同 key 不去重。
 */
void flatten_yaml_node_to_pairs(const YAML::Node& node,
                                const std::string& prefix,
                                std::vector<std::pair<std::string, std::string>>& out,
                                bool skip_empty_map_keys = true);

}  // namespace XtConfigYamlUtil
