#include "xt_config/yaml/YAMLPathUtil.h"

#include <yaml-cpp/yaml.h>

namespace XtConfigYamlUtil {

namespace {

template <typename EmitFn>
void flatten_yaml_node_impl(const YAML::Node& node,
                            const std::string& prefix,
                            bool skip_empty_map_keys,
                            EmitFn&& emit) {
    if (!node || node.IsNull()) return;
    if (node.IsMap()) {
        for (auto it = node.begin(); it != node.end(); ++it) {
            std::string key;
            try {
                key = it->first.as<std::string>();
            } catch (...) {
                continue;
            }
            if (skip_empty_map_keys && key.empty()) continue;
            const std::string child = prefix.empty() ? key : (prefix + "." + key);
            flatten_yaml_node_impl(it->second, child, skip_empty_map_keys, emit);
        }
        return;
    }
    if (node.IsSequence()) {
        for (size_t i = 0; i < node.size(); ++i) {
            const std::string child = prefix.empty() ? std::to_string(i)
                                                     : (prefix + "." + std::to_string(i));
            flatten_yaml_node_impl(node[i], child, skip_empty_map_keys, emit);
        }
        return;
    }
    if (prefix.empty()) return;
    try {
        emit(prefix, node.as<std::string>());
    } catch (...) {
    }
}

}

YAML::Node get_path_node(const YAML::Node& root, const std::string& path) {
    if (path.empty()) return YAML::Clone(root);
    YAML::Node cur = YAML::Clone(root);
    size_t start = 0;
    while (start < path.size()) {
        size_t dot = path.find('.', start);
        std::string key = (dot == std::string::npos) ? path.substr(start)
                                                      : path.substr(start, dot - start);
        if (!cur || !cur.IsDefined()) return YAML::Node();

        const YAML::Node ccur = cur;
        YAML::Node next = ccur[key];
        if (!next.IsDefined() || next.IsNull()) return YAML::Node();
        cur = next;
        if (dot == std::string::npos) break;
        start = dot + 1;
    }
    return cur;
}

void flatten_yaml_node_to_map(const YAML::Node& node,
                              const std::string& prefix,
                              std::unordered_map<std::string, std::string>& out,
                              bool skip_empty_map_keys) {
    flatten_yaml_node_impl(
        node,
        prefix,
        skip_empty_map_keys,
        [&](const std::string& key, const std::string& value) { out[key] = value; });
}

void flatten_yaml_node_to_pairs(const YAML::Node& node,
                                const std::string& prefix,
                                std::vector<std::pair<std::string, std::string>>& out,
                                bool skip_empty_map_keys) {
    flatten_yaml_node_impl(
        node,
        prefix,
        skip_empty_map_keys,
        [&](const std::string& key, const std::string& value) { out.emplace_back(key, value); });
}

}
