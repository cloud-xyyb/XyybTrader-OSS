#pragma once

/// @file secure_string_yaml.h
/// @brief yaml-cpp 到 secure_string 的窄接口。

#include <string>

#include <yaml-cpp/yaml.h>

#include "xt_util/string/secure_string.h"

namespace XtUtil {

/// @brief 从 YAML::Node 读取 scalar 字符串并立刻封装到 secure_string。
///
/// @param node  yaml-cpp 节点；nil / null / 非 scalar 会返回空 secure_string
/// @return      持有 secret 的 secure_string；空字符串与不存在节点均返回 empty()
inline secure_string secure_string_from_yaml(const YAML::Node& node) {
    if (!node || node.IsNull() || !node.IsScalar()) {
        return secure_string{};
    }
    std::string s;
    try {
        s = node.as<std::string>();
    } catch (...) {
        return secure_string{};
    }
    return secure_string{std::move(s)};
}

}  // namespace XtUtil
