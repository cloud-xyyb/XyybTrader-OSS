#pragma once

/// @file secure_string_yaml.h
/// @brief yaml-cpp 到 secure_string 的窄接口。

#include <string>

#include <yaml-cpp/yaml.h>

#include "xt_util/string/secure_string.h"

namespace XtUtil {

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

}
