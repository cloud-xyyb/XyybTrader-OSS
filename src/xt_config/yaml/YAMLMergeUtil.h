#pragma once

#include <string>

#include <yaml-cpp/yaml.h>

/// @file
/// @brief YAML overlay 合并与 profile 加载 helper。

namespace XtConfigYamlUtil {

YAML::Node deep_merge(const YAML::Node& base, const YAML::Node& overlay);

bool load_profile_with_common(const std::string& profile_path,
                              const std::string& common_file_name,
                              YAML::Node& out,
                              std::string* err);

bool load_profile_with_common_and_market(const std::string& profile_path,
                                         const std::string& common_file_name,
                                         const std::string& market_file_name,
                                         YAML::Node& out,
                                         std::string* err);

}
