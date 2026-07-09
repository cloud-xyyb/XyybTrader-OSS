#pragma once

#include <initializer_list>
#include <string>
#include <vector>

/// @file
/// @brief YAML unknown-key 和 enum 字段校验入口。
///
/// 本文件声明 unknown-key 路径规则，以及启动期 schema/enum 校验函数。
/// 校验错误追加到 `errors`，不抛异常。

namespace YAML {
class Node;
}

/**
 * @brief 配置 schema 规则集合（unknown-key validation rules）。
 *
 * @details
 * - `allowed_paths`：允许出现的路径模式。
 *   - `*` 匹配单层 map key（不匹配 `[]`）。
 *   - `[]` 匹配 sequence item。
 *   - 示例：`runtime.closeout.windows[].no_open_hhmm`
 * - `open_prefixes`：前缀命中后放开该子树（用于策略自定义参数等动态键）。
 *   - 注意：open_prefix 不自动放行其祖先节点；祖先路径须在 `allowed_paths` 中显式列出。
 *   - 示例：open_prefix 为 `"runtime.strategy"` 时，`allowed_paths` 需包含 `"runtime.strategy"`
 *     或其他能覆盖 `"runtime"` 中间节点的模式，否则 `"runtime"` 本身会报 unknown key。
 */
struct XtConfigSchemaRules {
    std::vector<std::string> allowed_paths;
    std::vector<std::string> open_prefixes;
};

void validate_unknown_config_keys(const YAML::Node& root,
                                  const XtConfigSchemaRules& rules,
                                  std::vector<std::string>& errors,
                                  const char* error_prefix = nullptr);

void validate_enum_field(const YAML::Node& root,
                         const std::string& path,
                         std::initializer_list<const char*> allowed_values,
                         std::vector<std::string>& errors,
                         const char* error_prefix = nullptr,
                         bool case_sensitive = true);
