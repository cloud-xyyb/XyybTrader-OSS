#pragma once

/// @file
/// @brief session filter 的 YAML 解析入口。
///
/// 解析入口留在 backtest infra；`xt_core/session/XtSessionFilter.h` 承担运行时判定模型，
/// 不承载 yaml-cpp 这类 config-layer 依赖。

#include "xt_core/session/XtSessionFilter.h"

namespace YAML {
class Node;
}

namespace xt_core {

/// @brief 从 YAML 节点解析 `xt_core::Config`。
///
/// 空节点、null 或 `enabled=false` 返回 disabled config。`enabled=true` 但 `sections`
/// 缺失或非数组时，返回 `enabled=true` 且 `sections` 为空；调用方以 `is_enabled(cfg)`
/// 判断实际启用状态。`sections` 接受标量字符串数组，单项格式为 `<start>-<end>`，
/// 时间格式沿用 `XtParseUtil::parse_time_with_ms`。数组路径会跳过非法 section，
/// 并在返回前执行 `normalize()`。
Config parse_config(const YAML::Node& node);

}  // namespace xt_core
