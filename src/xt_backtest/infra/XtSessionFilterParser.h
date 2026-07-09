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

Config parse_config(const YAML::Node& node);

}
