#pragma once

/// @file
/// @brief live config 的 schema 规则表。
///
/// allowed_paths 精确列出可识别 key，open_prefixes 放行动态子树；
/// 新增或删除 yaml key 时必须同步维护。

#include "ConfigSchemaValidator.h"  // XtConfigSchemaRules

/// @brief 构造 live config schema 规则集合。
///
/// `allowed_paths` 精确登记固定 key；`open_prefixes` 放行策略参数等动态子树。
XtConfigSchemaRules build_live_schema_rules();
