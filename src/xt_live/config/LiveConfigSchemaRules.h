#pragma once

/// @file
/// @brief live config 的 schema 规则表。
///
/// allowed_paths 精确列出可识别 key，open_prefixes 放行动态子树；
/// 新增或删除 yaml key 时必须同步维护。

#include "ConfigSchemaValidator.h"

XtConfigSchemaRules build_live_schema_rules();
