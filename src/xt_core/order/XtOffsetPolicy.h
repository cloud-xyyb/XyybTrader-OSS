#pragma once

#include <cstdint>
#include <string>

/// @file
/// @brief 品种级开平路由策略及配置解析 helper。

enum class XtOffsetPolicy : uint8_t {
    Inherit     = 0,
    OpenAllowed = 1,
    CloseFirst  = 2,
};

inline bool parse_offset_policy(const std::string& s, XtOffsetPolicy& out) {
    if (s == "close_first")  { out = XtOffsetPolicy::CloseFirst;  return true; }
    if (s == "open_allowed") { out = XtOffsetPolicy::OpenAllowed; return true; }
    return false;
}
