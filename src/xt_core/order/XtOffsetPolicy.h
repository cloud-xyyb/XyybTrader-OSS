#pragma once

#include <cstdint>
#include <string>

/// @file
/// @brief 品种级开平路由策略枚举与配置解析。
///
/// 有的品种允许同时持多空（锁仓）没问题，有的品种在高频下容易因「反向单发成了开仓」而积累
/// 锁仓对，宁可先平掉对侧再开。本文件的策略枚举就表达这个选择：`OpenAllowed` 让 Open 直通，
/// `CloseFirst` 优先用对侧可平腿改写成平仓；`Inherit` 只表示 catalog 未配、由调用方按 venue
/// 默认消解。

/// @brief 品种级开平路由策略。
///
/// `OpenAllowed` 让策略 Open 请求直通；`CloseFirst` 优先用对侧可平腿改写为平仓。
/// `Inherit` 只表示 catalog 未配置，调用方应在进入路由层前按 venue 默认值消解。
enum class XtOffsetPolicy : uint8_t {
    Inherit     = 0,  ///< 未配置；按 venue 默认解析（catalog 层消解，路由层不见）
    OpenAllowed = 1,  ///< Open 直通（默认；允许锁仓）
    CloseFirst  = 2,  ///< 平仓优先：改写 + 反向在途 defer
};


/// @brief 解析配置字符串；合法写入 out 返 true，非法返 false（调用方决定 fail 策略）。
inline bool parse_offset_policy(const std::string& s, XtOffsetPolicy& out) {
    if (s == "close_first")  { out = XtOffsetPolicy::CloseFirst;  return true; }
    if (s == "open_allowed") { out = XtOffsetPolicy::OpenAllowed; return true; }
    return false;
}
