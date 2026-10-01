#pragma once
#include <cstddef>
#include <cstdint>
#include <string>

#include "xt_util/string/XtStringUtil.h"
#include "xt_util/time/XtTimeParse.h"

/// @file
/// @brief YAML typed-read 与宽松标量解析 helper。
///
/// 本文件提供启动期配置解析用的 header-only helper：日期、布尔值、泛型 typed-read
/// 和正整数读取。解析失败不抛异常，返回调用方默认值或 helper 约定空值。
namespace XtConfigYamlUtil {

/// @brief 解析日期节点；支持字符串 "2022-01-01" 和整数 20220101。
/// @return 成功时返回 YYYYMMDD；缺失、空值或无法解析时返回 0。
template <typename NodeT>
inline uint32_t parse_date_node(const NodeT& n) {
    if (!n || !n.IsDefined() || n.IsNull()) return 0;
    try {
        std::string s = n.template as<std::string>();
        uint32_t d = XtTimeParse::parse_date_only(s);
        if (d) return d;
    } catch (...) {
    }
    try {
        return n.template as<uint32_t>();
    } catch (...) {
    }
    return 0;
}

/// @brief 宽松解析布尔值节点；支持 "yes"/"on"/"1" 等 scalar 表示。
/// @return 缺失、空值、非 scalar 或无法解析时返回调用方给定默认值。
template <typename NodeT>
inline bool parse_bool_node(const NodeT& n, bool def) {
    if (!n || n.IsNull()) return def;
    try {
        return n.template as<bool>();
    } catch (...) {
    }
    if (!n.IsScalar()) return def;
    try {
        std::string s = n.template as<std::string>();
        s = XtStringUtil::lower(s);
        if (s == "1" || s == "true" || s == "yes" || s == "on") return true;
        if (s == "0" || s == "false" || s == "no" || s == "off") return false;
    } catch (...) {
    }
    return def;
}

/// @brief 泛型 typed-read：读取 `parent[key]`，缺失或类型转换失败返回默认值。
/// @note 不向外抛出 YAML 转换异常；适用于启动期配置解析的容错默认值。
///       `NodeT` 同时支持 const / non-const `YAML::Node`。
template <typename T, typename NodeT>
inline T as_or(const NodeT& parent, const char* key, const T& def_val) {
    if (!parent || parent.IsNull() || !parent[key]) return def_val;
    try {
        return parent[key].template as<T>();
    } catch (...) {
        return def_val;
    }
}

/// @brief 读取正整数 size_t；缺失、非数字或 <=0 时返回默认值。
/// @note 先按 signed 宽整数读取，避免 YAML 中的负数转换为无符号大值。
template <typename NodeT>
inline std::size_t as_positive_size_or(const NodeT& parent, const char* key, std::size_t def_val) {
    if (!parent || parent.IsNull() || !parent[key]) return def_val;
    try {
        const long long v = parent[key].template as<long long>();
        if (v <= 0) return def_val;
        return static_cast<std::size_t>(v);
    } catch (...) {
        return def_val;
    }
}

}  // namespace XtConfigYamlUtil
