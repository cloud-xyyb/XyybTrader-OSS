#pragma once
#include <string>

/// @file XtStringUtil.h
/// @brief 冷路径字符串工具。
///
/// 用于配置解析、日志格式化和编码转换；热路径字符串操作优先使用固定缓冲区
/// 与 `snprintf` / `memcpy`。
namespace XtStringUtil {

std::string lower(const std::string& s);

std::string upper_without_space(const std::string& s);

std::string trim(const std::string& s);

std::string strip_dot_prefix(const std::string& s);

bool parse_double(const std::string& s, double& out);

std::string gbk_to_utf8_or_original(const char* s);

}
