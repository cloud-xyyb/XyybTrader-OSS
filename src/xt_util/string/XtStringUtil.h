#pragma once
#include <string>

/// @file XtStringUtil.h
/// @brief 冷路径字符串工具。
///
/// 用于配置解析、日志格式化和编码转换；热路径字符串操作优先使用固定缓冲区
/// 与 `snprintf` / `memcpy`。
namespace XtStringUtil {

/** @brief 转小写。 */
std::string lower(const std::string& s);
/** @brief 去除所有空白字符后转大写。 */
std::string upper_without_space(const std::string& s);
/** @brief 去除首尾空白字符。 */
std::string trim(const std::string& s);
/** @brief 去除首个 '.' 及其之前的前缀；没有 '.' 时原样返回。 */
std::string strip_dot_prefix(const std::string& s);
/** @brief 解析 double，成功返回 true 并写入 out。 */
bool parse_double(const std::string& s, double& out);

/// @brief 将 GBK 字节串转为 UTF-8；空指针/空串返回空串，转换失败时返回原字节串。
std::string gbk_to_utf8_or_original(const char* s);

}  // namespace XtStringUtil
