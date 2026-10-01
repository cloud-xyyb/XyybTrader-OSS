#pragma once

/// @file XtCStringUtil.h
/// @brief 热路径固定 char buffer 的 C-string helper。
///
/// helper 不分配内存、不依赖 locale；截断和空输入都以 null-terminated buffer 表示，
/// 不通过返回值报告错误。

#include <cstddef>

namespace XtCStringUtil {

/// @brief 判断 C-string 是否为 null 或空串。
///
/// 只检查首字节，不扫描完整字符串；适合热路径入参 guard。
inline bool is_empty(const char* s) noexcept {
    return s == nullptr || s[0] == '\0';
}

/// @brief 安全拷贝到固定 char buffer（bounded C-string copy）。
///
/// 保证目标缓冲区以 '\0' 结尾；src 为 null 时写入空串。输入过长时静默截断到
/// `cap - 1` 字节，不返回截断状态。
inline void copy(char* dst, std::size_t cap, const char* src) noexcept {
    if (!dst || cap == 0) return;
    if (!src) {
        dst[0] = '\0';
        return;
    }
    std::size_t n = 0;
    while (n + 1 < cap && src[n] != '\0') {
        dst[n] = src[n];
        ++n;
    }
    dst[n] = '\0';
}

template <std::size_t N>
inline void copy(char (&dst)[N], const char* src) noexcept {
    copy(dst, N, src);
}

}  // namespace XtCStringUtil
