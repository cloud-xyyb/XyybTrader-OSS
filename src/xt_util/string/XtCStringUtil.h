#pragma once

/// @file XtCStringUtil.h
/// @brief 热路径固定 char buffer 的 C-string helper。
///
/// helper 不分配内存、不依赖 locale；截断和空输入都以 null-terminated buffer 表示，
/// 不通过返回值报告错误。

#include <cstddef>

namespace XtCStringUtil {

inline bool is_empty(const char* s) noexcept {
    return s == nullptr || s[0] == '\0';
}

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

}
