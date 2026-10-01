#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

/// @file
/// @brief FNV-1a hash 与 transparent string lookup helper。
///
/// 本文件集中提供项目内 FNV-1a 常量、流式/单次 hash 函数，以及用于 string-keyed
/// `unordered_map` 的透明 hash/equal 类型。

namespace xt_core::sync {

/// FNV-1a 64-bit 标准常量（项目内字符串/字节 hash 的 SSOT）。
inline constexpr uint64_t kFnv1a64OffsetBasis = 14695981039346656037ULL;
inline constexpr uint64_t kFnv1a64Prime       = 1099511628211ULL;

/// @brief FNV-1a 64-bit 流式更新：把 [data, data+len) 混入既有 hash 状态。
/// @note 初始 state 取 `kFnv1a64OffsetBasis`；可多次调用累积（顺序敏感）。
/// @pre `data` 仅在 `len == 0` 时允许为 nullptr。
inline uint64_t fnv1a_update(uint64_t h, const void* data, size_t len) noexcept {
    const auto* p = static_cast<const unsigned char*>(data);
    for (size_t i = 0; i < len; ++i) {
        h ^= p[i];
        h *= kFnv1a64Prime;
    }
    return h;
}

/// @brief FNV-1a 64-bit single-shot：对 [data, data+len) 求 hash。
inline uint64_t fnv1a_bytes(const void* data, size_t len) noexcept {
    return fnv1a_update(kFnv1a64OffsetBasis, data, len);
}

/// @brief 透明 string hash/equal 的 hash 侧实现，委托到 `fnv1a_bytes`（同一 SSOT）。
///
/// @note C++17 `std::unordered_map::find` 仍可能构造 `std::string` 临时对象；真正的
/// heterogeneous lookup 取决于标准库和编译标准支持。
struct TransparentStringHash {
    using is_transparent = void;

    size_t operator()(std::string_view s) const noexcept {
        return static_cast<size_t>(fnv1a_bytes(s.data(), s.size()));
    }

    /// @pre `s` 不能为 nullptr；空字符串请传 `""`。
    size_t operator()(const char* s) const noexcept {
        return operator()(std::string_view{s});
    }

    size_t operator()(const std::string& s) const noexcept {
        return operator()(std::string_view{s});
    }
};

/// @brief 与 TransparentStringHash 配套的异构字符串相等比较。
struct TransparentStringEqual {
    using is_transparent = void;
    bool operator()(const std::string& a, const std::string& b) const { return a == b; }
    bool operator()(const std::string& a, const char* b) const { return a == b; }
    bool operator()(const char* a, const std::string& b) const { return b == a; }
    bool operator()(std::string_view a, const std::string& b) const {
        return a == std::string_view{b.data(), b.size()};
    }
    bool operator()(const std::string& a, std::string_view b) const {
        return std::string_view{a.data(), a.size()} == b;
    }
};

/// 便利类型别名：带透明 hash 的 string-keyed unordered_map。
template <typename V>
using TransparentStringMap = std::unordered_map<std::string, V, TransparentStringHash, TransparentStringEqual>;

}  // namespace xt_core::sync
