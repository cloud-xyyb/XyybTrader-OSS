#pragma once

#include <cassert>
#include <cmath>
#include <cstdint>

/// @file
/// @brief 全项目 fixed-point scale 与基础转换/算术工具。
///
/// 价格、数量、资金一律用 int64 定点数表示（value × kPriceFpScale），避免浮点
/// 误差在撮合、风控、账务链路里累积；本文件给出 double↔fp 边界转换与 fp 乘除法。

constexpr int64_t kPriceFpScale = 1'000'000;
static_assert(kPriceFpScale == 1'000'000, "kPriceFpScale invariant must hold");

inline int64_t to_fp_round(double v) noexcept {
    return static_cast<int64_t>(
        std::llround(static_cast<long double>(v) * static_cast<long double>(kPriceFpScale)));
}

inline int64_t to_fp_trunc(double v) noexcept {

    assert(std::isfinite(v) && "to_fp_trunc: non-finite input escaped upstream sanitize");
    return static_cast<int64_t>(v * static_cast<double>(kPriceFpScale));
}

namespace xt_fp {

constexpr int64_t kScale    = kPriceFpScale;
static_assert(kScale == kPriceFpScale, "xt_fp::kScale must equal kPriceFpScale (SSOT)");

inline int64_t to_fp(double value) noexcept {
    return ::to_fp_round(value);
}

inline double from_fp(int64_t value_fp) noexcept {
    return static_cast<double>(value_fp) / static_cast<double>(kScale);
}

inline int64_t mul(int64_t lhs_fp, int64_t rhs_fp) noexcept {
    const __int128 prod = static_cast<__int128>(lhs_fp) * static_cast<__int128>(rhs_fp);

    const __int128 half = (prod >= 0) ? (kScale / 2) : -(kScale / 2);
    return static_cast<int64_t>((prod + half) / kScale);
}

inline int64_t div(int64_t num_fp, int64_t den_fp) noexcept {
    if (den_fp == 0) return 0;
    const __int128 prod = static_cast<__int128>(num_fp) * static_cast<__int128>(kScale);

    const __int128 half = (prod >= 0) == (den_fp > 0) ? (den_fp / 2) : -(den_fp / 2);
    return static_cast<int64_t>((prod + half) / den_fp);
}

inline int64_t clamp_nonneg(int64_t v) noexcept {
    return v > 0 ? v : 0;
}

}
