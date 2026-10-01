#pragma once

#include <cassert>
#include <cmath>
#include <cstdint>

/// @file
/// @brief 全项目 fixed-point scale 与基础转换/算术工具。
///
/// 价格、数量、资金一律用 int64 定点数表示（value × kPriceFpScale），避免浮点
/// 误差在撮合、风控、账务链路里累积；本文件给出 double↔fp 边界转换与 fp 乘除法。

/// 全项目唯一定点缩放因子（fixed-point scale factor）。
/// 任何模块若声明自己的 scale 常量，必须 static_assert 等于本值。
constexpr int64_t kPriceFpScale = 1'000'000;
static_assert(kPriceFpScale == 1'000'000, "kPriceFpScale invariant must hold");

/// double → fixed-point，四舍五入（rounding via llround on long double 中间值）。
/// 适用：冷路径配置边界、财务结算（精度优先）。
inline int64_t to_fp_round(double v) noexcept {
    return static_cast<int64_t>(
        std::llround(static_cast<long double>(v) * static_cast<long double>(kPriceFpScale)));
}

/// double → fixed-point，截断（truncation via double 中间值）。
/// 适用：MD / 订单热路径（吞吐优先，CTP 数据已无 NaN/Inf）。
/// @pre 调用方保证输入是有限数；NaN/Inf 到整数转换不作为本层恢复语义。
inline int64_t to_fp_trunc(double v) noexcept {
    // debug tripwire：非有限输入在测试/调试构建立即暴露上游 sanitize 漏洞；
    // release (NDEBUG) 下 assert 编译为空，热路径零开销、行为不变。
    assert(std::isfinite(v) && "to_fp_trunc: non-finite input escaped upstream sanitize");
    return static_cast<int64_t>(v * static_cast<double>(kPriceFpScale));
}

/// @brief x1e6 fixed-point 算术工具命名空间。
///
/// `to_fp()` 保留 round 语义；tick/order 热路径输入边界需要截断时，
/// 调用方应显式使用全局 `::to_fp_trunc()`。
namespace xt_fp {

constexpr int64_t kScale    = kPriceFpScale;
static_assert(kScale == kPriceFpScale, "xt_fp::kScale must equal kPriceFpScale (SSOT)");

/// double → fixed-point，四舍五入（rounding）。
/// 通过 long double 中间值保留冷路径转换精度；热路径入口应显式调用 `to_fp_trunc`。
inline int64_t to_fp(double value) noexcept {
    return ::to_fp_round(value);
}

/// fixed-point → double。
/// 除法输出用于诊断/报表；财务路径应保留 fp 整数计算。
inline double from_fp(int64_t value_fp) noexcept {
    return static_cast<double>(value_fp) / static_cast<double>(kScale);
}

/// fixed-point 乘法：(lhs × rhs) / kScale，结果仍为 fixed-point。
/// 用 __int128 中间值防止溢出（lhs/rhs ≤ 9.2e18 / kScale = 9.2e12 时安全）。
/// `__int128` 是 GCC/Clang extension，用于避免 long double 软仿真路径。
inline int64_t mul(int64_t lhs_fp, int64_t rhs_fp) noexcept {
    const __int128 prod = static_cast<__int128>(lhs_fp) * static_cast<__int128>(rhs_fp);
    // half-away-from-zero rounding；负数用负 half，避免向 0 偏移。
    const __int128 half = (prod >= 0) ? (kScale / 2) : -(kScale / 2);
    return static_cast<int64_t>((prod + half) / kScale);
}

/// fixed-point 除法：(num / den) × kScale，结果仍为 fixed-point。除零返回 0。
/// 用 __int128 中间值同样避免 long double 软仿真。
inline int64_t div(int64_t num_fp, int64_t den_fp) noexcept {
    if (den_fp == 0) return 0;
    const __int128 prod = static_cast<__int128>(num_fp) * static_cast<__int128>(kScale);
    // half-away-from-zero rounding；half 的符号必须跟最终商方向一致。
    const __int128 half = (prod >= 0) == (den_fp > 0) ? (den_fp / 2) : -(den_fp / 2);
    return static_cast<int64_t>((prod + half) / den_fp);
}

/// fixed-point 数量/金额的非负钳制：负值按 0 返回。
inline int64_t clamp_nonneg(int64_t v) noexcept {
    return v > 0 ? v : 0;
}

}  // namespace xt_fp
