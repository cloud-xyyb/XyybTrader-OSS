#pragma once

#include <cstdint>

#include "XtTick.h"
#include "XtRawTick.h"
#include "XtFixedPoint.h"

/// @file
/// @brief 行情 double/fixed-point 转换与 trading_date fallback helper。
///
/// `to_core()` 位于 MD 热路径，将 `XtRawTick` 截断转换为 fixed-point `XtTick`；
/// `to_raw()` 只用于 debug/export/replay 边界。行情域 scale 全部锚定 `kPriceFpScale`。
namespace xt_tick_fp {

constexpr int64_t kPriceScale = kPriceFpScale;
constexpr int64_t kQtyScale = kPriceFpScale;
constexpr int64_t kTurnoverScale = kPriceFpScale;
constexpr int64_t kInterestScale = kPriceFpScale;

static_assert(kPriceScale == kPriceFpScale, "kPriceScale must equal kPriceFpScale (SSOT)");
static_assert(kQtyScale == kPriceFpScale, "kQtyScale must equal kPriceFpScale (SSOT)");
static_assert(kTurnoverScale == kPriceFpScale, "kTurnoverScale must equal kPriceFpScale (SSOT)");
static_assert(kInterestScale == kPriceFpScale, "kInterestScale must equal kPriceFpScale (SSOT)");

inline int64_t to_fp(double value, int64_t  ) {
    return ::to_fp_trunc(value);
}

inline double from_fp(int64_t value_fp, int64_t scale) {
    return static_cast<double>(value_fp) / static_cast<double>(scale);
}

}

namespace xt_tick_convert {

XtTick to_core(const XtRawTick& raw);

XtRawTick to_raw(const XtTick& core);

inline double price_from_fp(int64_t value_fp) {
    return xt_tick_fp::from_fp(value_fp, xt_tick_fp::kPriceScale);
}

inline double qty_from_fp(int64_t value_fp) {
    return xt_tick_fp::from_fp(value_fp, xt_tick_fp::kQtyScale);
}

inline double turnover_from_fp(int64_t value_fp) {
    return xt_tick_fp::from_fp(value_fp, xt_tick_fp::kTurnoverScale);
}

inline double interest_from_fp(int64_t value_fp) {
    return xt_tick_fp::from_fp(value_fp, xt_tick_fp::kInterestScale);
}

inline uint32_t trading_date(const XtTick& tick) {
    return tick.trading_date ? tick.trading_date : tick.action_date;
}

inline uint32_t trading_date(const XtRawTick& tick) {
    return tick.trading_date ? tick.trading_date : tick.action_date;
}

}
