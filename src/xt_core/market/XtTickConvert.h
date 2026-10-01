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

/// @note 累计成交额（total turnover）先以 double 进入本层；活跃合约日累计放大
///       1e6 后可能超过 2^53，`total_turnover_fp` 不承诺整数级精确。该字段用于
///       bar/VWAP/诊断聚合，不作为 account/risk/order/PnL 的 money-path 输入；
///       若未来需要精确成交额，需要改 wire 表示或 scale 语义。

/// @brief double -> fixed-point 的行情热路径截断转换。
/// @param value 外部数据源已校验为 finite 的行情数值。
/// @return `value * kPriceFpScale` 截断后的整数。
/// @note 第二个参数仅保留调用点字段域标识；当前行情域 scale 全部锚定到 `kPriceFpScale`。
inline int64_t to_fp(double value, int64_t /*scale*/) {
    return ::to_fp_trunc(value);
}

/// @brief fixed-point -> double 的边界反转换。
/// @return `value_fp / scale`，供撮合模型、bar 聚合、导出或诊断边界使用。
inline double from_fp(int64_t value_fp, int64_t scale) {
    return static_cast<double>(value_fp) / static_cast<double>(scale);
}

}  // namespace xt_tick_fp

namespace xt_tick_convert {

/// @brief 原始 double tick -> 核心 fixed-point tick。
///
/// 字符数组按固定长度复制并强制补 null terminator；数值字段按截断语义转换。
/// @pre `raw` 的数值字段已由 gateway/data source 边界保证为 finite。
XtTick to_core(const XtRawTick& raw);

/// @brief 核心 fixed-point tick -> 原始 double tick。
///
/// 主要用于 debug/export/replay 边界；内部策略、撮合和账户口径仍以 fp 整数为准。
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

/// @brief 返回 tick 的交易日；`trading_date == 0` 时回退到 `action_date`。
inline uint32_t trading_date(const XtTick& tick) {
    return tick.trading_date ? tick.trading_date : tick.action_date;
}

/// @brief 返回 raw tick 的交易日；`trading_date == 0` 时回退到 `action_date`。
inline uint32_t trading_date(const XtRawTick& tick) {
    return tick.trading_date ? tick.trading_date : tick.action_date;
}

// 需要 `XtBar` overload 的调用方包含 `xt_core/market/XtBarStruct.h`。

}  // namespace xt_tick_convert
