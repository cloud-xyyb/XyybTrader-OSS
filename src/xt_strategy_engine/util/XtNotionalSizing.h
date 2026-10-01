#pragma once

#include <cmath>

/// @file
/// @brief quote-notional 目标下的 base qty sizing helper。
///
/// 策略给出想开的名义金额，例如“买 100 USDT 的 BTC”；交易接口实际要的是
/// base 数量，例如 BTC 数量。本文件根据价格把 quote 金额换成 base qty，并检查
/// 最小下单金额。
///
/// 这里只按 `base_qty * price` 这一种口径计算，不处理合约乘数、手数单位或保证金折算。
namespace xt_sizing {

/// @brief 将 quote-notional 目标换算为 base qty，并按交易所数量步长处理。
///
/// `quote_notional <= 0` 或 `reference_price <= 0` 返回 0，调用方应视为不开仓。
/// `qty_step > 0` 时先按最近 step 量化；若量化后低于 `min_quote_notional`，
/// 再只向上补到刚好越过下限，避免 tight target 被交易所 MIN_NOTIONAL 拒绝。
/// `qty_step <= 0` 表示不做数量量化，也不会执行 min-notional step bump。
///
/// @warning 不保证结果仍接近目标 notional：step 过粗或 min-notional 过高时会放大
///          实际 exposure；调用方必须用配置上限或 risk 层继续约束。
inline double notional_to_qty(double quote_notional, double reference_price, double qty_step,
                              double min_quote_notional = 0.0) {
    if (quote_notional <= 0.0 || reference_price <= 0.0) return 0.0;
    double qty = quote_notional / reference_price;
    if (qty_step > 0.0) {
        qty = std::round(qty / qty_step) * qty_step;
        if (min_quote_notional > 0.0) {
            while (qty * reference_price < min_quote_notional) qty += qty_step;
        }
    }
    return qty > 0.0 ? qty : 0.0;
}

}  // namespace xt_sizing
