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

}
