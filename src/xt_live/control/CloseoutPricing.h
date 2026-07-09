#pragma once

/// @file
/// @brief live control 层的强平/force-exit 定价 helper
///
/// 手动强平或 HFT 收盘强平时被调用

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string>

#include "xt_core/order/XtOrderTypes.h"
#include "xt_live/gateway/binance/refdata/BinanceExchangeInfo.h"

namespace xt_live::control {

inline bool is_binance_usdt_symbol(const std::string& symbol) {
    std::string upper;
    upper.reserve(symbol.size());
    for (char c : symbol) upper.push_back(static_cast<char>(::toupper(c)));
    return upper.size() > 4 && upper.rfind("USDT") == upper.size() - 4;
}

inline int64_t aggressive_close_price_fp(int64_t mark_fp, bool buy_to_close, int buffer_bps = 10) {
    if (mark_fp <= 0) return 0;
    const int64_t buffer = std::max<int64_t>(mark_fp * buffer_bps / 10000, 1);
    if (buy_to_close) return mark_fp + buffer;
    return std::max<int64_t>(mark_fp - buffer, 1);
}

inline int64_t legal_close_price_fp(const std::string& symbol,
                                    int64_t price_fp,
                                    bool buy_to_close,
                                    int64_t price_tick_fp) {
    if (price_fp <= 0) return price_fp;
    if (is_binance_usdt_symbol(symbol)) {
        const double px      = order_exec_from_fp(price_fp);
        const double tick    = binance_u::symbol_tick_size(symbol);
        const double snapped = binance_u::snap_price_to_tick(px, tick, buy_to_close);
        return std::max<int64_t>(order_exec_to_fp(snapped), 1);
    }
    if (price_tick_fp <= 0) return price_fp;
    const int64_t q = price_fp / price_tick_fp;
    const int64_t r = price_fp % price_tick_fp;
    const int64_t snapped = (buy_to_close && r != 0) ? (q + 1) * price_tick_fp : q * price_tick_fp;
    return std::max<int64_t>(snapped, 1);
}

}
