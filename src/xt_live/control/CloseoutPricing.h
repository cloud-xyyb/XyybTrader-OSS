#pragma once

/// @file
/// @brief live control 层的强平/force-exit 定价 helper
///
/// 手动强平或 HFT 收盘强平时被调用

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string>

#include "xt_core/order/XtOrderTypes.h"               // order_exec_from_fp / order_exec_to_fp
#include "xt_live/gateway/binance/refdata/BinanceExchangeInfo.h"  // binance_u::symbol_tick_size / snap_price_to_tick

namespace xt_live::control {

/// @brief symbol 是否为 binance USDT 本位合约（决定 tick 来源走 binance 静态表）。
inline bool is_binance_usdt_symbol(const std::string& symbol) {
    std::string upper;
    upper.reserve(symbol.size());
    for (char c : symbol) upper.push_back(static_cast<char>(::toupper(c)));
    return upper.size() > 4 && upper.rfind("USDT") == upper.size() - 4;
}

/// @brief 激进平仓价：mark ± buffer，确保穿价方向成交。
///
/// buffer_bps 默认 10（10bps）。
/// @param mark_fp      标记价 fixed-point（<=0 返回 0）
/// @param buy_to_close 平空（买）→ 抬价；平多（卖）→ 压价
/// @param buffer_bps   穿价 buffer（基点 bps）
inline int64_t aggressive_close_price_fp(int64_t mark_fp, bool buy_to_close, int buffer_bps = 10) {
    if (mark_fp <= 0) return 0;
    const int64_t buffer = std::max<int64_t>(mark_fp * buffer_bps / 10000, 1);
    if (buy_to_close) return mark_fp + buffer;
    return std::max<int64_t>(mark_fp - buffer, 1);
}

/// @brief venue-neutral 合法平仓价：把 price_fp snap 到 venue tick，保持激进方向。
///
/// binance USDT → exchangeInfo 缓存 tick + 定向 snap；
/// 其他（CTP/XTP 期货）→ 按 price_tick_fp 做定向整 tick snap（买 ceil / 卖 floor，保持穿价）；
/// price_tick_fp<=0（未知）→ 原样透传（不劣于现状）。
/// @param symbol        合约代码
/// @param price_fp      待对齐价格 fixed-point（<=0 原样返回）
/// @param buy_to_close  买平 → 向上取整；卖平 → 向下取整
/// @param price_tick_fp 非 binance venue 的最小变动价位 fixed-point（来自 XtCommodityCatalog::price_tick）；0=未知不对齐
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
    if (price_tick_fp <= 0) return price_fp;  // 未知 tick：保守透传
    const int64_t q = price_fp / price_tick_fp;
    const int64_t r = price_fp % price_tick_fp;
    const int64_t snapped = (buy_to_close && r != 0) ? (q + 1) * price_tick_fp : q * price_tick_fp;
    return std::max<int64_t>(snapped, 1);
}

}  // namespace xt_live::control
