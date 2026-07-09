#pragma once

/// @file
/// @brief 把策略市价意图转换成可提交的 IOC 限价参数。

#include <cstdint>

#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtOrderTypes.h"
#include "api/ExecFlags.h"

namespace xt_strategy_engine {

constexpr int64_t kMarketSlippageBps = 100;

inline bool resolve_market_order(const XtTick* t,
                                 bool is_buy,
                                 int64_t slippage_bps,
                                 int64_t& out_price_fp,
                                 XtTimeInForce& out_tif,
                                 ExecFlags& out_flags) {
    if (!t) return false;
    int64_t band = 0;
    if (is_buy) {
        if (t->upper_limit_fp > 0) {
            band = t->upper_limit_fp;
        } else if (t->ask_price_fp[0] > 0) {
            band = t->ask_price_fp[0] + t->ask_price_fp[0] * slippage_bps / 10000;
        }
    } else {
        if (t->lower_limit_fp > 0) {
            band = t->lower_limit_fp;
        } else if (t->bid_price_fp[0] > 0) {
            band = t->bid_price_fp[0] - t->bid_price_fp[0] * slippage_bps / 10000;
        }
    }
    if (band <= 0) return false;
    out_price_fp = band;
    out_tif      = XtTimeInForce::IOC;
    out_flags    = with_flag(out_flags, ExecFlag::Market);
    return true;
}

}
