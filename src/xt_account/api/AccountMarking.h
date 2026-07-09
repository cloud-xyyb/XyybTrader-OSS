#pragma once

/// @file
/// @brief 账户持仓估值公式和 fixed-point 输入 DTO。
///
/// 本文件只提供纯计算 helper：调用方负责填入持仓数量、均价、mark price
/// 和期货合约乘数；函数不读取账户状态，也不维护缓存。

#include <cstdint>

#include "xt_core/XtFixedPoint.h"

namespace xt_account {

/** @brief 单合约估值输入，金额计算使用 fixed-point。
 *
 *  @note `vol_scale_fp` 默认是 fixed-point 1.0，仅用于 stock mode 或缺少品种目录时的
 *        单位乘数 fallback。
 *  @warning 期货模式下调用方必须从 commodity catalog 填入合约乘数；未填会让
 *           unrealized PnL 按 1x 估值，例如 rb=10x、cu=5x、IF=300x 会被相应低估。
 *           realized PnL 的成交路径由 `AccountState::on_trade_ex` 套用同一合约乘数。
 */
struct XtPositionMarkInput {
    int64_t long_qty = 0;
    int64_t long_avg_price = 0;
    int64_t short_qty = 0;
    int64_t short_avg_price = 0;
    int64_t mark_price = 0;
    int64_t vol_scale_fp = xt_fp::kScale;
};

inline int64_t position_unrealized(const XtPositionMarkInput& in) {
    const int64_t long_leg_raw  = xt_fp::mul(in.mark_price - in.long_avg_price, in.long_qty);
    const int64_t short_leg_raw = xt_fp::mul(in.short_avg_price - in.mark_price, in.short_qty);
    return xt_fp::mul(long_leg_raw + short_leg_raw, in.vol_scale_fp);
}

inline int64_t position_market_value(const XtPositionMarkInput& in) {
    return xt_fp::mul(xt_fp::mul(in.mark_price, in.long_qty - in.short_qty),
                      in.vol_scale_fp);
}

inline int64_t equity(int64_t cash,
                      int64_t total_unrealized,
                      int64_t total_market_value,
                      bool futures_mode) {
    return futures_mode ? (cash + total_unrealized) : (cash + total_market_value);
}

}
