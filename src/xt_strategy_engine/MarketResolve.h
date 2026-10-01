#pragma once

/// @file
/// @brief 把策略市价意图转换成可提交的 IOC 限价参数。

#include <cstdint>

#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtOrderTypes.h"  // XtTimeInForce
#include "api/ExecFlags.h"

namespace xt_strategy_engine {

/// @brief 市价单默认滑点保护带宽（basis points）。
///
/// 无日内涨跌停的 venue（如 binance perp）用 best-opposite × (1 ± bps/10000) 作激进限价；
/// 有涨跌停的 venue（如 CTP）直接用涨跌停价，不用本参数。1% = 100 bps。
constexpr int64_t kMarketSlippageBps = 100;

/// @brief 把市价意图解析为激进限价 IOC（marketable-limit-IOC）。
///
/// 价格优先用 tick 涨跌停价作为最大/最小保护价；无涨跌停时用
/// best-opposite × (1 ± slippage_bps/10000)（binance 等）。整数定点运算保证确定性。
/// 解析成功后下游全程是普通激进限价单（带真价 + Market tag），sim 与各 venue codec
/// 均复用现成限价路径。
///
/// @param t            当前合约 tick；nullptr 视为无行情。
/// @param is_buy       买方向吃 ask / 用涨停价；卖方向吃 bid / 用跌停价。
/// @param slippage_bps 无涨跌停时的滑点带宽 bps。
/// @param out_price_fp 成功时写出激进限价（fixed-point）。
/// @param out_tif      成功时写为 IOC。
/// @param out_flags    成功时叠加 ExecFlag::Market（保留入参已有 flag）。
/// @return 解析出正价格返回 true；无 tick 或无可用价返回 false，由调用方决定 passthrough/fail-closed。
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

}  // namespace xt_strategy_engine
