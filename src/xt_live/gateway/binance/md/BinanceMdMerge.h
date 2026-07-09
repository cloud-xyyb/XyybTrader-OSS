#pragma once

/// @file
/// @brief 合并 Binance bookTicker L1 与 depth book 盘口的 helper。
///
/// BinanceMdMerge 定义 `FreshL1` 和 `overlay_fresh_l1()`：gateway 组装
/// `XtRawTick` 时，用 bookTicker 最新 best bid/ask 替换 depth book 第一档，
/// 更深档仍来自 depth book。鲜 L1 无效或盘口交叉时返回 false，调用方保留原 depth 盘口。
///
/// 本文件不解析 WS payload，不维护 order book，也不发布 tick。

#include "xt_core/XtFixedPoint.h"

namespace binance_u {

/// @brief bookTicker 自带最优盘口（L1）鲜值缓存。
///
/// bookTicker 逐 BBO 变化实时推、自带 best bid/ask 价与量（wire 字段 `b/B/a/A`）；
/// depth diff ~250ms 滞后。tick 的 L1 取此鲜值、L2..N 仍取 depth book。
/// 仅 ws_thread 写读，无并发。
struct FreshL1 {
    bool   valid   = false;
    double bid_px  = 0.0;
    double bid_qty = 0.0;
    double ask_px  = 0.0;
    double ask_qty = 0.0;
};

inline bool overlay_fresh_l1(const FreshL1& l1, int n,
                             double* bp, double* bq,
                             double* ap, double* aq) noexcept {
    if (!l1.valid || n <= 0 || !bp || !bq || !ap || !aq) return false;
    if (l1.bid_px <= 0.0 || l1.ask_px <= 0.0) return false;

    if (::to_fp_trunc(l1.bid_qty) <= 0 || ::to_fp_trunc(l1.ask_qty) <= 0) return false;

    const int64_t fresh_bid_fp = ::to_fp_trunc(l1.bid_px);
    const int64_t fresh_ask_fp = ::to_fp_trunc(l1.ask_px);

    if (fresh_bid_fp >= fresh_ask_fp) return false;

    constexpr int kMaxLevels = 64;
    if (n > kMaxLevels) n = kMaxLevels;

    double tbp[kMaxLevels], tbq[kMaxLevels], tap[kMaxLevels], taq[kMaxLevels];

    tbp[0] = l1.bid_px; tbq[0] = l1.bid_qty;
    int wb = 1;
    for (int i = 0; i < n && wb < n; ++i) {
        if (bp[i] <= 0.0) continue;
        if (::to_fp_trunc(bp[i]) >= fresh_bid_fp) continue;
        tbp[wb] = bp[i]; tbq[wb] = bq[i]; ++wb;
    }
    for (int i = wb; i < n; ++i) { tbp[i] = 0.0; tbq[i] = 0.0; }

    tap[0] = l1.ask_px; taq[0] = l1.ask_qty;
    int wa = 1;
    for (int i = 0; i < n && wa < n; ++i) {
        if (ap[i] <= 0.0) continue;
        if (::to_fp_trunc(ap[i]) <= fresh_ask_fp) continue;
        tap[wa] = ap[i]; taq[wa] = aq[i]; ++wa;
    }
    for (int i = wa; i < n; ++i) { tap[i] = 0.0; taq[i] = 0.0; }

    for (int i = 0; i < n; ++i) {
        bp[i] = tbp[i]; bq[i] = tbq[i];
        ap[i] = tap[i]; aq[i] = taq[i];
    }
    return true;
}

}
