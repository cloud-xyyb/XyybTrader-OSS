#pragma once

/// @file
/// @brief 合并 Binance bookTicker L1 与 depth book 盘口的 helper。
///
/// BinanceMdMerge 定义 `FreshL1` 和 `overlay_fresh_l1()`：gateway 组装
/// `XtRawTick` 时，用 bookTicker 最新 best bid/ask 替换 depth book 第一档，
/// 更深档仍来自 depth book。鲜 L1 无效或盘口交叉时返回 false，调用方保留原 depth 盘口。
///
/// 本文件不解析 WS payload，不维护 order book，也不发布 tick。

#include "xt_core/XtFixedPoint.h"  // ::to_fp_trunc — 与 to_core()/recorder 同一截断语义

namespace binance_u {

/// @brief bookTicker 自带最优盘口（L1）鲜值缓存。
///
/// bookTicker 逐 BBO 变化实时推、自带 best bid/ask 价与量（wire 字段 `b/B/a/A`）；
/// depth diff ~250ms 滞后。tick 的 L1 取此鲜值、L2..N 仍取 depth book。
/// 仅 ws_thread 写读，无并发。
struct FreshL1 {
    bool   valid   = false;  ///< 是否已收到至少一条 bookTicker（重连/cold-start 时 false）。
    double bid_px  = 0.0;    ///< best bid price
    double bid_qty = 0.0;    ///< best bid qty
    double ask_px  = 0.0;    ///< best ask price
    double ask_qty = 0.0;    ///< best ask qty
};

/// @brief 用 bookTicker 鲜 L1 覆盖 depth top-N 数组，并裁掉破坏严格单调的更深档。
///
/// 在 push_tick 的 top_n(n) 数组上**原地**操作；recorder 与 cb 随后都从这同一份
/// 被覆盖+裁档的 20 档数组取数（单一构建点，杜绝 recorder/策略 L1 分叉）。
///
/// 裁档在 fixed-point 截断空间比较（`::to_fp_trunc`，与 `to_core()`/recorder 同语义，
/// **含等值剔除**）→ 保证过 `to_core()` 后 conformance `md_prefix_bad_` 严格单调不报警：
///   - bid：丢弃 `to_fp_trunc(depth_bid) >= to_fp_trunc(fresh_bid)` 的档；
///   - ask：丢弃 `to_fp_trunc(depth_ask) <= to_fp_trunc(fresh_ask)` 的档。
/// 余下档保持原相对序前移，尾部补 0。鲜 best_bid 取代 depth 顶档为 L1（bookTicker 更鲜，
/// depth 中 >= 鲜 best_bid 的档是已消失的陈旧档，丢弃正确）。
///
/// @param n  数组长度（push_tick 传 kRecDepth=20；内部上限 kMaxLevels=64 防越界）。
/// @return true 已覆盖；false 鲜 L1 无效（价/量非正）或自身交叉/触碰(bid>=ask) → 调用方保留原 depth。
/// @note 仅 ws_thread（push_tick 内）调用，无并发；纯函数，无副作用。
/// @note 单调保证的前提：(a) depth 档之间在 fp 截断空间 ≥1 单位（book 按 1e-4 keying，
///       ×1e6 截断后相邻真实档 ≥100 fp 单位，不会塌等值 → 仅比对"档 vs 鲜 L1"即足）；
///       (b) 鲜 L1 价 > 0 **且量经 ::to_fp_trunc 后 > 0**（qty >= 1e-6，与下游 fp 同口径）：
///       否则 L1 在 conformance present 谓词(px_fp>0 && qty_fp>0)下被判缺档 → 缺档后又出现
///       depth 真实档 = md_nonmono。overlay_fresh_l1 已内置该 guard（qty<=0 或 sub-1e-6 → 退回 depth）。
inline bool overlay_fresh_l1(const FreshL1& l1, int n,
                             double* bp, double* bq,
                             double* ap, double* aq) noexcept {
    if (!l1.valid || n <= 0 || !bp || !bq || !ap || !aq) return false;
    if (l1.bid_px <= 0.0 || l1.ask_px <= 0.0) return false;
    // 量必须 > 0 **且不被 fp 截断成 0**（0 < qty < 1e-6 也算缺）：否则 L1 在 conformance
    // present 谓词(px_fp>0 && qty_fp>0)下被判缺档 → 缺档后出现 depth 真实档 = md_nonmono
    // 误报。用 ::to_fp_trunc 与下游(to_core/recorder)同口径判 0。非正/截断为 0 → 退回 depth。
    if (::to_fp_trunc(l1.bid_qty) <= 0 || ::to_fp_trunc(l1.ask_qty) <= 0) return false;

    const int64_t fresh_bid_fp = ::to_fp_trunc(l1.bid_px);
    const int64_t fresh_ask_fp = ::to_fp_trunc(l1.ask_px);
    // 鲜 L1 自身交叉/触碰（异常，bookTicker 正常不会）→ 不覆盖，退回 depth 由调用方告警。
    if (fresh_bid_fp >= fresh_ask_fp) return false;

    constexpr int kMaxLevels = 64;
    if (n > kMaxLevels) n = kMaxLevels;

    // 写入临时缓冲再回拷：原地压缩(写指针可能领先读指针)会破坏未读档。
    double tbp[kMaxLevels], tbq[kMaxLevels], tap[kMaxLevels], taq[kMaxLevels];

    // bid：slot0 = 鲜 L1，其后保留 fp 严格低于鲜 best_bid 的 depth 档（含等值剔除）。
    tbp[0] = l1.bid_px; tbq[0] = l1.bid_qty;
    int wb = 1;
    for (int i = 0; i < n && wb < n; ++i) {
        if (bp[i] <= 0.0) continue;                          // 缺档
        if (::to_fp_trunc(bp[i]) >= fresh_bid_fp) continue;  // 陈旧顶档 / 等值 → 剔除
        tbp[wb] = bp[i]; tbq[wb] = bq[i]; ++wb;
    }
    for (int i = wb; i < n; ++i) { tbp[i] = 0.0; tbq[i] = 0.0; }

    // ask：slot0 = 鲜 L1，其后保留 fp 严格高于鲜 best_ask 的 depth 档。
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

}  // namespace binance_u
