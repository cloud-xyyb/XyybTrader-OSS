#pragma once

/// @file
/// @brief Binance aggTrade gap 检测与回补页累计 helper。

#include <cstdint>
#include <vector>

namespace binance_u {

/// @brief 计算 aggTrade aggregate-id 不连续时漏掉的成交数（gap size）。
///
/// binance aggregate trade id 在单 symbol 上单调递增；WS=TCP 连接内不丢消息，故
/// 正常流应连续（docs 未保证严格 +1，但实测连续 → 仅 `cur > last+1`
/// 视为 gap，宽松容忍交易所合法跳号由 caller 的 reconnect-anchored 触发吸收）。
///
/// @param last  上一笔已见的最高 agg-id（高水位）；0 = 尚无基线（首笔）。
/// @param cur   本笔 agg-id；<=0 = 字段缺失/异常。
/// @return 漏掉的成交数 `cur - last - 1`（cur > last+1 时）；否则 0（连续/重复/乱序/无基线）。
inline int64_t agg_gap_missed(int64_t last, int64_t cur) noexcept {
    if (cur <= 0 || last == 0) return 0;   // 首笔无基线，不判 gap
    if (cur <= last + 1) return 0;          // 连续（含重复/乱序，由高水位吸收）
    return cur - last - 1;
}

/// @brief 一笔回补 aggTrade（REST /fapi/v1/aggTrades 元素，仅取记账所需字段）。
struct AggTrade {
    int64_t a = 0;    ///< aggregate trade id（fromId 翻页锚点）
    double  q = 0.0;  ///< quantity（记账：total_volume += q）
    double  p = 0.0;  ///< price（last_price 参考，回补不 emit tick 故不强制用）
};

/// @brief 一页回补的区间界定累计结果。
struct AggBackfillPageResult {
    double  accounted_qty = 0.0;  ///< 本页 a∈[lo,hi) 的 q 之和
    int     accounted_n   = 0;    ///< 计入笔数
    int64_t max_a         = 0;    ///< 本页最大 agg-id（推进 fromId = max_a+1）
    bool    reached_hi    = false;///< 本页见到 a>=hi（追上 live resume 点，停止翻页）
};

/// @brief 处理一页回补 aggTrades —— **区间界定** [lo, hi) 累计。
///
/// gap 区间 [lo,hi) 的成交严格低于 live resume 点 hi，与 live 流不相交 →
/// 直接按区间累计即可，**无需 ring dedup**。caller 跨页累加 accounted_qty，
/// 用 max_a 推进 fromId，reached_hi 为停止信号。
inline AggBackfillPageResult process_agg_backfill_page(
        const std::vector<AggTrade>& page, int64_t lo, int64_t hi) noexcept {
    AggBackfillPageResult r;
    for (const auto& t : page) {
        if (t.a > r.max_a) r.max_a = t.a;
        if (t.a >= hi) { r.reached_hi = true; continue; }  // 追上 live，不计
        if (t.a < lo) continue;                            // 低于区间（fromId=lo 起一般不出现）
        r.accounted_qty += t.q;
        ++r.accounted_n;
    }
    return r;
}

}  // namespace binance_u
