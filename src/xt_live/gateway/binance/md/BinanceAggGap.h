#pragma once

/// @file
/// @brief Binance aggTrade gap 检测与回补页累计 helper。

#include <cstdint>
#include <vector>

namespace binance_u {

inline int64_t agg_gap_missed(int64_t last, int64_t cur) noexcept {
    if (cur <= 0 || last == 0) return 0;
    if (cur <= last + 1) return 0;
    return cur - last - 1;
}

/// @brief 一笔回补 aggTrade（REST /fapi/v1/aggTrades 元素，仅取记账所需字段）。
struct AggTrade {
    int64_t a = 0;
    double  q = 0.0;
    double  p = 0.0;
};

/// @brief 一页回补的区间界定累计结果。
struct AggBackfillPageResult {
    double  accounted_qty = 0.0;
    int     accounted_n   = 0;
    int64_t max_a         = 0;
    bool    reached_hi    = false;
};

inline AggBackfillPageResult process_agg_backfill_page(
        const std::vector<AggTrade>& page, int64_t lo, int64_t hi) noexcept {
    AggBackfillPageResult r;
    for (const auto& t : page) {
        if (t.a > r.max_a) r.max_a = t.a;
        if (t.a >= hi) { r.reached_hi = true; continue; }
        if (t.a < lo) continue;
        r.accounted_qty += t.q;
        ++r.accounted_n;
    }
    return r;
}

}
