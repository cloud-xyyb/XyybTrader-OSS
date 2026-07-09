#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstring>

#include "xt_core/order/XtOffset.h"

/// @file
/// @brief 平仓今昨路由与单笔限额切片 helper。
///
/// 本文件提供 venue-neutral 的 close slice 计算入口：
/// - `resolve_close_slices()` 负责按合约/今昨可平量生成平仓切片
/// - `chunk_slices_by_max_qty()` 负责按单笔限额拆分

namespace close_offset_core {

inline bool needs_close_today_split(const char* code) {
    if (!code) return false;
    char prefix[8] = {};
    size_t len = 0;
    for (const char* p = code; *p != '\0' && len < sizeof(prefix) - 1; ++p) {
        const unsigned char c = static_cast<unsigned char>(*p);
        if (!std::isalpha(c)) break;
        prefix[len++] = static_cast<char>(std::tolower(c));
    }
    if (len == 0) return false;
    static const char* kStrictSymbols[] = {
        "au", "ag", "cu", "al", "zn", "pb", "ni", "sn",
        "rb", "wr", "hc", "bu", "fu", "ru", "sp", "ss",
        "sc", "lu", "bc", "nr", "ec"
    };
    for (const char* it : kStrictSymbols) {
        if (std::strcmp(prefix, it) == 0) return true;
    }
    return false;
}

/// @brief 单笔平仓切片（single close slice）。
struct CloseSlice {

    XtOffset offset = XtOffset::Close;
    int64_t qty_fp = 0;
};

inline int resolve_close_slices(const char* code,
                                int64_t qty_fp,
                                int64_t today_fp,
                                int64_t yesterday_fp,
                                bool prefer_today,
                                CloseSlice* out) {
    if (qty_fp <= 0 || !out) return 0;

    if (needs_close_today_split(code)) {
        const int64_t available = today_fp + yesterday_fp;
        if (qty_fp > available) return 0;

        const int64_t  primary_fp  = prefer_today ? today_fp     : yesterday_fp;
        const XtOffset primary_off = prefer_today ? XtOffset::CloseToday : XtOffset::CloseYesterday;
        const int64_t  secondary_fp  = prefer_today ? yesterday_fp   : today_fp;
        const XtOffset secondary_off = prefer_today ? XtOffset::CloseYesterday : XtOffset::CloseToday;

        const int64_t alloc_primary   = std::min(qty_fp, primary_fp);
        const int64_t alloc_secondary = qty_fp - alloc_primary;

        int n = 0;
        if (alloc_primary   > 0) { out[n++] = CloseSlice{primary_off,   alloc_primary};   }
        if (alloc_secondary > 0) { out[n++] = CloseSlice{secondary_off, alloc_secondary}; }
        return n;
    }

    const int64_t available = today_fp + yesterday_fp;
    if (qty_fp > available) return 0;
    out[0] = CloseSlice{prefer_today ? XtOffset::CloseToday : XtOffset::Close, qty_fp};
    return 1;
}

inline int chunk_slices_by_max_qty(const CloseSlice* routed, int routed_count,
                                   int64_t max_order_qty_fp,
                                   CloseSlice* out, int max_out,
                                   bool& out_truncated) {
    out_truncated = false;
    int n = 0;
    for (int i = 0; i < routed_count; ++i) {
        int64_t left = routed[i].qty_fp;
        if (max_order_qty_fp <= 0) {
            if (n < max_out) out[n++] = routed[i];
            else out_truncated = true;
            continue;
        }
        while (left > 0 && n < max_out) {
            const int64_t send = std::min(left, max_order_qty_fp);
            out[n++] = CloseSlice{routed[i].offset, send};
            left -= send;
        }
        if (left > 0) out_truncated = true;
    }
    return n;
}

}
