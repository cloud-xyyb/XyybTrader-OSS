#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstring>

#include "xt_core/order/XtOffset.h"

/// @file
/// @brief 期货平仓今昨路由与单笔限额切片。
///
/// SHFE/INE 等交易所把持仓拆成平今、平昨两个独立仓位，平仓单必须显式指定
/// 今昨方向；同一次平仓意图还可能超过单笔最大委托量，需要拆成多笔发送。
/// 本文件把这两步收敛成两个纯函数：`resolve_close_slices()` 决定今昨怎么分配，
/// `chunk_slices_by_max_qty()` 把分配结果按上限切片。只做数量与方向的计算，
/// 不涉及下单、风控或账户状态；BT 和 live 网关共用同一份实现。

namespace close_offset_core {

/// @brief 通过合约代码前缀推断是否需要拆平今/平昨（code-prefix heuristic）。
///
/// SHFE（上期所）和 INE（上期能源）对平仓指令强制区分平今与平昨。
/// 当调用方只有合约代码、没有 exchange_id 时，本 helper 提取品种字母前缀并匹配
/// SHFE/INE 已知品种列表。
///
/// @note 此函数依赖硬编码品种列表，新品种上市时需手动更新。
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
    /// 平仓方向；严格区分今昨的交易所返回 CloseToday/CloseYesterday。
    XtOffset offset = XtOffset::Close;
    int64_t qty_fp = 0;  ///< fixed-point 数量，scale 与账户/订单层保持一致。
};

/// @brief 将平仓请求解析为今/昨切片（code-prefix 兜底版本）。
///
/// `qty_fp`、`today_fp`、`yesterday_fp` 使用 fixed-point 数量，调用方需传入非负可平量。
/// 对 SHFE/INE 前缀合约最多写入 2 个切片；非严格区分今昨的合约写入 1 个切片。
///
/// @return 写入 `out` 的切片数量；0 表示请求数量无效、可平量不足或输出指针为空。
/// @note 非严格区分今昨时，`prefer_today=true` 仍返回 CloseToday，供调用方保留显式平今意图。
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

/// @brief 按单笔限额把已路由切片拆为发送切片（BT/live 共享的唯一 chunking 实现）。
///
/// `max_order_qty_fp <= 0` 表示无单笔限额：逐片直拷（仍受 `max_out` 预算约束）。
/// 切片预算耗尽仍有余量未覆盖时置 `out_truncated`，caller 必须拒整笔——
/// 绝不静默少发后回报"已受理"（与开仓超限被风控拒对称）。
///
/// @param routed 今昨路由后的切片（`resolve_close_slices` 产物或直通单片）。
/// @param out 输出数组，容量 >= max_out。
/// @return 写入 `out` 的切片数。
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

}  // namespace close_offset_core
