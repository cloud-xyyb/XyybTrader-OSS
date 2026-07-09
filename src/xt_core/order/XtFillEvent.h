#pragma once

#include <cstdint>
#include <cstring>
#include <type_traits>

#include "XtOffset.h"
#include "XtOrderTypes.h"

/**
 * @brief 热路径成交事件（hot-path fill event）。
 *
 * 纯 POD、固定数组、无堆分配；用于策略 on_fill/on_trade 回调和账户/仓位更新。
 * 数值字段统一使用 `kOrderExecFpScale` fixed-point 口径。
 * broker 回报 ID、交易日等审计字段由冷路径 `XtTradeRecord` 承载。
 */
struct XtFillEvent {

    uint64_t order_id = 0;

    uint64_t fill_ts_us = 0;

    int64_t exec_price_fp = 0;

    int64_t qty_fp = 0;

    int64_t fee_fp = 0;

    int64_t slippage_fp = 0;

    XtOffset close_offset = XtOffset::Open;

    bool is_buy = true;

    bool is_today = false;

    uint8_t _pad[5] = {};

    char code[16] = {};

    double exec_price() const { return order_exec_from_fp(exec_price_fp); }
    double qty() const { return order_exec_from_fp(qty_fp); }
    double fee() const { return order_exec_from_fp(fee_fp); }
    double slippage() const { return order_exec_from_fp(slippage_fp); }
    bool is_close() const { return close_offset != XtOffset::Open; }
};

inline void fill_event_set_code(XtFillEvent& ev, const char* src) {
    if (!src) { ev.code[0] = '\0'; return; }
    const size_t n = ::strnlen(src, sizeof(ev.code) - 1);
    ::memcpy(ev.code, src, n);
    ev.code[n] = '\0';
}

static_assert(std::is_trivially_copyable_v<XtFillEvent>,
              "XtFillEvent must remain trivially_copyable for hot-path queue transport");
