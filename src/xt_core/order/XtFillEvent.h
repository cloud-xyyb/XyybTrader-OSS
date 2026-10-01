#pragma once

#include <cstdint>
#include <cstring>
#include <type_traits>

#include "XtOffset.h"
#include "XtOrderTypes.h"

/**
 * @brief 一笔成交的热路径表示。
 *
 * 成交（fill）发生后要立刻喂给策略 on_fill/on_trade 回调并更新账户/仓位，这条路径容不下
 * 堆分配或字符串开销，所以本结构是纯 POD + 固定数组，数值字段统一走 `kOrderExecFpScale`
 * fixed-point。只带记账必需的字段；broker 回报 ID、交易日等审计信息由冷路径的
 * `XtTradeRecord` 承载。
 */
struct XtFillEvent {
    // ── 成交标识 ──────────────────────────────────────────────

    /** 内部订单号（order_id），与 XtOrderExecUpdate::id 对齐，用于关联冷路径记录。 */
    uint64_t order_id = 0;

    /** 成交时间戳（Unix epoch us）。回测：合成事件时间；实盘：本地回调时间（local_cb_us）。 */
    uint64_t fill_ts_us = 0;

    // ── 成交数值（fixed-point，scale = kOrderExecFpScale = 1e6） ──────────────────────────────────────────────

    /** 实际成交价（exec price = 报单价 + slippage），固定点。 */
    int64_t exec_price_fp = 0;

    /** 本次成交量（fixed-point）。 */
    int64_t qty_fp = 0;

    /** 手续费（fee），固定点，负数表示返佣（rebate）。 */
    int64_t fee_fp = 0;

    /** 滑点成本（slippage = exec_price - order_price），固定点。 */
    int64_t slippage_fp = 0;

    // ── 成交方向与开平 ──────────────────────────────────────────────

    /** 开平语义（open/close offset）。 */
    XtOffset close_offset = XtOffset::Open;

    /** true = 买入，false = 卖出。 */
    bool is_buy = true;

    /** true = 平今仓（today position），false = 平昨仓或开仓。 */
    bool is_today = false;

    uint8_t _pad[5] = {}; // 显式 padding，保持 POD layout。

    // ── 合约码（固定大小，无堆分配） ──────────────────────────────────────────────

    /**
     * 合约码（null-terminated，最多 15 字节有效字符）。
     * 例："IF2403"、"AG2406"。
     * 赋值请用 fill_event_set_code() 辅助函数，避免手写 strncpy 边界。
     */
    char code[16] = {};

    // ── 访问器（accessor）：从 fixed-point 转回 double ──────────────────────────────────────────────

    double exec_price() const { return order_exec_from_fp(exec_price_fp); }
    double qty() const { return order_exec_from_fp(qty_fp); }
    double fee() const { return order_exec_from_fp(fee_fp); }
    double slippage() const { return order_exec_from_fp(slippage_fp); }
    bool is_close() const { return close_offset != XtOffset::Open; }
};

/**
 * @brief 安全设置 XtFillEvent::code（避免手写 strncpy 截断边界）。
 *
 * @param ev 目标 XtFillEvent。
 * @param src 来源合约码字符串；nullptr 会清空 code。
 */
inline void fill_event_set_code(XtFillEvent& ev, const char* src) {
    if (!src) { ev.code[0] = '\0'; return; }
    const size_t n = ::strnlen(src, sizeof(ev.code) - 1);
    ::memcpy(ev.code, src, n);
    ev.code[n] = '\0';
}

// POD invariant: hot-path fill event, no heap allocation.
static_assert(std::is_trivially_copyable_v<XtFillEvent>,
              "XtFillEvent must remain trivially_copyable for hot-path queue transport");
