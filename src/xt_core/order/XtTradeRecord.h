#pragma once

#include <cstdint>
#include <string>

#include "XtFixedPoint.h"
#include "XtFillEvent.h"
#include "XtOffset.h"
#include "XtOrderTypes.h"

/**
 * @brief 完整成交审计记录（full fill audit record）。
 *
 * 冷路径结构，承载 broker 回报 ID、自然日/交易日、成本与本地 trace 字段。
 * 策略回调和仓位更新使用 `to_fill_event()` 提取出的 `XtFillEvent`。
 */
struct XtTradeRecord {

    uint64_t order_id = 0;

    std::string code;

    bool is_buy = true;

    int64_t qty_fp = 0;

    double price = 0.0;

    double exec_price = 0.0;

    uint32_t order_date = 0;

    uint32_t order_time = 0;

    uint32_t trade_date = 0;

    uint32_t trade_time = 0;

    uint32_t trading_day = 0;

    uint64_t fill_ts_us = 0;

    bool is_close = false;

    bool is_today = false;

    XtOffset close_offset = XtOffset::Open;

    std::string exchange_id;

    std::string trade_id;

    std::string order_ref;

    std::string order_sys_id;

    bool is_passive = false;

    double fee = 0.0;

    double slippage = 0.0;

    std::string user_tag;

    std::string strategy_id;

    std::string trace_id;

    uint64_t local_submit_begin_us = 0;

    uint64_t local_submit_end_us = 0;

    uint64_t local_cb_us = 0;

    uint64_t local_strategy_us = 0;

    int64_t realized_pnl_fp = 0;

    uint32_t sid_id = 0;

    int64_t sim_response_latency_ns = -1;

    int64_t sim_visible_at_ns = -1;

    XtFillEvent to_fill_event() const {
        XtFillEvent f;
        f.order_id       = order_id;

        f.exec_price_fp  = ::to_fp_round(exec_price);
        f.qty_fp         = qty_fp;
        f.fee_fp         = ::to_fp_round(fee);
        f.slippage_fp    = ::to_fp_round(slippage);
        f.close_offset   = close_offset;
        f.is_buy         = is_buy;
        f.is_today       = is_today;
        fill_event_set_code(f, code.c_str());
        return f;
    }
};
