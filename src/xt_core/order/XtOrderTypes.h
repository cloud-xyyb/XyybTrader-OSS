#pragma once
#ifndef XT_CORE_ORDER_XT_ORDER_TYPES_H
#define XT_CORE_ORDER_XT_ORDER_TYPES_H
#include <cstdint>
#include <type_traits>

#include "XtOffset.h"
#include "XtFixedPoint.h"

/**
 * @file
 * @brief 订单执行事件的基础类型与 fixed-point helper。
 *
 * 本文件定义提交后执行链路共用的枚举和 POD：报单有效期、订单状态、
 * 执行增量更新和账户快照。
 * `order_exec_to_fp()` / `order_exec_from_fp()` 统一 `_fp` 字段与 double 边界的转换口径；
 * `order_status_*()` 提供状态字符串、终态判断和更新排序。
 *
 * 这些类型不携带 broker/system order id 等 venue-specific 字段。
 */

enum class XtTimeInForce {
    GTC,
    IOC,
    FOK
};

constexpr int64_t kOrderExecFpScale = kPriceFpScale;

static_assert(kOrderExecFpScale == kPriceFpScale,
              "kOrderExecFpScale must equal kPriceFpScale (SSOT)");

inline int64_t order_exec_to_fp(double v) {
    return ::to_fp_trunc(v);
}

inline double order_exec_from_fp(int64_t v) {
    return static_cast<double>(v) / static_cast<double>(kOrderExecFpScale);
}

constexpr uint64_t kReservedProbeCancelOrderId = 0xFFFFFFFFFFFFFF00ULL;

enum class XtOrderStatus {
    Submitted,
    Accepted,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Recovered
};

inline const char* order_status_to_str(XtOrderStatus s) {
    switch (s) {
    case XtOrderStatus::Submitted:       return "Submitted";
    case XtOrderStatus::Accepted:        return "Accepted";
    case XtOrderStatus::PartiallyFilled: return "PartiallyFilled";
    case XtOrderStatus::Filled:          return "Filled";
    case XtOrderStatus::Canceled:        return "Canceled";
    case XtOrderStatus::Rejected:        return "Rejected";
    case XtOrderStatus::Recovered:       return "Recovered";
    default:                             return "Unknown";
    }
}

inline bool order_status_is_final(XtOrderStatus s) {
    return s == XtOrderStatus::Filled ||
           s == XtOrderStatus::Canceled ||
           s == XtOrderStatus::Rejected;
}

inline int order_status_rank(XtOrderStatus s) noexcept {
    switch (s) {
    case XtOrderStatus::Submitted:        return 0;
    case XtOrderStatus::Accepted:         return 1;
    case XtOrderStatus::PartiallyFilled:  return 2;
    case XtOrderStatus::Filled:           return 4;
    case XtOrderStatus::Canceled:         return 5;
    case XtOrderStatus::Rejected:         return 6;
    case XtOrderStatus::Recovered:        return 1;
    }
    return 0;
}

/**
 * @brief 热路径订单执行增量更新。
 *
 * 来源为 exchange gateway 或 sim match engine。结构保持 trivially_copyable：
 * symbol 与 strategy 均使用整型 intern key，成交量字段区分累计成交
 * `filled_fp`、本次增量 `last_fill_fp` 和剩余量 `leaves_fp`。
 */
struct XtOrderExecUpdate {

    uint64_t id = 0;

    uint64_t symbol_id = 0;

    bool is_buy = true;

    XtOffset close_offset = XtOffset::Open;

    XtOrderStatus status = XtOrderStatus::Submitted;

    int64_t price_fp = 0;

    int64_t qty_fp = 0;

    int64_t filled_fp = 0;

    int64_t leaves_fp = 0;

    int64_t last_fill_fp = 0;

    uint64_t exch_ts_us = 0;

    uint64_t local_cb_us = 0;

    uint64_t local_cb_ns = 0;

    uint64_t local_strategy_us = 0;

    uint32_t sid_id = 0;

    bool is_close() const { return close_offset != XtOffset::Open; }
    double price() const { return order_exec_from_fp(price_fp); }
    double qty() const { return order_exec_from_fp(qty_fp); }
    double filled() const { return order_exec_from_fp(filled_fp); }
    double remaining() const { return order_exec_from_fp(leaves_fp); }
    double last_fill() const { return order_exec_from_fp(last_fill_fp); }
};

/**
 * @brief 成交后账户快照（hot-path account snapshot）。
 *
 * @details
 * 经独立 on_account 回调推送（与 on_fill 独立），表示成交后的账户状态。
 * 所有值为快照（非增量）。
 */
struct XtAccountUpdate {

    int64_t cash_fp = 0;

    int64_t equity_fp = 0;

    int64_t unrealized_fp = 0;

    double cash() const { return order_exec_from_fp(cash_fp); }
    double equity() const { return order_exec_from_fp(equity_fp); }
    double unrealized() const { return order_exec_from_fp(unrealized_fp); }
};

static_assert(std::is_trivially_copyable_v<XtOrderExecUpdate>,
              "XtOrderExecUpdate must be trivially_copyable for hot-path POD invariant; "
              "uses uint32 sid_id (not std::string strategy_id) to satisfy this.");

#endif
