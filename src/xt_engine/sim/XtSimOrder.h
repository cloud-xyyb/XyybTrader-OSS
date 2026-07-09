#pragma once
#include <cstdint>
#include <string>
#include "xt_core/order/XtOffset.h"
#include "xt_core/order/XtOrderTypes.h"

/// @brief 表示 HFT 回测撮合中的单笔模拟订单。
///
/// `XtOrder` 是策略提交的订单意图；`XtSimOrder` 是该意图进入回测撮合后的可变状态。
/// 它保存成交进度、排队位置、ACK/TTL、撤单延迟和回报生成所需的模拟字段。
///
/// `XtOrderManager` 创建并更新本结构，`XtOrderBook` 只按 id 引用挂单。
/// 对象只在回测撮合线程内读写，不提供并发同步。
/// 价格、数量和成交量使用 `kOrderExecFpScale` fixed-point；`queue_ahead` 使用
/// double，以匹配队列模型和 L2 盘口数量。
struct XtSimOrder {

    uint64_t id = 0;
    std::string code;
    bool is_buy = true;
    int64_t price_fp = 0;
    int64_t qty_fp   = 0;

    int64_t filled_fp   = 0;
    double  queue_ahead = 0.0;
    XtOrderStatus status = XtOrderStatus::Submitted;
    XtTimeInForce tif = XtTimeInForce::GTC;

    uint64_t create_tick = 0;
    uint32_t create_time_ms = 0;
    uint32_t order_date = 0;
    uint32_t order_time = 0;
    uint64_t ack_due_tick = 0;
    uint32_t ack_due_time_ms = 0;
    uint32_t fill_eligible_time_ms = 0;

    uint32_t cancel_due_time_ms = 0;

    int64_t response_latency_ns = -1;

    bool is_passive = true;

    bool is_resubmit = false;
    std::string user_tag;
    XtOffset close_offset = XtOffset::Open;

    int64_t remaining_fp() const noexcept { return qty_fp - filled_fp; }

    double remaining() const noexcept { return order_exec_from_fp(remaining_fp()); }
};
