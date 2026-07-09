#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "api/StrategySid.h"
#include "xt_core/order/XtOffset.h"

/**
 * @brief Recovery 反向灌 alive in-flight order 的 POD（plain old data）描述。
 *
 * @details
 * Recovery 场景下，gateway 从 exchange 当前状态重建仍活跃的 in-flight order，
 * 并以本 POD 批量上报。`local_id` 来源于 exchange clientOrderId / OrderRef
 * 反解；`filled_qty_fp` 来源于 venue 历史成交累计；所有 `*_fp` 字段使用
 * order execution fixed-point scale。
 *
 * @note 非热路径：通常在 reconnect / login 完成后批量产生一次。
 */
struct AliveInflightOrder {
    uint64_t local_id = 0;
    std::string code;
    bool is_buy = true;
    int64_t total_qty_fp = 0;
    int64_t filled_qty_fp = 0;
    int64_t price_fp = 0;
    XtOffset close_offset = XtOffset::Open;
    StrategySid strategy_sid{};
};

// 跨 engine 共享 ReverseSync batch event payload。
// `orders` 是同一 recovery 批次的 exchange-alive in-flight orders；
// `is_cold_start` 标记 cold-start 首次 recovery 与 warm reconnect。
struct ReverseSyncBatchEventPayload {
    std::vector<AliveInflightOrder> orders;
    bool is_cold_start = false;
};
