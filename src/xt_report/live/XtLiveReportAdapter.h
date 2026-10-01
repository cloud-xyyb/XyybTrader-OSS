#pragma once

/// @file
/// @brief 成交记录到 live 报表事件的转换 helper。
///
/// 控制环 drain 出的 XtTradeRecord 在这里换算 wall-clock 时间戳，
/// 转成 fills.csv 使用的事件结构。

#include <cstdint>
#include <vector>

#include "xt_core/order/XtTradeRecord.h"
#include "xt_report/core/XtReportTypes.h"

namespace xt_report {

/// @brief 从成交记录提取 fills.csv 的 wall-clock `ts_ms`。
///
/// 优先使用 `fill_ts_us` epoch 微秒；缺失时用 `trade_date/trade_time`
/// 经 xt_time::to_ns 换算；字段缺失或越界时回退到调用方传入的 wall-clock。
/// 本接口不得返回 packed timestamp 或 steady-clock。
///
/// @param fallback_wall_ms 调用方当前 system_clock epoch ms。
int64_t trade_ts_ms_from_record(const XtTradeRecord& tr, int64_t fallback_wall_ms) noexcept;

/// @brief 将 recent trades 转换为 XtLiveFillEvent 列表。
///
/// `out` 会被 clear；`qty_fp <= 0` 的记录被跳过。`trace_id` 优先使用
/// `XtTradeRecord::trace_id`，为空时回退到 `user_tag`。
///
/// @param fallback_wall_ms 调用方当前 system_clock epoch ms。
void build_live_fill_events(const std::vector<XtTradeRecord>& recent_trades,
                            int64_t fallback_wall_ms,
                            std::vector<XtLiveFillEvent>& out);

}  // namespace xt_report
