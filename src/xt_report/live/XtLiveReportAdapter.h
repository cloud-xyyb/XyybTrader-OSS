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

int64_t trade_ts_ms_from_record(const XtTradeRecord& tr, int64_t fallback_wall_ms) noexcept;

void build_live_fill_events(const std::vector<XtTradeRecord>& recent_trades,
                            int64_t fallback_wall_ms,
                            std::vector<XtLiveFillEvent>& out);

}
