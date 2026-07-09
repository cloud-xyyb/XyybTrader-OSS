#pragma once

#include <cstdint>
#include <string>

#include "xt_report/core/XtReportTypes.h"

struct XtTradeRecord;

namespace xt_report {

bool report_wall_clock_in_range(uint32_t date, uint32_t time_ms) noexcept;

double finite_or_zero(double v) noexcept;

/// @brief 事件到 ReportEvent 的转换器。
///
/// 回测和实盘的成交/KPI/持仓事件在这里转成统一的报表行结构；
/// 所有方法 static 无状态，不做 I/O，调用方负责把 Row 写入 sink。
struct XtReportEventBuilder {

    static XtFillRow build_fill(const XtTradeRow& trade);

    static XtFillRow build_fill(const XtLiveFillEvent& fill);

    static XtFillRow build_fill(const XtTradeRecord& tr);

    static XtKpi1sRow build_kpi(int64_t ts_sec,
                                double pnl_net, double pnl_gross,
                                double turnover, double uptime_sec,
                                const OrderRatios& ratios,
                                const LatencyStats& latency,
                                bool kill_switch_active = false);

private:

    static const char* fill_close_offset_label(bool is_close, XtOffset close_offset);
    static const char* fill_close_offset_label_live(bool is_close, bool is_today);
};

}
