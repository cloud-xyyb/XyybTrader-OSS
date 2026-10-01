#pragma once

#include <cstdint>
#include <string>

#include "xt_report/core/XtReportTypes.h"

struct XtTradeRecord;

namespace xt_report {

/// @brief 报表 wall clock 字段 range 校验。
///
/// date 必须是合法 YYYYMMDD，HH/MM/SS 必须在界；time_ms==0 表示合法午夜。
/// “字段缺失”由调用点自行定义，例如 builder 把 date==0||time==0 视为缺失。
bool report_wall_clock_in_range(uint32_t date, uint32_t time_ms) noexcept;

/// @brief 非有限 double 收敛 0 的 fail-safe（防 nan/inf token 污染 CSV 下游聚合）。
double finite_or_zero(double v) noexcept;

/// @brief 事件到 ReportEvent 的转换器。
///
/// 回测和实盘的成交/KPI/持仓事件在这里转成统一的报表行结构；
/// 所有方法 static 无状态，不做 I/O，调用方负责把 Row 写入 sink。
struct XtReportEventBuilder {

    // ── 成交转换 ─────────────────────────────────
    /// @brief 将回测成交输入转换为 `fills.csv` 行。
    ///
    /// `ts_ms` 使用 `trade_date/trade_time` 生成 Unix epoch ms；解析失败时保留
    /// 非正值，调用方不得把它当 packed timestamp。
    /// @note 生产仅经 build_fill(XtTradeRecord) 内部复用；保持 public 供测试作
    ///       schema 覆盖入口直用。
    static XtFillRow build_fill(const XtTradeRow& trade);

    /// @brief 将 live fill adapter 输出转换为 `fills.csv` 行。
    ///
    /// 保留 `ts_event_ns/ts_recv_ns` 双时间戳；缺失时以 `ts_ms` 派生。
    static XtFillRow build_fill(const XtLiveFillEvent& fill);
    /// @brief 将引擎成交记录转换为回测 `fills.csv` 行。
    static XtFillRow build_fill(const XtTradeRecord& tr);

    // ── KPI 转换 ──────────────────────────────────
    /// @brief 构造 `kpi_1s.csv` 行。
    ///
    /// `ratios` 与 `latency` 由调用方按当前窗口计算；`kill_switch_active` 写成 CSV
    /// 的 0/1 标志。
    static XtKpi1sRow build_kpi(int64_t ts_sec,
                                double pnl_net, double pnl_gross,
                                double turnover, double uptime_sec,
                                const OrderRatios& ratios,
                                const LatencyStats& latency,
                                bool kill_switch_active = false);

private:
    // 内部 helper（仅 build_fill 两路径使用）：开平字段 → `close_offset` 标签。
    static const char* fill_close_offset_label(bool is_close, XtOffset close_offset);
    static const char* fill_close_offset_label_live(bool is_close, bool is_today);
};

} // namespace xt_report
