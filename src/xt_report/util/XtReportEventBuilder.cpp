#include "XtReportEventBuilder.h"

#include <cmath>

#include "xt_core/order/XtTradeRecord.h"
#include "xt_util/time/XtTime.h"

namespace xt_report {

// ── 共享 range core + fail-safe（header 声明，live adapter 复用）─────────────

bool report_wall_clock_in_range(uint32_t date, uint32_t time_ms) noexcept {
    const int y = static_cast<int>(date / 10000u);
    const unsigned m = static_cast<unsigned>((date / 100u) % 100u);
    const unsigned d = static_cast<unsigned>(date % 100u);
    if (y <= 0 || m < 1 || m > 12 || d < 1 || d > 31) return false;

    const uint32_t hms = time_ms / 1000u;
    const uint32_t hh = hms / 10000u;
    const uint32_t mm = (hms / 100u) % 100u;
    const uint32_t ss = hms % 100u;
    return hh <= 23u && mm <= 59u && ss <= 59u;
}

// 非有限 double 收敛 0（上游 venue/engine 契约保证有限；此为 fail-safe，
// 防 nan/inf token 写进 CSV 污染下游 analyzer 聚合）。守点 = 裸 double 直通字段
// 的 builder 入口（fill 的 price/fee/slippage + kpi 的 pnl/turnover）；
// Position/AccountTruth 行的全部 populate 点（XtHftControlReportEmit 的
// build_per_symbol_snapshots / to_account_truth_row）经核实均由 xt_fp::from_fp
// （int64 定点）派生，latency/ratio 由内部直方图/分母守卫派生——结构性有限，
// 不重复设有限性守卫；非-fp 来源的 populate 点必须在入口调用 finite_or_zero。
double finite_or_zero(double v) noexcept { return std::isfinite(v) ? v : 0.0; }

// ── 文件作用域时间转换 ──────────────────────────────────
//
// XtTradeRow 缺失或畸形 wall clock 输出 -1；有效字段的 epoch-ms 换算委托
// xt_time::to_ns，日期算术由 xt_util/time 统一承载。
namespace {

// builder 侧「字段缺失」门：date==0||time==0 视为缺失（与 range core 分层，
// 保持 build_fill 输出字节不变）。
bool valid_report_wall_clock(uint32_t date, uint32_t time_ms) noexcept {
    if (date == 0 || time_ms == 0) return false;
    return report_wall_clock_in_range(date, time_ms);
}

int64_t wall_clock_ts_ms(uint32_t date, uint32_t time_ms) noexcept {
    if (!valid_report_wall_clock(date, time_ms)) return -1;
    return xt_time::to_ns(date, time_ms) / 1'000'000LL;
}

} // namespace

const char* XtReportEventBuilder::fill_close_offset_label(bool is_close, XtOffset close_offset) {
    if (!is_close) return "OPEN";
    switch (close_offset) {
    case XtOffset::CloseToday:    return "TODAY";
    case XtOffset::CloseYesterday: return "YESTERDAY";
    case XtOffset::Close:
    default:                       return "AUTO";
    }
}

const char* XtReportEventBuilder::fill_close_offset_label_live(bool is_close, bool is_today) {
    if (!is_close) return "OPEN";
    return is_today ? "TODAY" : "YESTERDAY";
}

// ── 成交转换 ──────────────────────────────────────────────────────────

XtFillRow XtReportEventBuilder::build_fill(const XtTradeRow& t) {
    XtFillRow f;
    // Backtest fill 写 epoch-ms；wall clock 缺失或畸形时保留 -1 invalid 信号。
    f.ts_ms        = wall_clock_ts_ms(t.trade_date, t.trade_time);
    // ts_event_ns / ts_recv_ns 双字段 (schema v2)。
    // BT 路径: ts_event_ns = ts_recv_ns = wall_clock ns (sim engine 内 event/recv 同
    // 一时刻;driver 单线程零网络延迟)。
    f.ts_event_ns  = (f.ts_ms > 0) ? f.ts_ms * 1'000'000LL : 0;
    f.ts_recv_ns   = f.ts_event_ns;
    f.order_id     = t.order_id;
    f.side         = t.is_buy ? "BUY" : "SELL";
    f.qty          = finite_or_zero(t.qty);
    f.price        = finite_or_zero(t.exec_price);
    f.fee          = finite_or_zero(t.fee);
    f.slippage     = finite_or_zero(t.slippage);
    f.is_close     = t.is_close ? 1 : 0;
    // 契约：非 close 行 is_today 恒 0（XtReportTypes.h），builder 强制不依赖 caller 纪律。
    f.is_today     = (t.is_close && t.is_today) ? 1 : 0;
    f.close_offset = fill_close_offset_label(t.is_close, t.close_offset);
    f.trace_id     = t.user_tag;
    // 回测无独立 broker 交易日，trading_day 通常等同 trade_date。
    f.trading_day  = t.trading_day;
    return f;
}

XtFillRow XtReportEventBuilder::build_fill(const XtTradeRecord& tr) {
    // 复用 XtTradeRow 路径生成共享列；strategy_id / symbol / realized_pnl
    // 来自 engine 归因后的 XtTradeRecord。
    XtTradeRow row;
    row.order_id     = tr.order_id;
    row.code         = tr.code;
    row.is_buy       = tr.is_buy;
    row.qty          = order_exec_from_fp(tr.qty_fp);
    row.exec_price   = tr.exec_price;
    row.fee          = tr.fee;
    row.slippage     = tr.slippage;
    row.order_date   = tr.order_date;
    row.order_time   = tr.order_time;
    row.trade_date   = tr.trade_date;
    row.trade_time   = tr.trade_time;
    // 回测无独立 broker 交易日，缺省时退化到成交自然日。
    row.trading_day  = tr.trading_day != 0 ? tr.trading_day : tr.trade_date;
    row.user_tag     = tr.user_tag;
    row.is_close     = tr.is_close;
    row.is_today     = tr.is_today;
    row.close_offset = tr.close_offset;

    XtFillRow f = build_fill(row);
    f.strategy_id     = tr.strategy_id;
    f.symbol          = tr.code;
    f.realized_pnl_fp = tr.realized_pnl_fp;
    return f;
}

XtFillRow XtReportEventBuilder::build_fill(const XtLiveFillEvent& fill) {
    XtFillRow f;
    f.strategy_id   = fill.strategy_id;
    f.symbol        = fill.symbol;
    f.ts_ms        = fill.ts_ms;
    // Schema v2 拆分 exchange event time 与 local receive time；调用方未填双时间戳时
    // 用 ts_ms 补齐，保持 fills.csv wall-clock contract。
    f.ts_event_ns  = fill.ts_event_ns > 0
        ? fill.ts_event_ns
        : ((fill.ts_ms > 0) ? fill.ts_ms * 1'000'000LL : 0);
    f.ts_recv_ns   = fill.ts_recv_ns > 0 ? fill.ts_recv_ns : f.ts_event_ns;
    f.order_id     = fill.order_id;
    f.trade_id     = fill.trade_id;
    f.side         = fill.is_buy ? "BUY" : "SELL";
    f.qty          = finite_or_zero(fill.qty);
    f.price        = finite_or_zero(fill.price);
    f.fee          = finite_or_zero(fill.fee);
    f.slippage     = finite_or_zero(fill.slippage);
    f.is_close     = fill.is_close ? 1 : 0;
    // 同 XtTradeRow 路径：非 close 行 is_today 恒 0。
    f.is_today     = (fill.is_close && fill.is_today) ? 1 : 0;
    f.close_offset = fill_close_offset_label_live(fill.is_close, fill.is_today);
    f.trace_id     = fill.trace_id;
    // live trading_day 由 venue/gateway 写入；0 交给下游按 ts_ms 自然日兜底。
    f.trading_day  = fill.trading_day;
    f.realized_pnl_fp = fill.realized_pnl_fp;
    return f;
}

// ── KPI 转换 ──────────────────────────────────────────────────────────

XtKpi1sRow XtReportEventBuilder::build_kpi(int64_t ts_sec,
                                          double pnl_net, double pnl_gross,
                                          double turnover, double uptime_sec,
                                          const OrderRatios& ratios,
                                          const LatencyStats& latency,
                                          bool kill_switch_active) {
    // XtKpi1sRow.latency 是 LatencyStats 值类型成员，整 struct 赋值即 memcpy 等价路径。
    XtKpi1sRow row;
    row.ts_sec                = ts_sec;
    row.pnl_net               = finite_or_zero(pnl_net);
    row.pnl_gross             = finite_or_zero(pnl_gross);
    row.turnover              = finite_or_zero(turnover);
    row.fill_ratio            = ratios.fill_ratio;
    row.cancel_ratio          = ratios.cancel_ratio;
    row.order_cancel_ratio    = ratios.order_cancel_ratio;
    row.partial_cancel_ratio  = ratios.partial_cancel_ratio;
    row.reject_ratio          = ratios.reject_ratio;
    row.latency               = latency;
    row.uptime_sec            = uptime_sec;
    row.kill_switch_active    = kill_switch_active ? 1 : 0;
    return row;
}

} // namespace xt_report
