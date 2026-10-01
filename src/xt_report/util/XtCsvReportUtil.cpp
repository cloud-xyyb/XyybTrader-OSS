#include "XtCsvReportUtil.h"

#include <algorithm>
#include <cstdio>

namespace xt_report {

std::string csv_escape(const std::string& in) {
    // C0 控制字符（\r\n 除外）剔除：NUL 会截断下游 C-string 解析器，其余 C0
    // 在诊断字段（symbol/strategy_id/trace_id）无合法语义；\r\n 走 RFC-4180 quote
    // 保留。公式注入（=+-@ 前导）为已知限制不做转义：下游是 pandas/analyzer
    // 非电子表格，前置引号会污染合法数据。
    bool need_quote = false;
    bool has_ctrl   = false;
    for (char c : in) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') { need_quote = true; continue; }
        if (static_cast<unsigned char>(c) < 0x20) has_ctrl = true;
    }
    if (!need_quote && !has_ctrl) return in;  // 干净输入零拷贝（热路径常态）
    std::string out;
    out.reserve(in.size() + 8);
    if (need_quote) out.push_back('"');
    for (char c : in) {
        if (static_cast<unsigned char>(c) < 0x20 && c != '\n' && c != '\r') continue;
        if (c == '"') out.push_back('"');
        out.push_back(c);
    }
    if (need_quote) out.push_back('"');
    return out;
}

std::string format_dims_prefix(const XtContractDims& dims) {
    return format_dims_prefix(dims, std::string{}, std::string{});
}

std::string format_dims_prefix(const XtContractDims& dims,
                               const std::string& strategy_id_override,
                               const std::string& symbol_override) {
    std::string prefix;
    const std::string& strategy_id = strategy_id_override.empty()
        ? dims.strategy_id : strategy_id_override;
    const std::string& symbol = symbol_override.empty()
        ? dims.symbol : symbol_override;
    prefix += csv_escape(dims.mode);         prefix += ',';
    prefix += csv_escape(dims.run_id);       prefix += ',';
    prefix += csv_escape(strategy_id);       prefix += ',';
    prefix += csv_escape(symbol);            prefix += ',';
    prefix += csv_escape(dims.config_hash);  prefix += ',';
    prefix += csv_escape(dims.code_version); prefix += ',';
    prefix += csv_escape(dims.session_date);
    return prefix;
}


// snprintf 返回「应写长度」，截断时 n >= 缓冲容量；append 必须钳到实写上限 N-1，
// 否则越界读栈。%.2f/%.6f 对大 magnitude 有限值（如 1e300 → 300+ 字符）并非定宽；
// 截断时丢失该 block 从溢出点起的尾部列（行终止符由各 to_csv_line 独立追加恒闭合，
// 无 UB 无行粘连）；正常值域远小于容量不触发，上游 finite 守卫另拦 nan/inf。
template <size_t N>
static void append_fmt(std::string& line, const char (&buf)[N], int n) {
    if (n <= 0) return;
    line.append(buf, std::min(static_cast<size_t>(n), N - 1));
}

static void append_latency5(std::string& line,
                            double avg, double p50, double p90, double p95, double p99) {
    char buf[96];
    int n = std::snprintf(buf, sizeof(buf), ",%.2f,%.2f,%.2f,%.2f,%.2f",
                          avg, p50, p90, p95, p99);
    append_fmt(line, buf, n);
}

static void append_latency4(std::string& line,
                            double avg, double p50, double p90, double p99) {
    char buf[80];
    int n = std::snprintf(buf, sizeof(buf), ",%.2f,%.2f,%.2f,%.2f",
                          avg, p50, p90, p99);
    append_fmt(line, buf, n);
}

std::string to_csv_line(const std::string& dims_prefix, const XtKpi1sRow& row) {
    // latency fields 通过 row.latency.xxx access (XtKpi1sRow nested LatencyStats)。
    std::string line;
    line.reserve(768);
    line += dims_prefix;
    const auto& l = row.latency;
    char buf[256];
    int n = std::snprintf(buf, sizeof(buf),
                          ",%lld,%.6f,%.6f,%.6f"
                          ",%.6f,%.6f,%.6f,%.6f,%.6f",
                          static_cast<long long>(row.ts_sec),
                          row.pnl_gross, row.pnl_net, row.turnover,
                          row.fill_ratio, row.cancel_ratio, row.order_cancel_ratio,
                          row.partial_cancel_ratio, row.reject_ratio);
    append_fmt(line, buf, n);
    append_latency5(line, l.submit_to_ack_us_avg, l.submit_to_ack_us_p50,
                    l.submit_to_ack_us_p90, l.submit_to_ack_us_p95,
                    l.submit_to_ack_us_p99);
    append_latency5(line, l.submit_to_trade_ack_us_avg, l.submit_to_trade_ack_us_p50,
                    l.submit_to_trade_ack_us_p90, l.submit_to_trade_ack_us_p95,
                    l.submit_to_trade_ack_us_p99);
    append_latency5(line, l.uds_first_savings_us_avg, l.uds_first_savings_us_p50,
                    l.uds_first_savings_us_p90, l.uds_first_savings_us_p95,
                    l.uds_first_savings_us_p99);
    append_latency5(line, l.submit_to_fill_us_avg, l.submit_to_fill_us_p50,
                    l.submit_to_fill_us_p90, l.submit_to_fill_us_p95,
                    l.submit_to_fill_us_p99);
    append_latency4(line, l.strategy_us_avg, l.strategy_us_p50,
                    l.strategy_us_p90, l.strategy_us_p99);
    append_latency4(line, l.local_submit_us_avg, l.local_submit_us_p50,
                    l.local_submit_us_p90, l.local_submit_us_p99);
    append_latency4(line, l.ack_path_us_avg, l.ack_path_us_p50,
                    l.ack_path_us_p90, l.ack_path_us_p99);
    append_latency4(line, l.fill_after_ack_us_avg, l.fill_after_ack_us_p50,
                    l.fill_after_ack_us_p90, l.fill_after_ack_us_p99);
    append_latency4(line, l.event_to_ack_us_avg, l.event_to_ack_us_p50,
                    l.event_to_ack_us_p90, l.event_to_ack_us_p99);
    append_latency4(line, l.event_to_fill_us_avg, l.event_to_fill_us_p50,
                    l.event_to_fill_us_p90, l.event_to_fill_us_p99);
    n = std::snprintf(buf, sizeof(buf), ",%llu,%.3f,%llu,%.2f,%.2f,%d",
                          static_cast<unsigned long long>(l.risk_block_count),
                          l.md_gap_ms_p99,
                          static_cast<unsigned long long>(l.event_drop),
                          l.queue_depth_p99, row.uptime_sec,
                          row.kill_switch_active);
    append_fmt(line, buf, n);
    append_latency5(line, l.submit_to_ack_paired_us_avg, l.submit_to_ack_paired_us_p50,
                    l.submit_to_ack_paired_us_p90, l.submit_to_ack_paired_us_p95,
                    l.submit_to_ack_paired_us_p99);
    n = std::snprintf(buf, sizeof(buf), ",%.3f", l.md_gap_ms_peak);
    append_fmt(line, buf, n);
    // Per-stage 样本计数（fail-closed budget 消费）；append-at-end 保 md_gap_ms_peak 之后。
    n = std::snprintf(buf, sizeof(buf), ",%llu,%llu,%llu",
                      static_cast<unsigned long long>(l.submit_to_ack_us_samples),
                      static_cast<unsigned long long>(l.fill_after_ack_us_samples),
                      static_cast<unsigned long long>(l.local_submit_us_samples));
    append_fmt(line, buf, n);
    // 其余 stage 组样本计数，append 在上面 3 列之后（顺序与 struct 追加序一致）。
    n = std::snprintf(buf, sizeof(buf), ",%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu",
                      static_cast<unsigned long long>(l.submit_to_ack_paired_us_samples),
                      static_cast<unsigned long long>(l.submit_to_trade_ack_us_samples),
                      static_cast<unsigned long long>(l.uds_first_savings_us_samples),
                      static_cast<unsigned long long>(l.submit_to_fill_us_samples),
                      static_cast<unsigned long long>(l.strategy_us_samples),
                      static_cast<unsigned long long>(l.ack_path_us_samples),
                      static_cast<unsigned long long>(l.event_to_ack_us_samples),
                      static_cast<unsigned long long>(l.event_to_fill_us_samples));
    append_fmt(line, buf, n);
    line += '\n';
    return line;
}

std::string to_csv_line(const std::string& dims_prefix, const XtFillRow& row) {
    std::string line;
    line.reserve(256);
    line += dims_prefix;
    char buf[128];
    int n = std::snprintf(buf, sizeof(buf), ",%lld,%llu,",
                          static_cast<long long>(row.ts_ms),
                          static_cast<unsigned long long>(row.order_id));
    append_fmt(line, buf, n);
    line += csv_escape(row.side);
    n = std::snprintf(buf, sizeof(buf), ",%.6f,%.6f,%.6f,%.6f,%d,%d,",
                      row.qty, row.price, row.fee, row.slippage,
                      row.is_close, row.is_today);
    append_fmt(line, buf, n);
    line += csv_escape(row.close_offset);
    line += ',';
    line += csv_escape(row.trace_id);
    // trading_day (YYYYMMDD) — 0 时打印 0 (下游据此判断未填, 退化到 ts_ms 自然日).
    // ts_event_ns + ts_recv_ns 双 ns 字段附在末尾,跨 BT/Live 统一 latency 计算口径。
    n = std::snprintf(buf, sizeof(buf), ",%u,%lld,%lld,%llu,%.6f",
                      row.trading_day,
                      static_cast<long long>(row.ts_event_ns),
                      static_cast<long long>(row.ts_recv_ns),
                      static_cast<unsigned long long>(row.trade_id),
                      static_cast<double>(row.realized_pnl_fp) / 1e6);
    append_fmt(line, buf, n);
    line += '\n';
    return line;
}

// ── CSV 表头 ──────────────────────────────────────────────────────

std::string csv_header_kpi_1s() {
    return std::string(kDimsHeader) +
           ",ts_sec,pnl_gross,pnl_net,turnover,"
           "fill_ratio,cancel_ratio,order_cancel_ratio,partial_cancel_ratio,reject_ratio,"
           "submit_to_ack_us_avg,submit_to_ack_us_p50,submit_to_ack_us_p90,submit_to_ack_us_p95,submit_to_ack_us_p99,"
           "submit_to_trade_ack_us_avg,submit_to_trade_ack_us_p50,submit_to_trade_ack_us_p90,submit_to_trade_ack_us_p95,submit_to_trade_ack_us_p99,"
           "uds_first_savings_us_avg,uds_first_savings_us_p50,uds_first_savings_us_p90,uds_first_savings_us_p95,uds_first_savings_us_p99,"
           "submit_to_fill_us_avg,submit_to_fill_us_p50,submit_to_fill_us_p90,submit_to_fill_us_p95,submit_to_fill_us_p99,"
           "strategy_us_avg,strategy_us_p50,strategy_us_p90,strategy_us_p99,"
           "local_submit_us_avg,local_submit_us_p50,local_submit_us_p90,local_submit_us_p99,"
           "ack_path_us_avg,ack_path_us_p50,ack_path_us_p90,ack_path_us_p99,"
           "fill_after_ack_us_avg,fill_after_ack_us_p50,fill_after_ack_us_p90,fill_after_ack_us_p99,"
           "event_to_ack_us_avg,event_to_ack_us_p50,event_to_ack_us_p90,event_to_ack_us_p99,"
           "event_to_fill_us_avg,event_to_fill_us_p50,event_to_fill_us_p90,event_to_fill_us_p99,"
           "risk_block_count,md_gap_ms_p99,event_drop,queue_depth_p99,uptime_sec,"
           // kill_switch_active: 0=正常 / 1=kill_switch 激活（需人工介入）
           "kill_switch_active,"
           "submit_to_ack_paired_us_avg,submit_to_ack_paired_us_p50,submit_to_ack_paired_us_p90,submit_to_ack_paired_us_p95,submit_to_ack_paired_us_p99,"
           // md_gap_ms_peak: 与 md_gap_ms_p99 配对（p99=窗口真分位 / peak=窗口最大值）。
           "md_gap_ms_peak,"
           // Per-stage 样本计数（fail-closed budget 消费）；append-at-end，md_gap_ms_peak
           // 不再是末列。全仓 grep 确认无按位置/负索引读 kpi_1s 末列的消费者。
           "submit_to_ack_us_samples,fill_after_ack_us_samples,local_submit_us_samples,"
           // 其余 stage 组样本计数，append 在上面 3 列之后。
           "submit_to_ack_paired_us_samples,submit_to_trade_ack_us_samples,uds_first_savings_us_samples,"
           "submit_to_fill_us_samples,strategy_us_samples,ack_path_us_samples,"
           "event_to_ack_us_samples,event_to_fill_us_samples\n";
}

std::string csv_header_fill() {
    // trading_day (YYYYMMDD): 交易日, 夜盘归次日 (CTP broker TradingDay 权威).
    // 放在 trace_id 后保证向前兼容 (老脚本按列名读仍可; 按位置读的脚本须更新).
    // ts_event_ns + ts_recv_ns 双 ns 字段附末尾 (业内 nautilus / hftbacktest 对齐)。
    // trade_id follows the time fields so older name-based readers remain stable.
    return std::string(kDimsHeader) +
           ",ts_ms,order_id,side,qty,price,fee,slippage,is_close,is_today,close_offset,trace_id,trading_day,ts_event_ns,ts_recv_ns,trade_id,realized_pnl\n";
}

// 多合约持仓快照: 注意 dims_prefix.symbol 是 strategy 主合约, 行内 symbol
// 才是每个持仓合约 (换月期会出现 dims.symbol=ag2606 但行 symbol=ag2612).
// 字段包括: position (qty/long_qty/short_qty/avg_price/mark_price/unrealized_pnl)
// 与价格 snapshot (last_price/bid_price/ask_price/volume)。
std::string csv_header_position_snapshot() {
    return std::string(kDimsHeader) +
           ",ts_sec,position_symbol,qty,long_qty,short_qty,avg_price,mark_price,unrealized_pnl"
           ",last_price,bid_price,ask_price,volume\n";
}

std::string csv_header_account_truth() {
    return std::string(kDimsHeader) +
           ",ts_sec,wallet_balance,position_value,unrealized_pnl,fees_today,external_cashflow_today,truth_ts_ns,trading_day\n";
}

std::string to_csv_line(const std::string& dims_prefix, const XtPositionRow& row) {
    std::string line;
    line.reserve(256);
    line += dims_prefix;
    // 变长 symbol 直接拼接，不经定长 buf；数值字段走 append_fmt 钳截断
    // （%.6f 对大 magnitude 非定宽，钳不住会越界读栈）。
    char buf[256];
    int n = std::snprintf(buf, sizeof(buf), ",%lld,",
                          static_cast<long long>(row.ts_sec));
    append_fmt(line, buf, n);
    line += csv_escape(row.symbol);
    n = std::snprintf(buf, sizeof(buf),
                      ",%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%lld",
                      row.qty, row.long_qty, row.short_qty,
                      row.avg_price, row.mark_price, row.unrealized_pnl,
                      row.last_price, row.bid_price, row.ask_price,
                      static_cast<long long>(row.volume));
    append_fmt(line, buf, n);
    // '\n' 独立追加（不入 format 串）：截断只丢单元格内容，行终止符恒闭合，
    // 下游行级解析不会发生行粘连。
    line += '\n';
    return line;
}

std::string to_csv_line(const std::string& dims_prefix, const XtAccountTruthRow& row) {
    std::string line;
    line.reserve(192);
    line += dims_prefix;
    char buf[192];
    int n = std::snprintf(buf, sizeof(buf),
                          ",%lld,%.6f,%.6f,%.6f,%.6f,%.6f,%llu,%lld",
                          static_cast<long long>(row.ts_sec),
                          row.wallet_balance,
                          row.position_value,
                          row.unrealized_pnl,
                          row.fees_today,
                          row.external_cashflow_today,
                          static_cast<unsigned long long>(row.truth_ts_ns),
                          static_cast<long long>(row.trading_day));
    append_fmt(line, buf, n);
    // '\n' 独立追加，同上：行终止符不受截断影响。
    line += '\n';
    return line;
}

} // namespace xt_report
