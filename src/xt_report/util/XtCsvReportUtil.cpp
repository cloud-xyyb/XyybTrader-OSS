#include "XtCsvReportUtil.h"

#include <algorithm>
#include <cstdio>

namespace xt_report {

std::string csv_escape(const std::string& in) {

    bool need_quote = false;
    bool has_ctrl   = false;
    for (char c : in) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') { need_quote = true; continue; }
        if (static_cast<unsigned char>(c) < 0x20) has_ctrl = true;
    }
    if (!need_quote && !has_ctrl) return in;
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

    n = std::snprintf(buf, sizeof(buf), ",%llu,%llu,%llu",
                      static_cast<unsigned long long>(l.submit_to_ack_us_samples),
                      static_cast<unsigned long long>(l.fill_after_ack_us_samples),
                      static_cast<unsigned long long>(l.local_submit_us_samples));
    append_fmt(line, buf, n);

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

           "kill_switch_active,"
           "submit_to_ack_paired_us_avg,submit_to_ack_paired_us_p50,submit_to_ack_paired_us_p90,submit_to_ack_paired_us_p95,submit_to_ack_paired_us_p99,"

           "md_gap_ms_peak,"

           "submit_to_ack_us_samples,fill_after_ack_us_samples,local_submit_us_samples,"

           "submit_to_ack_paired_us_samples,submit_to_trade_ack_us_samples,uds_first_savings_us_samples,"
           "submit_to_fill_us_samples,strategy_us_samples,ack_path_us_samples,"
           "event_to_ack_us_samples,event_to_fill_us_samples\n";
}

std::string csv_header_fill() {

    return std::string(kDimsHeader) +
           ",ts_ms,order_id,side,qty,price,fee,slippage,is_close,is_today,close_offset,trace_id,trading_day,ts_event_ns,ts_recv_ns,trade_id,realized_pnl\n";
}

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

    line += '\n';
    return line;
}

}
