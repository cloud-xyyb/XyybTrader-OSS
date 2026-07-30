#pragma once

/// @file
/// @brief 报表 CSV 各行的数据结构。
///
/// 字段名直接对应 CSV header，Grafana/exporter/analyzer 按名消费；
/// 重命名字段就是破坏下游契约。

#include <cstdint>
#include <string>

#include "xt_core/order/XtOffset.h"

namespace xt_report {

/// @brief 每行 CSV 的公共维度前缀。
struct XtContractDims {
    std::string mode;
    std::string run_id;
    std::string strategy_id;
    std::string symbol;
    std::string config_hash;
    std::string code_version;
    std::string session_date;
};

/// @brief KPI CSV 中的延迟与队列统计字段。
///
/// 除 `md_gap_ms_p99` 外，`*_us_*` 延迟字段单位均为微秒。
/// 字段名直接对应 CSV header，重命名会影响 Grafana/exporter/analyzer。
struct LatencyStats {

    double submit_to_ack_us_avg  = 0.0;
    double submit_to_ack_us_p50  = 0.0;
    double submit_to_ack_us_p90  = 0.0;
    double submit_to_ack_us_p95  = 0.0;
    double submit_to_ack_us_p99  = 0.0;

    double submit_to_ack_paired_us_avg = 0.0;
    double submit_to_ack_paired_us_p50 = 0.0;
    double submit_to_ack_paired_us_p90 = 0.0;
    double submit_to_ack_paired_us_p95 = 0.0;
    double submit_to_ack_paired_us_p99 = 0.0;

    double submit_to_trade_ack_us_avg = 0.0;
    double submit_to_trade_ack_us_p50 = 0.0;
    double submit_to_trade_ack_us_p90 = 0.0;
    double submit_to_trade_ack_us_p95 = 0.0;
    double submit_to_trade_ack_us_p99 = 0.0;

    double uds_first_savings_us_avg = 0.0;
    double uds_first_savings_us_p50 = 0.0;
    double uds_first_savings_us_p90 = 0.0;
    double uds_first_savings_us_p95 = 0.0;
    double uds_first_savings_us_p99 = 0.0;

    double submit_to_fill_us_avg = 0.0;
    double submit_to_fill_us_p50 = 0.0;
    double submit_to_fill_us_p90 = 0.0;
    double submit_to_fill_us_p95 = 0.0;
    double submit_to_fill_us_p99 = 0.0;

    double strategy_us_avg       = 0.0;
    double strategy_us_p50       = 0.0;
    double strategy_us_p90       = 0.0;
    double strategy_us_p99       = 0.0;

    double local_submit_us_avg   = 0.0;
    double local_submit_us_p50   = 0.0;
    double local_submit_us_p90   = 0.0;
    double local_submit_us_p99   = 0.0;

    double ack_path_us_avg       = 0.0;
    double ack_path_us_p50       = 0.0;
    double ack_path_us_p90       = 0.0;
    double ack_path_us_p99       = 0.0;

    double fill_after_ack_us_avg = 0.0;
    double fill_after_ack_us_p50 = 0.0;
    double fill_after_ack_us_p90 = 0.0;
    double fill_after_ack_us_p99 = 0.0;

    double event_to_ack_us_avg   = 0.0;
    double event_to_ack_us_p50   = 0.0;
    double event_to_ack_us_p90   = 0.0;
    double event_to_ack_us_p99   = 0.0;
    double event_to_fill_us_avg  = 0.0;
    double event_to_fill_us_p50  = 0.0;
    double event_to_fill_us_p90  = 0.0;
    double event_to_fill_us_p99  = 0.0;

    double md_gap_ms_p99         = 0.0;

    uint64_t event_drop          = 0;

    double queue_depth_p99       = 0.0;

    uint64_t risk_block_count    = 0;

    double md_gap_ms_peak        = 0.0;

    uint64_t submit_to_ack_us_samples  = 0;
    uint64_t fill_after_ack_us_samples = 0;
    uint64_t local_submit_us_samples   = 0;

    uint64_t submit_to_ack_paired_us_samples = 0;
    uint64_t submit_to_trade_ack_us_samples  = 0;
    uint64_t uds_first_savings_us_samples    = 0;
    uint64_t submit_to_fill_us_samples       = 0;
    uint64_t strategy_us_samples             = 0;
    uint64_t ack_path_us_samples             = 0;
    uint64_t event_to_ack_us_samples         = 0;
    uint64_t event_to_fill_us_samples        = 0;
};

/// @brief `kpi_1s.csv` 行。
struct XtKpi1sRow {
    int64_t ts_sec = 0;
    double pnl_gross = 0.0;
    double pnl_net = 0.0;
    double turnover = 0.0;
    double fill_ratio = 0.0;
    double cancel_ratio = 0.0;
    double order_cancel_ratio = 0.0;
    double partial_cancel_ratio = 0.0;
    double reject_ratio = 0.0;

    LatencyStats latency;

    double uptime_sec = 0.0;
    int kill_switch_active = 0;
};

/// @brief `position_snapshot_1s.csv` 行：单合约持仓 + 行情快照。
///
/// double 数值由 fixed-point(1e6) 转回手数或价格；`volume` 保留 venue 原始累计量。
/// CSV 公共维度中的 symbol 是默认主合约，行内 `symbol` 才是本行持仓合约。
struct XtPositionRow {
    int64_t ts_sec = 0;
    std::string symbol;
    double qty = 0.0;
    double long_qty = 0.0;
    double short_qty = 0.0;
    double avg_price = 0.0;
    double mark_price = 0.0;
    double unrealized_pnl = 0.0;
    double last_price = 0.0;
    double bid_price = 0.0;
    double ask_price = 0.0;
    int64_t volume = 0;
};

/// @brief `account_truth_1s.csv` 行：跨 venue 账户真值快照。
///
/// 该行与 KPI 分离；Binance/CTP 通过 IAccountTruthSource 填同一 schema。
struct XtAccountTruthRow {
    int64_t ts_sec = 0;
    double wallet_balance = 0.0;
    double position_value = 0.0;
    double unrealized_pnl = 0.0;
    double fees_today = 0.0;
    double external_cashflow_today = 0.0;
    uint64_t truth_ts_ns = 0;
    int64_t trading_day = 0;
};

/// @brief `fills.csv` 行。
struct XtFillRow {
    std::string strategy_id;
    std::string symbol;
    int64_t ts_ms = 0;

    int64_t ts_event_ns = 0;
    int64_t ts_recv_ns = 0;
    uint64_t order_id = 0;
    uint64_t trade_id = 0;
    std::string side;
    double qty = 0.0;
    double price = 0.0;
    double fee = 0.0;
    double slippage = 0.0;
    int is_close = 0;
    int is_today = 0;
    std::string close_offset;
    std::string trace_id;
    uint32_t trading_day = 0;
    int64_t realized_pnl_fp = 0;
};

/// @brief Backtest 内部成交输入；由 XtReportEventBuilder 转换为 XtFillRow。
struct XtTradeRow {
    uint64_t order_id = 0;
    std::string code;
    bool is_buy = true;
    double qty = 0.0;
    double exec_price = 0.0;
    double fee = 0.0;
    double slippage = 0.0;
    uint32_t order_date = 0;
    uint32_t order_time = 0;
    uint32_t trade_date = 0;
    uint32_t trade_time = 0;
    uint32_t trading_day = 0;
    std::string user_tag;
    bool is_close = false;
    bool is_today = false;
    XtOffset close_offset = XtOffset::Close;
};

/// @brief KPI 订单比例字段。
struct OrderRatios {
    double fill_ratio   = 0.0;
    double cancel_ratio = 0.0;
    double partial_cancel_ratio = 0.0;
    double reject_ratio = 0.0;
    double order_cancel_ratio = 0.0;
};

/// @brief Live 成交输入；由 XtReportEventBuilder 转换为 XtFillRow。
struct XtLiveFillEvent {
    std::string strategy_id;
    std::string symbol;
    int64_t     ts_ms    = 0;
    int64_t     ts_event_ns = 0;
    int64_t     ts_recv_ns = 0;
    uint64_t    order_id = 0;
    uint64_t    trade_id = 0;
    bool        is_buy   = true;
    double      qty      = 0.0;
    double      price    = 0.0;
    double      fee      = 0.0;
    double      slippage = 0.0;
    bool        is_close = false;
    bool        is_today = false;
    std::string trace_id;
    uint32_t    trading_day = 0;
    int64_t     realized_pnl_fp = 0;
};

}
