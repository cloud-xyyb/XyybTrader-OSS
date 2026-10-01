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
    std::string mode;           ///< 运行模式（"backtest" / "live"）
    std::string run_id;         ///< 本次运行唯一 ID
    std::string strategy_id;    ///< 默认策略标识；行内字段可覆盖
    std::string symbol;         ///< 默认合约代码；行内字段可覆盖
    std::string config_hash;    ///< 配置文件哈希（可复现性追踪）
    std::string code_version;   ///< 代码版本号
    std::string session_date;   ///< 交易日（YYYYMMDD）
};

/// @brief KPI CSV 中的延迟与队列统计字段。
///
/// 除 `md_gap_ms_p99` 外，`*_us_*` 延迟字段单位均为微秒。
/// 字段名直接对应 CSV header，重命名会影响 Grafana/exporter/analyzer。
struct LatencyStats {
    // effective submit→ack：first-of-arrival，UDS-first dedup 后的策略推进口径。
    double submit_to_ack_us_avg  = 0.0;
    double submit_to_ack_us_p50  = 0.0;
    double submit_to_ack_us_p90  = 0.0;
    double submit_to_ack_us_p95  = 0.0;
    double submit_to_ack_us_p99  = 0.0;
    // paired-only submit→ack：仅统计同时有 raw UDS 与 raw trade ACK 的订单。
    double submit_to_ack_paired_us_avg = 0.0;
    double submit_to_ack_paired_us_p50 = 0.0;
    double submit_to_ack_paired_us_p90 = 0.0;
    double submit_to_ack_paired_us_p95 = 0.0;
    double submit_to_ack_paired_us_p99 = 0.0;
    // raw trade ACK：不经 UDS-first dedup，用于和 effective ack 对比。
    double submit_to_trade_ack_us_avg = 0.0;
    double submit_to_trade_ack_us_p50 = 0.0;
    double submit_to_trade_ack_us_p90 = 0.0;
    double submit_to_trade_ack_us_p95 = 0.0;
    double submit_to_trade_ack_us_p99 = 0.0;
    // UDS-first 提前量：raw_trade_ack - effective_ack。
    double uds_first_savings_us_avg = 0.0;
    double uds_first_savings_us_p50 = 0.0;
    double uds_first_savings_us_p90 = 0.0;
    double uds_first_savings_us_p95 = 0.0;
    double uds_first_savings_us_p99 = 0.0;
    // submit→fill：订单提交开始到成交回报。
    double submit_to_fill_us_avg = 0.0;
    double submit_to_fill_us_p50 = 0.0;
    double submit_to_fill_us_p90 = 0.0;
    double submit_to_fill_us_p95 = 0.0;
    double submit_to_fill_us_p99 = 0.0;
    // strategy callback 执行耗时。
    double strategy_us_avg       = 0.0;
    double strategy_us_p50       = 0.0;
    double strategy_us_p90       = 0.0;
    double strategy_us_p99       = 0.0;
    // 本地 submit wire 段：submit_begin_us → submit_end_us。
    double local_submit_us_avg   = 0.0;
    double local_submit_us_p50   = 0.0;
    double local_submit_us_p90   = 0.0;
    double local_submit_us_p99   = 0.0;
    // ACK 回程段：submit_end_us → ack_us。
    double ack_path_us_avg       = 0.0;
    double ack_path_us_p50       = 0.0;
    double ack_path_us_p90       = 0.0;
    double ack_path_us_p99       = 0.0;
    // ACK→fill：ack_us → fill_us。
    double fill_after_ack_us_avg = 0.0;
    double fill_after_ack_us_p50 = 0.0;
    double fill_after_ack_us_p90 = 0.0;
    double fill_after_ack_us_p99 = 0.0;
    // 触发事件→ACK / fill：从 tick/strategy trigger timestamp 计时。
    double event_to_ack_us_avg   = 0.0;
    double event_to_ack_us_p50   = 0.0;
    double event_to_ack_us_p90   = 0.0;
    double event_to_ack_us_p99   = 0.0;
    double event_to_fill_us_avg  = 0.0;
    double event_to_fill_us_p50  = 0.0;
    double event_to_fill_us_p90  = 0.0;
    double event_to_fill_us_p99  = 0.0;
    // 行情 tick 队列滞留（enqueue→consume）窗口真 p99，单位毫秒（直方图桶上界，保守）。
    double md_gap_ms_p99         = 0.0;
    // report/event loop 观察到的 drop 计数。
    uint64_t event_drop          = 0;
    // 运行期事件队列 backlog 的 instant 快照（非 peak、非 p99）：HFT=td_event_backlog /
    // CTA=tick_backlog / SEL=ctrl queue_size。字段名 queue_depth_p99 为历史命名，保留以守
    // CSV/Prom 契约（golden）。
    double queue_depth_p99       = 0.0;
    // pre-trade 风控拒单累计总数（RiskStatsSnapshot::total_rejects()，record_reject 单调计数）。
    uint64_t risk_block_count    = 0;
    // 行情 tick 队列滞留窗口 observed peak，单位毫秒（向前兼容追加）。
    double md_gap_ms_peak        = 0.0;
    // Per-stage 样本计数（fail-closed budget 消费；0 → NO_DATA 而非"低延迟绿灯"）。
    // stem 与对应 CSV latency 列同名（tracker stage submit_to_ack / ack_to_fill /
    // submit_wire）；CSV 追加在 md_gap_ms_peak 之后，md_gap_ms_peak 不再是末列。
    uint64_t submit_to_ack_us_samples  = 0;
    uint64_t fill_after_ack_us_samples = 0;
    uint64_t local_submit_us_samples   = 0;
    // 其余 latency stage 组的样本计数（同一 fail-closed budget 语义），append-only
    // 补齐；tracker stage 对应关系：submit_to_ack_paired / submit_to_trade_ack /
    // uds_first_savings / submit_to_fill / strategy_processing / ack_path /
    // event_to_ack / event_to_fill。CSV 追加在上面 3 列之后。
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
    int64_t ts_sec = 0;                   ///< Unix epoch seconds。
    double pnl_gross = 0.0;               ///< 未扣 fee/slippage 的 PnL。
    double pnl_net = 0.0;                 ///< 扣除 fee/slippage 后的 PnL。
    double turnover = 0.0;                ///< 成交额。
    double fill_ratio = 0.0;              ///< 口径：submit_success / submit_attempts。
    double cancel_ratio = 0.0;            ///< 主动撤单 API 成功率，见 OrderRatios。
    double order_cancel_ratio = 0.0;      ///< 终态为 canceled 的订单比例。
    double partial_cancel_ratio = 0.0;    ///< 部分成交后撤单比例。
    double reject_ratio = 0.0;            ///< 拒单数 / submit_attempts。

    LatencyStats latency;

    double uptime_sec = 0.0;              ///< 进程运行秒数。
    int kill_switch_active = 0;           ///< 0=正常，1=kill switch active。
};

/// @brief `position_snapshot_1s.csv` 行：单合约持仓 + 行情快照。
///
/// double 数值由 fixed-point(1e6) 转回手数或价格；`volume` 保留 venue 原始累计量。
/// CSV 公共维度中的 symbol 是默认主合约，行内 `symbol` 才是本行持仓合约。
struct XtPositionRow {
    int64_t ts_sec = 0;           ///< Unix epoch seconds。
    std::string symbol;           ///< 本行持仓合约。
    double qty = 0.0;              ///< 净仓 (+多 / -空), 单位: 手
    double long_qty = 0.0;         ///< 多头仓量 (手)
    double short_qty = 0.0;        ///< 空头仓量 (手)
    double avg_price = 0.0;        ///< 开仓均价 (RMB)
    double mark_price = 0.0;       ///< 市场最新价 (RMB, 用于 mark-to-market) — 与 last_price 同义
    double unrealized_pnl = 0.0;   ///< 未实现盈亏 (RMB)
    double last_price = 0.0;       ///< 最新成交价 (RMB) — 通常等同 mark_price
    double bid_price = 0.0;        ///< 买一价 (RMB) — 真值, 0 隐式 stale
    double ask_price = 0.0;        ///< 卖一价 (RMB) — 真值, 0 隐式 stale
    int64_t volume = 0;            ///< 累计成交量 (手)
};

/// @brief `account_truth_1s.csv` 行：跨 venue 账户真值快照。
///
/// 该行与 KPI 分离；Binance/CTP 通过 IAccountTruthSource 填同一 schema。
struct XtAccountTruthRow {
    int64_t ts_sec = 0;                    ///< emit 时间，Unix epoch seconds。
    double wallet_balance = 0.0;    ///< 现金/权益口径余额 (venue wallet/balance)
    double position_value = 0.0;    ///< sum(qty * mark/settlement price)
    double unrealized_pnl = 0.0;    ///< venue/API 或 xt_account::AccountStateStore 未实现盈亏
    double fees_today = 0.0;        ///< 本 trading_day 累计手续费
    double external_cashflow_today = 0.0; ///< 本 trading_day 外部资金流 (入金/转入为正)
    uint64_t truth_ts_ns = 0;       ///< truth snapshot wall-clock timestamp ns
    int64_t trading_day = 0;        ///< venue-specific trading day (YYYYMMDD)
};

/// @brief `fills.csv` 行。
struct XtFillRow {
    std::string strategy_id;        ///< 行级策略维度 override。
    std::string symbol;             ///< 行级合约维度 override。
    int64_t ts_ms = 0;              ///< wall-clock epoch ms 兼容字段。
    /// Schema v2 双时间戳，单位 ns since epoch。
    /// `ts_event_ns` 为交易所/模拟成交时刻，`ts_recv_ns` 为本地接收时刻。
    int64_t ts_event_ns = 0;
    int64_t ts_recv_ns = 0;
    uint64_t order_id = 0;          ///< 逻辑订单 id。
    uint64_t trade_id = 0;          ///< venue 成交 id；不可得时为 0。
    std::string side;               ///< "BUY" / "SELL"。
    double qty = 0.0;               ///< 成交数量，单位手。
    double price = 0.0;             ///< 成交价。
    double fee = 0.0;
    double slippage = 0.0;
    int is_close = 0;               ///< 0=open，1=close。
    int is_today = 0;               ///< 平今标志；非 close 时为 0。
    std::string close_offset;       ///< "OPEN" / "TODAY" / "YESTERDAY" / "AUTO"。
    std::string trace_id;           ///< submit/fill 关联 id。
    uint32_t trading_day = 0;       ///< YYYYMMDD；0 时下游退化到 ts_ms 自然日。
    int64_t realized_pnl_fp = 0;    ///< 单笔 realized PnL，fixed-point(1e6)。
};

/// @brief Backtest 内部成交输入；由 XtReportEventBuilder 转换为 XtFillRow。
struct XtTradeRow {
    uint64_t order_id = 0;
    std::string code;
    bool is_buy = true;             ///< true=BUY，false=SELL。
    double qty = 0.0;               ///< 成交数量，单位手。
    double exec_price = 0.0;        ///< 成交价。
    double fee = 0.0;
    double slippage = 0.0;
    uint32_t order_date = 0;        ///< YYYYMMDD。
    uint32_t order_time = 0;        ///< HHMMSSmmm。
    uint32_t trade_date = 0;        ///< YYYYMMDD。
    uint32_t trade_time = 0;        ///< HHMMSSmmm。
    uint32_t trading_day = 0;       ///< 回测通常等同 trade_date。
    std::string user_tag;           ///< 写入 XtFillRow::trace_id。
    bool is_close = false;          ///< true=平仓。
    bool is_today = false;          ///< true=平今。
    XtOffset close_offset = XtOffset::Close; ///< 平仓口径标签来源。
};

/// @brief KPI 订单比例字段。
struct OrderRatios {
    double fill_ratio   = 0.0;          ///< 口径：submit_success / submit_attempts。
    double cancel_ratio = 0.0;          ///< cancel_success / cancel_attempts。
    double partial_cancel_ratio = 0.0;  ///< partial-filled 后 canceled 的订单比例。
    double reject_ratio = 0.0;          ///< rejects / submit_attempts。
    double order_cancel_ratio = 0.0;    ///< canceled_terminal_total / submit_success。
};

/// @brief Live 成交输入；由 XtReportEventBuilder 转换为 XtFillRow。
struct XtLiveFillEvent {
    std::string strategy_id;        ///< 行级策略维度 override。
    std::string symbol;             ///< 行级合约维度 override。
    int64_t     ts_ms    = 0;       ///< wall-clock epoch ms 兼容字段。
    int64_t     ts_event_ns = 0;
    int64_t     ts_recv_ns = 0;
    uint64_t    order_id = 0;       ///< 逻辑订单 id。
    uint64_t    trade_id = 0;       ///< venue 成交 id；不可得时为 0。
    bool        is_buy   = true;    ///< true=BUY，false=SELL。
    double      qty      = 0.0;     ///< 成交数量，单位手。
    double      price    = 0.0;     ///< 成交价。
    double      fee      = 0.0;
    double      slippage = 0.0;
    bool        is_close = false;   ///< true=平仓。
    bool        is_today = false;   ///< true=平今。
    std::string trace_id;           ///< submit/fill 关联 id。
    uint32_t    trading_day = 0;    ///< broker/venue trading day，YYYYMMDD。
    int64_t     realized_pnl_fp = 0; ///< 单笔 realized PnL，fixed-point(1e6)。
};

} // namespace xt_report
