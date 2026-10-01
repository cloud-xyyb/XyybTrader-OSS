#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "xt_report/core/XtReportTypes.h"
#include "xt_report/core/IReportSink.h"

namespace xt_report {

/// @brief live 报表的产出协调器。
///
/// live 运行期持续产出两类报表行：每笔成交和每秒 KPI（equity/PnL/延迟分位/
/// 订单比率）。
/// XtLiveReporter 维护 session 的 equity 基线、fee/滑点/成交累计
/// 和 KPI 写出节流，把构造好的行写入 IReportSink；sink 写失败累计为 drop_count 供观测。
/// 不持有 sink，调用方保证其生命周期覆盖 reporter；写入方法单 writer 调用，
/// 查询 accessor 可跨线程读。
class XtLiveReporter {
public:
    struct Config {
        int flush_ms = 1000;  ///< KPI 最小写出间隔，单位 ms。
    };

    /// @param start_ms session 起点，steady ms；用于 KPI uptime 计算。
    XtLiveReporter(IReportSink& sink, const Config& cfg, int64_t start_ms);

    /// @brief 写成交行并更新 session 累计 fee/slippage/fill count。
    void on_fill(const XtFillRow& fill,
                 double fee_delta,
                 double slippage_delta = 0.0);

    /// @brief 更新 last equity；baseline 优先由 set_baseline_equity 显式设置，
    /// 未显式设置时连续两次稳定权益（>1 且差 <1）才 auto-latch 为起点。
    void on_equity(double equity);

    /// @brief 显式设置 PnL baseline；首次成功后保持 start_equity 不变。
    ///
    /// HftRunnerControl 应在 trading_ready 后调用，避免账户初始化期 transient equity
    /// 被自动 latch 成 PnL 起点。
    void set_baseline_equity(double equity);

    /// @brief 若达到 flush_ms 间隔则写出一行 kpi_1s。
    ///
    /// @param interval_turnover 本次 KPI 窗口成交额，由调用方按周期传入。
    /// @return true 表示实际写出；false 表示被时间间隔节流。
    bool try_flush_kpi(int64_t now_ms,
                       double interval_turnover,
                       const OrderRatios& ratios,
                       const LatencyStats& latency,
                       bool kill_switch_active = false);

    /// @brief 不检查 flush_ms，直接写出一行 kpi_1s。
    void force_flush_kpi(int64_t now_ms,
                         double interval_turnover,
                         const OrderRatios& ratios,
                         const LatencyStats& latency,
                         bool kill_switch_active = false);

    /// @brief 写入每秒多合约持仓 + 行情快照行；空列表不写行。
    ///
    /// `ts_sec` 会覆盖每个 row 的 ts_sec；行内 symbol 表示对应持仓合约。
    void flush_position_snapshot(int64_t ts_sec, const std::vector<XtPositionRow>& rows);

    /// @brief 写入账户真值快照；`ts_sec` 会覆盖 row.ts_sec。
    void flush_account_truth(int64_t ts_sec, const XtAccountTruthRow& row);

    /// @brief Session 结束持久化：sink.sync_flush + equity baseline 兜底初始化。
    void flush_run_summary();

    int64_t start_ms() const { return start_ms_; }
    uint64_t cumulative_fill_count() const { return cumulative_fill_count_; }

    /// @brief Session 累计净 PnL；baseline 未初始化时返回 0。
    double session_pnl_net() const {
        return equity_initialized_ ? (last_equity_ - start_equity_) : 0.0;
    }
    bool   equity_initialized() const { return equity_initialized_; }

    /// @brief sink 写入失败累计次数。
    uint64_t sink_drop_count() const noexcept {
        return drop_count_.load(std::memory_order_relaxed);
    }

private:
    void write_kpi(int64_t now_ms, double interval_turnover,
                   const OrderRatios& ratios, const LatencyStats& latency,
                   bool kill_switch_active);

    IReportSink& sink_;
    Config cfg_;
    int64_t start_ms_;
    int64_t last_flush_ms_ = 0;

    // Equity baseline 状态。
    bool   equity_initialized_ = false;
    double start_equity_       = 0.0;
    double last_equity_        = 0.0;
    // 显式 baseline 漏调时的兜底：连续两次稳定 equity 才 latch。
    bool   auto_latch_pending_    = false;
    double auto_latch_candidate_  = 0.0;

    // Session 累计统计。
    double   cumulative_fee_        = 0.0;
    double   cumulative_slippage_   = 0.0;
    uint64_t cumulative_fill_count_ = 0;

    // 单 writer 更新，外部控制面/Prometheus 可跨线程读取。
    std::atomic<uint64_t> drop_count_{0};
};

} // namespace xt_report
