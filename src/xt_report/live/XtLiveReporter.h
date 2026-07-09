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
        int flush_ms = 1000;
    };

    XtLiveReporter(IReportSink& sink, const Config& cfg, int64_t start_ms);

    void on_fill(const XtFillRow& fill,
                 double fee_delta,
                 double slippage_delta = 0.0);

    void on_equity(double equity);

    void set_baseline_equity(double equity);

    bool try_flush_kpi(int64_t now_ms,
                       double interval_turnover,
                       const OrderRatios& ratios,
                       const LatencyStats& latency,
                       bool kill_switch_active = false);

    void force_flush_kpi(int64_t now_ms,
                         double interval_turnover,
                         const OrderRatios& ratios,
                         const LatencyStats& latency,
                         bool kill_switch_active = false);

    void flush_position_snapshot(int64_t ts_sec, const std::vector<XtPositionRow>& rows);

    void flush_account_truth(int64_t ts_sec, const XtAccountTruthRow& row);

    void flush_run_summary();

    int64_t start_ms() const { return start_ms_; }
    uint64_t cumulative_fill_count() const { return cumulative_fill_count_; }

    double session_pnl_net() const {
        return equity_initialized_ ? (last_equity_ - start_equity_) : 0.0;
    }
    bool   equity_initialized() const { return equity_initialized_; }

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

    bool   equity_initialized_ = false;
    double start_equity_       = 0.0;
    double last_equity_        = 0.0;

    bool   auto_latch_pending_    = false;
    double auto_latch_candidate_  = 0.0;

    double   cumulative_fee_        = 0.0;
    double   cumulative_slippage_   = 0.0;
    uint64_t cumulative_fill_count_ = 0;

    std::atomic<uint64_t> drop_count_{0};
};

}
