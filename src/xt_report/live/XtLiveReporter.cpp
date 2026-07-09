#include "XtLiveReporter.h"

#include <cmath>

#include "XtLogger.h"
#include "xt_report/util/XtReportEventBuilder.h"

namespace xt_report {

XtLiveReporter::XtLiveReporter(IReportSink& sink, const Config& cfg, int64_t start_ms)
    : sink_(sink), cfg_(cfg), start_ms_(start_ms) {}

void XtLiveReporter::on_fill(const XtFillRow& fill,
                             double fee_delta,
                             double slippage_delta) {
    if (!sink_.on_fill(fill)) {
        XtLogger::warn_m("REPORT", "on_fill drop oid={} ts={}",
                         fill.order_id, fill.ts_ms);
        drop_count_.fetch_add(1, std::memory_order_relaxed);
    }
    cumulative_fee_        += fee_delta;
    cumulative_slippage_   += slippage_delta;
    cumulative_fill_count_ += 1;
}

void XtLiveReporter::on_equity(double equity) {
    if (!std::isfinite(equity)) return;

    last_equity_ = equity;
    if (equity_initialized_) return;

    if (equity > 1.0) {
        if (auto_latch_pending_ && std::fabs(auto_latch_candidate_ - equity) < 1.0) {
            start_equity_ = equity;
            equity_initialized_ = true;
            auto_latch_pending_ = false;
        } else {
            auto_latch_candidate_ = equity;
            auto_latch_pending_ = true;
        }
    }
}

void XtLiveReporter::flush_position_snapshot(int64_t ts_sec,
                                             const std::vector<XtPositionRow>& rows) {
    for (const auto& r : rows) {
        XtPositionRow row = r;
        row.ts_sec = ts_sec;
        if (!sink_.on_position_snapshot(row)) {
            XtLogger::warn_m("REPORT", "on_position_snapshot drop sym={} ts={}",
                             row.symbol, row.ts_sec);
            drop_count_.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

void XtLiveReporter::flush_account_truth(int64_t ts_sec, const XtAccountTruthRow& row) {
    XtAccountTruthRow out = row;
    out.ts_sec = ts_sec;
    if (!sink_.on_account_truth(out)) {
        XtLogger::warn_m("REPORT", "on_account_truth drop ts={}", out.ts_sec);
        drop_count_.fetch_add(1, std::memory_order_relaxed);
    }
}

void XtLiveReporter::set_baseline_equity(double equity) {
    if (!std::isfinite(equity)) return;
    if (equity_initialized_) return;
    start_equity_       = equity;
    last_equity_        = equity;
    equity_initialized_ = true;
    auto_latch_pending_ = false;
}

bool XtLiveReporter::try_flush_kpi(int64_t now_ms,
                                       double interval_turnover,
                                       const OrderRatios& ratios,
                                       const LatencyStats& latency,
                                       bool kill_switch_active) {
    const int flush_ms = cfg_.flush_ms > 0 ? cfg_.flush_ms : 1000;
    if (last_flush_ms_ != 0 && now_ms - last_flush_ms_ < flush_ms) return false;
    write_kpi(now_ms, interval_turnover, ratios, latency, kill_switch_active);
    return true;
}

void XtLiveReporter::force_flush_kpi(int64_t now_ms,
                                         double interval_turnover,
                                         const OrderRatios& ratios,
                                         const LatencyStats& latency,
                                         bool kill_switch_active) {
    write_kpi(now_ms, interval_turnover, ratios, latency, kill_switch_active);
}

void XtLiveReporter::write_kpi(int64_t now_ms,
                                   double interval_turnover,
                                   const OrderRatios& ratios,
                                   const LatencyStats& latency,
                                   bool kill_switch_active) {
    const double pnl_net   = session_pnl_net();

    const double pnl_gross = pnl_net + cumulative_fee_ + cumulative_slippage_;
    const double uptime    = now_ms >= start_ms_
                                 ? static_cast<double>(now_ms - start_ms_) / 1000.0
                                 : 0.0;

    const auto kpi_row = XtReportEventBuilder::build_kpi(
        now_ms / 1000, pnl_net, pnl_gross, interval_turnover, uptime, ratios, latency,
        kill_switch_active);
    if (!sink_.on_kpi_1s(kpi_row)) {
        XtLogger::warn_m("REPORT", "on_kpi_1s drop ts={}", kpi_row.ts_sec);
        drop_count_.fetch_add(1, std::memory_order_relaxed);
    }

    last_flush_ms_ = now_ms;
}

void XtLiveReporter::flush_run_summary() {

    if (!equity_initialized_) {
        start_equity_ = last_equity_;
        equity_initialized_ = true;
    }

    if (!sink_.sync_flush()) {
        XtLogger::error_m("REPORT", "live sink_.sync_flush() failed (disk-full / failbit / fsync error)");
        drop_count_.fetch_add(1, std::memory_order_relaxed);
    }
}

}
