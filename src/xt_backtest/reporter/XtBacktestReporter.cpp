#include "XtBacktestReporter.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <utility>
#include <vector>

#include "XtLogger.h"
#include "xt_core/market/XtRawTick.h"
#include "XtFixedPoint.h"
#include "xt_backtest/common/XtBacktestReport.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_risk/engine/XtRiskEngine.h"

bool XtBacktestReporter::init(xt_report::IReportSink& report_sink,
                                   std::unique_ptr<XtEventStreamSink> event_stream_sink) {
    sink_ = &report_sink;
    event_writer_ = std::move(event_stream_sink);
    return true;
}

void XtBacktestReporter::on_tick_pre(int64_t ts, const XtRawTick& t) {

    last_tick_ts_ = ts;
    event_writer_->on_tick(last_tick_ts_, t);

    const uint32_t td = t.trading_date != 0 ? t.trading_date : t.action_date;
    if (td != 0) {

        if (current_trading_date_ != 0 && td != current_trading_date_ && account_probe_) {
            const AccountEod eod = account_probe_();
            daily_breakdown_.on_day_account(current_trading_date_, eod.equity, eod.cash,
                                            eod.frozen);
        }
        current_trading_date_ = td;
        daily_breakdown_.on_tick(td);
    }
}

void XtBacktestReporter::on_tick_post(int64_t  , size_t active_orders) {

    event_writer_->on_sys_stats(last_tick_ts_, active_orders);
}

bool XtBacktestReporter::flush() {

    if (current_trading_date_ != 0 && account_probe_) {
        const AccountEod eod = account_probe_();
        daily_breakdown_.on_day_account(current_trading_date_, eod.equity, eod.cash, eod.frozen);
    }

    for (const auto& f : fills_) {
        if (!sink_->on_fill(f)) {
            XtLogger::warn_m("REPORT", "on_fill drop oid={} ts={}", f.order_id, f.ts_ms);
            ++drop_count_;
        }
    }

    bool ok = true;
    if (!sink_->sync_flush()) {
        XtLogger::error_m("REPORT", "sink_->sync_flush() failed (disk-full / failbit / fsync error)");
        ok = false;
    }
    event_writer_->flush();
    return ok;
}

void XtBacktestReporter::on_fill(const xt_report::XtFillRow& fill, const XtTradeRecord& raw) {
    ++trade_count_;

    const uint32_t td =
        fill.trading_day != 0 ? fill.trading_day :
        (raw.trade_date != 0 ? raw.trade_date : current_trading_date_);
    daily_breakdown_.on_fill(td);

    fills_.push_back(fill);

    {
        auto& row = fills_.back();
        if (raw.sim_visible_at_ns >= 0) {
            row.ts_event_ns = raw.sim_visible_at_ns;
            row.ts_recv_ns  = raw.sim_visible_at_ns;
            row.ts_ms       = raw.sim_visible_at_ns / 1'000'000LL;
        } else if (last_tick_ts_ >= 1'000'000'000LL) {
            const auto ad = static_cast<uint32_t>(last_tick_ts_ / 1'000'000'000LL);
            const auto at = static_cast<uint32_t>(last_tick_ts_ % 1'000'000'000LL);
            row.ts_ms       = xt_time::to_ns(ad, at) / 1'000'000LL;
            row.ts_event_ns = row.ts_ms * 1'000'000LL;
            row.ts_recv_ns  = row.ts_event_ns;
        }
    }

    event_writer_->on_trade(
        last_tick_ts_ >= 1'000'000'000LL ? last_tick_ts_ : packed_ts_from_trade(raw), raw);

}

void XtBacktestReporter::on_order_update(const XtOrderExecUpdate& u) {

    event_writer_->on_order_update(last_tick_ts_, u);
}

void XtBacktestReporter::on_order_submit(int64_t ts,
                                              uint64_t id,
                                              const char* code,
                                              bool is_buy,
                                              int64_t price_fp,
                                              int64_t qty_fp,
                                              std::string_view user_tag,
                                              bool is_close,
                                              bool is_today) {

    daily_breakdown_.on_submit(current_trading_date_);
    event_writer_->on_order_submit(ts, id, code, is_buy, price_fp, qty_fp, user_tag, is_close, is_today);
}

void XtBacktestReporter::on_risk_reject(int64_t ts,
                                             const char* code,
                                             bool is_buy,
                                             int64_t price_fp,
                                             int64_t qty_fp,
                                             const std::string& reason) {
    (void)is_buy;
    const double price = xt_fp::from_fp(price_fp);
    const double qty = xt_fp::from_fp(qty_fp);
    (void)ts;
    int& cnt = risk_rejects_[reason];
    cnt += 1;

    daily_breakdown_.on_risk_reject(current_trading_date_, reason);

    if (cnt <= 5) {
        XtLogger::error_m("RISK", "reject {} {} @{} reason={}", code ? code : "", qty, price, reason);
    }
}

void XtBacktestReporter::on_margin_reject(const xt_core::XtOrder& req,
                                          const XtRiskResult& decision,
                                          int64_t  ,
                                          std::string_view  ) {
    on_risk_reject(last_tick_ts_, req.code, req.is_buy, req.price_fp, req.qty_fp,
                   decision.reason_text);
}

void XtBacktestReporter::on_reject(const xt_core::XtOrder& req,
                                   const XtRiskResult& decision,
                                   std::string_view  ) {
    on_risk_reject(last_tick_ts_, req.code, req.is_buy, req.price_fp, req.qty_fp,
                   decision.reason_text);
}

bool XtBacktestReporter::write_daily_breakdown(const std::string& out_path,
                                               uint32_t cfg_start_date,
                                               uint32_t cfg_end_date) const {

    return daily_breakdown_.write_json(out_path, cfg_start_date, cfg_end_date);
}
