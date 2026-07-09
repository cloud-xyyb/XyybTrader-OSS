#include "XtTradeExcursionTracker.h"

#include <cstdio>

#include "XtFixedPoint.h"
#include "xt_account/state/AccountStateStore.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_logger/XtLogger.h"

namespace xt_bt::hft {

namespace {

std::string px(int64_t v)  { char b[40]; std::snprintf(b, sizeof(b), "%.6f", xt_fp::from_fp(v)); return b; }
std::string pnl(int64_t v) { char b[40]; std::snprintf(b, sizeof(b), "%.4f", xt_fp::from_fp(v)); return b; }
std::string qty(int64_t v) { char b[40]; std::snprintf(b, sizeof(b), "%.4f", order_exec_from_fp(v)); return b; }
}

void XtTradeExcursionTracker::init(const std::string& report_dir) {
    const std::string path = report_dir + "/trade_excursions.csv";
    out_.open(path, std::ios::out | std::ios::trunc);
    if (out_) {
        out_ << "open_date,open_time,close_date,close_time,code,side,qty,entry_px,exit_px,"
                "realized_pnl,mae,mfe,mae_px,mfe_px,ticks_held\n";
    } else {
        XtLogger::error_m("BT", "trade_excursions.csv open failed: {}", path);
    }
}

void XtTradeExcursionTracker::start_segment_(Seg& s, int64_t qty_fp, int64_t avg_fp,
                                             uint32_t date, uint32_t time) {
    s.open = true;
    s.entry_qty_fp = qty_fp;
    s.entry_avg_fp = avg_fp;
    s.open_date = date;
    s.open_time = time;
    s.realized_acc_fp = 0;
    s.mae_fp = 0;
    s.mfe_fp = 0;
    s.mae_px_fp = avg_fp;
    s.mfe_px_fp = avg_fp;
    s.ticks_held = 0;
}

void XtTradeExcursionTracker::on_tick(const XtTick& tick, const xt_account::AccountStateStore& account) {
    auto it = seg_.find(tick.code);
    if (it == seg_.end() || !it->second.open) return;
    Seg& s = it->second;
    const auto pos = account.snapshot_position(tick.code);
    if (pos.qty == 0) return;
    const int64_t u = pos.unrealized_pnl;
    if (u < s.mae_fp) { s.mae_fp = u; s.mae_px_fp = pos.mark_price; }
    if (u > s.mfe_fp) { s.mfe_fp = u; s.mfe_px_fp = pos.mark_price; }
    ++s.ticks_held;
}

void XtTradeExcursionTracker::on_fill(const XtTradeRecord& tr, int64_t before_qty_fp,
                                      int64_t after_qty_fp, int64_t after_avg_fp,
                                      int64_t realized_fp) {
    Seg& s = seg_[tr.code];
    const bool was_zero = (before_qty_fp == 0);
    const bool is_zero  = (after_qty_fp == 0);
    const bool flip = !was_zero && !is_zero && ((before_qty_fp > 0) != (after_qty_fp > 0));
    const int64_t exit_px_fp = to_fp_round(tr.exec_price);

    if (was_zero && !is_zero) {
        start_segment_(s, after_qty_fp, after_avg_fp, tr.trade_date, tr.trade_time);
    } else if (!was_zero && is_zero) {
        if (s.open) {
            s.realized_acc_fp += realized_fp;
            emit_(tr.code, s, exit_px_fp, tr.trade_date, tr.trade_time);
        }
        s.open = false;
    } else if (flip) {
        if (s.open) {
            s.realized_acc_fp += realized_fp;
            emit_(tr.code, s, exit_px_fp, tr.trade_date, tr.trade_time);
        }
        start_segment_(s, after_qty_fp, after_avg_fp, tr.trade_date, tr.trade_time);
    } else {
        if (!s.open) {
            start_segment_(s, after_qty_fp, after_avg_fp, tr.trade_date, tr.trade_time);
        }
        s.realized_acc_fp += realized_fp;
    }
}

void XtTradeExcursionTracker::emit_(const std::string& code, const Seg& s, int64_t exit_px_fp,
                                    uint32_t close_date, uint32_t close_time) {
    if (!out_) return;
    out_ << s.open_date << ',' << s.open_time << ',' << close_date << ',' << close_time << ','
         << code << ',' << (s.entry_qty_fp >= 0 ? "LONG" : "SHORT") << ','
         << qty(s.entry_qty_fp >= 0 ? s.entry_qty_fp : -s.entry_qty_fp) << ','
         << px(s.entry_avg_fp) << ',' << px(exit_px_fp) << ','
         << pnl(s.realized_acc_fp) << ',' << pnl(s.mae_fp) << ',' << pnl(s.mfe_fp) << ','
         << px(s.mae_px_fp) << ',' << px(s.mfe_px_fp) << ',' << s.ticks_held << '\n';
}

void XtTradeExcursionTracker::flush() {
    for (const auto& [code, s] : seg_) {
        if (s.open) ++dropped_open_at_eof_;
    }
    if (dropped_open_at_eof_ > 0) {
        XtLogger::info_m("BT", "trade_excursions: dropped {} open-at-EOF segment(s)",
                         dropped_open_at_eof_);
    }
    if (out_) out_.flush();
    out_.close();
}

}
