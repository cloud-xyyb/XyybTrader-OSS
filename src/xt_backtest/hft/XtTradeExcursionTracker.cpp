#include "XtTradeExcursionTracker.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>

#include "XtFixedPoint.h"
#include "xt_account/state/AccountStateStore.h"
#include "xt_config/catalog/XtCommodityCatalog.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_logger/XtLogger.h"

namespace xt_bt::hft {

namespace {
// 列格式:价格/盈亏走 fixed-point→double,手数走 order-exec 标度。与 fills.csv 同口径。
std::string px(int64_t v)  { char b[40]; std::snprintf(b, sizeof(b), "%.6f", xt_fp::from_fp(v)); return b; }
std::string pnl(int64_t v) { char b[40]; std::snprintf(b, sizeof(b), "%.4f", xt_fp::from_fp(v)); return b; }
std::string qty(int64_t v) { char b[40]; std::snprintf(b, sizeof(b), "%.4f", order_exec_from_fp(v)); return b; }

/// @brief 由 apply 前后净仓推导本笔的减仓量与新开量（都取正数量级）。
/// 只看净仓变化，与 venue 的开平标记无关：同侧缩量 = 纯减仓，穿零 = 先平满旧侧再开新侧。
struct NetDelta {
    int64_t matched = 0;  ///< 与既有批次配对的减仓量。
    int64_t opened  = 0;  ///< 落到新批次的开仓量。
};

NetDelta split_net_delta(int64_t before, int64_t after) {
    NetDelta d;
    const int64_t before_abs = std::abs(before);
    const int64_t after_abs  = std::abs(after);
    if (before == 0) {
        d.opened = after_abs;
    } else if (after == 0) {
        d.matched = before_abs;
    } else if ((before > 0) == (after > 0)) {
        if (after_abs < before_abs) d.matched = before_abs - after_abs;
        else                        d.opened  = after_abs - before_abs;
    } else {                                   // 穿零反手
        d.matched = before_abs;
        d.opened  = after_abs;
    }
    return d;
}
}  // namespace

void XtTradeExcursionTracker::init(const std::string& report_dir,
                                   const XtCommodityCatalog* catalog) {
    catalog_ = catalog;
    const std::string path = report_dir + "/trade_excursions.csv";
    out_.open(path, std::ios::out | std::ios::trunc);
    if (out_) {
        out_ << "open_date,open_time,close_date,close_time,code,side,qty,entry_px,exit_px,"
                "realized_pnl,mae,mfe,mae_px,mfe_px,ticks_held\n";
    } else {
        XtLogger::error_m("BT", "trade_excursions.csv open failed: {}", path);
    }
}

int64_t XtTradeExcursionTracker::vol_scale_fp_(CodeState& st, const char* code) {
    if (st.vol_scale_fp == 0) {
        // catalog 缺失/未收录/非正 vol_scale → 1.0 fp，与 AccountState 的 fallback 同口径。
        st.vol_scale_fp = xt_fp::kScale;
        if (catalog_) {
            const XtCommodityInfo* info = catalog_->find(code);
            if (info && info->vol_scale > 0.0) st.vol_scale_fp = xt_fp::to_fp(info->vol_scale);
        }
    }
    return st.vol_scale_fp;
}

void XtTradeExcursionTracker::resync_lots_(CodeState& st, int64_t before_qty_fp,
                                           int64_t entry_px_fp, uint32_t date, uint32_t time) {
    ++resync_count_;
    st.lots.clear();
    if (before_qty_fp == 0) return;
    Lot l;
    l.qty_fp      = std::abs(before_qty_fp);
    l.is_long     = before_qty_fp > 0;
    l.entry_px_fp = entry_px_fp;
    l.open_date   = date;
    l.open_time   = time;
    l.mae_px_fp   = entry_px_fp;
    l.mfe_px_fp   = entry_px_fp;
    st.lots.push_back(l);
}

void XtTradeExcursionTracker::on_tick(const XtTick& tick,
                                      const xt_account::AccountStateStore& account) {
    auto it = state_.find(tick.code);
    if (it == state_.end() || it->second.lots.empty()) return;

    // mark 以账户为准（已被本 tick 的 market_tick 刷新）；symbol 未收录时回退 tick 自身 last。
    const auto pos = account.snapshot_position(tick.code);
    const int64_t mark = pos.mark_price != 0 ? pos.mark_price : tick.price_fp;
    if (mark == 0) return;

    for (Lot& l : it->second.lots) {
        const int64_t diff = l.is_long ? (mark - l.entry_px_fp) : (l.entry_px_fp - mark);
        if (diff < l.mae_diff_fp) { l.mae_diff_fp = diff; l.mae_px_fp = mark; }
        if (diff > l.mfe_diff_fp) { l.mfe_diff_fp = diff; l.mfe_px_fp = mark; }
        ++l.ticks_held;
    }
}

void XtTradeExcursionTracker::on_fill(const XtTradeRecord& tr, int64_t before_qty_fp,
                                      int64_t after_qty_fp, int64_t after_avg_fp,
                                      int64_t realized_fp) {
    CodeState& st = state_[tr.code];
    const int64_t exit_px_fp = to_fp_round(tr.exec_price);
    const NetDelta d = split_net_delta(before_qty_fp, after_qty_fp);
    int64_t fifo_realized_fp = 0;

    if (d.matched > 0) {
        // 批次账本必须与账户净仓一致，否则配对量没有依据（正常 BT 路径恒一致）。
        int64_t book = 0;
        bool side_ok = true;
        for (const Lot& l : st.lots) {
            book += l.qty_fp;
            if (l.is_long != (before_qty_fp > 0)) side_ok = false;
        }
        if (!side_ok || book != std::abs(before_qty_fp)) {
            // 非 flip 时 after_avg 就是未变的持仓均价，可当重建批次的开仓价；flip 后
            // after_avg 已属新侧，只能退回本笔成交价。
            const bool flip = before_qty_fp != 0 && after_qty_fp != 0 &&
                              ((before_qty_fp > 0) != (after_qty_fp > 0));
            const int64_t px_fp = (!flip && after_avg_fp != 0) ? after_avg_fp : exit_px_fp;
            resync_lots_(st, before_qty_fp, px_fp, tr.trade_date, tr.trade_time);
        }

        const int64_t vol_scale = vol_scale_fp_(st, tr.code.c_str());
        int64_t remaining = d.matched;
        while (remaining > 0 && !st.lots.empty()) {
            Lot& l = st.lots.front();
            const int64_t take = std::min(remaining, l.qty_fp);
            remaining -= take;
            // FIFO 成本基准：本行 realized 只由本行自己的 entry/exit/qty 决定，故
            // 下游按 realized 正负分胜负行时与该行价格一致（账户移动均价口径会翻号）。
            // 乘序与 MAE/MFE 一致（先 ×qty 再 ×vol_scale），保证同行各金额列同粒度。
            const int64_t diff_fp =
                l.is_long ? (exit_px_fp - l.entry_px_fp) : (l.entry_px_fp - exit_px_fp);
            const int64_t slice_realized =
                xt_fp::mul(xt_fp::mul(diff_fp, take), vol_scale);
            fifo_realized_fp += slice_realized;
            emit_(tr.code, l, take, exit_px_fp, slice_realized, vol_scale,
                  tr.trade_date, tr.trade_time);
            l.qty_fp -= take;
            if (l.qty_fp <= 0) st.lots.pop_front();
        }
    }
    // 两口径差（见 header @note）：批次未平完时逐笔有差，只记录不修正。
    realized_basis_gap_fp_ += realized_fp - fifo_realized_fp;

    if (d.opened > 0) {
        Lot l;
        l.qty_fp      = d.opened;
        l.is_long     = after_qty_fp > 0;
        l.entry_px_fp = exit_px_fp;
        l.open_date   = tr.trade_date;
        l.open_time   = tr.trade_time;
        l.mae_px_fp   = exit_px_fp;
        l.mfe_px_fp   = exit_px_fp;
        st.lots.push_back(l);
    }
}

void XtTradeExcursionTracker::emit_(const std::string& code, const Lot& lot,
                                    int64_t slice_qty_fp, int64_t exit_px_fp,
                                    int64_t realized_fp, int64_t vol_scale_fp,
                                    uint32_t close_date, uint32_t close_time) {
    if (!out_) return;
    // 价差 running 极值 × 本次配对量 × 合约乘数 = 该配对的金额口径 MAE/MFE。
    const int64_t mae_fp = xt_fp::mul(xt_fp::mul(lot.mae_diff_fp, slice_qty_fp), vol_scale_fp);
    const int64_t mfe_fp = xt_fp::mul(xt_fp::mul(lot.mfe_diff_fp, slice_qty_fp), vol_scale_fp);
    out_ << lot.open_date << ',' << lot.open_time << ',' << close_date << ',' << close_time << ','
         << code << ',' << (lot.is_long ? "LONG" : "SHORT") << ','
         << qty(slice_qty_fp) << ','
         << px(lot.entry_px_fp) << ',' << px(exit_px_fp) << ','
         << pnl(realized_fp) << ',' << pnl(mae_fp) << ',' << pnl(mfe_fp) << ','
         << px(lot.mae_px_fp) << ',' << px(lot.mfe_px_fp) << ',' << lot.ticks_held << '\n';
}

void XtTradeExcursionTracker::flush() {
    for (const auto& [code, st] : state_) {
        dropped_open_at_eof_ += static_cast<int64_t>(st.lots.size());
    }
    if (dropped_open_at_eof_ > 0) {
        XtLogger::info_m("BT", "trade_excursions: dropped {} open-at-EOF lot(s)",
                         dropped_open_at_eof_);
    }
    if (resync_count_ > 0) {
        XtLogger::warn_m("BT", "trade_excursions: {} lot-ledger resync(s) vs account net position",
                         resync_count_);
    }
    if (realized_basis_gap_fp_ != 0) {
        // 期望内的口径差（FIFO vs 账户移动均价），不是错误：批次未平完时不相消。
        XtLogger::info_m("BT",
                         "trade_excursions: realized basis gap (account moving-average minus "
                         "FIFO) = {:.4f}",
                         xt_fp::from_fp(realized_basis_gap_fp_));
    }
    if (out_) out_.flush();
    out_.close();
}

}  // namespace xt_bt::hft
