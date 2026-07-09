#include "ctp/recovery/CTPRecovery.h"

#include <cstdio>

#include "recording/XtLiveRecorder.h"
#include "xt_util/time/XtSteadyClock.h"

namespace {

double clamp_non_negative(double v) { return v > 0.0 ? v : 0.0; }

bool is_pos_today(char d) {

    return d == THOST_FTDC_PSD_Today || d == '1';
}

bool is_pos_history(char d) {
    return d == THOST_FTDC_PSD_History || d == '2';
}

double clamp_today(double today, double total) {
    if (today <= 0.0 || total <= 0.0) return 0.0;
    return today < total ? today : total;
}

void merge_side_qty_avg(double row_qty, double row_avg,
                        double& side_qty, double& side_avg) {
    if (row_qty <= 0.0) return;
    const double total = side_qty + row_qty;
    if (total > 0.0)
        side_avg = (side_avg * side_qty + row_avg * row_qty) / total;
    side_qty = total;
}

}

CtpRecovery::CtpRecovery(HostPorts ports)
    : publish_positions_(std::move(ports.publish_positions)),
      start_position_query_(std::move(ports.start_position_query)),
      start_account_query_(std::move(ports.start_account_query)),
      start_order_query_(std::move(ports.start_order_query)),
      start_trade_query_(std::move(ports.start_trade_query)),
      publish_ready_(std::move(ports.publish_ready)) {}

void CtpRecovery::set_recovery_enabled(bool enabled) {
    std::lock_guard<std::mutex> guard(mu_);
    recovery_enabled_ = enabled;
    recovering_.store(enabled && !recovery_done_, std::memory_order_release);
}

bool CtpRecovery::recovery_enabled() const {
    std::lock_guard<std::mutex> guard(mu_);
    return recovery_enabled_;
}

bool CtpRecovery::is_recovering() const {

    return recovering_.load(std::memory_order_acquire);
}

void CtpRecovery::reset() {
    std::lock_guard<std::mutex> guard(mu_);
    recovery_done_       = false;
    recovered_positions_ = false;
    recovered_account_   = false;
    recovered_orders_    = false;
    recovered_trades_    = false;
    ready_emitted_       = false;

    ++reset_gen_;
    pos_cache_.clear();
    pos_bucket_cache_.clear();
    recovering_.store(recovery_enabled_, std::memory_order_release);
}

void CtpRecovery::on_login() {
    QueryStarter start_query;
    bool         enabled;
    bool         account_done;
    {
        std::lock_guard<std::mutex> guard(mu_);
        enabled      = recovery_enabled_;
        account_done = recovered_account_;

        start_query  = enabled ? start_position_query_ : start_account_query_;
    }
    if (enabled) {
        if (start_query) start_query();
        return;
    }

    if (!account_done && start_query) start_query();
    else                             emit_ready();
}

void CtpRecovery::on_position(const CThostFtdcInvestorPositionField* pPosition, bool bIsLast) {
    PositionsPublisher      publish_positions;
    QueryStarter            start_account_query;
    std::vector<PositionSnapshot> out;
    {
        std::lock_guard<std::mutex> guard(mu_);
        if (!recovery_enabled_) return;
        if (pPosition && pPosition->InstrumentID[0])
            merge_position_row(pPosition);
        if (!bIsLast) return;
        recovered_positions_ = true;
        out                 = collect_snapshots();
        publish_positions   = publish_positions_;
        start_account_query = start_account_query_;
    }

    if (publish_positions)   publish_positions(out);
    if (start_account_query) start_account_query();
}

void CtpRecovery::on_account_done(bool bIsLast) {
    if (!bIsLast) return;
    QueryStarter start_order_query;
    bool         do_ready = false;
    {
        std::lock_guard<std::mutex> guard(mu_);
        recovered_account_ = true;
        if (recovery_enabled_) {
            if (recovered_positions_ && !recovered_orders_)
                start_order_query = start_order_query_;
        } else {
            recovery_done_ = true;
            do_ready = true;
            recovering_.store(false, std::memory_order_release);
        }
    }

    if (start_order_query) { start_order_query(); return; }
    if (do_ready) emit_ready();
}

void CtpRecovery::on_order_done(bool bIsLast) {
    if (!bIsLast) return;
    QueryStarter start_trade_query;
    bool         done = false;
    {
        std::lock_guard<std::mutex> guard(mu_);
        recovered_orders_ = true;
        if (recovery_enabled_) {
            if (!recovered_trades_)
                start_trade_query = start_trade_query_;
            else {

                recovery_done_ = true;
                done = true;
                recovering_.store(false, std::memory_order_release);
            }
        }
    }

    if (start_trade_query) { start_trade_query(); return; }
    if (done) emit_ready();
}

void CtpRecovery::on_trade_done(bool bIsLast) {
    if (!bIsLast) return;
    bool done = false;
    {
        std::lock_guard<std::mutex> guard(mu_);
        recovered_trades_ = true;

        if (recovery_enabled_ && recovered_orders_) {
            recovery_done_ = true;
            done = true;
            recovering_.store(false, std::memory_order_release);
        }
    }

    if (done) emit_ready();
}

void CtpRecovery::accumulate_side(SideBucketAgg& side,
                                   double qty, double today_field,
                                   double yd, char pos_date) {
    side.total += qty;
    if (today_field > 0.0) {
        side.today_by_field += today_field;
        side.saw_today_field = true;
    }
    if (yd > 0.0) {
        side.yd_sum      += yd;
        side.saw_yd_field = true;
    }
    if (is_pos_today(pos_date)) {
        side.today_by_date += qty;
        side.saw_pos_date   = true;
    } else if (is_pos_history(pos_date)) {

        side.saw_pos_date = true;
    }
}

double CtpRecovery::resolve_today_qty(const SideBucketAgg& s) {

    if (s.total <= 0.0) return 0.0;
    if (s.saw_today_field) return clamp_today(s.today_by_field, s.total);
    if (s.saw_yd_field)    return s.total - clamp_today(s.yd_sum, s.total);
    if (s.saw_pos_date)    return clamp_today(s.today_by_date, s.total);
    return 0.0;
}

void CtpRecovery::merge_position_row(const CThostFtdcInvestorPositionField* row) {

    const double qty = clamp_non_negative(static_cast<double>(row->Position));
    if (qty <= 0.0) return;

    const std::string code = row->InstrumentID;
    auto& snap = pos_cache_[code];
    auto& agg  = pos_bucket_cache_[code];
    snap.code  = code;

    const double today_field = clamp_non_negative(static_cast<double>(row->TodayPosition));
    const double yd          = clamp_non_negative(static_cast<double>(row->YdPosition));
    const double avg         = row->PositionCost > 0.0
                               ? row->PositionCost / qty : 0.0;
    const char   pos_date    = row->PositionDate;

    const double long_frozen  = clamp_non_negative(static_cast<double>(row->LongFrozen));
    const double short_frozen = clamp_non_negative(static_cast<double>(row->ShortFrozen));

    if (row->PosiDirection == THOST_FTDC_PD_Long) {
        merge_side_qty_avg(qty, avg, snap.long_qty, snap.long_avg);
        accumulate_side(agg.long_side, qty, today_field, yd, pos_date);
        snap.long_frozen  += long_frozen;
        snap.short_frozen += short_frozen;
    } else if (row->PosiDirection == THOST_FTDC_PD_Short) {
        merge_side_qty_avg(qty, avg, snap.short_qty, snap.short_avg);
        accumulate_side(agg.short_side, qty, today_field, yd, pos_date);
        snap.long_frozen  += long_frozen;
        snap.short_frozen += short_frozen;
    }
}

std::vector<CtpRecovery::PositionSnapshot> CtpRecovery::collect_snapshots() {
    std::vector<PositionSnapshot> out;
    out.reserve(pos_cache_.size());
    for (auto& [code, snap] : pos_cache_) {
        const auto it = pos_bucket_cache_.find(code);
        if (it != pos_bucket_cache_.end()) {
            const PositionBucketAgg& bkt = it->second;
            snap.long_today        = resolve_today_qty(bkt.long_side);
            snap.short_today       = resolve_today_qty(bkt.short_side);
        }
        out.push_back(snap);
    }
    pos_cache_.clear();
    pos_bucket_cache_.clear();
    return out;
}

void CtpRecovery::emit_ready() {
    ReadyPublisher publish_ready;
    uint32_t gen_at_emit;
    {
        std::lock_guard<std::mutex> guard(mu_);
        if (ready_emitted_) return;
        ready_emitted_ = true;
        gen_at_emit = reset_gen_;
        publish_ready = publish_ready_;
    }
    if (auto* rec = XtLiveRecorder::global()) {
        const uint64_t ts_local_ns = XtSteadyClock::steady_now_ns();
        rec->emit_conn_state(ts_local_ns, "td", "ready", "");
    }

    {
        std::lock_guard<std::mutex> guard(mu_);
        if (reset_gen_ != gen_at_emit) return;
    }
    if (publish_ready) publish_ready();
}
