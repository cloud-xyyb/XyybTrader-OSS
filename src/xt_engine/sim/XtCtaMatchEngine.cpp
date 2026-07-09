#include "xt_engine/sim/XtCtaMatchEngine.h"

#include <climits>
#include <cstring>

#include "xt_util/time/XtTime.h"

bool XtCtaMatchEngine::cancel(uint64_t order_id) {
    for (auto it = pending_.begin(); it != pending_.end(); ++it) {
        if (it->order_id != order_id) {
            continue;
        }

        publish_order_update_(it->order_id, it->code, it->is_buy, it->offset,
                              it->limit_price_fp, it->qty_fp,
                              XtOrderStatus::Canceled,
                               0,
                               it->qty_fp,
                               0,
                               0,
                               0);
        pending_.erase(it);
        return true;
    }
    return false;
}

int XtCtaMatchEngine::process_pending(const char* code, int64_t price_fp,
                                      uint32_t date, uint32_t time_ms) {
    int fills = 0;
    for (auto it = pending_.begin(); it != pending_.end(); ) {
        if (std::strcmp(it->code, code) != 0) { ++it; continue; }

        if (it->limit_price_fp > 0) {
            if ( it->is_buy && price_fp > it->limit_price_fp) { ++it; continue; }
            if (!it->is_buy && price_fp < it->limit_price_fp) { ++it; continue; }
        }
        if (do_fill_(it->code, it->is_buy, it->qty_fp, price_fp,
                     it->tag, it->offset, date, time_ms, it->order_id)) {
            ++fills;
        }
        it = pending_.erase(it);
    }
    return fills;
}

int XtCtaMatchEngine::process_pending_ignore_limit(const char* code, int64_t price_fp,
                                                   uint32_t date, uint32_t time_ms) {
    int fills = 0;
    for (auto it = pending_.begin(); it != pending_.end(); ) {
        if (std::strcmp(it->code, code) != 0) { ++it; continue; }
        if (do_fill_(it->code, it->is_buy, it->qty_fp, price_fp,
                     it->tag, it->offset, date, time_ms, it->order_id)) {
            ++fills;
        }
        it = pending_.erase(it);
    }
    return fills;
}

bool XtCtaMatchEngine::fill_immediate(const char* code, bool is_buy, int64_t qty_fp,
                                      int64_t price_fp, const char* tag, XtOffset offset,
                                      uint32_t date, uint32_t time_ms) {
    return do_fill_(code, is_buy, qty_fp, price_fp, tag, offset, date, time_ms);
}

int XtCtaMatchEngine::flush_session_end(uint32_t date, uint32_t time_ms) {
    int fills = 0;
    for (const auto& pf : pending_) {
        if (pf.limit_price_fp > 0) {

            publish_order_update_(pf.order_id, pf.code, pf.is_buy, pf.offset,
                                  pf.limit_price_fp, pf.qty_fp,
                                  XtOrderStatus::Canceled,
                                   0,
                                   pf.qty_fp,
                                   0,
                                  date, time_ms);
            continue;
        }
        auto it = last_price_fp_.find(pf.code);
        if (it != last_price_fp_.end() && it->second > 0) {
            if (do_fill_(pf.code, pf.is_buy, pf.qty_fp, it->second,
                         pf.tag, pf.offset, date, time_ms, pf.order_id)) {
                ++fills;
            }
        } else {

            publish_order_update_(pf.order_id, pf.code, pf.is_buy, pf.offset,
                                  pf.limit_price_fp, pf.qty_fp,
                                  XtOrderStatus::Canceled,
                                   0,
                                   pf.qty_fp,
                                   0,
                                  date, time_ms);
        }
    }
    pending_.clear();

    promote_due_(INT64_MAX);
    return fills;
}

int XtCtaMatchEngine::advance_clock(uint32_t date, uint32_t time_ms) {
    const int64_t now_ns = xt_time::to_ns(date, time_ms);

    constexpr int64_t kSegmentWrapBackwardNs = 3'600'000'000'000LL;
    if (cur_clock_ns_ != INT64_MIN &&
        cur_clock_ns_ - now_ns > kSegmentWrapBackwardNs) {
        for (auto& dt : delay_trades_) {
            if (dt.visible_at_ns > now_ns) dt.visible_at_ns = now_ns;
        }
        for (auto& du : delay_updates_) {
            if (du.visible_at_ns > now_ns) du.visible_at_ns = now_ns;
        }
        if (!delay_trades_.empty() || !delay_updates_.empty()) {
            earliest_due_ns_ = now_ns;
        }
    }
    cur_clock_ns_ = now_ns;
    if (now_ns < earliest_due_ns_) return 0;
    return promote_due_(now_ns);
}

void XtCtaMatchEngine::publish_order_update_(uint64_t order_id,
                                             const char* code,
                                             bool is_buy,
                                             XtOffset offset,
                                             int64_t price_fp,
                                             int64_t qty_fp,
                                             XtOrderStatus status,
                                             int64_t filled_fp,
                                             int64_t leaves_fp,
                                             int64_t last_fill_fp,
                                             uint32_t date,
                                             uint32_t time_ms) {
    if (order_id == 0) return;
    PendingOrderUpdate u{};
    u.order_id     = order_id;
    std::strncpy(u.code, code ? code : "", sizeof(u.code) - 1);
    u.is_buy       = is_buy;
    u.offset       = offset;
    u.price_fp     = price_fp;
    u.qty_fp       = qty_fp;
    u.status       = status;
    u.filled_fp    = filled_fp;
    u.leaves_fp    = leaves_fp;
    u.last_fill_fp = last_fill_fp;
    u.date         = date;
    u.time_ms      = time_ms;

    const int64_t now_ns = (date != 0)
                         ? xt_time::to_ns(date, time_ms)
                         : cur_clock_ns_;
    enqueue_delayed_update_(u, now_ns);
}

bool XtCtaMatchEngine::do_fill_(const char* code, bool is_buy, int64_t qty_fp,
                                int64_t price_fp, const char* tag, XtOffset offset,
                                uint32_t date, uint32_t time_ms,
                                uint64_t order_id) {

    XtTradeRecord tr;
    tr.code         = code ? code : "";
    tr.is_buy       = is_buy;
    tr.qty_fp       = qty_fp;
    tr.price        = order_exec_from_fp(price_fp);
    tr.exec_price   = order_exec_from_fp(price_fp);
    tr.order_date   = date;
    tr.order_time   = time_ms;
    tr.trade_date   = date;
    tr.trade_time   = time_ms;
    tr.user_tag     = tag ? tag : "";
    tr.is_close     = false;
    tr.close_offset = offset;
    tr.is_today     = (offset == XtOffset::CloseToday);
    tr.order_id     = (order_id != 0) ? order_id : next_order_id_++;

    tr.fill_ts_us   = static_cast<uint64_t>(date)    * 1'000'000'000ULL
                    + static_cast<uint64_t>(time_ms) * 1000ULL;

    const int64_t fill_ns = xt_time::to_ns(date, time_ms);
    enqueue_delayed_trade_(tr, fill_ns);

    publish_order_update_(order_id, code, is_buy, offset, price_fp, qty_fp,
                          XtOrderStatus::Filled,  qty_fp,
                           0,  qty_fp,
                          date, time_ms);
    return true;
}

void XtCtaMatchEngine::enqueue_delayed_trade_(const XtTradeRecord& tr, int64_t now_ns) {
    if (cfg_.fill_latency_ns <= 0) {
        visible_trades_.push_back(tr);
        return;
    }
    const int64_t visible_at = now_ns + cfg_.fill_latency_ns;
    if (visible_at <= cur_clock_ns_) {
        visible_trades_.push_back(tr);
        return;
    }
    delay_trades_.push_back({tr, visible_at});
    if (visible_at < earliest_due_ns_) earliest_due_ns_ = visible_at;
}

void XtCtaMatchEngine::enqueue_delayed_update_(const PendingOrderUpdate& upd, int64_t now_ns) {
    if (now_ns == INT64_MIN || cfg_.cancel_latency_ns <= 0) {
        visible_updates_.push_back(upd);
        return;
    }
    const int64_t visible_at = now_ns + cfg_.cancel_latency_ns;
    if (visible_at <= cur_clock_ns_) {
        visible_updates_.push_back(upd);
        return;
    }
    delay_updates_.push_back({upd, visible_at});
    if (visible_at < earliest_due_ns_) earliest_due_ns_ = visible_at;
}

int XtCtaMatchEngine::promote_due_(int64_t now_ns) {
    int released = 0;

    for (size_t i = 0; i < delay_trades_.size(); ) {
        if (delay_trades_[i].visible_at_ns <= now_ns) {
            visible_trades_.push_back(std::move(delay_trades_[i].trade));
            delay_trades_[i] = std::move(delay_trades_.back());
            delay_trades_.pop_back();
            ++released;
        } else {
            ++i;
        }
    }
    for (size_t i = 0; i < delay_updates_.size(); ) {
        if (delay_updates_[i].visible_at_ns <= now_ns) {
            visible_updates_.push_back(std::move(delay_updates_[i].update));
            delay_updates_[i] = std::move(delay_updates_.back());
            delay_updates_.pop_back();
            ++released;
        } else {
            ++i;
        }
    }
    recompute_earliest_due_();
    return released;
}

void XtCtaMatchEngine::recompute_earliest_due_() {
    int64_t e = INT64_MAX;
    for (const auto& d : delay_trades_)  if (d.visible_at_ns < e) e = d.visible_at_ns;
    for (const auto& d : delay_updates_) if (d.visible_at_ns < e) e = d.visible_at_ns;
    earliest_due_ns_ = e;
}
