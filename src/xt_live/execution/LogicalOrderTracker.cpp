#include "LogicalOrderTracker.h"

#include <cassert>
#include <cstring>

#include "xt_util/time/XtSteadyClock.h"

bool LogicalOrderTracker::Entry::all_done() const noexcept {
    for (uint8_t i = 0; i < slice_count; ++i) {
        if (!LogicalOrderTracker::is_terminal(slice_status[i])) return false;
    }
    return slice_count > 0;
}

int64_t LogicalOrderTracker::Entry::total_filled_fp() const noexcept {
    int64_t sum = 0;
    for (uint8_t i = 0; i < slice_count; ++i) {
        sum += slice_filled_fp[i];
    }
    return sum;
}

int64_t LogicalOrderTracker::Entry::working_leaves_fp() const noexcept {
    int64_t sum = 0;
    for (uint8_t i = 0; i < slice_count; ++i) {
        if (LogicalOrderTracker::is_terminal(slice_status[i])) continue;

        const int64_t leaves = slice_qty_fp[i] - slice_filled_fp[i];
        if (leaves > 0) sum += leaves;
    }
    return sum;
}

bool LogicalOrderTracker::is_terminal(XtOrderStatus s) noexcept {
    return s == XtOrderStatus::Filled
        || s == XtOrderStatus::Canceled
        || s == XtOrderStatus::Rejected;
}

XtOrderStatus LogicalOrderTracker::aggregate_status(const Entry& e,
                                                     int64_t total_filled_fp) noexcept {

    bool all_terminal   = true;
    bool any_rejected   = false;
    for (uint8_t i = 0; i < e.slice_count; ++i) {
        if (!is_terminal(e.slice_status[i])) {
            all_terminal = false;
        }
        if (e.slice_status[i] == XtOrderStatus::Rejected) {
            any_rejected = true;
        }
    }

    if (all_terminal) {

        if (total_filled_fp >= e.total_qty_fp) {
            return XtOrderStatus::Filled;
        }
        if (total_filled_fp == 0) {

            return any_rejected ? XtOrderStatus::Rejected : XtOrderStatus::Canceled;
        }

        return XtOrderStatus::Canceled;
    }

    if (total_filled_fp > 0) {
        return XtOrderStatus::PartiallyFilled;
    }

    return XtOrderStatus::Submitted;
}

uint64_t LogicalOrderTracker::register_multi_slice(const uint64_t* phys_ids,
                                                    const int64_t*  slice_qty_fps,
                                                    uint8_t         slice_count,
                                                    int64_t         total_qty_fp,
                                                    bool            is_buy,
                                                    int64_t         price_fp,
                                                    XtOffset        close_offset,
                                                    uint64_t        symbol_id) {
    assert(slice_count >= 2 && "单切片不需要注册，直接透传");
    assert(slice_count <= kMaxSlices);

    std::lock_guard<std::mutex> lock(mu_);

    const uint64_t clord_id = next_clord_id_++;

    Entry e;
    e.clord_id     = clord_id;
    e.total_qty_fp = total_qty_fp;
    e.slice_count  = slice_count;
    e.is_buy       = is_buy;
    e.price_fp     = price_fp;
    e.close_offset = close_offset;
    e.symbol_id    = symbol_id;

    e.register_ms = XtSteadyClock::steady_now_ms();

    for (uint8_t i = 0; i < slice_count; ++i) {
        e.phys_ids[i]        = phys_ids[i];
        e.slice_qty_fp[i]    = slice_qty_fps[i];
        e.slice_filled_fp[i] = 0;
        e.slice_status[i]    = XtOrderStatus::Submitted;
    }

    entries_.emplace(clord_id, e);
    for (uint8_t i = 0; i < slice_count; ++i) {
        phys_to_clord_.emplace(phys_ids[i], clord_id);
    }

    return clord_id;
}

void LogicalOrderTracker::translate(const XtOrderExecUpdate& phys_upd,
                                    XtOrderExecUpdate&       out) {
    std::lock_guard<std::mutex> lock(mu_);

    auto clord_it = phys_to_clord_.find(phys_upd.id);
    if (clord_it == phys_to_clord_.end()) {
        out = phys_upd;
        return;
    }

    const uint64_t clord_id = clord_it->second;
    auto entry_it = entries_.find(clord_id);
    if (entry_it == entries_.end()) {

        out = phys_upd;
        return;
    }
    Entry& e = entry_it->second;

    uint8_t slice_idx = e.slice_count;
    for (uint8_t i = 0; i < e.slice_count; ++i) {
        if (e.phys_ids[i] == phys_upd.id) {
            slice_idx = i;
            break;
        }
    }
    if (slice_idx == e.slice_count) {

        out = phys_upd;
        return;
    }

    e.slice_filled_fp[slice_idx] = phys_upd.filled_fp;
    e.slice_status[slice_idx]    = phys_upd.status;

    const int64_t agg_filled = e.total_filled_fp();
    out = phys_upd;

    out.id        = clord_id;
    out.filled_fp = agg_filled;

    out.leaves_fp = e.working_leaves_fp();
    out.qty_fp    = e.total_qty_fp;
    out.status    = aggregate_status(e, agg_filled);

    out.is_buy       = e.is_buy;
    out.price_fp     = e.price_fp;
    out.close_offset = e.close_offset;
    out.symbol_id    = e.symbol_id != 0 ? e.symbol_id : phys_upd.symbol_id;
}

uint8_t LogicalOrderTracker::expand_to_phys(uint64_t  clord_id,
                                             uint64_t* phys_out,
                                             uint8_t   max_count) const {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = entries_.find(clord_id);
    if (it == entries_.end()) return 0;

    const Entry& e = it->second;
    const uint8_t n = (e.slice_count < max_count) ? e.slice_count : max_count;
    for (uint8_t i = 0; i < n; ++i) {
        phys_out[i] = e.phys_ids[i];
    }
    return n;
}

uint64_t LogicalOrderTracker::lookup_done_clord(uint64_t phys_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    const auto map_it = phys_to_clord_.find(phys_id);
    if (map_it == phys_to_clord_.end()) return 0;
    const auto entry_it = entries_.find(map_it->second);
    if (entry_it == entries_.end()) return 0;
    return entry_it->second.all_done() ? entry_it->second.clord_id : 0;
}

void LogicalOrderTracker::cleanup_done() {
    cleanup_done_at(XtSteadyClock::steady_now_ms());
}

size_t LogicalOrderTracker::cleanup_done_at(uint64_t now_ms) {
    std::lock_guard<std::mutex> lock(mu_);
    size_t reaped = 0;
    auto it = entries_.begin();
    while (it != entries_.end()) {
        Entry& e = it->second;
        if (!e.all_done()) {

            e.all_done_since_ms = 0;
            ++it;
            continue;
        }
        if (e.all_done_since_ms == 0) {
            e.all_done_since_ms = now_ms;
            ++it;
            continue;
        }

        if (now_ms < e.all_done_since_ms ||
            now_ms - e.all_done_since_ms < kDoneEntryGraceMs) {
            ++it;
            continue;
        }
        for (uint8_t i = 0; i < e.slice_count; ++i) {
            phys_to_clord_.erase(e.phys_ids[i]);
        }
        it = entries_.erase(it);
        ++reaped;
    }
    return reaped;
}

size_t LogicalOrderTracker::translate_open_orders(XtOrderExecUpdate* rows, size_t n) const {
    if (!rows || n == 0) return 0;
    std::lock_guard<std::mutex> lock(mu_);
    if (phys_to_clord_.empty()) return n;

    constexpr size_t kMaxDistinctClords = 64;
    uint64_t seen[kMaxDistinctClords];
    size_t   seen_count = 0;
    size_t   w = 0;
    for (size_t i = 0; i < n; ++i) {
        const auto map_it = phys_to_clord_.find(rows[i].id);
        if (map_it == phys_to_clord_.end()) {
            rows[w++] = rows[i];
            continue;
        }
        const uint64_t clord    = map_it->second;
        const auto     entry_it = entries_.find(clord);
        if (entry_it == entries_.end()) {
            rows[w++] = rows[i];
            continue;
        }
        bool dup = false;
        for (size_t k = 0; k < seen_count; ++k) {
            if (seen[k] == clord) {
                dup = true;
                break;
            }
        }
        if (dup) continue;
        if (seen_count >= kMaxDistinctClords) {
            rows[w++] = rows[i];
            continue;
        }
        seen[seen_count++] = clord;

        const Entry&  e          = entry_it->second;
        const int64_t agg_filled = e.total_filled_fp();
        XtOrderExecUpdate out    = rows[i];
        out.id           = clord;
        out.qty_fp       = e.total_qty_fp;
        out.filled_fp    = agg_filled;
        out.leaves_fp    = e.working_leaves_fp();
        out.status       = aggregate_status(e, agg_filled);
        out.is_buy       = e.is_buy;
        out.price_fp     = e.price_fp;
        out.close_offset = e.close_offset;
        if (e.symbol_id != 0) out.symbol_id = e.symbol_id;
        rows[w++] = out;
    }
    return w;
}

size_t LogicalOrderTracker::cleanup_stale(uint64_t now_ms, uint64_t stale_threshold_ms) {
    std::lock_guard<std::mutex> lock(mu_);
    size_t removed = 0;
    auto it = entries_.begin();
    while (it != entries_.end()) {
        const Entry& e = it->second;
        if (!e.all_done() && e.register_ms > 0 &&
            now_ms > e.register_ms &&
            (now_ms - e.register_ms) > stale_threshold_ms) {
            for (uint8_t i = 0; i < e.slice_count; ++i) {
                phys_to_clord_.erase(e.phys_ids[i]);
            }
            it = entries_.erase(it);
            ++removed;
        } else {
            ++it;
        }
    }
    return removed;
}
