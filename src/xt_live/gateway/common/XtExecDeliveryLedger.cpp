#include "xt_live/gateway/common/XtExecDeliveryLedger.h"

#include <utility>

namespace xt_live::gateway {

std::atomic<uint64_t>& exec_delivery_terminal_evicted_total() {
    static std::atomic<uint64_t> v{0};
    return v;
}

XtExecDeliveryLedger::XtExecDeliveryLedger(std::size_t terminal_cap)
    : terminal_cap_(terminal_cap == 0 ? 1 : terminal_cap) {}

void XtExecDeliveryLedger::register_order(uint64_t local_id, OrderIdentity identity) {
    if (local_id == 0) {
        invalid_input_dropped_.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    std::lock_guard<std::mutex> guard(mu_);
    auto it = records_.find(local_id);
    if (it != records_.end()) {

        erase_record_unlocked_(it);
    }
    ExecOrderRecord rec;
    rec.local_id = local_id;
    rec.sid_id = identity.sid_id;
    rec.code = std::move(identity.code);
    rec.qty_fp = identity.qty_fp;
    rec.is_buy = identity.is_buy;
    rec.close_offset = identity.close_offset;
    rec.absorbed_watermark_fp = identity.absorbed_watermark_fp;
    rec.session_gen = identity.session_gen;
    if (!identity.venue_key.empty()) {
        rec.venue_key = std::move(identity.venue_key);
        bind_venue_key_unlocked_(local_id, rec.venue_key);
    }
    records_.emplace(local_id, std::move(rec));
}

void XtExecDeliveryLedger::rollback_register(uint64_t local_id) {
    std::lock_guard<std::mutex> guard(mu_);
    auto it = records_.find(local_id);
    if (it == records_.end()) return;
    erase_record_unlocked_(it);
}

bool XtExecDeliveryLedger::record_fill(uint64_t local_id, FillArgs fill) {
    if (fill.fill_key.empty()) {
        invalid_input_dropped_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    std::lock_guard<std::mutex> guard(mu_);
    if (fill_key_index_.find(fill.fill_key) != fill_key_index_.end()) {
        duplicate_fill_suppressed_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    auto it = (local_id != 0) ? records_.find(local_id) : records_.end();
    if (it == records_.end()) {

        fill_record_miss_.fetch_add(1, std::memory_order_relaxed);
        fill_key_index_.insert(fill.fill_key);
        orderless_fifo_.push_back(std::move(fill.fill_key));
        enforce_orderless_cap_unlocked_();
        return true;
    }
    ExecOrderRecord& rec = it->second;
    rec.cum_filled_fp += fill.qty_fp;
    fill_key_index_.insert(fill.fill_key);
    ExecFillEntry entry;
    entry.fill_key = std::move(fill.fill_key);
    entry.qty_fp = fill.qty_fp;
    entry.source = fill.source;
    entry.absorbed = fill.absorbed;
    entry.query_gen = fill.query_gen;
    rec.fills.push_back(std::move(entry));
    return true;
}

bool XtExecDeliveryLedger::fill_accounted(const std::string& fill_key) const {
    if (fill_key.empty()) return false;
    std::lock_guard<std::mutex> guard(mu_);
    return fill_key_index_.find(fill_key) != fill_key_index_.end();
}

int64_t XtExecDeliveryLedger::cum_filled_fp(uint64_t local_id) const {
    std::lock_guard<std::mutex> guard(mu_);
    auto it = records_.find(local_id);
    return (it == records_.end()) ? 0 : it->second.cum_filled_fp;
}

bool XtExecDeliveryLedger::record_terminal(uint64_t local_id, const TerminalRecord& terminal) {
    if (local_id == 0) {
        invalid_input_dropped_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    std::lock_guard<std::mutex> guard(mu_);
    auto it = records_.find(local_id);
    if (it == records_.end()) {

        terminal_record_miss_.fetch_add(1, std::memory_order_relaxed);
        ExecOrderRecord rec;
        rec.local_id = local_id;
        it = records_.emplace(local_id, std::move(rec)).first;
    }
    ExecOrderRecord& rec = it->second;
    if (rec.has_terminal) {
        duplicate_terminal_ignored_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    rec.has_terminal = true;
    rec.terminal = terminal;
    ++terminal_records_;
    terminal_fifo_.push_back(local_id);
    enforce_terminal_cap_unlocked_();
    return true;
}

bool XtExecDeliveryLedger::terminal_record(uint64_t local_id, TerminalRecord& out) const {
    std::lock_guard<std::mutex> guard(mu_);
    auto it = records_.find(local_id);
    if (it == records_.end() || !it->second.has_terminal) return false;
    out = it->second.terminal;
    return true;
}

void XtExecDeliveryLedger::record_cancel_attempt(uint64_t local_id, uint64_t ts_ms,
                                                 CancelOutcome outcome) {
    std::lock_guard<std::mutex> guard(mu_);
    auto it = records_.find(local_id);
    if (it == records_.end()) {
        cancel_attempt_miss_.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    ExecOrderRecord& rec = it->second;
    rec.cancel_attempts[rec.cancel_attempt_total % kCancelAttemptRing] =
        CancelAttempt{ts_ms, outcome};
    ++rec.cancel_attempt_total;
}

void XtExecDeliveryLedger::set_venue_key(uint64_t local_id, const std::string& venue_key) {
    if (venue_key.empty()) return;
    std::lock_guard<std::mutex> guard(mu_);
    auto it = records_.find(local_id);
    if (it == records_.end()) {
        venue_key_miss_.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    ExecOrderRecord& rec = it->second;
    if (rec.venue_key == venue_key) return;
    if (!rec.venue_key.empty()) {
        auto vit = venue_key_to_local_.find(rec.venue_key);
        if (vit != venue_key_to_local_.end() && vit->second == local_id) {
            venue_key_to_local_.erase(vit);
        }
    }
    rec.venue_key = venue_key;
    bind_venue_key_unlocked_(local_id, rec.venue_key);
}

std::optional<uint64_t> XtExecDeliveryLedger::local_for_venue_key(
    const std::string& venue_key) const {
    if (venue_key.empty()) return std::nullopt;
    std::lock_guard<std::mutex> guard(mu_);
    auto it = venue_key_to_local_.find(venue_key);
    if (it == venue_key_to_local_.end()) return std::nullopt;
    return it->second;
}

std::vector<uint64_t> XtExecDeliveryLedger::open_ids() const {
    std::vector<uint64_t> out;
    std::lock_guard<std::mutex> guard(mu_);
    out.reserve(records_.size() - terminal_records_);
    for (const auto& [id, rec] : records_) {
        if (!rec.has_terminal) out.push_back(id);
    }
    return out;
}

bool XtExecDeliveryLedger::get_record(uint64_t local_id, ExecOrderRecord& out) const {
    std::lock_guard<std::mutex> guard(mu_);
    auto it = records_.find(local_id);
    if (it == records_.end()) return false;
    out = it->second;
    return true;
}

void XtExecDeliveryLedger::clear_session() {
    std::lock_guard<std::mutex> guard(mu_);
    uint64_t cleared = 0;
    while (!terminal_fifo_.empty()) {
        const uint64_t id = terminal_fifo_.front();
        terminal_fifo_.pop_front();
        auto it = records_.find(id);
        if (it == records_.end() || !it->second.has_terminal) continue;
        erase_record_unlocked_(it);
        ++cleared;
    }
    cleared += orderless_fifo_.size();
    for (const auto& key : orderless_fifo_) {
        fill_key_index_.erase(key);
    }
    orderless_fifo_.clear();
    session_clear_total_.fetch_add(1, std::memory_order_relaxed);
    session_cleared_entries_.fetch_add(cleared, std::memory_order_relaxed);
}

XtExecDeliveryLedger::Stats XtExecDeliveryLedger::stats() const {
    Stats s;
    {
        std::lock_guard<std::mutex> guard(mu_);
        s.terminal_records = terminal_records_;
        s.open_records = records_.size() - terminal_records_;
        s.accounted_fill_keys = fill_key_index_.size();
        s.orderless_fill_keys = orderless_fifo_.size();
    }
    s.duplicate_fill_suppressed = duplicate_fill_suppressed_.load(std::memory_order_relaxed);
    s.duplicate_terminal_ignored = duplicate_terminal_ignored_.load(std::memory_order_relaxed);
    s.fill_record_miss = fill_record_miss_.load(std::memory_order_relaxed);
    s.terminal_record_miss = terminal_record_miss_.load(std::memory_order_relaxed);
    s.cancel_attempt_miss = cancel_attempt_miss_.load(std::memory_order_relaxed);
    s.venue_key_miss = venue_key_miss_.load(std::memory_order_relaxed);
    s.venue_key_collision = venue_key_collision_.load(std::memory_order_relaxed);
    s.evicted_terminal_records = evicted_terminal_records_.load(std::memory_order_relaxed);
    s.evicted_orderless_keys = evicted_orderless_keys_.load(std::memory_order_relaxed);
    s.session_clear_total = session_clear_total_.load(std::memory_order_relaxed);
    s.session_cleared_entries = session_cleared_entries_.load(std::memory_order_relaxed);
    s.invalid_input_dropped = invalid_input_dropped_.load(std::memory_order_relaxed);
    return s;
}

void XtExecDeliveryLedger::bind_venue_key_unlocked_(uint64_t local_id,
                                                    const std::string& venue_key) {
    auto [vit, inserted] = venue_key_to_local_.try_emplace(venue_key, local_id);
    if (!inserted && vit->second != local_id) {

        venue_key_collision_.fetch_add(1, std::memory_order_relaxed);
        vit->second = local_id;
    }
}

void XtExecDeliveryLedger::erase_record_unlocked_(RecordMap::iterator it) {
    ExecOrderRecord& rec = it->second;
    if (!rec.venue_key.empty()) {
        auto vit = venue_key_to_local_.find(rec.venue_key);
        if (vit != venue_key_to_local_.end() && vit->second == rec.local_id) {
            venue_key_to_local_.erase(vit);
        }
    }
    for (const auto& f : rec.fills) {
        fill_key_index_.erase(f.fill_key);
    }
    if (rec.has_terminal && terminal_records_ > 0) --terminal_records_;
    records_.erase(it);
}

void XtExecDeliveryLedger::enforce_terminal_cap_unlocked_() {
    while (terminal_fifo_.size() > terminal_cap_) {
        const uint64_t old_id = terminal_fifo_.front();
        terminal_fifo_.pop_front();
        auto it = records_.find(old_id);

        if (it == records_.end() || !it->second.has_terminal) continue;
        erase_record_unlocked_(it);
        evicted_terminal_records_.fetch_add(1, std::memory_order_relaxed);
        exec_delivery_terminal_evicted_total().fetch_add(1, std::memory_order_relaxed);
    }
}

void XtExecDeliveryLedger::enforce_orderless_cap_unlocked_() {
    while (orderless_fifo_.size() > terminal_cap_) {
        fill_key_index_.erase(orderless_fifo_.front());
        orderless_fifo_.pop_front();
        evicted_orderless_keys_.fetch_add(1, std::memory_order_relaxed);
    }
}

}
