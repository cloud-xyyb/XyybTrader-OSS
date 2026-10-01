#include "XtStopOrderBook.h"

#include <cstring>

namespace xt_strategy_engine {

namespace {

void copy_bounded(char* dst, std::size_t cap, const char* src) {
    if (!src) { dst[0] = '\0'; return; }
    std::strncpy(dst, src, cap - 1);
    dst[cap - 1] = '\0';
}

} // namespace

bool XtStopOrderBook::code_matches_(const StopSlot& s, const char* code) {
    return code && std::strncmp(s.code, code, sizeof(s.code)) == 0;
}

StopSlot* XtStopOrderBook::new_slot_(const char* code, bool is_buy, int64_t qty_fp,
                                     XtOffset offset, const char* tag) {
    if (!code || code[0] == '\0' || qty_fp <= 0) return nullptr;
    if (slots_.size() >= kMaxStopSlotsPerCtx) return nullptr;
    slots_.emplace_back();
    StopSlot& s = slots_.back();
    s.stop_id = next_stop_id_++;
    s.state = StopSlotState::Armed;
    copy_bounded(s.code, sizeof(s.code), code);
    s.is_buy = is_buy;
    s.qty_fp = qty_fp;
    s.offset = offset;
    copy_bounded(s.tag, sizeof(s.tag), tag);
    return &s;
}

uint64_t XtStopOrderBook::arm_fixed(const char* code, bool is_buy,
                                    int64_t trigger_price_fp, int64_t qty_fp,
                                    XtOffset offset, const char* tag) {
    if (trigger_price_fp <= 0) return 0;
    StopSlot* s = new_slot_(code, is_buy, qty_fp, offset, tag);
    if (!s) return 0;
    s->is_trailing = false;
    s->trigger_price_fp = trigger_price_fp;
    return s->stop_id;
}

uint64_t XtStopOrderBook::arm_trailing(const char* code, bool is_buy,
                                       int64_t distance_fp, int64_t qty_fp,
                                       XtOffset offset, const char* tag,
                                       int64_t seed_price_fp) {
    if (distance_fp <= 0 || seed_price_fp < 0) return 0;
    StopSlot* s = new_slot_(code, is_buy, qty_fp, offset, tag);
    if (!s) return 0;
    s->is_trailing = true;
    s->distance_fp = distance_fp;
    s->watermark_fp = seed_price_fp; // 0 = lazy-seed
    return s->stop_id;
}

bool XtStopOrderBook::disarm(uint64_t stop_id, uint64_t& out_fired_order_id) {
    out_fired_order_id = 0;
    for (std::size_t i = 0; i < slots_.size(); ++i) {
        if (slots_[i].stop_id != stop_id) continue;
        out_fired_order_id = slots_[i].fired_order_id;
        erase_at_(i);
        return true;
    }
    return false;
}

void XtStopOrderBook::check(const char* code, int64_t last_fp,
                            std::vector<uint64_t>& out_fired_ids) {
    if (!code || last_fp <= 0) return;
    for (StopSlot& s : slots_) {
        if (!code_matches_(s, code)) continue;
        if (s.is_trailing) {
            // 水位对含 Fired 的全部 slot 持续更新；lazy-seed 当 tick 不会
            // 触发（|last - 水位| = 0 < distance）。
            if (s.watermark_fp == 0) {
                s.watermark_fp = last_fp;
            } else if (s.is_buy) {
                if (last_fp < s.watermark_fp) s.watermark_fp = last_fp;
            } else {
                if (last_fp > s.watermark_fp) s.watermark_fp = last_fp;
            }
        }
        if (s.state != StopSlotState::Armed) continue;
        bool hit;
        if (s.is_trailing) {
            hit = s.is_buy ? (last_fp >= s.watermark_fp + s.distance_fp)
                           : (last_fp <= s.watermark_fp - s.distance_fp);
        } else {
            hit = s.is_buy ? (last_fp >= s.trigger_price_fp)
                           : (last_fp <= s.trigger_price_fp);
        }
        if (hit) out_fired_ids.push_back(s.stop_id);
    }
}

StopSlot* XtStopOrderBook::find(uint64_t stop_id) {
    for (StopSlot& s : slots_) {
        if (s.stop_id == stop_id) return &s;
    }
    return nullptr;
}

void XtStopOrderBook::on_fired(uint64_t stop_id, uint64_t order_id,
                               int64_t fired_qty_fp) {
    StopSlot* s = find(stop_id);
    if (!s || s->state != StopSlotState::Armed) return;
    s->state = StopSlotState::Fired;
    s->fired_order_id = order_id;
    s->fired_qty_fp = fired_qty_fp;
    ++s->fire_attempts;
}

void XtStopOrderBook::consume(uint64_t stop_id) {
    for (std::size_t i = 0; i < slots_.size(); ++i) {
        if (slots_[i].stop_id != stop_id) continue;
        erase_at_(i);
        return;
    }
}

StopTerminalAction XtStopOrderBook::on_order_terminal(uint64_t order_id,
                                                      int64_t remaining_fp) {
    if (order_id == 0) return StopTerminalAction::kIgnored;
    for (std::size_t i = 0; i < slots_.size(); ++i) {
        StopSlot& s = slots_[i];
        if (s.state != StopSlotState::Fired || s.fired_order_id != order_id) continue;

        if (remaining_fp <= 0) { // 本次 fire 足额成交
            erase_at_(i);
            return StopTerminalAction::kConsumed;
        }
        // 零/部分成交：登记量按实际成交递减后余量重挂。
        const int64_t filled_fp = s.fired_qty_fp > remaining_fp
                                      ? s.fired_qty_fp - remaining_fp
                                      : 0;
        s.qty_fp -= filled_fp;
        if (s.qty_fp <= 0) {
            erase_at_(i);
            return StopTerminalAction::kConsumed;
        }
        if (s.fire_attempts >= kMaxStopFireAttempts) {
            erase_at_(i);
            return StopTerminalAction::kExhausted;
        }
        s.state = StopSlotState::Armed;
        s.fired_order_id = 0;
        s.fired_qty_fp = 0;
        // 水位保留：re-arm 后 trailing 以最新水位继续，不重播种。
        return StopTerminalAction::kRearmed;
    }
    return StopTerminalAction::kIgnored;
}

std::size_t XtStopOrderBook::clear_for_code(const char* code) {
    if (!code) return 0;
    std::size_t cleared = 0;
    for (std::size_t i = 0; i < slots_.size();) {
        if (code_matches_(slots_[i], code)) {
            erase_at_(i);
            ++cleared;
        } else {
            ++i;
        }
    }
    return cleared;
}

void XtStopOrderBook::erase_at_(std::size_t idx) {
    slots_[idx] = slots_.back();
    slots_.pop_back();
}

} // namespace xt_strategy_engine
