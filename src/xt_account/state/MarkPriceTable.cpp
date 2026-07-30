#include "xt_account/state/MarkPriceTable.h"

#include <cstdio>
#include <string>

namespace xt_account {

void MarkPriceTable::reset() {
    if (frozen_.load(std::memory_order_acquire)) {
        return;
    }
    marks_.fill(0);
    has_mark_.fill(false);
    registry_.reset();
}

void MarkPriceTable::pre_warm_one(std::string_view code) {
    if (code.empty()) return;

    const std::string buf(code);
    registry_.register_or_get(buf.c_str());
}

bool MarkPriceTable::on_market_tick(const char* code, int64_t mark_price) {
    if (!code || code[0] == '\0') {
        return false;
    }

    if (mark_price <= 0) {
        return false;
    }
    auto id = registry_.id_of(code);
    if (id == xt_core::kInvalidInstrumentId) {

        if (frozen_.load(std::memory_order_acquire)) {
            warn_unknown_frozen_code_("on_market_tick", code);
            return false;
        }
        id = registry_.register_or_get(code);
        if (id == xt_core::kInvalidInstrumentId) {
            return false;
        }
    }
    marks_[id] = mark_price;
    has_mark_[id] = true;
    return true;
}

bool MarkPriceTable::on_set_position(const char* code, int64_t mark_price_hint) {
    if (!code || code[0] == '\0') {
        return false;
    }
    auto id = registry_.id_of(code);
    if (id == xt_core::kInvalidInstrumentId) {
        if (frozen_.load(std::memory_order_acquire)) {
            warn_unknown_frozen_code_("on_set_position", code);
            return false;
        }
        id = registry_.register_or_get(code);
    }
    if (id == xt_core::kInvalidInstrumentId) {
        return false;
    }
    if (mark_price_hint > 0) {

        marks_[id] = mark_price_hint;
        has_mark_[id] = true;
    }
    return true;
}

bool MarkPriceTable::on_trade(const char* code) {
    if (!code || code[0] == '\0') {
        return false;
    }
    auto id = registry_.id_of(code);
    if (id == xt_core::kInvalidInstrumentId) {
        if (frozen_.load(std::memory_order_acquire)) {
            warn_unknown_frozen_code_("on_trade", code);
            return false;
        }
        id = registry_.register_or_get(code);
    }
    return id != xt_core::kInvalidInstrumentId;
}

int64_t MarkPriceTable::mark_price(const char* code) const {
    if (!code || code[0] == '\0') {
        return 0;
    }
    const auto id = registry_.id_of(code);
    if (id == xt_core::kInvalidInstrumentId || !has_mark_[id]) {
        return 0;
    }
    return marks_[id];
}

void MarkPriceTable::warn_unknown_frozen_code_(const char* origin, const char* code) {
    if (warned_unknown_frozen_) {
        return;
    }
    warned_unknown_frozen_ = true;
    std::fprintf(stderr,
                 "[xt_account] WARN: MarkPriceTable::%s unknown code='%s' "
                 "after freeze(); skipping insert (further unknown-code warnings suppressed; "
                 "per-code naming lives in AccountStateStore quarantine)\n",
                 origin, code);
}

}
