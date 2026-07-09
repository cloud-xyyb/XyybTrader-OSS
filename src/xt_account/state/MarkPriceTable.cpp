#include "xt_account/state/MarkPriceTable.h"

#include <algorithm>
#include <cassert>
#include <cstdio>

namespace xt_account {

void MarkPriceTable::reset() {
    if (frozen_.load(std::memory_order_acquire)) {
        return;
    }
    marks_.fill(0);
    has_mark_.fill(false);
    registry_.reset();
    tracked_codes_.clear();
    tracked_codes_compat_.clear();
}

void MarkPriceTable::pre_warm(const std::vector<std::string>& known_codes) {
    for (const auto& code : known_codes) {
        if (code.empty()) {
            continue;
        }
        const auto id = registry_.register_or_get(code.c_str());
        if (id == xt_core::kInvalidInstrumentId) {
            continue;
        }
        add_tracked_code_(code.c_str());
    }
}

void MarkPriceTable::pre_warm_one(std::string_view code) {
    if (code.empty()) return;

    const std::string buf(code);
    const auto id = registry_.register_or_get(buf.c_str());
    if (id == xt_core::kInvalidInstrumentId) return;
    add_tracked_code_(buf.c_str());
}

void MarkPriceTable::on_market_tick(const char* code, int64_t mark_price) {
    if (!code || code[0] == '\0') {
        return;
    }

    if (mark_price <= 0) {
        return;
    }
    auto id = registry_.id_of(code);
    if (id == xt_core::kInvalidInstrumentId) {

        if (frozen_.load(std::memory_order_acquire)) {
            if (warned_frozen_codes_.insert(code).second) {
                std::fprintf(stderr,
                             "[xt_account] WARN: MarkPriceTable::on_market_tick unknown code='%s' "
                             "after freeze(); skipping insert (repeats for this code suppressed)\n",
                             code);
            }
            assert(false && "MarkPriceTable hot-path insert after freeze");
            return;
        }
        id = registry_.register_or_get(code);
        if (id == xt_core::kInvalidInstrumentId) {
            return;
        }
        add_tracked_code_(code);
    }
    marks_[id] = mark_price;
    has_mark_[id] = true;
}

void MarkPriceTable::on_set_position(const char* code, int64_t mark_price_hint) {
    if (!code || code[0] == '\0') {
        return;
    }
    auto id = registry_.id_of(code);
    if (id == xt_core::kInvalidInstrumentId) {
        if (frozen_.load(std::memory_order_acquire)) {
            if (warned_frozen_codes_.insert(code).second) {
                std::fprintf(stderr,
                             "[xt_account] WARN: MarkPriceTable::on_set_position unknown code='%s' "
                             "after freeze(); skipping insert (repeats for this code suppressed)\n",
                             code);
            }
            assert(false && "MarkPriceTable hot-path insert after freeze");
            return;
        }
        id = registry_.register_or_get(code);
    }
    if (id == xt_core::kInvalidInstrumentId) {
        return;
    }
    add_tracked_code_(code);
    if (mark_price_hint > 0) {

        marks_[id] = mark_price_hint;
        has_mark_[id] = true;
    }
}

void MarkPriceTable::on_trade(const char* code) {
    if (!code || code[0] == '\0') {
        return;
    }
    auto id = registry_.id_of(code);
    if (id == xt_core::kInvalidInstrumentId) {
        if (frozen_.load(std::memory_order_acquire)) {
            if (warned_frozen_codes_.insert(code).second) {
                std::fprintf(stderr,
                             "[xt_account] WARN: MarkPriceTable::on_trade unknown code='%s' "
                             "after freeze(); skipping insert (repeats for this code suppressed)\n",
                             code);
            }
            assert(false && "MarkPriceTable hot-path insert after freeze");
            return;
        }
        id = registry_.register_or_get(code);
    }
    if (id == xt_core::kInvalidInstrumentId) {
        return;
    }
    add_tracked_code_(code);
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

void MarkPriceTable::add_tracked_code_(const char* code) {
    auto inserted = tracked_codes_compat_.insert(code);
    if (!inserted.second) {
        return;
    }

    tracked_codes_.push_back(*inserted.first);
    std::sort(tracked_codes_.begin(), tracked_codes_.end());
}

}
