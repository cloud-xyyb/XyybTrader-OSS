#include "xt_backtest/data/event_stream/XtBookReplayer.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <limits>
#include <stdexcept>

XtBookReplayer::XtBookReplayer(std::size_t max_levels, std::size_t warmup_events)
    : warmup_events_(warmup_events) {
    (void)max_levels;
}

void XtBookReplayer::process_event(std::string_view side,
                                   double price,
                                   double qty,
                                   std::string_view event_type,
                                   std::optional<int64_t> first_update_id,
                                   std::optional<int64_t> final_update_id,
                                   std::optional<int64_t> prev_final_update_id) {
    const bool is_snapshot = equals_ignore_case(event_type, "snapshot");
    const bool is_update = equals_ignore_case(event_type, "update");
    const bool is_clear = equals_ignore_case(event_type, "clear");

    if (is_snapshot) {
        process_snapshot_start(final_update_id);
    } else {
        snapshot_group_id_.reset();
    }

    if (is_clear) {

        clear_book();
        in_warmup_ = true;
        events_seen_ = 0;
        return;
    }

    if (!is_update && !is_snapshot) {
        throw std::invalid_argument("unsupported event_type");
    }

    apply_level(side, price, qty);
    ++events_seen_;

    if (is_snapshot || (in_warmup_ && events_seen_ >= warmup_events_)) {
        in_warmup_ = false;
    }
}

XtBookReplayer::L5Snapshot XtBookReplayer::l5_snapshot() const {
    L5Snapshot out{};
    const double nan = std::numeric_limits<double>::quiet_NaN();
    for (int i = 0; i < 5; ++i) {
        out.bid_prices[i] = nan;
        out.bid_qtys[i] = nan;
        out.ask_prices[i] = nan;
        out.ask_qtys[i] = nan;
    }

    std::pair<int64_t, double> top_bids[5];
    std::pair<int64_t, double> top_asks[5];
    int nb = 0;
    int na = 0;

    auto insert_top5_desc = [](std::pair<int64_t, double>* arr, int& n,
                                int64_t key, double qty) {
        if (n < 5) {
            int i = n;
            while (i > 0 && arr[i - 1].first < key) { arr[i] = arr[i - 1]; --i; }
            arr[i] = {key, qty};
            ++n;
        } else if (key > arr[4].first) {
            int i = 4;
            while (i > 0 && arr[i - 1].first < key) { arr[i] = arr[i - 1]; --i; }
            arr[i] = {key, qty};
        }
    };

    auto insert_top5_asc = [](std::pair<int64_t, double>* arr, int& n,
                               int64_t key, double qty) {
        if (n < 5) {
            int i = n;
            while (i > 0 && arr[i - 1].first > key) { arr[i] = arr[i - 1]; --i; }
            arr[i] = {key, qty};
            ++n;
        } else if (key < arr[4].first) {
            int i = 4;
            while (i > 0 && arr[i - 1].first > key) { arr[i] = arr[i - 1]; --i; }
            arr[i] = {key, qty};
        }
    };

    for (const auto& kv : bids_) {
        if (kv.second > 0.0) insert_top5_desc(top_bids, nb, kv.first, kv.second);
    }
    for (const auto& kv : asks_) {
        if (kv.second > 0.0) insert_top5_asc(top_asks, na, kv.first, kv.second);
    }

    for (int i = 0; i < nb; ++i) {
        out.bid_prices[i] = key_to_price(top_bids[i].first);
        out.bid_qtys[i] = top_bids[i].second;
    }
    for (int i = 0; i < na; ++i) {
        out.ask_prices[i] = key_to_price(top_asks[i].first);
        out.ask_qtys[i] = top_asks[i].second;
    }
    return out;
}

void XtBookReplayer::reset() {
    clear_book();
    in_warmup_ = true;
    events_seen_ = 0;
    snapshot_group_id_.reset();
}

int64_t XtBookReplayer::price_to_key(double price) const {
    return static_cast<int64_t>(std::llround(price * kPriceScale));
}

double XtBookReplayer::key_to_price(int64_t key) const {
    return static_cast<double>(key) / kPriceScale;
}

void XtBookReplayer::process_snapshot_start(std::optional<int64_t> final_update_id) {
    if (!final_update_id || snapshot_group_id_ != final_update_id) {
        clear_book();
        snapshot_group_id_ = final_update_id;
    }
}

void XtBookReplayer::apply_level(std::string_view side, double price, double qty) {

    const int64_t key = price_to_key(price);
    if (equals_ignore_case(side, "bid")) {
        if (qty <= 0.0) {
            bids_.erase(key);
        } else {
            bids_[key] = qty;
        }
        return;
    }
    if (equals_ignore_case(side, "ask")) {
        if (qty <= 0.0) {
            asks_.erase(key);
        } else {
            asks_[key] = qty;
        }
        return;
    }
    throw std::invalid_argument("unsupported side");
}

void XtBookReplayer::clear_book() {
    bids_.clear();
    asks_.clear();
}

bool XtBookReplayer::equals_ignore_case(std::string_view lhs, std::string_view rhs) {
    if (lhs.size() != rhs.size()) return false;
    for (std::size_t i = 0; i < lhs.size(); ++i) {
        const char a = static_cast<char>(std::tolower(static_cast<unsigned char>(lhs[i])));
        const char b = static_cast<char>(std::tolower(static_cast<unsigned char>(rhs[i])));
        if (a != b) return false;
    }
    return true;
}
