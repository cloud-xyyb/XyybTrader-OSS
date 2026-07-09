#include "BinanceOrderBook.h"

#include <algorithm>
#include <cmath>

namespace binance_u {

int64_t BinanceOrderBook::price_to_key(double price) noexcept {

    return static_cast<int64_t>(std::llround(price * static_cast<double>(kPriceScale)));
}

double BinanceOrderBook::key_to_price(int64_t key) noexcept {
    return static_cast<double>(key) / static_cast<double>(kPriceScale);
}

template <typename LevelsMap>
void BinanceOrderBook::apply_updates(LevelsMap& levels,
                                     const std::vector<std::pair<double, double>>& updates) {

    for (const auto& [price, qty] : updates) {
        const int64_t key = price_to_key(price);
        if (qty > 0.0) {
            levels[key] = qty;
        } else {
            levels.erase(key);
        }
    }
}

void BinanceOrderBook::apply_snapshot(uint64_t lastUpdateId,
                                     const std::vector<std::pair<double, double>>& bids,
                                     const std::vector<std::pair<double, double>>& asks) {
    bids_.clear();
    asks_.clear();
    for (const auto& [price, qty] : bids) {
        if (qty > 0.0) bids_[price_to_key(price)] = qty;
    }
    for (const auto& [price, qty] : asks) {
        if (qty > 0.0) asks_[price_to_key(price)] = qty;
    }
    last_update_id_ = lastUpdateId;
    has_snapshot_ = true;
    first_after_snap_ = true;
    prev_diff_u_ = 0;
}

bool BinanceOrderBook::apply_diff(uint64_t U, uint64_t u, uint64_t pu,
                                  const std::vector<std::pair<double, double>>& bid_updates,
                                  const std::vector<std::pair<double, double>>& ask_updates) {
    if (!has_snapshot_) {

        return false;
    }

    if (first_after_snap_) {
        if (u < last_update_id_) return true;
        if (U > last_update_id_) return false;

        apply_updates(bids_, bid_updates);
        apply_updates(asks_, ask_updates);
        last_update_id_ = u;
        prev_diff_u_ = u;
        first_after_snap_ = false;
        return true;
    }
    if (pu != prev_diff_u_) return false;
    apply_updates(bids_, bid_updates);
    apply_updates(asks_, ask_updates);
    last_update_id_ = u;
    prev_diff_u_ = u;
    return true;
}

void BinanceOrderBook::top_n(std::size_t n,
                             double* out_bid_prices, double* out_bid_qtys,
                             double* out_ask_prices, double* out_ask_qtys) const {

    std::size_t i = 0;
    for (auto it = bids_.begin(); it != bids_.end() && i < n; ++it, ++i) {
        out_bid_prices[i] = key_to_price(it->first);
        out_bid_qtys[i]   = it->second;
    }
    for (; i < n; ++i) {
        out_bid_prices[i] = 0.0;
        out_bid_qtys[i]   = 0.0;
    }

    i = 0;
    for (auto it = asks_.begin(); it != asks_.end() && i < n; ++it, ++i) {
        out_ask_prices[i] = key_to_price(it->first);
        out_ask_qtys[i]   = it->second;
    }
    for (; i < n; ++i) {
        out_ask_prices[i] = 0.0;
        out_ask_qtys[i]   = 0.0;
    }
}

void BinanceOrderBook::reset() noexcept {
    bids_.clear();
    asks_.clear();
    last_update_id_ = 0;
    prev_diff_u_ = 0;
    has_snapshot_ = false;
    first_after_snap_ = true;
}

template void BinanceOrderBook::apply_updates<std::map<int64_t, double, std::greater<int64_t>>>(
    std::map<int64_t, double, std::greater<int64_t>>&,
    const std::vector<std::pair<double, double>>&);
template void BinanceOrderBook::apply_updates<std::map<int64_t, double>>(
    std::map<int64_t, double>&,
    const std::vector<std::pair<double, double>>&);

}
