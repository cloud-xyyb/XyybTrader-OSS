#include "state/AccountState.h"

#include "XtFixedPoint.h"

#include <algorithm>

namespace {

inline void release_order_frozen(int64_t& frozen_cash,
                                 int64_t& frozen_by_orders,
                                 AccountState::FrozenOrderSlot& slot,
                                 int64_t release) {
    if (release <= 0 || slot.frozen <= 0) return;
    release = std::min(release, slot.frozen);
    frozen_cash -= release;
    if (frozen_cash < 0) frozen_cash = 0;
    frozen_by_orders -= release;
    slot.frozen -= release;
    if (slot.frozen < 0) slot.frozen = 0;
}

}

AccountState::AccountState(const Config& cfg, CtorToken)
    : cash_(xt_fp::to_fp(cfg.initial_cash)),
      t1_enabled_(cfg.t1_enabled),
      futures_mode_(cfg.futures_mode),
      catalog_(cfg.catalog) {}

AccountState::Position& AccountState::get_pos(const char* code) {
    if (__builtin_expect(!code || code[0] == '\0', 0)) {
        static Position empty{};
        return empty;
    }
    return pos_[code];
}

const AccountState::Position* AccountState::find_pos(const char* code) const {
    if (!code || code[0] == '\0') return nullptr;
    auto it = pos_.find(code);
    if (it == pos_.end()) return nullptr;
    return &it->second;
}

void AccountState::on_session_begin(uint32_t action_date) {
    last_action_date_ = action_date;

    if (!t1_enabled_ && !futures_mode_) return;
    for (auto& kv : pos_) {
        kv.second.long_today  = 0;
        kv.second.short_today = 0;
    }
}

void AccountState::reset_positions() {

    frozen_cash_ = 0;
    frozen_by_orders_ = 0;
    pos_.clear();
    order_slots_.clear();
}

void AccountState::set_cash(int64_t cash) {
    cash_ = cash;
}

void AccountState::set_position(const char* code,
                           int64_t long_qty,
                           int64_t long_avg_price,
                           int64_t short_qty,
                           int64_t short_avg_price,
                           int64_t long_today,
                           int64_t short_today,
                           int64_t broker_long_frozen_fp,
                           int64_t broker_short_frozen_fp,
                           int64_t broker_frozen_last_update_ms) {
    Position& p = get_pos(code);

    p.long_qty        = long_qty;
    p.long_avg_price  = long_avg_price;
    p.short_qty       = short_qty;
    p.short_avg_price = short_avg_price;
    p.long_today  = (long_today  > 0) ? std::min(long_today,  long_qty)  : 0;
    p.short_today = (short_today > 0) ? std::min(short_today, short_qty) : 0;

    p.broker_long_frozen_fp  = broker_long_frozen_fp  > 0 ? broker_long_frozen_fp  : 0;
    p.broker_short_frozen_fp = broker_short_frozen_fp > 0 ? broker_short_frozen_fp : 0;

    p.broker_frozen_last_update_ms = broker_frozen_last_update_ms;
}

void AccountState::on_order_accepted(uint64_t order_id, int64_t required_margin, int64_t per_unit_margin) {
    if (order_id == 0 || required_margin <= 0) return;

    frozen_cash_ += required_margin;

    auto it = order_slots_.find(order_id);
    if (it != order_slots_.end()) frozen_by_orders_ -= it->second.frozen;
    frozen_by_orders_ += required_margin;
    order_slots_[order_id] = {required_margin, 0, per_unit_margin};
}

void AccountState::on_order_update(uint64_t order_id, int64_t filled, int64_t remaining, bool is_final) {
    if (order_id == 0) return;
    auto it = order_slots_.find(order_id);

    if (it == order_slots_.end()) return;

    FrozenOrderSlot& slot = it->second;

    const int64_t filled_delta = filled - slot.filled;
    if (filled_delta > 0) {
        const int64_t release = xt_fp::mul(slot.per_unit, filled_delta);
        release_order_frozen(frozen_cash_, frozen_by_orders_, slot, release);
        slot.filled = filled;
    }
    if (!is_final) return;

    (void)remaining;
    release_order_frozen(frozen_cash_, frozen_by_orders_, slot, slot.frozen);
    order_slots_.erase(it);
}

void AccountState::on_daily_settlement(int64_t realized_delta, int64_t settlement_fee) {

    realized_pnl_ += realized_delta;
    cash_ += realized_delta;
    cash_ -= settlement_fee;
    fees_ += settlement_fee;
}

void AccountState::on_cash_transfer(int64_t amount) {
    cash_ += amount;
}

[[gnu::always_inline]] static int64_t apply_close(int64_t qty, int64_t price,
                            bool is_close, bool is_today,
                            int64_t& side_qty, int64_t& side_today, int64_t& side_avg,
                            bool is_long_side, int64_t& realized_pnl,
                            int64_t vol_scale_fp) {
    if (side_qty <= 0) return 0;

    const int64_t yesterday = std::max<int64_t>(0, side_qty - side_today);
    const int64_t can_close = (is_close && is_today && side_today > 0) ? side_today : side_qty;
    const int64_t close_qty = std::min(qty, can_close);

    const int64_t price_diff_pnl = is_long_side
        ? xt_fp::mul(price - side_avg, close_qty)
        : xt_fp::mul(side_avg - price, close_qty);
    realized_pnl += xt_fp::mul(price_diff_pnl, vol_scale_fp);

    side_qty -= close_qty;
    if (side_qty <= 0) {
        side_qty   = 0;
        side_avg   = 0;
        side_today = 0;
    } else if (side_today > 0) {
        const int64_t close_today = is_today
            ? close_qty
            : std::max<int64_t>(0, close_qty - yesterday);
        side_today -= std::min(close_today, side_today);
    }
    return close_qty;
}

[[gnu::always_inline]] static void apply_open(int64_t open_qty, int64_t price, bool track_today,
                       int64_t& side_qty, int64_t& side_today, int64_t& side_avg) {
    const int64_t new_qty = side_qty + open_qty;
    side_avg = (new_qty > 0)
        ? xt_fp::div(xt_fp::mul(side_avg, side_qty) + xt_fp::mul(price, open_qty), new_qty)
        : 0;
    side_qty = new_qty;
    if (track_today) side_today += open_qty;
}

void AccountState::on_trade_ex(const char* code,
                                bool is_buy, int64_t qty, int64_t price, int64_t fee,
                                bool is_close, bool is_today) {
    Position&     pos         = get_pos(code);
    const int64_t pnl_before  = realized_pnl_;
    const bool    track_today = t1_enabled_ || futures_mode_;

    int64_t vol_scale_fp = xt_fp::kScale;
    if (futures_mode_ && catalog_) {
        const XtCommodityInfo* info = catalog_->find(code);
        if (info && info->vol_scale > 0.0)
            vol_scale_fp = xt_fp::to_fp(info->vol_scale);
    }

    if (is_buy) {

        const int64_t close_qty = apply_close(qty, price, is_close, is_today,
                                               pos.short_qty, pos.short_today, pos.short_avg_price,
                                                false, realized_pnl_,
                                               vol_scale_fp);
        if (!is_close && close_qty < qty)
            apply_open(qty - close_qty, price, track_today,
                       pos.long_qty, pos.long_today, pos.long_avg_price);
        if (!futures_mode_) cash_ -= xt_fp::mul(price, qty);
    } else {

        const int64_t close_qty = apply_close(qty, price, is_close, is_today,
                                               pos.long_qty, pos.long_today, pos.long_avg_price,
                                                true, realized_pnl_,
                                               vol_scale_fp);
        if (!is_close && close_qty < qty)
            apply_open(qty - close_qty, price, track_today,
                       pos.short_qty, pos.short_today, pos.short_avg_price);
        if (!futures_mode_) cash_ += xt_fp::mul(price, qty);
    }

    if (futures_mode_) cash_ += realized_pnl_ - pnl_before;
    cash_ -= fee;
    fees_ += fee;
}

int64_t AccountState::cash() const { return cash_; }
int64_t AccountState::frozen_cash() const { return frozen_cash_; }
int64_t AccountState::frozen_by_orders() const { return frozen_by_orders_; }
int64_t AccountState::available_cash() const { return cash_ - frozen_cash_; }

int64_t AccountState::realized_pnl() const { return realized_pnl_; }
int64_t AccountState::fees() const { return fees_; }
bool AccountState::futures_mode() const { return futures_mode_; }
const XtCommodityCatalog* AccountState::commodity_catalog() const { return catalog_; }

const std::unordered_map<uint64_t, AccountState::FrozenOrderSlot>& AccountState::order_slots() const {
    return order_slots_;
}

int64_t AccountState::avg_price_of_(const Position& p) {
    if (p.long_qty > 0 && p.short_qty == 0) return p.long_avg_price;
    if (p.short_qty > 0 && p.long_qty == 0) return p.short_avg_price;
    if (p.long_qty > 0 && p.short_qty > 0) {
        const int64_t net = p.long_qty - p.short_qty;
        if (net > 0) return p.long_avg_price;
        if (net < 0) return p.short_avg_price;
    }
    return 0;
}

int64_t AccountState::available_sell_of_(const Position& p, bool is_today, bool t1_enabled) {
    int64_t available = 0;
    if (is_today) {
        available = p.long_today;
    } else {
        available = p.long_qty;
        if (t1_enabled) available -= p.long_today;
    }
    return available < 0 ? 0 : available;
}

int64_t AccountState::available_cover_of_(const Position& p, bool is_today, bool t1_enabled) {
    int64_t available = 0;
    if (is_today) {
        available = p.short_today;
    } else {
        available = p.short_qty;
        if (t1_enabled) available -= p.short_today;
    }
    return available < 0 ? 0 : available;
}

AccountState::PositionReadout AccountState::position_readout(const char* code) const {
    PositionReadout out;
    const Position* p = find_pos(code);
    if (!p) return out;
    out.qty = p->long_qty - p->short_qty;
    out.avg_price = avg_price_of_(*p);
    out.long_qty = p->long_qty;
    out.long_avg_price = p->long_avg_price;
    out.short_qty = p->short_qty;
    out.short_avg_price = p->short_avg_price;
    out.long_today_qty = p->long_today;
    out.short_today_qty = p->short_today;
    out.broker_long_frozen_fp = p->broker_long_frozen_fp;
    out.broker_short_frozen_fp = p->broker_short_frozen_fp;
    out.broker_frozen_last_update_ms = p->broker_frozen_last_update_ms;
    out.available_sell_today_qty = available_sell_of_(*p,  true, t1_enabled_);
    out.available_sell_total_qty = available_sell_of_(*p,  false, t1_enabled_);
    out.available_cover_today_qty = available_cover_of_(*p,  true, t1_enabled_);
    out.available_cover_total_qty = available_cover_of_(*p,  false, t1_enabled_);
    return out;
}

bool AccountState::has_today_split() const {
    return t1_enabled_ || futures_mode_;
}
