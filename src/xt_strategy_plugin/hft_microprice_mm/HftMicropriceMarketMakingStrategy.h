#pragma once

/// @file
/// @brief 基于 L1 microprice 与 inventory skew 的双边做市策略。
///
/// 策略在 engine owner thread 上维护每侧最多一张 GTC 报价。
/// 撤单后必须等待订单终态才允许同侧重挂；Risk/STP 仍由执行层负责。
///
/// @warning 净仓记账 + `XtOffset::Open`，educational backtest 专用，不得直接改 live profile；
/// cancel-pending 侧的解锁完全依赖订单终态交付（backtest 撤单必产终态；live 下若撤单丢失
/// 需引擎 lost-terminal 对账兜底，本策略不自行重撤）。

#include "api/IStrategyContext.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_strategy_engine/XtStrategyBase.h"
#include "xt_strategy_engine/XtStrategyInitConfig.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>

class HftMicropriceMarketMakingStrategy final : public XtStrategyBase {
public:
    void init(const XtStrategyInitConfig& cfg) override {
        symbol_                 = cfg.get_string("symbol", "");
        tick_size_fp_           = cfg.get_int("tick_size_fp", 0);
        quote_width_ticks_      = cfg.get_int("quote_width_ticks", 1);
        inventory_skew_ticks_   = cfg.get_int("inventory_skew_ticks", 1);
        requote_ticks_          = cfg.get_int("requote_ticks", 1);
        quote_throttle_ms_      = cfg.get_int("quote_throttle_ms", 100);
        max_spread_ticks_       = cfg.get_int("max_spread_ticks", 10);

        const bool qty_ok = positive_fp_from_double_(
            cfg.get_double("quote_qty", 0.0), &quote_qty_fp_);
        const bool max_pos_ok = positive_fp_from_double_(
            cfg.get_double("max_position", 0.0), &max_position_fp_);

        config_valid_ = !symbol_.empty() && qty_ok && max_pos_ok &&
                        max_position_fp_ >= quote_qty_fp_ &&
                        tick_size_fp_ > 0 && quote_width_ticks_ > 0 &&
                        inventory_skew_ticks_ >= 0 && requote_ticks_ > 0 &&
                        quote_throttle_ms_ >= 0 && max_spread_ticks_ > 0;
    }

    void on_init(IStrategyContext* ctx) override {
        if (!ctx) return;
        if (!config_valid_) {
            ctx->log_warn("hft_microprice_mm invalid config; strategy remains fail-closed");
            return;
        }
        ctx->sub_tick(symbol_.c_str());
        ctx->declare_position_interest(symbol_.c_str());
    }

    uint64_t capabilities() const noexcept override {
        return to_capability_bit(StrategyCapability::SubscribesTick) |
               to_capability_bit(StrategyCapability::RequiresOnOrder);
    }

    void on_tick(IStrategyContext* ctx, const XtTick& tick) override {
        if (!ctx || !config_valid_) return;

        if (!pending_symbol_.empty()) {
            cancel_quotes_(ctx);
            return;
        }
        if (std::strcmp(tick.code, symbol_.c_str()) != 0) return;

        if (!market_is_quotable_(tick) || !ctx->can_submit()) {
            cancel_quotes_(ctx);
            return;
        }
        if (!decision_interval_elapsed_(tick)) return;

        const __int128 confirmed_wide =
            static_cast<__int128>(ctx->get_position_fp(symbol_.c_str())) -
            static_cast<__int128>(ctx->projected_inflight_fp(symbol_.c_str()));
        int64_t confirmed_fp = 0;
        if (!narrow_i64_(confirmed_wide, &confirmed_fp)) {

            cancel_quotes_(ctx);
            mark_decision_time_(tick);
            has_last_confirmed_ = false;
            return;
        }

        int64_t target_bid_fp = 0;
        int64_t target_ask_fp = 0;
        if (!compute_targets_(tick, confirmed_fp, &target_bid_fp, &target_ask_fp)) {

            cancel_quotes_(ctx);
            mark_decision_(tick, confirmed_fp);
            return;
        }

        const bool bid_drift = quote_drifted_(bid_quote_, target_bid_fp);
        const bool ask_drift = quote_drifted_(ask_quote_, target_ask_fp);
        if (bid_drift) request_cancel_(ctx, bid_quote_);
        if (ask_drift) request_cancel_(ctx, ask_quote_);

        const bool position_changed =
            !has_last_confirmed_ || confirmed_fp != last_confirmed_fp_;
        const bool may_submit = !bid_quote_.active() || !ask_quote_.active();
        if (!may_submit && !position_changed) {
            mark_decision_(tick, confirmed_fp);
            return;
        }

        ExposureSnapshot exposure;
        if (!read_exposure_(ctx, &exposure)) {

            cancel_quotes_(ctx);
            mark_decision_(tick, confirmed_fp);
            return;
        }

        const __int128 cap = static_cast<__int128>(max_position_fp_);
        const __int128 confirmed = static_cast<__int128>(confirmed_fp);
        const bool existing_bid_safe = confirmed + exposure.buy_leaves_fp <= cap;
        const bool existing_ask_safe = confirmed - exposure.sell_leaves_fp >= -cap;
        if (bid_quote_.active() && !existing_bid_safe) request_cancel_(ctx, bid_quote_);
        if (ask_quote_.active() && !existing_ask_safe) request_cancel_(ctx, ask_quote_);

        const bool allow_new_bid =
            confirmed + exposure.buy_leaves_fp + quote_qty_fp_ <= cap;
        const bool allow_new_ask =
            confirmed - exposure.sell_leaves_fp - quote_qty_fp_ >= -cap;

        if (!bid_quote_.active() && allow_new_bid &&
            !would_cross_opposite_(true, target_bid_fp)) {
            submit_quote_(ctx, bid_quote_, true, target_bid_fp);
        }
        if (!ask_quote_.active() && allow_new_ask &&
            !would_cross_opposite_(false, target_ask_fp)) {
            submit_quote_(ctx, ask_quote_, false, target_ask_fp);
        }

        mark_decision_(tick, confirmed_fp);
    }

    void on_order(IStrategyContext* ctx, const XtOrderExecUpdate& update) override {
        if (!ctx) return;
        apply_terminal_(bid_quote_, update);
        apply_terminal_(ask_quote_, update);
        activate_pending_symbol_(ctx);
    }

    void on_session_end(IStrategyContext* ctx, uint32_t) override {
        if (ctx) cancel_quotes_(ctx);
    }

    void on_channel_lost(IStrategyContext* ctx, const char*, int) override {
        if (ctx) cancel_quotes_(ctx);
    }

    void on_roll(IStrategyContext* ctx, const char*, const char* new_code,
                 uint32_t, int64_t) override {
        if (!ctx || !new_code || new_code[0] == '\0') return;
        pending_symbol_ = new_code;
        cancel_quotes_(ctx);
        activate_pending_symbol_(ctx);
    }

private:
    struct QuoteState {
        uint64_t order_id = 0;
        int64_t price_fp = 0;
        bool cancel_pending = false;

        [[nodiscard]] bool active() const noexcept { return order_id != 0; }
    };

    struct ExposureSnapshot {
        __int128 buy_leaves_fp = 0;
        __int128 sell_leaves_fp = 0;
    };

    static constexpr size_t kOpenOrderCapacity = 64;

    static bool positive_fp_from_double_(double value, int64_t* out) noexcept {
        if (!out || !std::isfinite(value) || value <= 0.0) return false;
        const double scaled = value * static_cast<double>(kOrderExecFpScale);
        if (!std::isfinite(scaled) || scaled < 1.0 ||
            scaled >= static_cast<double>(std::numeric_limits<int64_t>::max())) {
            return false;
        }
        *out = static_cast<int64_t>(scaled);
        return *out > 0;
    }

    static bool narrow_i64_(__int128 value, int64_t* out) noexcept {

        if (!out || value < std::numeric_limits<int64_t>::min() ||
            value > std::numeric_limits<int64_t>::max()) {
            return false;
        }
        *out = static_cast<int64_t>(value);
        return true;
    }

    static bool action_time_ms_(uint32_t action_time, uint32_t* out) noexcept {
        if (!out) return false;
        const uint32_t hh = action_time / 10'000'000U;
        const uint32_t mm = (action_time / 100'000U) % 100U;
        const uint32_t ss = (action_time / 1'000U) % 100U;
        const uint32_t ms = action_time % 1'000U;
        if (hh >= 24 || mm >= 60 || ss >= 60) return false;
        *out = hh * 3'600'000U + mm * 60'000U + ss * 1'000U + ms;
        return true;
    }

    [[nodiscard]] bool market_is_quotable_(const XtTick& tick) const noexcept {

        const int64_t bid = tick.bid_price_fp[0];
        const int64_t ask = tick.ask_price_fp[0];
        const int64_t bid_qty = tick.bid_qty_fp[0];
        const int64_t ask_qty = tick.ask_qty_fp[0];
        if (bid <= 0 || ask <= bid || bid_qty <= 0 || ask_qty <= 0) return false;
        const __int128 spread = static_cast<__int128>(ask) - bid;
        return spread <= static_cast<__int128>(max_spread_ticks_) * tick_size_fp_;
    }

    [[nodiscard]] bool decision_interval_elapsed_(const XtTick& tick) const noexcept {
        if (quote_throttle_ms_ == 0 || !has_last_decision_) return true;
        if (tick.action_date != last_decision_date_) return true;

        uint32_t previous_ms = 0;
        uint32_t current_ms = 0;

        if (!action_time_ms_(last_decision_time_, &previous_ms) ||
            !action_time_ms_(tick.action_time, &current_ms) ||
            current_ms < previous_ms) {
            return false;
        }
        return current_ms - previous_ms >= static_cast<uint32_t>(quote_throttle_ms_);
    }

    [[nodiscard]] bool compute_targets_(const XtTick& tick, int64_t confirmed_fp,
                                        int64_t* out_bid_fp,
                                        int64_t* out_ask_fp) const noexcept {
        const __int128 bid = tick.bid_price_fp[0];
        const __int128 ask = tick.ask_price_fp[0];
        const __int128 bid_qty = tick.bid_qty_fp[0];
        const __int128 ask_qty = tick.ask_qty_fp[0];
        const __int128 denominator = bid_qty + ask_qty;
        if (denominator <= 0) return false;

        const __int128 microprice =
            bid + ((ask - bid) * bid_qty) / denominator;

        const __int128 inventory_units =
            static_cast<__int128>(confirmed_fp) / quote_qty_fp_;
        const __int128 inventory_skew =
            inventory_units * inventory_skew_ticks_ * tick_size_fp_;
        const __int128 reservation = microprice - inventory_skew;
        const __int128 quote_width =
            static_cast<__int128>(quote_width_ticks_) * tick_size_fp_;

        const __int128 raw_bid = reservation - quote_width;
        const __int128 raw_ask = reservation + quote_width;
        if (raw_bid <= 0 || raw_ask <= 0) return false;

        __int128 rounded_bid = (raw_bid / tick_size_fp_) * tick_size_fp_;
        __int128 rounded_ask =
            ((raw_ask + tick_size_fp_ - 1) / tick_size_fp_) * tick_size_fp_;
        rounded_bid = std::min(rounded_bid, bid);
        rounded_ask = std::max(rounded_ask, ask);
        if (rounded_bid <= 0 || rounded_ask <= rounded_bid) return false;

        return narrow_i64_(rounded_bid, out_bid_fp) &&
               narrow_i64_(rounded_ask, out_ask_fp);
    }

    [[nodiscard]] bool quote_drifted_(const QuoteState& quote,
                                      int64_t target_price_fp) const noexcept {

        if (!quote.active() || quote.cancel_pending) return false;
        __int128 diff = static_cast<__int128>(quote.price_fp) - target_price_fp;
        if (diff < 0) diff = -diff;
        return diff >= static_cast<__int128>(requote_ticks_) * tick_size_fp_;
    }

    [[nodiscard]] bool read_exposure_(IStrategyContext* ctx,
                                      ExposureSnapshot* out) const {
        if (!ctx || !out) return false;
        XtOrderExecUpdate open_orders[kOpenOrderCapacity];
        const size_t count = ctx->get_open_orders(
            symbol_.c_str(), open_orders, kOpenOrderCapacity);

        if (count >= kOpenOrderCapacity) return false;

        out->buy_leaves_fp = 0;
        out->sell_leaves_fp = 0;
        for (size_t i = 0; i < count; ++i) {
            if (open_orders[i].leaves_fp <= 0) continue;
            if (open_orders[i].is_buy) {
                out->buy_leaves_fp += open_orders[i].leaves_fp;
            } else {
                out->sell_leaves_fp += open_orders[i].leaves_fp;
            }
        }
        return true;
    }

    [[nodiscard]] bool would_cross_opposite_(bool is_buy,
                                              int64_t price_fp) const noexcept {

        const QuoteState& opposite = is_buy ? ask_quote_ : bid_quote_;
        if (!opposite.active()) return false;
        return is_buy ? price_fp >= opposite.price_fp
                      : price_fp <= opposite.price_fp;
    }

    static void request_cancel_(IStrategyContext* ctx, QuoteState& quote) {
        if (!ctx || !quote.active() || quote.cancel_pending) return;
        ctx->cancel(quote.order_id);

        quote.cancel_pending = true;
    }

    void cancel_quotes_(IStrategyContext* ctx) {
        request_cancel_(ctx, bid_quote_);
        request_cancel_(ctx, ask_quote_);
    }

    void submit_quote_(IStrategyContext* ctx, QuoteState& quote,
                       bool is_buy, int64_t price_fp) {
        if (!ctx || quote.active()) return;
        ctx->set_next_order_reason(
            is_buy ? "microprice_mm|side=bid" : "microprice_mm|side=ask");
        const uint64_t order_id = is_buy
            ? ctx->buy_fp(symbol_.c_str(), price_fp, quote_qty_fp_,
                          IStrategyContext::TIF_GTC, "microprice_bid", XtOffset::Open)
            : ctx->sell_fp(symbol_.c_str(), price_fp, quote_qty_fp_,
                           IStrategyContext::TIF_GTC, "microprice_ask", XtOffset::Open);

        if (order_id == 0) return;
        quote.order_id = order_id;
        quote.price_fp = price_fp;
        quote.cancel_pending = false;
    }

    static void apply_terminal_(QuoteState& quote,
                                const XtOrderExecUpdate& update) noexcept {

        if (quote.order_id == update.id && order_status_is_final(update.status)) {
            quote = {};
        }
    }

    void activate_pending_symbol_(IStrategyContext* ctx) {
        if (!ctx || pending_symbol_.empty() ||
            bid_quote_.active() || ask_quote_.active()) {
            return;
        }
        symbol_ = pending_symbol_;
        pending_symbol_.clear();
        has_last_decision_ = false;
        has_last_confirmed_ = false;

        ctx->sub_tick(symbol_.c_str());
        ctx->declare_position_interest(symbol_.c_str());
    }

    void mark_decision_time_(const XtTick& tick) noexcept {

        uint32_t anchor_ms = 0;
        if (!action_time_ms_(tick.action_time, &anchor_ms)) return;
        last_decision_date_ = tick.action_date;
        last_decision_time_ = tick.action_time;
        has_last_decision_ = true;
    }

    void mark_decision_(const XtTick& tick, int64_t confirmed_fp) noexcept {
        mark_decision_time_(tick);
        last_confirmed_fp_ = confirmed_fp;
        has_last_confirmed_ = true;
    }

    std::string symbol_;
    std::string pending_symbol_;

    int64_t quote_qty_fp_ = 0;

    int64_t max_position_fp_ = 0;
    int64_t tick_size_fp_ = 0;
    int quote_width_ticks_ = 1;

    int inventory_skew_ticks_ = 1;
    int requote_ticks_ = 1;

    int quote_throttle_ms_ = 100;
    int max_spread_ticks_ = 10;
    bool config_valid_ = false;

    QuoteState bid_quote_;
    QuoteState ask_quote_;

    uint32_t last_decision_date_ = 0;
    uint32_t last_decision_time_ = 0;

    int64_t last_confirmed_fp_ = 0;
    bool has_last_decision_ = false;
    bool has_last_confirmed_ = false;
};
