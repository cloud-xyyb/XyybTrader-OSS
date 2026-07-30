#pragma once

#include "xt_strategy_engine/XtStrategyBase.h"
#include "xt_strategy_engine/XtStrategyInitConfig.h"
#include "xt_strategy_engine/util/XtRolloverHelper.h"
#include "api/IStrategyContext.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtOrderTypes.h"

#include <cstring>
#include <cstdio>
#include <string>
#include <unordered_map>
#include <unordered_set>

class HftMarketMakingStrategy : public XtStrategyBase {
public:
    void init(const XtStrategyInitConfig& cfg) override {
        symbol_              = cfg.get_string("symbol", "");
        qty_                 = cfg.get_double("qty", 0.001);
        bid_offset_ticks_    = cfg.get_int   ("bid_offset_ticks",    1);
        ask_offset_ticks_    = cfg.get_int   ("ask_offset_ticks",    1);
        requote_ticks_       = cfg.get_int   ("requote_ticks",       2);
        quote_throttle_ms_   = cfg.get_int   ("quote_throttle_ms",   500);
        max_position_        = cfg.get_double("max_position",        0.01);
        inventory_target_    = cfg.get_double("inventory_target",    0.0);
        max_spread_ticks_    = cfg.get_int   ("max_spread_ticks",    100);

        tick_size_fp_        = cfg.get_int   ("tick_size_fp",        100);

        if (tick_size_fp_ <= 0) tick_size_fp_ = 100;
        debug_first_quotes_  = cfg.get_int   ("debug_first_quotes",  0);
    }

    void on_init(IStrategyContext* ctx) override {
        if (!symbol_.empty()) {
            ctx->sub_tick(symbol_.c_str());

            positions_.on_update(symbol_.c_str(),
                ctx->get_position_fp(symbol_.c_str()) - ctx->projected_inflight_fp(symbol_.c_str()));
        }
    }

    void on_params(IStrategyContext* ctx) override {
        if (!ctx) return;
        const double new_qty            = ctx->get_param_double("qty",              qty_);
        const int    new_bid_off        = static_cast<int>(ctx->get_param_int("bid_offset_ticks",  bid_offset_ticks_));
        const int    new_ask_off        = static_cast<int>(ctx->get_param_int("ask_offset_ticks",  ask_offset_ticks_));
        const int    new_requote        = static_cast<int>(ctx->get_param_int("requote_ticks",     requote_ticks_));
        const int    new_throttle       = static_cast<int>(ctx->get_param_int("quote_throttle_ms", quote_throttle_ms_));
        const double new_max_pos        = ctx->get_param_double("max_position",     max_position_);
        const double new_inv_target     = ctx->get_param_double("inventory_target", inventory_target_);
        const int    new_max_spread     = static_cast<int>(ctx->get_param_int("max_spread_ticks",  max_spread_ticks_));

        const double safe_qty       = (new_qty       > 0.0) ? new_qty       : qty_;
        const int    safe_bid       = (new_bid_off   >= 0)  ? new_bid_off   : bid_offset_ticks_;
        const int    safe_ask       = (new_ask_off   >= 0)  ? new_ask_off   : ask_offset_ticks_;
        const int    safe_requote   = (new_requote   >  0)  ? new_requote   : requote_ticks_;
        const int    safe_throttle  = (new_throttle  >= 0)  ? new_throttle  : quote_throttle_ms_;
        const double safe_max_pos   = (new_max_pos   >  0.0)? new_max_pos   : max_position_;
        const int    safe_max_spr   = (new_max_spread > 0)  ? new_max_spread: max_spread_ticks_;

        const bool changed =
            (safe_qty      != qty_)              ||
            (safe_bid      != bid_offset_ticks_) ||
            (safe_ask      != ask_offset_ticks_) ||
            (safe_requote  != requote_ticks_)    ||
            (safe_throttle != quote_throttle_ms_)||
            (safe_max_pos  != max_position_)     ||
            (new_inv_target!= inventory_target_) ||
            (safe_max_spr  != max_spread_ticks_);

        qty_                 = safe_qty;
        bid_offset_ticks_    = safe_bid;
        ask_offset_ticks_    = safe_ask;
        requote_ticks_       = safe_requote;
        quote_throttle_ms_   = safe_throttle;
        max_position_        = safe_max_pos;
        inventory_target_    = new_inv_target;
        max_spread_ticks_    = safe_max_spr;

        if (changed) {
            char buf[256];
            std::snprintf(buf, sizeof(buf),
                "hft_market_making on_params applied qty=%.6f bid_off=%d ask_off=%d "
                "requote=%d throttle=%dms max_pos=%.4f inv_tgt=%.4f max_spread=%d",
                qty_, bid_offset_ticks_, ask_offset_ticks_, requote_ticks_,
                quote_throttle_ms_, max_position_, inventory_target_, max_spread_ticks_);
            ctx->log_info(buf);
        }
    }

    uint64_t capabilities() const noexcept override {
        return to_capability_bit(StrategyCapability::SubscribesTick)
             | to_capability_bit(StrategyCapability::RequiresOnFill)
             | to_capability_bit(StrategyCapability::RequiresOnOrder);
    }

    void on_tick(IStrategyContext* ctx, const XtTick& tk) override {
        if (symbol_.empty() || std::strcmp(tk.code, symbol_.c_str()) != 0) return;

        sync_position_(ctx);
        if (!ctx->can_submit()) return;

        const int64_t best_bid = tk.bid_price_fp[0];
        const int64_t best_ask = tk.ask_price_fp[0];
        if (best_bid <= 0 || best_ask <= 0 || best_bid >= best_ask) return;

        const int64_t spread_ticks = (best_ask - best_bid) / tick_size_fp_;
        if (spread_ticks > max_spread_ticks_) return;

        if (quote_throttle_ms_ > 0 && last_quote_at_ != 0) {
            const int32_t delta_ms = action_time_delta_ms(last_quote_at_, tk.action_time);
            if (delta_ms < quote_throttle_ms_) return;
        }

        int64_t buy_leaves_fp = 0, sell_leaves_fp = 0;
        {
            XtOrderExecUpdate open_buf[64];
            const size_t n_open = ctx->get_open_orders(symbol_.c_str(), open_buf, 64);
            for (size_t i = 0; i < n_open; ++i) {
                if (open_buf[i].leaves_fp <= 0) continue;
                (open_buf[i].is_buy ? buy_leaves_fp : sell_leaves_fp)
                    += open_buf[i].leaves_fp;
            }
        }

        const double net_pos    = order_exec_from_fp(
            positions_.logical_fp() + buy_leaves_fp - sell_leaves_fp);
        const double diff       = net_pos - inventory_target_;
        int bid_skew = 0, ask_skew = 0;
        if      (diff >  qty_) bid_skew = 2;
        else if (diff < -qty_) ask_skew = 2;

        const double worst_long  = order_exec_from_fp(
            positions_.logical_fp() + buy_leaves_fp);
        const double worst_short = order_exec_from_fp(
            positions_.logical_fp() - sell_leaves_fp);
        const bool block_long   = (worst_long  >=  max_position_);
        const bool block_short  = (worst_short <= -max_position_);

        const int64_t target_bid_px = best_bid - static_cast<int64_t>(bid_offset_ticks_ + bid_skew) * tick_size_fp_;
        const int64_t target_ask_px = best_ask + static_cast<int64_t>(ask_offset_ticks_ + ask_skew) * tick_size_fp_;

        const int64_t bid_drift_ticks = std::abs(target_bid_px - last_bid_px_) / tick_size_fp_;
        const int64_t ask_drift_ticks = std::abs(target_ask_px - last_ask_px_) / tick_size_fp_;
        const bool need_requote =
            in_flight_oids_.empty()                                       ||
            (last_bid_px_ != 0 && bid_drift_ticks >= requote_ticks_)      ||
            (last_ask_px_ != 0 && ask_drift_ticks >= requote_ticks_);
        if (!need_requote) return;

        for (const auto oid : in_flight_oids_) ctx->cancel(oid);
        in_flight_oids_.clear();

        last_bid_px_ = 0;
        last_ask_px_ = 0;

        const int64_t qty_fp = order_exec_to_fp(qty_);
        char reason_buf[64];

        if (!block_long) {
            std::snprintf(reason_buf, sizeof(reason_buf),
                "mm|side=bid|skew=%d|inv=%.4f", bid_skew, net_pos);
            ctx->set_next_order_reason(reason_buf);
            const uint64_t bid_oid = xt_rollover::submit_and_track(
                ctx, orders_, positions_, symbol_.c_str(),  true,
                target_bid_px, qty_fp, IStrategyContext::TIF_GTC,
                "mm_bid", XtOffset::Open);
            if (bid_oid != 0) {
                in_flight_oids_.insert(bid_oid);
                last_bid_px_ = target_bid_px;
            }
        }
        if (!block_short) {
            std::snprintf(reason_buf, sizeof(reason_buf),
                "mm|side=ask|skew=%d|inv=%.4f", ask_skew, net_pos);
            ctx->set_next_order_reason(reason_buf);
            const uint64_t ask_oid = xt_rollover::submit_and_track(
                ctx, orders_, positions_, symbol_.c_str(),  false,
                target_ask_px, qty_fp, IStrategyContext::TIF_GTC,
                "mm_ask", XtOffset::Open);
            if (ask_oid != 0) {
                in_flight_oids_.insert(ask_oid);
                last_ask_px_ = target_ask_px;
            }
        }

        last_quote_at_ = tk.action_time;
        ++quotes_emitted_;

        if (debug_quotes_emitted_ < debug_first_quotes_) {
            char buf[384];
            std::snprintf(buf, sizeof(buf),
                "hft_market_making QUOTE#%d action_time=%u net_pos=%.4f "
                "best_bid=%.6f best_ask=%.6f bid_px=%.6f ask_px=%.6f "
                "skew=(%d,%d) block=(%c,%c)",
                quotes_emitted_, tk.action_time, net_pos,
                order_exec_from_fp(best_bid), order_exec_from_fp(best_ask),
                order_exec_from_fp(target_bid_px), order_exec_from_fp(target_ask_px),
                bid_skew, ask_skew, block_long ? 'Y' : 'N', block_short ? 'Y' : 'N');
            ctx->log_info(buf);
            ++debug_quotes_emitted_;
        }
    }

    void on_fill(IStrategyContext* ctx, const XtFillEvent&  ) override {
        sync_position_(ctx);
    }

    void on_order(IStrategyContext* ctx,
                  const XtOrderExecUpdate& upd) override {
        orders_.apply_update(upd);

        const bool is_terminal =
            upd.status == XtOrderStatus::Filled   ||
            upd.status == XtOrderStatus::Canceled ||
            upd.status == XtOrderStatus::Rejected;
        if (is_terminal) in_flight_oids_.erase(upd.id);

        if (debug_quotes_emitted_ <= debug_first_quotes_) {
            const char* status =
                (upd.filled_fp > 0)                       ? "FILLED" :
                (upd.status == XtOrderStatus::Canceled)   ? "CANCEL" :
                (upd.status == XtOrderStatus::Rejected)   ? "REJECT" : "UPDATE";
            char buf[256];
            std::snprintf(buf, sizeof(buf),
                "hft_market_making ORDRPT oid=%lu %s price=%.6f filled=%.6f side=%s",
                (unsigned long)upd.id, status,
                order_exec_from_fp(upd.price_fp),
                order_exec_from_fp(upd.filled_fp),
                upd.is_buy ? "BUY" : "SELL");
            ctx->log_info(buf);
        }
    }

    void on_roll(IStrategyContext* ctx, const char* old_code,
                 const char* new_code, uint32_t  ,
                 int64_t  ) override {
        xt_rollover::migrate_gtc_orders(ctx, orders_, old_code, new_code);
        positions_.touch(old_code);
        positions_.on_update(new_code,
            ctx->get_position_fp(new_code) - ctx->projected_inflight_fp(new_code));
        symbol_ = new_code;
        in_flight_oids_.clear();
        last_bid_px_ = 0;
        last_ask_px_ = 0;
        ctx->sub_tick(new_code);
    }

private:

    static uint32_t action_time_to_ms_of_day(uint32_t at) noexcept {
        const uint32_t hh = at / 10000000u;
        const uint32_t mm = (at / 100000u) % 100u;
        const uint32_t ss = (at / 1000u)   % 100u;
        const uint32_t ms = at             % 1000u;
        return hh * 3600000u + mm * 60000u + ss * 1000u + ms;
    }

    static int32_t action_time_delta_ms(uint32_t prev, uint32_t cur) noexcept {
        const int64_t cur_ms  = static_cast<int64_t>(action_time_to_ms_of_day(cur));
        const int64_t prev_ms = static_cast<int64_t>(action_time_to_ms_of_day(prev));
        int64_t delta = cur_ms - prev_ms;
        if (delta < 0) delta += 86'400'000;
        return static_cast<int32_t>(delta);
    }

    void sync_position_(IStrategyContext* ctx) {
        if (!ctx || symbol_.empty()) return;
        positions_.on_update(symbol_.c_str(),
            ctx->get_position_fp(symbol_.c_str()) - ctx->projected_inflight_fp(symbol_.c_str()));
    }

    std::string symbol_;
    xt_rollover::PositionAggregator   positions_;
    xt_rollover::WorkingOrderRegistry orders_;

    double  qty_                = 0.001;
    int     bid_offset_ticks_   = 1;
    int     ask_offset_ticks_   = 1;
    int     requote_ticks_      = 2;
    int     quote_throttle_ms_  = 500;
    double  max_position_       = 0.01;
    double  inventory_target_   = 0.0;
    int     max_spread_ticks_   = 100;
    int64_t tick_size_fp_       = 100;

    uint32_t last_quote_at_     = 0;
    int64_t  last_bid_px_       = 0;
    int64_t  last_ask_px_       = 0;
    std::unordered_set<uint64_t> in_flight_oids_;

    int debug_first_quotes_     = 0;
    int debug_quotes_emitted_   = 0;
    int quotes_emitted_         = 0;
};
