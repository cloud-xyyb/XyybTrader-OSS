#pragma once

#include "xt_strategy_engine/XtStrategyBase.h"
#include "xt_strategy_engine/XtStrategyInitConfig.h"
#include "xt_strategy_engine/util/XtRolloverHelper.h"
#include "xt_strategy_engine/util/XtNotionalSizing.h"
#include "api/IStrategyContext.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtOrderTypes.h"

#include <cstring>
#include <cstdio>
#include <string>
#include <unordered_map>
#include <unordered_set>

class HftTickRevertStrategy : public XtStrategyBase {
public:
    static int64_t decision_price_fp(const XtTick& tk) noexcept {
        if (tk.price_fp > 0) return tk.price_fp;
        const int64_t bid = tk.bid_price_fp[0];
        const int64_t ask = tk.ask_price_fp[0];
        if (bid > 0 && ask > 0) {
            return bid <= ask ? bid + (ask - bid) / 2
                              : ask + (bid - ask) / 2;
        }
        if (bid > 0) return bid;
        if (ask > 0) return ask;
        return 0;
    }

    void init(const XtStrategyInitConfig& cfg) override {
        symbol_       = cfg.get_string("symbol", "");
        tick_window_  = cfg.get_int   ("tick_window", 100);
        qty_          = cfg.get_double("qty",         1.0);
        use_ioc_      = cfg.get_int   ("use_ioc",     1) != 0;

        close_throttle_ms_ = cfg.get_int("close_throttle_ms", 200);

        close_reject_backoff_ms_ = cfg.get_int("close_reject_backoff_ms", 3000);

        close_expire_backoff_ms_ = cfg.get_int("close_expire_backoff_ms", 0);
        debug_first_ticks_ = cfg.get_int("debug_first_ticks", 0);
        debug_first_decisions_ = cfg.get_int("debug_first_decisions", 0);

        passive_depth_level_ = cfg.get_int("passive_depth_level", -1);

        maker_ttl_ms_ = cfg.get_int("maker_ttl_ms", 0);

        order_notional_usdt_ = cfg.get_double("order_notional_usdt", 0.0);
        qty_step_            = cfg.get_double("qty_step", 0.001);
        min_order_notional_  = cfg.get_double("min_order_notional", 0.0);
    }

    void on_init(IStrategyContext* ctx) override {
        if (!symbol_.empty()) {
            ctx->sub_tick(symbol_.c_str());

            positions_.on_update(symbol_.c_str(),
                ctx->get_position_fp(symbol_.c_str()) - ctx->projected_inflight_fp(symbol_.c_str()));
        }

        if (maker_ttl_ms_ > 0 && use_ioc_) {
            ctx->log_warn("hft_tick_revert maker_ttl_ms set but use_ioc=1 — TTL only applies to GTC maker mode (ignored)");
        }
    }

    void on_params(IStrategyContext* ctx) override {
        if (!ctx) return;

        const int new_tick_window = static_cast<int>(
            ctx->get_param_int("tick_window", static_cast<int64_t>(tick_window_)));
        const double new_qty =
            ctx->get_param_double("qty", qty_);
        const int new_close_throttle_ms = static_cast<int>(
            ctx->get_param_int("close_throttle_ms", static_cast<int64_t>(close_throttle_ms_)));
        const int new_passive_depth_level = static_cast<int>(
            ctx->get_param_int("passive_depth_level", static_cast<int64_t>(passive_depth_level_)));
        const int new_debug_first_ticks = static_cast<int>(
            ctx->get_param_int("debug_first_ticks", static_cast<int64_t>(debug_first_ticks_)));
        const int new_debug_first_decisions = static_cast<int>(
            ctx->get_param_int("debug_first_decisions", static_cast<int64_t>(debug_first_decisions_)));

        const int safe_tick_window = (new_tick_window > 0) ? new_tick_window : tick_window_;

        const double safe_qty = (new_qty > 0.0) ? new_qty : qty_;

        const int safe_throttle = (new_close_throttle_ms >= 0) ? new_close_throttle_ms : close_throttle_ms_;

        const int safe_passive =
            (new_passive_depth_level >= -1 && new_passive_depth_level <= 4)
                ? new_passive_depth_level : passive_depth_level_;

        const bool changed = (safe_tick_window != tick_window_) ||
                              (safe_qty != qty_) ||
                              (safe_throttle != close_throttle_ms_) ||
                              (safe_passive != passive_depth_level_) ||
                              (new_debug_first_ticks != debug_first_ticks_) ||
                              (new_debug_first_decisions != debug_first_decisions_);

        tick_window_           = safe_tick_window;
        qty_                   = safe_qty;
        close_throttle_ms_     = safe_throttle;
        passive_depth_level_   = safe_passive;
        debug_first_ticks_     = new_debug_first_ticks;
        debug_first_decisions_ = new_debug_first_decisions;

        if (changed) {
            char buf[256];
            std::snprintf(buf, sizeof(buf),
                          "hft_tick_revert on_params applied tick_window=%d qty=%.6f "
                          "close_throttle_ms=%d passive_depth_level=%d",
                          tick_window_, qty_, close_throttle_ms_, passive_depth_level_);
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

        sync_position_and_clear_backoff_(ctx);

        if (!ttl_orders_.empty()) {
            for (auto it = ttl_orders_.begin(); it != ttl_orders_.end();) {
                const int32_t age_ms = action_time_delta_ms(it->second, tk.action_time);

                if (age_ms >= maker_ttl_ms_ && age_ms < 86'000'000) {
                    ctx->cancel(it->first);

                    it = ttl_orders_.erase(it);
                } else {
                    ++it;
                }
            }
        }

        if (!ctx->can_submit()) return;

        ++count_;

        if (debug_tick_emitted_ < debug_first_ticks_) {
            char buf[256];
            std::snprintf(buf, sizeof(buf),
                          "hft_tick_revert tick#%d code=%s price=%.6f bid0=%.6f ask0=%.6f action_time=%u",
                          count_, tk.code,
                          order_exec_from_fp(tk.price_fp),
                          order_exec_from_fp(tk.bid_price_fp[0]),
                          order_exec_from_fp(tk.ask_price_fp[0]),
                          tk.action_time);
            ctx->log_info(buf);
            ++debug_tick_emitted_;
        }

        const int64_t ref_price_fp = decision_price_fp(tk);
        if (ref_price_fp <= 0) {
            count_ = 0;
            start_price_fp_ = 0;
            return;
        }

        if (count_ == 1) {
            start_price_fp_ = ref_price_fp;
            return;
        }

        if (count_ < tick_window_) return;

        const int64_t end_price_fp = ref_price_fp;
        const int64_t qty_fp       = order_exec_to_fp(qty_);

        const int64_t open_qty_fp = (order_notional_usdt_ > 0.0)
            ? order_exec_to_fp(xt_sizing::notional_to_qty(
                  order_notional_usdt_, order_exec_from_fp(end_price_fp), qty_step_,
                  min_order_notional_))
            : qty_fp;

        if (order_notional_usdt_ > 0.0 && open_qty_fp <= 0 && !notional_zero_warned_) {
            notional_zero_warned_ = true;
            char nzbuf[208];
            std::snprintf(nzbuf, sizeof(nzbuf),
                "hft_tick_revert NOTIONAL-SIZING produced qty=0 (order_notional_usdt=%.2f, "
                "qty_step=%.6f) — opens skipped; lower the target or check qty_step",
                order_notional_usdt_, qty_step_);
            ctx->log_warn(nzbuf);
        }

        const int64_t cur_pos      = positions_.logical_fp()
                                   + ctx->projected_inflight_fp(symbol_.c_str());

        xt_runtime::XtStraPositionDetail pos_det{};
        ctx->get_position_detail(symbol_.c_str(), &pos_det, 1);

        const int64_t real_long_fp  = order_exec_to_fp(pos_det.long_side.position);
        const int64_t real_short_fp = order_exec_to_fp(pos_det.short_side.position);
        int64_t long_leg_fp, short_leg_fp;
        if (real_long_fp > 0 && real_short_fp > 0) {
            long_leg_fp  = real_long_fp;
            short_leg_fp = real_short_fp;
        } else {
            long_leg_fp  = (cur_pos > 0) ?  cur_pos : 0;
            short_leg_fp = (cur_pos < 0) ? -cur_pos : 0;
        }

        const auto tif = use_ioc_ ? IStrategyContext::TIF_IOC : IStrategyContext::TIF_GTC;
        int64_t buy_px, sell_px;
        if (use_ioc_) {
            const XtTick* snap = ctx->get_last_tick(symbol_.c_str());
            buy_px  = (snap && snap->ask_price_fp[0] > 0) ? snap->ask_price_fp[0]
                     : (tk.ask_price_fp[0] > 0 ? tk.ask_price_fp[0] : end_price_fp);
            sell_px = (snap && snap->bid_price_fp[0] > 0) ? snap->bid_price_fp[0]
                     : (tk.bid_price_fp[0] > 0 ? tk.bid_price_fp[0] : end_price_fp);
        } else if (passive_depth_level_ >= 0 && passive_depth_level_ < 5) {

            const XtTick* snap = ctx->get_last_tick(symbol_.c_str());
            const int64_t bid_lv = (snap ? snap->bid_price_fp[passive_depth_level_] : 0);
            const int64_t ask_lv = (snap ? snap->ask_price_fp[passive_depth_level_] : 0);
            buy_px  = bid_lv > 0 ? bid_lv : end_price_fp;
            sell_px = ask_lv > 0 ? ask_lv : end_price_fp;
        } else {
            buy_px  = end_price_fp;
            sell_px = end_price_fp;
        }
        if (buy_px <= 0 || sell_px <= 0) {
            count_ = 0;
            start_price_fp_ = 0;
            return;
        }

        if (debug_decision_emitted_ < debug_first_decisions_) {
            const char* signal = "SKIP";
            double px = order_exec_from_fp(end_price_fp);
            if (end_price_fp > start_price_fp_) {
                signal = "SELL";
                px = order_exec_from_fp(sell_px);
            } else if (end_price_fp < start_price_fp_) {
                signal = "BUY";
                px = order_exec_from_fp(buy_px);
            }
            char buf[512];
            std::snprintf(buf, sizeof(buf),
                          "hft_tick_revert decision action_time=%u start=%.6f end=%.6f pos=%.6f signal=%s px=%.6f bid=%.6f ask=%.6f",
                          tk.action_time,
                          order_exec_from_fp(start_price_fp_),
                          order_exec_from_fp(end_price_fp),
                          order_exec_from_fp(cur_pos),
                          signal,
                          px,
                          order_exec_from_fp(tk.bid_price_fp[0]),
                          order_exec_from_fp(tk.ask_price_fp[0]));
            ctx->log_info(buf);
            ++debug_decision_emitted_;
        }

        const auto close_off = use_ioc_ ? XtOffset::Close : XtOffset::Open;

        const bool will_close = use_ioc_ &&
                                ((end_price_fp > start_price_fp_ && long_leg_fp > 0) ||
                                 (end_price_fp < start_price_fp_ && short_leg_fp > 0));
        if (will_close) {

            if (!close_oids_.empty()) {
                count_ = 0;
                start_price_fp_ = 0;
                return;
            }

            if (close_throttle_ms_ > 0 && last_close_at_ != 0) {
                const int32_t delta_ms = action_time_delta_ms(last_close_at_, tk.action_time);
                if (delta_ms < close_throttle_ms_) {
                    count_ = 0;
                    start_price_fp_ = 0;
                    return;
                }
            }

            if (close_reject_backoff_ms_ > 0 && last_close_reject_at_ != 0) {
                const int32_t delta_ms = action_time_delta_ms(last_close_reject_at_, tk.action_time);
                if (delta_ms < close_reject_backoff_ms_) {
                    count_ = 0;
                    start_price_fp_ = 0;
                    return;
                }
                last_close_reject_at_ = 0;
            }

            if (close_expire_backoff_ms_ > 0 && last_close_expire_at_ != 0) {
                const int32_t delta_ms = action_time_delta_ms(last_close_expire_at_, tk.action_time);
                if (delta_ms < close_expire_backoff_ms_) {
                    count_ = 0;
                    start_price_fp_ = 0;
                    return;
                }
                last_close_expire_at_ = 0;
            }
        }

        uint64_t oid = 0;
        const char* tag = nullptr;

        char reason_buf[48] = {};
        const double delta_ticks = order_exec_from_fp(end_price_fp - start_price_fp_);

        const int64_t close_long_qty_fp  = use_ioc_ ? long_leg_fp  : qty_fp;
        const int64_t close_short_qty_fp = use_ioc_ ? short_leg_fp : qty_fp;

        if (end_price_fp > start_price_fp_) {
            if (long_leg_fp > 0) {
                tag = "rev_close_long";
                std::snprintf(reason_buf, sizeof(reason_buf),
                              "tick_revert|dir=down|delta=%+.4f", delta_ticks);
                ctx->set_next_order_reason(reason_buf);
                oid = xt_rollover::submit_and_track(ctx, orders_, positions_, symbol_.c_str(),  false,
                                              sell_px, close_long_qty_fp, tif, tag, close_off);
                if (oid != 0 && use_ioc_) {
                    last_close_at_ = tk.action_time;
                    close_oids_.insert(oid);
                }
            } else if (long_leg_fp == 0 && short_leg_fp == 0 && cur_pos == 0 && open_qty_fp > 0) {
                tag = "rev_short";
                std::snprintf(reason_buf, sizeof(reason_buf),
                              "tick_revert|dir=down|delta=%+.4f", delta_ticks);
                ctx->set_next_order_reason(reason_buf);
                oid = xt_rollover::submit_and_track(ctx, orders_, positions_, symbol_.c_str(),  false,
                                              sell_px, open_qty_fp, tif, tag, XtOffset::Open);
            }
        } else if (end_price_fp < start_price_fp_) {
            if (short_leg_fp > 0) {
                tag = "rev_close_short";
                std::snprintf(reason_buf, sizeof(reason_buf),
                              "tick_revert|dir=up|delta=%+.4f", delta_ticks);
                ctx->set_next_order_reason(reason_buf);
                oid = xt_rollover::submit_and_track(ctx, orders_, positions_, symbol_.c_str(),  true,
                                              buy_px, close_short_qty_fp, tif, tag, close_off);
                if (oid != 0 && use_ioc_) {
                    last_close_at_ = tk.action_time;
                    close_oids_.insert(oid);
                }
            } else if (long_leg_fp == 0 && short_leg_fp == 0 && cur_pos == 0 && open_qty_fp > 0) {
                tag = "rev_long";
                std::snprintf(reason_buf, sizeof(reason_buf),
                              "tick_revert|dir=up|delta=%+.4f", delta_ticks);
                ctx->set_next_order_reason(reason_buf);
                oid = xt_rollover::submit_and_track(ctx, orders_, positions_, symbol_.c_str(),  true,
                                              buy_px, open_qty_fp, tif, tag, XtOffset::Open);
            }
        }

        if (oid != 0 && maker_ttl_ms_ > 0 && !use_ioc_) {
            ttl_orders_.emplace(oid, tk.action_time);
        }
        if (oid != 0 && debug_decision_emitted_ <= debug_first_decisions_) {
            const char* side = (end_price_fp > start_price_fp_) ? "SELL" : "BUY";
            double px = (end_price_fp > start_price_fp_) ? order_exec_from_fp(sell_px) : order_exec_from_fp(buy_px);
            char dbg[256];
            std::snprintf(dbg, sizeof(dbg),
                          "hft_tick_revert ORDER oid=%lu tick=%u side=%s px=%.1f pos=%ld tag=%s",
                          (unsigned long)oid, tk.action_time, side, px,
                          (long)cur_pos, tag ? tag : "?");
            ctx->log_info(dbg);
        }

        count_          = 0;
        start_price_fp_ = 0;
    }

    void on_fill(IStrategyContext* ctx, const XtFillEvent&  ) override {
        sync_position_and_clear_backoff_(ctx);
    }

    void on_order(IStrategyContext* ctx,
                  const XtOrderExecUpdate& upd) override {
        orders_.apply_update(upd);

        const bool tracked_close = (close_oids_.count(upd.id) > 0);

        const bool is_terminal =
            upd.status == XtOrderStatus::Filled   ||
            upd.status == XtOrderStatus::Canceled ||
            upd.status == XtOrderStatus::Rejected;
        if (is_terminal) {
            close_oids_.erase(upd.id);
            ttl_orders_.erase(upd.id);
        }

        if (tracked_close && upd.status == XtOrderStatus::Rejected && last_close_at_ != 0) {
            last_close_reject_at_ = last_close_at_;
        }

        if (tracked_close && upd.status == XtOrderStatus::Canceled &&
            upd.filled_fp == 0 && last_close_at_ != 0) {
            last_close_expire_at_ = last_close_at_;
        }

        if (debug_decision_emitted_ <= debug_first_decisions_) {
            const char* status = "?";
            if (upd.filled_fp > 0) status = "FILLED";
            else if (upd.status == XtOrderStatus::Canceled) status = "CANCEL";
            else if (upd.status == XtOrderStatus::Rejected) status = "REJECT";
            else status = "UPDATE";
            uint32_t fill_tick_time = 0;
            if (upd.filled_fp > 0) {
                const XtTick* snap = ctx->get_last_tick(symbol_.c_str());
                if (snap) fill_tick_time = snap->action_time;
            }
            char dbg[256];
            std::snprintf(dbg, sizeof(dbg),
                          "hft_tick_revert ORDRPT oid=%lu %s price=%.1f filled=%.1f side=%s fill_tick=%u",
                          (unsigned long)upd.id, status,
                          order_exec_from_fp(upd.price_fp),
                          order_exec_from_fp(upd.filled_fp),
                          upd.is_buy ? "BUY" : "SELL",
                          fill_tick_time);
            ctx->log_info(dbg);
        }
    }

    void on_roll(IStrategyContext* ctx, const char* old_code,
                 const char* new_code, uint32_t  ,
                 int64_t  ) override {
        xt_rollover::migrate_gtc_orders(ctx, orders_, old_code, new_code);

        ttl_orders_.clear();
        positions_.touch(old_code);
        positions_.on_update(new_code,
            ctx->get_position_fp(new_code) - ctx->projected_inflight_fp(new_code));
        symbol_ = new_code;
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

    void sync_position_and_clear_backoff_(IStrategyContext* ctx) {
        if (!ctx || symbol_.empty()) return;
        const int64_t old_pos = positions_.logical_fp();
        positions_.on_update(symbol_.c_str(),
            ctx->get_position_fp(symbol_.c_str()) - ctx->projected_inflight_fp(symbol_.c_str()));
        const int64_t new_pos = positions_.logical_fp();
        if (old_pos != new_pos) {
            last_close_reject_at_ = 0;
            last_close_expire_at_ = 0;
        }
    }

    std::string symbol_;
    xt_rollover::PositionAggregator   positions_;
    xt_rollover::WorkingOrderRegistry orders_;
    int         tick_window_   = 100;
    double      qty_           = 1.0;
    double      order_notional_usdt_ = 0.0;
    double      qty_step_      = 0.001;
    double      min_order_notional_ = 0.0;
    bool        notional_zero_warned_ = false;
    bool        use_ioc_       = true;
    int         passive_depth_level_ = -1;
    int         maker_ttl_ms_ = 0;
    std::unordered_map<uint64_t, uint32_t> ttl_orders_;
    int         close_throttle_ms_ = 200;
    int         close_reject_backoff_ms_ = 3000;
    uint32_t    last_close_at_ = 0;
    uint32_t    last_close_reject_at_ = 0;
    int         close_expire_backoff_ms_ = 0;
    uint32_t    last_close_expire_at_ = 0;
    std::unordered_set<uint64_t> close_oids_;
    int         debug_first_ticks_ = 0;
    int         debug_first_decisions_ = 0;
    int         debug_tick_emitted_ = 0;
    int         debug_decision_emitted_ = 0;
    int         count_         = 0;
    int64_t     start_price_fp_ = 0;
};
