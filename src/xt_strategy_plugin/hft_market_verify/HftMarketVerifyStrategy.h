#pragma once

#include "xt_strategy_engine/XtStrategyBase.h"
#include "xt_strategy_engine/XtStrategyInitConfig.h"
#include "api/IStrategyContext.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtFillEvent.h"

#include <cstdint>
#include <cstdio>
#include <string>

class HftMarketVerifyStrategy : public XtStrategyBase {
public:
    void init(const XtStrategyInitConfig& cfg) override {
        symbol_       = cfg.get_string("symbol", "");
        qty_          = cfg.get_double("qty", 0.002);
        is_buy_       = cfg.get_int("verify_buy", 1) != 0;
        warmup_ms_    = cfg.get_int("warmup_ms", 3000);
        max_attempts_ = static_cast<int>(cfg.get_int("max_attempts", 50));
        warmup_ticks_ = cfg.get_int("warmup_ticks", 8000);
    }

    void on_init(IStrategyContext* ctx) override {
        if (!symbol_.empty()) ctx->sub_tick(symbol_.c_str());
        char buf[224];
        std::snprintf(buf, sizeof(buf),
            "mkt_verify READY symbol=%s qty=%.6f side=%s warmup_ms=%lld — awaiting td-ready then fire ONE market order",
            symbol_.c_str(), qty_, is_buy_ ? "BUY" : "SELL", (long long)warmup_ms_);
        ctx->log_info(buf);
    }

    void on_tick(IStrategyContext* ctx, const XtTick& tk) override {
        if (open_latched_) return;
        ++tick_count_;
        const int64_t now_ms = packed_hms_to_ms(ctx->get_time_ms());
        if (first_ms_ < 0) first_ms_ = now_ms;
        const int64_t elapsed = now_ms - first_ms_;

        const bool warmed = (elapsed >= warmup_ms_) || (tick_count_ >= warmup_ticks_);
        if (!warmed) return;

        const bool resolvable = is_buy_
            ? (tk.ask_price_fp[0] > 0 || tk.upper_limit_fp > 0)
            : (tk.bid_price_fp[0] > 0 || tk.lower_limit_fp > 0);
        if (!resolvable) return;
        if (attempts_ >= max_attempts_) {
            if (!gave_up_) { gave_up_ = true;
                ctx->log_warn("mkt_verify OPEN gave up after max_attempts — all submits rejected (id==0)"); }
            return;
        }
        ++attempts_;
        const int64_t qty_fp = order_exec_to_fp(qty_);
        open_id_ = is_buy_
            ? ctx->buy_fp (symbol_.c_str(),  0, qty_fp,
                           XtTimeInForce::GTC, "mkt_verify", XtOffset::Open)
            : ctx->sell_fp(symbol_.c_str(),  0, qty_fp,
                           XtTimeInForce::GTC, "mkt_verify", XtOffset::Open);
        char buf[288];
        std::snprintf(buf, sizeof(buf),
            "mkt_verify OPEN attempt=%d id=%llu %s symbol=%s qty=%.6f price_fp=0(market) "
            "ref bid_fp=%lld ask_fp=%lld up_fp=%lld dn_fp=%lld cash_fp=%lld",
            attempts_, (unsigned long long)open_id_, is_buy_ ? "BUY" : "SELL",
            symbol_.c_str(), qty_,
            (long long)tk.bid_price_fp[0], (long long)tk.ask_price_fp[0],
            (long long)tk.upper_limit_fp, (long long)tk.lower_limit_fp,
            (long long)ctx->get_cash_fp());
        ctx->log_info(buf);
        if (open_id_ != 0) {
            open_latched_ = true;
        } else {
            ctx->log_warn("mkt_verify OPEN rejected (id==0) — guard/risk/price<=0/notional/margin/td-not-ready; will retry next tick");
        }
    }

    void on_fill(IStrategyContext* ctx, const XtFillEvent& f) override {
        char buf[224];
        std::snprintf(buf, sizeof(buf),
            "mkt_verify FILL side=%s exec_price=%.8f qty=%.8f",
            f.is_buy ? "BUY" : "SELL", f.exec_price(), f.qty());
        ctx->log_info(buf);
        if (sent_close_) return;
        sent_close_ = true;

        const int64_t qty_fp = order_exec_to_fp(qty_);
        close_id_ = is_buy_
            ? ctx->sell_fp(symbol_.c_str(),  0, qty_fp,
                           XtTimeInForce::GTC, "mkt_verify_close", XtOffset::Close)
            : ctx->buy_fp (symbol_.c_str(),  0, qty_fp,
                           XtTimeInForce::GTC, "mkt_verify_close", XtOffset::Close);
        std::snprintf(buf, sizeof(buf),
            "mkt_verify CLOSE id=%llu %s symbol=%s qty=%.6f price_fp=0(market) — flatten",
            (unsigned long long)close_id_, is_buy_ ? "SELL" : "BUY", symbol_.c_str(), qty_);
        ctx->log_info(buf);
    }

private:

    static int64_t packed_hms_to_ms(uint32_t t) {
        const int hh  = static_cast<int>(t / 10000000);
        const int mm  = static_cast<int>((t / 100000) % 100);
        const int ss  = static_cast<int>((t / 1000) % 100);
        const int mmm = static_cast<int>(t % 1000);
        return (static_cast<int64_t>((hh * 60 + mm) * 60 + ss)) * 1000 + mmm;
    }

    std::string symbol_;
    double      qty_          = 0.002;
    bool        is_buy_       = true;
    int64_t     warmup_ms_    = 3000;
    int64_t     warmup_ticks_ = 8000;
    int         max_attempts_ = 50;

    int64_t     first_ms_     = -1;
    long        tick_count_   = 0;
    int         attempts_     = 0;
    bool        open_latched_ = false;
    bool        gave_up_      = false;
    bool        sent_close_   = false;
    uint64_t    open_id_      = 0;
    uint64_t    close_id_     = 0;
};
