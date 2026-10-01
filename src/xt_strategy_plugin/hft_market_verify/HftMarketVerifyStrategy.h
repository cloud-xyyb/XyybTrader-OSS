#pragma once
// -----------------------------------------------------------------------------
// HftMarketVerifyStrategy — 一次性市价单实盘验收策略（market_order_support Stage 4）
// -----------------------------------------------------------------------------
// [用途]
//   触发 **一笔** 市价单（buy_fp/sell_fp 传 price_fp==0），验证 XtStrategyContext 把
//   市价意图解析为激进限价 IOC（CTP 用 tick 涨跌停价 / binance 用 best-opposite±1%
//   = kMarketSlippageBps），下游走现成限价 codec 真成交。成交后立刻发一笔反向市价单
//   平回，避免留仓。**仅用于受控时段的 demo 验收，非生产策略。**
//
// [参数]（init from YAML，runtime.strategies[].<key>）
//   symbol       下单合约（SHFE 用 ag2606/ag2612 / binance 用 BTCUSDT）
//   qty          手数/张数（binance 0.002 BTC ≈ $120+ 过 min_notional；CTP 1 手）
//   verify_buy   1 = 开多（买市价），0 = 开空（卖市价）。默认 1。
//   warmup_ms    首次发单前的预热毫秒数（等 td_ready / 账户快照到位）。默认 3000。
//   max_attempts 发单被拒时的最大重试次数（每 tick 一次）。默认 50。
//
// [td-ready 竞态处理] runner 在 td 就绪前也可能派发 on_tick；若此时发单会被拒。故
//   预热一段 wall-clock（get_time_ms()）再发；发单返回 0（被拒）则后续 tick 重试，
//   直到受理或 max_attempts。warmup_ticks 是 clock 不可用时的死锁兜底。
//
// [ABI] 与 hft_tick_revert 同：XtStrategyBase 子类 + XT_STRATEGY_ABI_EXPORTS（见 .cpp）。
// -----------------------------------------------------------------------------

#include "xt_strategy_engine/XtStrategyBase.h"
#include "xt_strategy_engine/XtStrategyInitConfig.h"
#include "api/IStrategyContext.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtOrderTypes.h"   // XtTimeInForce / XtOffset / order_exec_to_fp
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
        // 预热：让 td 就绪（账户快照）再发单；clock 不可用时按 tick 数兜底。
        const bool warmed = (elapsed >= warmup_ms_) || (tick_count_ >= warmup_ticks_);
        if (!warmed) return;
        // 仅当解析一定成功时才发：buy 需 ask 或涨停价；sell 需 bid 或跌停价。
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
            ? ctx->buy_fp (symbol_.c_str(), /*price_fp=*/0, qty_fp,
                           XtTimeInForce::GTC, "mkt_verify", XtOffset::Open)
            : ctx->sell_fp(symbol_.c_str(), /*price_fp=*/0, qty_fp,
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
            open_latched_ = true;   // 受理 → 停止再发
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
        if (sent_close_) return;   // 已平回（或这是平回单自己的成交）→ 不再反向
        sent_close_ = true;
        // 反向市价单平回（同样 price==0 → 解析为激进限价 IOC），Close offset。
        const int64_t qty_fp = order_exec_to_fp(qty_);
        close_id_ = is_buy_
            ? ctx->sell_fp(symbol_.c_str(), /*price_fp=*/0, qty_fp,
                           XtTimeInForce::GTC, "mkt_verify_close", XtOffset::Close)
            : ctx->buy_fp (symbol_.c_str(), /*price_fp=*/0, qty_fp,
                           XtTimeInForce::GTC, "mkt_verify_close", XtOffset::Close);
        std::snprintf(buf, sizeof(buf),
            "mkt_verify CLOSE id=%llu %s symbol=%s qty=%.6f price_fp=0(market) — flatten",
            (unsigned long long)close_id_, is_buy_ ? "SELL" : "BUY", symbol_.c_str(), qty_);
        ctx->log_info(buf);
    }

private:
    // get_time_ms() 返回 HHMMSSmmm 打包十进制 → 当日绝对毫秒（用于 wall-clock 预热）。
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
