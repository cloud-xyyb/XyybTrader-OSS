#pragma once

#include "xt_strategy_engine/XtStrategyBase.h"
#include "xt_strategy_engine/XtStrategyInitConfig.h"
#include "api/IStrategyContext.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/market/XtBarStruct.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtOffset.h"
#include "xt_core/order/XtFillEvent.h"

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unordered_map>

class HftConformanceStrategy : public XtStrategyBase {
public:

    void init(const XtStrategyInitConfig& cfg) override {
        symbol_       = cfg.get_string("symbol", "");
        const std::string venue = cfg.get_string("venue", "ctp");
        venue_is_ctp_ = (venue != "binance");
        qty_          = cfg.get_double("qty", 1.0);
        warmup_ms_    = cfg.get_int("warmup_ms", 3000);
        warmup_ticks_ = cfg.get_int("warmup_ticks", 5000);

        max_total_orders_        = cfg.get_int("max_total_orders", 200000);
        decision_interval_ticks_ = static_cast<int>(cfg.get_int("decision_interval_ticks", 1));
        fire_per_tick_           = static_cast<int>(cfg.get_int("fire_per_tick", 4));
        fill_per_tick_           = static_cast<int>(cfg.get_int("fill_per_tick", 0));
        flatten_interval_ticks_  = static_cast<int>(cfg.get_int("flatten_interval_ticks", 400));
        heartbeat_ticks_         = static_cast<int>(cfg.get_int("heartbeat_ticks", 2000));
        stuck_timeout_ticks_     = static_cast<int>(cfg.get_int("stuck_timeout_ticks", 1000));

        stuck_viol_grace_ms_     = cfg.get_int("stuck_viol_grace_ms", 40000);
        far_offset_ticks_        = cfg.get_int("far_offset_ticks", 100);
        max_orders_per_min_      = cfg.get_int("max_orders_per_min", 0);
        max_pos_qty_abs_fp_      = order_exec_to_fp(cfg.get_double("max_pos_qty_abs", qty_ * 5.0));

        flatten_chunk_qty_fp_    = order_exec_to_fp(cfg.get_double("flatten_chunk_qty", qty_));

        risk_probe_qty_fp_       = order_exec_to_fp(cfg.get_double("risk_probe_qty", 1.0e9));

        audit_interval_ticks_    = cfg.get_int("audit_interval_ticks", 4000);
        audit_timeout_ticks_     = cfg.get_int("audit_timeout_ticks", 3000);
        audit_settle_ticks_      = cfg.get_int("audit_settle_ticks", 50);
        recent_ttl_ticks_        = cfg.get_int("recent_terminal_ttl_ticks", 600);
        reject_storm_window_     = cfg.get_int("reject_storm_window", 200);

        reject_storm_pct_        = cfg.get_int("reject_storm_pct", 50);

        s_risk_probe_pos_        = cfg.get_int("s_risk_probe_pos", 0) != 0;
        risk_probe_pos_qty_fp_   = order_exec_to_fp(cfg.get_double("risk_probe_pos_qty", 0.0));
        s_risk_probe_notional_   = cfg.get_int("s_risk_probe_notional", 0) != 0;
        risk_probe_notional_qty_fp_ = order_exec_to_fp(cfg.get_double("risk_probe_notional_qty", 0.0));
        s_probe_cancel_unknown_  = cfg.get_int("s_probe_cancel_unknown", 1) != 0;

        s_rapid_submit_ = cfg.get_int("s_rapid_submit", 1) != 0;
        s_cancel_storm_ = cfg.get_int("s_cancel_storm", 1) != 0;
        s_market_ioc_   = cfg.get_int("s_market_ioc",   1) != 0;
        s_flip_         = cfg.get_int("s_flip",         1) != 0;
        s_tif_matrix_   = cfg.get_int("s_tif_matrix",   1) != 0;
        s_risk_probe_   = cfg.get_int("s_risk_probe",   1) != 0;
        s_query_sweep_  = cfg.get_int("s_query_sweep",  1) != 0;
        s_userdata_     = cfg.get_int("s_userdata",     1) != 0;
        s_md_oracle_    = cfg.get_int("s_md_oracle",    1) != 0;
        s_book_oracle_  = cfg.get_int("s_book_oracle",  1) != 0;

        probe_every_decisions_ = cfg.get_int("probe_every_decisions", 32);
        if (probe_every_decisions_ < 1) probe_every_decisions_ = 1;

        cancel_retry_ticks_ = stuck_timeout_ticks_ > 1
            ? std::min<int64_t>(kCancelRetryCooldownTicks,
                                std::max<int64_t>(50, stuck_timeout_ticks_ / 2))
            : kCancelRetryCooldownTicks;
    }

    void on_init(IStrategyContext* ctx) override {
        if (symbol_.empty()) { ctx->log_warn("conf INIT symbol empty — strategy inert"); return; }
        ctx->sub_tick(symbol_.c_str());
        ctx->declare_position_interest(symbol_.c_str());

        if (s_userdata_) run_userdata_init_checks_(ctx);

        (void)ctx->get_param_int("fire_per_tick", -1);
        (void)ctx->get_param_double("qty", -1.0);
        (void)ctx->get_param_bool("s_query_sweep", false);

        char buf[320];
        std::snprintf(buf, sizeof(buf),
            "conf INIT symbol=%s venue=%s qty=%.6f fire/tick=%d fill/tick=%d max_total=%lld "
            "audit[itv=%lld to=%lld settle=%lld] storm[w=%lld pct=%lld] "
            "scenarios[rs=%d cs=%d mioc=%d flip=%d tif=%d risk=%d q=%d ud=%d posP=%d notP=%d]",
            symbol_.c_str(), venue_is_ctp_ ? "ctp" : "binance", qty_, fire_per_tick_, fill_per_tick_,
            (long long)max_total_orders_,
            (long long)audit_interval_ticks_, (long long)audit_timeout_ticks_,
            (long long)audit_settle_ticks_,
            (long long)reject_storm_window_, (long long)reject_storm_pct_,
            s_rapid_submit_, s_cancel_storm_, s_market_ioc_, s_flip_,
            s_tif_matrix_, s_risk_probe_, s_query_sweep_, s_userdata_,
            s_risk_probe_pos_, s_risk_probe_notional_);
        ctx->log_info(buf);
    }

    uint64_t capabilities() const noexcept override {
        return to_capability_bit(StrategyCapability::SubscribesTick)
             | to_capability_bit(StrategyCapability::RequiresOnFill)
             | to_capability_bit(StrategyCapability::RequiresOnOrder);
    }

    void on_tick(IStrategyContext* ctx, const XtTick& tk) override {
        if (symbol_.empty() || std::strcmp(tk.code, symbol_.c_str()) != 0) return;
        const int64_t t0 = now_us();

        if (prev_tick_us_ > 0) {
            const int64_t gap = t0 - prev_tick_us_;
            if (gap >= 0 && gap < kTickGapSkipUs) lat_tick_gap_.add(gap);
        }
        prev_tick_us_ = t0;
        // view 因果性检测窗口：本 tick 内提交的单若在本 tick 收到回报 = 同步撞合泄漏。
        struct TickGuard { bool& f; explicit TickGuard(bool& x) : f(x) { f = true; } ~TickGuard() { f = false; } } _tg(inside_on_tick_);
        on_tick_impl_(ctx, tk);
        lat_on_tick_.add(now_us() - t0);
    }

    void on_order(IStrategyContext* ctx, const XtOrderExecUpdate& u) override {
        auto it = inflight_.find(u.id);
        if (it == inflight_.end()) { on_order_not_inflight_(ctx, u); return; }
        Inflight& o = it->second;
        if (o.first_ack_us == 0) {
            o.first_ack_us = now_us();
            lat_ack_.add(o.first_ack_us - o.submit_us);
        }

        if (u.exch_ts_us == 0 && o.submit_tick_seq == tick_seq_ && inside_on_tick_) {
            violation(ctx, "view_causality", "order update observed within same on_tick as submit");
        }
        if (u.filled_fp > o.cum_upd_fill_fp) o.cum_upd_fill_fp = u.filled_fp;

        if (u.qty_fp > 0 && (o.venue_qty_fp == 0 || u.qty_fp < o.venue_qty_fp))
            o.venue_qty_fp = u.qty_fp;
        if (!order_status_is_final(u.status)) {
            check_book_leaves_(ctx, o);
            return;
        }
        ++terminal_total_;
        if (u.status == XtOrderStatus::Rejected) {
            if (o.expect_reject) { ++rejects_expected_; probe_hit_(o.scenario); }
            else { ++rejects_unexpected_; ++window_unexp_; }
        }
        const int64_t tnow = now_us();
        if (o.expect_fast_terminal) lat_fast_term_.add(tnow - o.submit_us);
        if (o.cancel_req_us > 0 && u.status == XtOrderStatus::Canceled)
            lat_cancel_.add(tnow - o.cancel_req_us);

        TerminalRec rec;
        rec.qty_fp = o.qty_fp; rec.cum_onfill_fp = o.cum_fill_fp;
        rec.terminal_filled_fp = u.filled_fp; rec.terminal_tick = tick_seq_;
        rec.tif = o.tif; rec.offset = o.offset; rec.status = u.status;
        rec.expect_reject = o.expect_reject; rec.overfill_flagged = o.overfill_flagged;
        recent_terminal_[u.id] = rec;
        inflight_.erase(it);
        check_reject_storm_(ctx);
    }

    void on_fill(IStrategyContext* ctx, const XtFillEvent& f) override {
        ++fills_total_;
        apply_self_fill_(f);
        auto it = inflight_.find(f.order_id);
        if (it != inflight_.end()) {
            it->second.cum_fill_fp += f.qty_fp;
            check_overfill_(ctx, f.order_id, it->second.cum_fill_fp, it->second.qty_fp,
                            it->second.overfill_flagged);
            check_book_leaves_(ctx, it->second);
            return;
        }
        auto rt = recent_terminal_.find(f.order_id);
        if (rt != recent_terminal_.end()) {
            TerminalRec& rec = rt->second;
            rec.cum_onfill_fp += f.qty_fp;
            check_overfill_(ctx, f.order_id, rec.cum_onfill_fp, rec.qty_fp, rec.overfill_flagged);
            if (rec.status == XtOrderStatus::Rejected) {
                violation(ctx, "fill_after_reject", "fill arrived for rejected order");
            } else if (rec.status == XtOrderStatus::Canceled) {
                ++fill_after_cancel_total_;
            }
            return;
        }
        ++unknown_fill_total_;
    }

    void on_account(IStrategyContext*  , const XtAccountUpdate&  ) override {
        ++account_update_total_;
    }

    void on_session_begin(IStrategyContext* ctx, uint32_t trading_date) override {
        ++session_begin_total_;
        if (last_trading_date_ != 0 && trading_date < last_trading_date_) {
            violation(ctx, "session_monotonic", "session_begin trading_date went backwards");
        }
        last_trading_date_ = trading_date;
    }

    void on_session_end(IStrategyContext*  , uint32_t  ) override {
        ++session_end_total_;
    }

    void on_roll(IStrategyContext* ctx, const char* old_code, const char* new_code,
                 uint32_t  , int64_t  ) override {
        ++roll_total_;

        if (new_code && new_code[0]) {
            symbol_ = new_code;
            ctx->sub_tick(new_code);
            self_pos_fp_ = confirmed_pos_(ctx);
            self_pos_anchored_ = true;
        }
        char buf[160];
        std::snprintf(buf, sizeof(buf), "conf ROLL %s -> %s (retarget)",
                      old_code ? old_code : "", new_code ? new_code : "");
        ctx->log_info(buf);
    }

    void on_params(IStrategyContext* ctx) override {
        ++params_update_total_;

        (void)ctx->get_param_int("fire_per_tick", -1);
    }

    void on_channel_ready(IStrategyContext* ctx, const char* channel) override {
        ++channel_ready_total_;
        last_channel_ready_tick_ = tick_seq_;
        char buf[96];
        std::snprintf(buf, sizeof(buf), "conf CHANNEL ready %s", channel ? channel : "?");
        ctx->log_info(buf);
    }

    void on_channel_lost(IStrategyContext* ctx, const char* channel, int reason) override {
        ++channel_lost_total_;
        char buf[112];
        std::snprintf(buf, sizeof(buf), "conf CHANNEL lost %s reason=%d",
                      channel ? channel : "?", reason);
        ctx->log_warn(buf);
    }

private:

    enum Scenario {
        SC_CANCEL_STORM = 0, SC_MARKET_IOC, SC_FLIP, SC_TIF_MATRIX,
        SC_RISK_PROBE, SC_RISK_PROBE_POS, SC_RISK_PROBE_NOTIONAL, SC_COUNT
    };
    enum class AuditState { Normal, Drain, Settle };

    struct Inflight {
        uint64_t      id            = 0;
        int           scenario      = -1;
        bool          is_buy        = true;
        int64_t       qty_fp        = 0;
        int64_t       cum_fill_fp   = 0;
        int64_t       cum_upd_fill_fp = 0;
        int64_t       venue_qty_fp  = 0;
        bool          book_viol_flagged = false;
        XtTimeInForce tif           = XtTimeInForce::GTC;
        XtOffset      offset        = XtOffset::Open;
        int64_t       submit_tick_seq = 0;
        int64_t       submit_us     = 0;
        int64_t       first_ack_us  = 0;
        int64_t       cancel_req_us = 0;
        int64_t       cancel_req_tick = -1;
        int64_t       cancel_first_tick = -1;
        bool          expect_reject = false;
        bool          expect_fast_terminal = false;
        bool          overfill_flagged = false;
    };

    /// 终态后的消账暂存：等 TTL 内的晚到 fill 归账后定稿检查。
    struct TerminalRec {
        int64_t       qty_fp             = 0;
        int64_t       cum_onfill_fp      = 0;
        int64_t       terminal_filled_fp = 0;
        int64_t       terminal_tick      = 0;
        XtTimeInForce tif                = XtTimeInForce::GTC;
        XtOffset      offset             = XtOffset::Open;
        XtOrderStatus status             = XtOrderStatus::Canceled;
        bool          expect_reject      = false;
        bool          overfill_flagged   = false;
        bool          stuck              = false;
        bool          stuck_fast         = false;
        bool          got_late_terminal  = false;
        int64_t       stuck_evict_us     = 0;
        bool          had_terminal_conflict = false;
    };

    /// 固定桶延迟直方图（µs）；无堆分配，percentile 返回桶上界估计。
    struct LatHist {
        static constexpr int kN = 20;
        static constexpr int64_t kBounds[kN - 1] = {
            5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000,
            100000, 200000, 500000, 1000000, 2000000, 5000000};
        uint64_t cnt[kN] = {};
        uint64_t n = 0;
        int64_t  max_us = 0;
        void add(int64_t us) noexcept {
            if (us < 0) us = 0;
            int i = 0;
            while (i < kN - 1 && us > kBounds[i]) ++i;
            ++cnt[i]; ++n;
            if (us > max_us) max_us = us;
        }
        int64_t pct(int p) const noexcept {
            if (n == 0) return 0;
            const uint64_t target = (n * (uint64_t)p + 99) / 100;
            uint64_t acc = 0;
            for (int i = 0; i < kN; ++i) {
                acc += cnt[i];
                if (acc >= target) return i < kN - 1 ? kBounds[i] : max_us;
            }
            return max_us;
        }
    };

    void on_tick_impl_(IStrategyContext* ctx, const XtTick& tk) {
        ++tick_seq_;

        const int64_t now_ms = packed_hms_to_ms(ctx->get_time_ms());
        if (first_ms_ < 0) first_ms_ = now_ms;
        const bool warmed = (now_ms - first_ms_ >= warmup_ms_) || (tick_seq_ >= warmup_ticks_);
        if (!warmed) return;

        if (!self_pos_anchored_) {
            if (!ctx->can_submit()) return;
            self_pos_fp_ = confirmed_pos_(ctx);
            self_pos_anchored_ = true;
            char abuf[96];
            std::snprintf(abuf, sizeof(abuf), "conf ANCHOR self_pos_fp=%lld",
                          (long long)self_pos_fp_);
            ctx->log_info(abuf);
        }

        observe_drift_(ctx);
        if (s_md_oracle_) check_md_tick_(ctx, tk);
        if (s_query_sweep_) run_query_sweep(ctx, tk);
        check_stuck_orders(ctx);
        if ((tick_seq_ & kRecentPurgeMask) == 0) purge_recent_terminals_(ctx);
        if (audit_state_ == AuditState::Normal &&
            flatten_interval_ticks_ > 0 && (tick_seq_ % flatten_interval_ticks_) == 0)
            flatten(ctx, tk, "periodic");
        if (heartbeat_ticks_ > 0 && (tick_seq_ % heartbeat_ticks_) == 0)
            heartbeat(ctx);

        if (audit_interval_ticks_ > 0) run_audit_machine_(ctx, tk);
        if (audit_state_ != AuditState::Normal) return;

        if (!ctx->can_submit()) return;
        if (decision_interval_ticks_ > 1 && (tick_seq_ % decision_interval_ticks_) != 0) return;
        if (orders_total_ >= max_total_orders_) return;
        if (!order_budget_ok(now_ms)) { ++budget_skip_total_; return; }

        if (s_rapid_submit_) {
            for (int i = 0; i < fire_per_tick_ && orders_total_ < max_total_orders_
                            && order_budget_ok(now_ms); ++i)
                fire_rapid_submit(ctx, tk);
        }

        if (fill_per_tick_ > 0) {
            int64_t fs_proj_fp = 0;
            for (int i = 0; i < fill_per_tick_ && orders_total_ < max_total_orders_
                            && order_budget_ok(now_ms); ++i)
                fire_fill_storm(ctx, tk, fs_proj_fp);
        }

        if (order_budget_ok(now_ms)) dispatch_rotating_scenario(ctx, tk);
    }

    int64_t aggressive_buy_px(const XtTick& tk) const noexcept {

        if (!venue_is_ctp_) return 0;
        if (tk.upper_limit_fp > 0) return tk.upper_limit_fp;
        if (tk.ask_price_fp[0] > 0) return tk.ask_price_fp[0];
        return tk.price_fp;
    }
    int64_t aggressive_sell_px(const XtTick& tk) const noexcept {
        if (!venue_is_ctp_) return 0;
        if (tk.lower_limit_fp > 0) return tk.lower_limit_fp;
        if (tk.bid_price_fp[0] > 0) return tk.bid_price_fp[0];
        return tk.price_fp;
    }
    int64_t ref_mid_px(const XtTick& tk) const noexcept {
        if (tk.price_fp > 0) return tk.price_fp;
        const int64_t b = tk.bid_price_fp[0], a = tk.ask_price_fp[0];
        if (b > 0 && a > 0) return (a + b) / 2;
        return b > 0 ? b : a;
    }
    int64_t price_tick_fp(IStrategyContext* ctx, const XtTick& tk) noexcept {

        if (cached_price_tick_fp_ > 0) return cached_price_tick_fp_;
        if (const auto* ci = ctx->get_contract_info(symbol_.c_str())) {
            const int64_t t = order_exec_to_fp(ci->price_tick);
            if (t > 0) { cached_price_tick_fp_ = t; return t; }
        }
        const int64_t mid = ref_mid_px(tk);
        int64_t t = mid / 10000;
        return t > 0 ? t : 1;
    }

    static bool md_prefix_bad_(const int64_t* px, const int64_t* qty, bool descending) noexcept {
        bool gap_seen = false;
        int64_t prev = 0;
        for (int i = 0; i < XtTick::DEPTH; ++i) {
            const bool present = px[i] > 0 && qty[i] > 0;
            if (!present) { gap_seen = true; continue; }
            if (gap_seen) return true;
            if (prev != 0) {
                if (descending && px[i] >= prev) return true;
                if (!descending && px[i] <= prev) return true;
            }
            prev = px[i];
        }
        return false;
    }

    void check_md_tick_(IStrategyContext* ctx, const XtTick& tk) {
        const int64_t b0 = tk.bid_price_fp[0], a0 = tk.ask_price_fp[0];
        const bool both_l1 = (b0 > 0 && a0 > 0);

        if (!both_l1) {
            ++md_one_sided_total_;
            if (!venue_is_ctp_ && (b0 == 0 || a0 == 0) && !viol_once_md_l1_) {
                viol_once_md_l1_ = true;
                violation(ctx, "md_l1_absent", "binance tick missing L1 bid/ask");
            }
        }

        if (both_l1) {
            if (b0 > a0) {
                ++md_crossed_total_;
                md_viol_crossed_(ctx, b0, a0);
            } else if (b0 == a0) {
                if (!venue_is_ctp_) { ++md_crossed_total_; md_viol_crossed_(ctx, b0, a0); }
                else                  ++md_touch_total_;
            }
        }

        if ((b0 > 0 || a0 > 0) &&
            (md_prefix_bad_(tk.bid_price_fp, tk.bid_qty_fp,  true) ||
             md_prefix_bad_(tk.ask_price_fp, tk.ask_qty_fp,  false))) {
            ++md_nonmono_total_;
            if (!viol_once_md_nonmono_) {
                viol_once_md_nonmono_ = true;
                violation(ctx, "md_nonmono", "real-level depth prefix not monotonic / level after gap");
            }
        }
    }

    void md_viol_crossed_(IStrategyContext* ctx, int64_t b0, int64_t a0) {
        if (viol_once_md_crossed_) return;
        viol_once_md_crossed_ = true;
        char buf[96];
        std::snprintf(buf, sizeof(buf), "bid0_fp=%lld ask0_fp=%lld", (long long)b0, (long long)a0);
        violation(ctx, "md_crossed", buf);
    }

    bool can_open_side(int64_t pos, bool is_buy) const noexcept {

        int64_t outstanding = 0;
        for (const auto& kv : inflight_) {
            const Inflight& o = kv.second;
            if (o.is_buy == is_buy && o.offset == XtOffset::Open && !o.expect_reject)
                outstanding += o.qty_fp;
        }
        const int64_t eff = is_buy ? pos + outstanding : pos - outstanding;
        if (eff >= max_pos_qty_abs_fp_  && is_buy)  return false;
        if (eff <= -max_pos_qty_abs_fp_ && !is_buy) return false;
        return true;
    }

    uint64_t submit(IStrategyContext* ctx, int scenario, bool is_buy, int64_t price_fp,
                    int64_t qty_fp, XtTimeInForce tif, XtOffset offset,
                    const char* tag, bool expect_reject, bool expect_fast_terminal) {
        ctx->set_next_order_reason(tag);
        const int64_t t_sub = now_us();
        const uint64_t id = is_buy
            ? ctx->buy_fp (symbol_.c_str(), price_fp, qty_fp, tif, tag, offset)
            : ctx->sell_fp(symbol_.c_str(), price_fp, qty_fp, tif, tag, offset);
        ++orders_total_;
        ++window_orders_;
        if (id == 0) {

            const auto rc = ctx->last_reject_code();
            using RC = xt_runtime::XtRejectCode;
            const bool infra_gated =
                rc == RC::CloseoutNoOpen || rc == RC::KillSwitchActive ||
                rc == RC::Gated || rc == RC::OpenHalted || rc == RC::TradingNotReady ||

                rc == RC::SymbolBlacklisted || rc == RC::OrderCountExceeded ||
                rc == RC::OrderRateExceeded || rc == RC::SelfMatchPostTrade ||
                rc == RC::AnnualLossExceeded || rc == RC::StrategyMaxPosQtyExceeded ||
                rc == RC::StrategyDailyLossExceeded || rc == RC::StrategyKillSwitchActive ||
                rc == RC::StrategyBlockedByWatchdog;

            const bool close_friction =
                rc == RC::CloseSyncPending || rc == RC::InsufficientCloseToday ||
                rc == RC::InsufficientCloseYesterday || rc == RC::PositionSyncStale ||
                rc == RC::OppositeOpenPending ||
                (rc == RC::PositionLimit && offset != XtOffset::Open);
            if (expect_reject) { ++rejects_expected_; probe_hit_(scenario); }
            else if (infra_gated)   ++rejects_gated_;
            else if (close_friction || (rc == RC::Unknown && offset != XtOffset::Open))
                ++rejects_close_local_;
            else { ++rejects_unexpected_; ++window_unexp_; }
            check_reject_storm_(ctx);
            return 0;
        }
        consume_order_budget();
        Inflight o;
        o.id = id; o.scenario = scenario; o.is_buy = is_buy; o.qty_fp = qty_fp;
        o.venue_qty_fp = qty_fp;
        o.tif = tif; o.offset = offset; o.submit_tick_seq = tick_seq_;
        o.submit_us = t_sub;
        o.expect_reject = expect_reject; o.expect_fast_terminal = expect_fast_terminal;
        inflight_.emplace(id, o);
        check_reject_storm_(ctx);
        return id;
    }

    void request_cancel_(IStrategyContext* ctx, uint64_t id, Inflight& o) {
        if (o.cancel_req_tick >= 0 &&
            (tick_seq_ - o.cancel_req_tick) <= cancel_retry_ticks_) return;
        ctx->cancel(id);
        o.cancel_req_tick = tick_seq_;
        if (o.cancel_first_tick < 0) o.cancel_first_tick = tick_seq_;
        if (o.cancel_req_us == 0) o.cancel_req_us = now_us();
        ++cancels_req_total_;
    }

    void fire_rapid_submit(IStrategyContext* ctx, const XtTick& tk) {
        const int64_t tickfp = price_tick_fp(ctx, tk);
        const int64_t off = tickfp * far_offset_ticks_;

        const bool is_buy = (rotor_++ & 1) == 0;
        if (!can_open_side(confirmed_pos_(ctx), is_buy)) return;
        const int64_t mid = ref_mid_px(tk);
        if (mid <= 0) return;
        const int64_t px = is_buy ? (mid - off > 0 ? mid - off : tickfp) : (mid + off);
        submit(ctx, -1  , is_buy, px, order_exec_to_fp(qty_),
               XtTimeInForce::GTC, XtOffset::Open, "rapid_submit",
                false,  false);
    }

    void dispatch_rotating_scenario(IStrategyContext* ctx, const XtTick& tk) {
        for (int n = 0; n < SC_COUNT; ++n) {
            const int sc = (scenario_cursor_++) % SC_COUNT;
            const bool enabled =
                (sc == SC_CANCEL_STORM && s_cancel_storm_) ||
                (sc == SC_MARKET_IOC   && s_market_ioc_)   ||
                (sc == SC_FLIP         && s_flip_)          ||
                (sc == SC_TIF_MATRIX   && s_tif_matrix_)    ||
                (sc == SC_RISK_PROBE   && s_risk_probe_)    ||
                (sc == SC_RISK_PROBE_POS && s_risk_probe_pos_ && risk_probe_pos_qty_fp_ > 0) ||
                (sc == SC_RISK_PROBE_NOTIONAL && s_risk_probe_notional_ && risk_probe_notional_qty_fp_ > 0);
            if (!enabled) continue;
            switch (sc) {
                case SC_CANCEL_STORM: fire_cancel_storm(ctx, tk); break;
                case SC_MARKET_IOC:   fire_market_ioc(ctx, tk);   break;
                case SC_FLIP:         fire_flip(ctx, tk);         break;
                case SC_TIF_MATRIX:   fire_tif_matrix(ctx, tk);   break;
                case SC_RISK_PROBE:   fire_risk_probe(ctx, tk);   break;
                case SC_RISK_PROBE_POS:      fire_risk_probe_pos(ctx, tk);      break;
                case SC_RISK_PROBE_NOTIONAL: fire_risk_probe_notional(ctx, tk); break;
                default: break;
            }
            return;
        }
    }

    void fire_cancel_storm(IStrategyContext* ctx, const XtTick& tk) {
        const int64_t tickfp = price_tick_fp(ctx, tk);
        const int64_t mid = ref_mid_px(tk);
        if (mid <= 0) return;
        const bool is_buy = (rotor_++ & 1) == 0;
        if (!can_open_side(confirmed_pos_(ctx), is_buy)) return;
        const int64_t px = is_buy ? (mid - tickfp * far_offset_ticks_) : (mid + tickfp * far_offset_ticks_);
        if (px <= 0) return;
        const uint64_t id = submit(ctx, SC_CANCEL_STORM, is_buy, px, order_exec_to_fp(qty_),
                                   XtTimeInForce::GTC, XtOffset::Open, "cancel_storm",
                                   false,  true);
        if (id != 0) {
            auto it = inflight_.find(id);
            if (it != inflight_.end()) request_cancel_(ctx, id, it->second);
        }
    }

    void fire_market_ioc(IStrategyContext* ctx, const XtTick& tk) {
        const int64_t pos = confirmed_pos_(ctx);
        const bool is_buy = oscillation_side(pos);
        const XtOffset offset = infer_offset_from_pos(pos, is_buy);
        if (offset == XtOffset::Open && !can_open_side(pos, is_buy)) return;
        const int64_t px = is_buy ? aggressive_buy_px(tk) : aggressive_sell_px(tk);
        if (venue_is_ctp_ && px <= 0) return;
        submit(ctx, SC_MARKET_IOC, is_buy, px, order_exec_to_fp(qty_),
               XtTimeInForce::IOC, offset, "market_ioc", false,  true);
    }

    void fire_fill_storm(IStrategyContext* ctx, const XtTick& tk, int64_t& proj_fp) {

        const int64_t eff_pos = confirmed_pos_(ctx) + proj_fp;
        const bool is_buy = oscillation_side(eff_pos);
        const XtOffset offset = infer_offset_from_pos(eff_pos, is_buy);
        if (offset == XtOffset::Open && !can_open_side(eff_pos, is_buy)) return;
        const int64_t px = is_buy ? aggressive_buy_px(tk) : aggressive_sell_px(tk);
        if (venue_is_ctp_ && px <= 0) return;
        const int64_t qfp = order_exec_to_fp(qty_);
        const uint64_t id = submit(ctx, SC_MARKET_IOC, is_buy, px, qfp,
                                   XtTimeInForce::IOC, offset, "fill_storm",
                                    false,  true);
        if (id != 0) proj_fp += is_buy ? qfp : -qfp;
    }

    void fire_flip(IStrategyContext* ctx, const XtTick& tk) {

        const int64_t pos = confirmed_pos_(ctx);
        bool is_buy;
        if (pos > 0)      is_buy = false;
        else if (pos < 0) is_buy = true;
        else              is_buy = (rotor_++ & 1) == 0;
        const XtOffset offset = infer_offset_from_pos(pos, is_buy);
        if (offset == XtOffset::Open && !can_open_side(pos, is_buy)) return;
        const int64_t px = is_buy ? aggressive_buy_px(tk) : aggressive_sell_px(tk);
        if (venue_is_ctp_ && px <= 0) return;
        submit(ctx, SC_FLIP, is_buy, px, order_exec_to_fp(qty_),
               XtTimeInForce::IOC, offset, "flip", false,  true);
    }

    void fire_tif_matrix(IStrategyContext* ctx, const XtTick& tk) {
        const XtTimeInForce tifs[3] = { XtTimeInForce::GTC, XtTimeInForce::IOC, XtTimeInForce::FOK };
        const XtTimeInForce tif = tifs[(tif_cursor_++) % 3];
        const bool is_buy = (rotor_++ & 1) == 0;
        if (!can_open_side(confirmed_pos_(ctx), is_buy)) return;

        int64_t px;
        bool fast_terminal;
        if (tif == XtTimeInForce::GTC) {
            const int64_t mid = ref_mid_px(tk);
            if (mid <= 0) return;
            px = is_buy ? (mid - price_tick_fp(ctx, tk) * far_offset_ticks_)
                        : (mid + price_tick_fp(ctx, tk) * far_offset_ticks_);
            fast_terminal = false;
        } else {
            px = is_buy ? aggressive_buy_px(tk) : aggressive_sell_px(tk);
            fast_terminal = true;
        }
        if (venue_is_ctp_ && px <= 0) return;
        submit(ctx, SC_TIF_MATRIX, is_buy, px, order_exec_to_fp(qty_),
               tif, XtOffset::Open, "tif_matrix", false, fast_terminal);
    }

    void fire_risk_probe(IStrategyContext* ctx, const XtTick& tk) {
        if ((probe_pacer_q_++ % probe_every_decisions_) != 0) return;

        const int64_t mid = ref_mid_px(tk);
        if (mid <= 0) return;
        const bool is_buy = (rotor_++ & 1) == 0;
        const int64_t px = is_buy ? (mid / 2 > 0 ? mid / 2 : 1) : (mid * 2);
        const uint64_t id = submit(ctx, SC_RISK_PROBE, is_buy, px, risk_probe_qty_fp_,
                                   XtTimeInForce::GTC, XtOffset::Open, "risk_probe",
                                    true,  true);
        if (id != 0) { ++probe_miss_total_; probe_cancel_(ctx, id); }
    }

    void fire_risk_probe_pos(IStrategyContext* ctx, const XtTick& tk) {
        if ((probe_pacer_p_++ % probe_every_decisions_) != 0) return;
        const int64_t mid = ref_mid_px(tk);
        if (mid <= 0) return;

        const bool is_buy = confirmed_pos_(ctx) >= 0;
        const int64_t px = is_buy ? (mid / 2 > 0 ? mid / 2 : 1) : (mid * 2);
        const uint64_t id = submit(ctx, SC_RISK_PROBE_POS, is_buy, px, risk_probe_pos_qty_fp_,
                                   XtTimeInForce::GTC, XtOffset::Open, "risk_probe_pos",
                                    true,  true);
        if (id != 0) { ++probe_miss_total_; probe_cancel_(ctx, id); }
    }

    void fire_risk_probe_notional(IStrategyContext* ctx, const XtTick& tk) {
        if ((probe_pacer_n_++ % probe_every_decisions_) != 0) return;
        const int64_t tickfp = price_tick_fp(ctx, tk);
        const int64_t mid = ref_mid_px(tk);
        if (mid <= 0) return;
        const bool is_buy = (rotor_++ & 1) == 0;
        const int64_t px = is_buy ? (mid - tickfp * far_offset_ticks_) : (mid + tickfp * far_offset_ticks_);
        if (px <= 0) return;
        const uint64_t id = submit(ctx, SC_RISK_PROBE_NOTIONAL, is_buy, px,
                                   risk_probe_notional_qty_fp_,
                                   XtTimeInForce::GTC, XtOffset::Open, "risk_probe_notional",
                                    true,  true);
        if (id != 0) { ++probe_miss_total_; probe_cancel_(ctx, id); }
    }

    void probe_cancel_(IStrategyContext* ctx, uint64_t id) {
        auto it = inflight_.find(id);
        if (it != inflight_.end()) request_cancel_(ctx, id, it->second);
    }

    void run_query_sweep(IStrategyContext* ctx, const XtTick& tk) {
        const char* c = symbol_.c_str();
        (void)ctx->get_position_fp(c);
        (void)ctx->projected_inflight_fp(c);
        (void)ctx->get_position_avg_price_fp(c);
        (void)ctx->get_position_unrealized_fp(c);
        const int64_t cash  = ctx->get_cash_fp();
        const int64_t equity= ctx->get_equity_fp();
        (void)ctx->get_last_price_fp(c);
        (void)ctx->get_last_tick(c);
        (void)ctx->get_contract_info(c);
        XtOrderExecUpdate ob[32];
        (void)ctx->get_open_orders(c, ob, 32);
        (void)tk;

        if (equity < -kSaneFloorFp) {
            char buf[160];
            std::snprintf(buf, sizeof(buf), "equity_sane equity_fp=%lld cash_fp=%lld",
                          (long long)equity, (long long)cash);
            violation(ctx, "equity_sane", buf);
        }
        ++sweep_rotor_;
        if ((sweep_rotor_ & kSweepDetailMask) == 0) sweep_pos_detail_(ctx);
        if ((sweep_rotor_ & kSweepHeavyMask) == 0)  sweep_history_(ctx);
        if ((sweep_rotor_ & kSweepHeavyMask) == 0)  sweep_dates_(ctx);
    }

    void sweep_pos_detail_(IStrategyContext* ctx) {
        xt_runtime::XtStraPositionDetail pd[4];
        const size_t n = ctx->get_position_detail(symbol_.c_str(), pd, 4);
        if (n == 0) return;
        if (n > 4) { violation(ctx, "pos_detail_overflow", "get_position_detail count > max"); return; }
        const int64_t net_fp = order_exec_to_fp(pd[0].long_side.position - pd[0].short_side.position);
        const int64_t confirmed = confirmed_pos_(ctx);
        if (!fp_near(net_fp, confirmed, pos_eps_fp() * 2) && !viol_once_pos_detail_) {
            viol_once_pos_detail_ = true;
            char buf[160];
            std::snprintf(buf, sizeof(buf), "detail_net_fp=%lld confirmed_fp=%lld",
                          (long long)net_fp, (long long)confirmed);
            violation(ctx, "pos_detail_mismatch", buf);
        }
    }

    void sweep_history_(IStrategyContext* ctx) {
        XtTick tb[32];
        const size_t nt = ctx->get_ticks(symbol_.c_str(), tb, 32);
        if (nt > 32) { violation(ctx, "ticks_overflow", "get_ticks count > max"); return; }

        const bool ticks_order_recovery_window =
            last_channel_ready_tick_ >= 0 &&
            (int64_t)tick_seq_ - last_channel_ready_tick_ < kTicksOrderRecoveryWindowTicks;
        for (size_t i = 1; i < nt; ++i) {
            const uint64_t a = (uint64_t)tb[i-1].action_date * 1000000000ULL + tb[i-1].action_time;
            const uint64_t b = (uint64_t)tb[i].action_date   * 1000000000ULL + tb[i].action_time;
            if (b < a) {
                ++ticks_order_total_;
                if (!viol_once_ticks_order_ && !ticks_order_recovery_window && venue_is_ctp_) {
                    viol_once_ticks_order_ = true;
                    violation(ctx, "ticks_order", "get_ticks history not time-ordered (steady-state)");
                }
            }
        }
        XtBar bb[16];
        const size_t nb = ctx->get_bars(symbol_.c_str(), "m1", bb, 16);
        if (nb > 16) violation(ctx, "bars_overflow", "get_bars count > max");
    }

    void sweep_dates_(IStrategyContext* ctx) {
        const uint32_t d = ctx->get_date();
        const uint32_t td = ctx->get_trading_date();
        const auto sane = [](uint32_t x) { return x == 0 || (x >= 19900101 && x <= 21001231); };
        if ((!sane(d) || !sane(td)) && !viol_once_date_) {
            viol_once_date_ = true;
            char buf[96];
            std::snprintf(buf, sizeof(buf), "date=%u trading_date=%u", d, td);
            violation(ctx, "date_sane", buf);
        }
    }

    void check_stuck_orders(IStrategyContext* ctx) {
        if (stuck_timeout_ticks_ <= 0) return;
        for (auto it = inflight_.begin(); it != inflight_.end(); ) {
            const Inflight& o = it->second;
            const bool fast_stuck = o.expect_fast_terminal &&
                (tick_seq_ - o.submit_tick_seq) > stuck_timeout_ticks_;

            const bool cancel_stuck = !fast_stuck && o.cancel_first_tick >= 0 &&
                (tick_seq_ - o.cancel_first_tick) > stuck_timeout_ticks_;
            if (fast_stuck || cancel_stuck) {

                if (fast_stuck) ++stuck_terminal_soft_total_; else ++stuck_cancel_soft_total_;
                char buf[176];
                std::snprintf(buf, sizeof(buf),
                    "conf STUCK-EVICT id=%llu sc=%d age_ticks=%lld (%s; soft, viol after grace)",
                    (unsigned long long)o.id, o.scenario,
                    (long long)(tick_seq_ - o.submit_tick_seq),
                    fast_stuck ? "IOC/FOK/cancelled never terminalized"
                               : "cancel requested, never terminalized");
                ctx->log_warn(buf);

                char dump[320];
                std::snprintf(dump, sizeof(dump),
                    "conf STUCK-EVICT-DUMP id=%llu acked=%d cancel_req=%d submit_us=%lld "
                    "ack_us=%lld cancel_us=%lld leaves_fp=%lld cum_fill_fp=%lld upd_fill_fp=%lld "
                    "tif=%d offset=%d",
                    (unsigned long long)o.id, o.first_ack_us != 0 ? 1 : 0,
                    o.cancel_first_tick >= 0 ? 1 : 0,
                    (long long)o.submit_us, (long long)o.first_ack_us, (long long)o.cancel_req_us,
                    (long long)(o.qty_fp - o.cum_fill_fp), (long long)o.cum_fill_fp,
                    (long long)o.cum_upd_fill_fp, (int)o.tif, (int)o.offset);
                ctx->log_warn(dump);

                TerminalRec rec;
                rec.qty_fp = o.qty_fp; rec.cum_onfill_fp = o.cum_fill_fp;
                rec.terminal_tick = tick_seq_; rec.tif = o.tif; rec.offset = o.offset;
                rec.stuck = true; rec.stuck_fast = fast_stuck;
                rec.stuck_evict_us = now_us();
                rec.overfill_flagged = o.overfill_flagged;
                recent_terminal_[o.id] = rec;
                it = inflight_.erase(it);
            } else {
                ++it;
            }
        }
    }

    void purge_recent_terminals_(IStrategyContext* ctx) {
        const int64_t now = now_us();
        for (auto it = recent_terminal_.begin(); it != recent_terminal_.end(); ) {
            const TerminalRec& rec = it->second;
            const int64_t age_ticks = tick_seq_ - rec.terminal_tick;
            if (age_ticks <= recent_ttl_ticks_) { ++it; continue; }
            if (rec.stuck && !rec.got_late_terminal) {
                const bool wall_grace = stuck_viol_grace_ms_ > 0 &&
                    now - rec.stuck_evict_us >= stuck_viol_grace_ms_ * 1000;
                const bool tick_grace =
                    age_ticks >= kStuckViolGraceTickMult * stuck_timeout_ticks_;
                if (!wall_grace && !tick_grace) { ++it; continue; }
            }
            finalize_terminal_(ctx, it->first, rec);
            it = recent_terminal_.erase(it);
        }
    }

    void finalize_terminal_(IStrategyContext* ctx, uint64_t id, const TerminalRec& rec) {
        if (rec.stuck && !rec.got_late_terminal) {

            char sbuf[176];
            std::snprintf(sbuf, sizeof(sbuf),
                "id=%llu no terminal after grace (evict_age_ticks=%lld grace_ms=%lld)",
                (unsigned long long)id,
                (long long)(tick_seq_ - rec.terminal_tick),
                (long long)stuck_viol_grace_ms_);
            violation(ctx, rec.stuck_fast ? "stuck_terminal" : "stuck_cancel", sbuf);
            return;
        }
        char buf[200];
        if (rec.status == XtOrderStatus::Filled &&
            !fp_near(rec.terminal_filled_fp, rec.qty_fp, kQtyEpsFp)) {

            const bool reduce_only_amend_down =
                !venue_is_ctp_ && rec.offset == XtOffset::Close &&
                rec.terminal_filled_fp < rec.qty_fp;
            if (reduce_only_amend_down) {
                ++reduce_only_amend_down_total_;
            } else {
                std::snprintf(buf, sizeof(buf), "id=%llu filled_fp=%lld qty_fp=%lld",
                              (unsigned long long)id, (long long)rec.terminal_filled_fp,
                              (long long)rec.qty_fp);
                violation(ctx, "filled_qty_mismatch", buf);
            }
        }
        if (rec.tif == XtTimeInForce::FOK && rec.status != XtOrderStatus::Rejected &&
            rec.terminal_filled_fp != 0 &&
            !fp_near(rec.terminal_filled_fp, rec.qty_fp, kQtyEpsFp)) {
            std::snprintf(buf, sizeof(buf), "id=%llu filled_fp=%lld qty_fp=%lld",
                          (unsigned long long)id, (long long)rec.terminal_filled_fp,
                          (long long)rec.qty_fp);
            violation(ctx, "fok_partial", buf);
        }

        if (!fp_near(rec.cum_onfill_fp, rec.terminal_filled_fp, kQtyEpsFp)) {
            std::snprintf(buf, sizeof(buf), "id=%llu onfill_sum_fp=%lld terminal_filled_fp=%lld",
                          (unsigned long long)id, (long long)rec.cum_onfill_fp,
                          (long long)rec.terminal_filled_fp);

            violation(ctx, rec.had_terminal_conflict ? "fill_sum_mismatch_conflict"
                                                     : "fill_sum_mismatch", buf);
        }
    }

    void check_overfill_(IStrategyContext* ctx, uint64_t id, int64_t cum_fp, int64_t qty_fp,
                         bool& flagged) {
        if (flagged || cum_fp <= qty_fp + kQtyEpsFp) return;
        flagged = true;
        char buf[160];
        std::snprintf(buf, sizeof(buf), "id=%llu cum_fill_fp=%lld qty_fp=%lld",
                      (unsigned long long)id, (long long)cum_fp, (long long)qty_fp);
        violation(ctx, "overfill", buf);
    }

    void check_book_leaves_(IStrategyContext* ctx, Inflight& o) {
        if (!s_book_oracle_ || o.qty_fp <= 0 || o.book_viol_flagged) return;
        const int64_t cum = std::max(o.cum_upd_fill_fp, o.cum_fill_fp);

        const int64_t basis = o.venue_qty_fp > 0 ? o.venue_qty_fp : o.qty_fp;
        const int64_t expected = basis > cum ? basis - cum : 0;
        if (expected <= 0) return;
        XtOrderExecUpdate ob[kOrphanSweepBuf];
        const size_t n = ctx->get_open_orders(symbol_.c_str(), ob, kOrphanSweepBuf);
        if (n >= kOrphanSweepBuf) return;
        for (size_t i = 0; i < n; ++i) {
            if (ob[i].id != o.id) continue;
            if (ob[i].leaves_fp != expected) {

                if (!venue_is_ctp_ && o.offset == XtOffset::Close &&
                    ob[i].qty_fp > 0 && ob[i].qty_fp < basis) {
                    o.book_viol_flagged = true;
                    ++book_amend_down_total_;
                    return;
                }
                o.book_viol_flagged = true;
                char buf[200];
                std::snprintf(buf, sizeof(buf),
                              "id=%llu book_leaves=%lld expected=%lld qty=%lld upd_cum=%lld fill_cum=%lld",
                              (unsigned long long)o.id, (long long)ob[i].leaves_fp,
                              (long long)expected, (long long)o.qty_fp,
                              (long long)o.cum_upd_fill_fp, (long long)o.cum_fill_fp);
                violation(ctx, "book_leaves_drift", buf);
            }
            return;
        }
        o.book_viol_flagged = true;
        char buf[160];
        std::snprintf(buf, sizeof(buf), "id=%llu expected_leaves=%lld qty=%lld",
                      (unsigned long long)o.id, (long long)expected, (long long)o.qty_fp);
        violation(ctx, "book_order_lost", buf);
    }

    void on_order_not_inflight_(IStrategyContext* ctx, const XtOrderExecUpdate& u) {
        auto rt = recent_terminal_.find(u.id);
        if (rt == recent_terminal_.end()) { ++unknown_update_total_; return; }
        TerminalRec& rec = rt->second;
        if (order_status_is_final(u.status)) {
            if (rec.stuck && !rec.got_late_terminal) {

                rec.got_late_terminal = true;
                rec.status = u.status;
                rec.terminal_filled_fp = u.filled_fp;
                ++stuck_late_terminal_total_;
            } else if (u.status == rec.status) {

                char buf[120];
                std::snprintf(buf, sizeof(buf), "id=%llu status=%d repeated",
                              (unsigned long long)u.id, (int)u.status);
                violation(ctx, "double_terminal", buf);
            } else {

                ++terminal_conflict_total_;
                rec.had_terminal_conflict = true;
                if (u.filled_fp > rec.terminal_filled_fp) {
                    rec.terminal_filled_fp = u.filled_fp;
                    rec.status = u.status;
                }
            }
        } else {
            ++late_update_after_terminal_;
        }
    }

    void check_reject_storm_(IStrategyContext* ctx) {
        if (reject_storm_window_ <= 0 || window_orders_ < reject_storm_window_) return;
        const int64_t pct = window_unexp_ * 100 / window_orders_;
        if (pct >= reject_storm_pct_) {
            char buf[160];
            std::snprintf(buf, sizeof(buf), "window=%lld unexp=%lld pct=%lld",
                          (long long)window_orders_, (long long)window_unexp_, (long long)pct);
            violation(ctx, "reject_storm", buf);
        }
        window_orders_ = 0;
        window_unexp_  = 0;
    }

    void probe_hit_(int scenario) noexcept {
        switch (scenario) {
            case SC_RISK_PROBE:          ++probe_qty_hits_;      break;
            case SC_RISK_PROBE_POS:      ++probe_pos_hits_;      break;
            case SC_RISK_PROBE_NOTIONAL: ++probe_notional_hits_; break;
            default: break;
        }
    }

    void run_audit_machine_(IStrategyContext* ctx, const XtTick& tk) {
        switch (audit_state_) {
        case AuditState::Normal:
            if (tick_seq_ - last_audit_end_tick_ < audit_interval_ticks_) return;
            audit_state_ = AuditState::Drain;
            audit_active_ticks_ = 0;
            flatten(ctx, tk, "audit");
            return;
        case AuditState::Drain: {
            if (!ctx->can_submit()) return;
            ++audit_active_ticks_;
            if ((audit_active_ticks_ % kAuditDrainFlattenTicks) == 0) flatten(ctx, tk, "audit");
            const int64_t confirmed = confirmed_pos_(ctx);
            if (inflight_.empty() && llabs_(confirmed) <= pos_eps_fp()) {
                audit_state_ = AuditState::Settle;
                settle_left_ = audit_settle_ticks_;
                return;
            }
            audit_timeout_check_(ctx, confirmed);
            return;
        }
        case AuditState::Settle: {
            if (!ctx->can_submit()) return;
            ++audit_active_ticks_;
            const int64_t confirmed = confirmed_pos_(ctx);
            if (!inflight_.empty() || llabs_(confirmed) > pos_eps_fp()) {
                audit_state_ = AuditState::Drain;
                return;
            }
            if (--settle_left_ > 0) { audit_timeout_check_(ctx, confirmed); return; }
            audit_settle_asserts_(ctx);
            audit_end_( true);
            return;
        }
        }
    }

    void audit_timeout_check_(IStrategyContext* ctx, int64_t confirmed) {
        if (audit_timeout_ticks_ <= 0 || audit_active_ticks_ <= audit_timeout_ticks_) return;
        char buf[200];
        std::snprintf(buf, sizeof(buf),
            "drain not quiesced in %lld active ticks (pos_fp=%lld inflight=%zu) — flatten/cancel 链路疑似卡死",
            (long long)audit_active_ticks_, (long long)confirmed, inflight_.size());
        violation(ctx, "audit_quiesce_timeout", buf);
        audit_end_( false);
    }

    void audit_settle_asserts_(IStrategyContext* ctx) {
        char buf[200];
        const int64_t projected = ctx->projected_inflight_fp(symbol_.c_str());
        if (projected != 0) {
            std::snprintf(buf, sizeof(buf), "projected_fp=%lld with empty inflight",
                          (long long)projected);
            violation(ctx, "projected_nonzero", buf);
        }
        const int64_t confirmed = confirmed_pos_(ctx);
        if (llabs_(confirmed) > pos_eps_fp()) {
            std::snprintf(buf, sizeof(buf), "pos_fp=%lld after flatten quiesce",
                          (long long)confirmed);
            violation(ctx, "flatten_converge", buf);
        }
        const int64_t drift = confirmed - self_pos_fp_;
        if (llabs_(drift) > pos_eps_fp()) {

            const bool recovery_in_window = last_channel_ready_tick_ > last_audit_end_tick_;
            if (audit_cycles_ == 0 && !audit_grace_used_) {

                audit_grace_used_ = true;
                ++audit_grace_realign_total_;
            } else if (recovery_in_window) {
                ++recovery_drift_realign_total_;
            } else {
                std::snprintf(buf, sizeof(buf),
                    "confirmed_fp=%lld self_fp=%lld drift_fp=%lld at quiesce",
                    (long long)confirmed, (long long)self_pos_fp_, (long long)drift);
                violation(ctx, "pos_drift_quiesce", buf);
            }
        }
        self_pos_fp_ = confirmed;
    }

    void audit_end_(bool completed) {
        if (completed) ++audit_cycles_;
        audit_state_ = AuditState::Normal;
        last_audit_end_tick_ = tick_seq_;
    }

    int64_t confirmed_pos_(IStrategyContext* ctx) {
        return ctx->get_position_fp(symbol_.c_str()) -
               ctx->projected_inflight_fp(symbol_.c_str());
    }

    void observe_drift_(IStrategyContext* ctx) {
        const int64_t real_pos = confirmed_pos_(ctx);
        last_pos_drift_fp_ = real_pos - self_pos_fp_;
        const int64_t ad = llabs_(last_pos_drift_fp_);
        if (ad > max_abs_drift_fp_) max_abs_drift_fp_ = ad;
        if (ad > pos_eps_fp()) ++pos_desync_ticks_;
    }

    void flatten(IStrategyContext* ctx, const XtTick& tk, const char* why) {

        const int64_t pos = confirmed_pos_(ctx);
        const bool close_is_buy = pos < 0;

        int64_t close_outstanding_fp = 0;
        for (auto& kv : inflight_) {
            Inflight& o = kv.second;
            if (o.scenario == -2 && o.is_buy == close_is_buy) close_outstanding_fp += o.qty_fp;
            request_cancel_(ctx, kv.first, o);
        }

        sweep_orphan_orders_(ctx);

        if (pos != 0) {
            const int64_t px = close_is_buy ? aggressive_buy_px(tk) : aggressive_sell_px(tk);
            if (!venue_is_ctp_ || px > 0) {
                const int64_t total_abs = pos < 0 ? -pos : pos;
                const int64_t to_close = total_abs - close_outstanding_fp;
                const int64_t chunk = flatten_chunk_qty_fp_ > 0 ? flatten_chunk_qty_fp_ : to_close;
                int64_t remaining = to_close;
                for (int n2 = 0; n2 < kMaxFlattenChunks && remaining > 0; ++n2) {
                    const int64_t q = remaining < chunk ? remaining : chunk;
                    submit(ctx, -2  , close_is_buy, px, q,
                           XtTimeInForce::IOC, XtOffset::Close, "flatten", false, true);
                    remaining -= q;
                }
            }
        }
        char buf[176];
        std::snprintf(buf, sizeof(buf), "conf FLATTEN(%s) pos_fp=%lld proj_fp=%lld inflight=%zu",
                      why, (long long)pos,
                      (long long)ctx->projected_inflight_fp(symbol_.c_str()),
                      inflight_.size());
        ctx->log_info(buf);
    }

    void sweep_orphan_orders_(IStrategyContext* ctx) {
        XtOrderExecUpdate ob[kOrphanSweepBuf];
        const size_t n = ctx->get_open_orders(symbol_.c_str(), ob, kOrphanSweepBuf);
        int sweep_budget = kOrphanSweepPerFlatten;
        for (size_t i = 0; i < n && sweep_budget > 0; ++i) {
            const uint64_t oid = ob[i].id;
            if (oid == 0 || inflight_.count(oid) != 0) continue;

            if (orphan_cancel_count_[oid] >= kOrphanCancelGiveUp) continue;
            auto it = orphan_cancel_tick_.find(oid);
            if (it != orphan_cancel_tick_.end() &&
                (tick_seq_ - it->second) <= kCancelRetryCooldownTicks) continue;
            ctx->cancel(oid);
            orphan_cancel_tick_[oid] = tick_seq_;
            ++cancels_req_total_;
            ++orphan_cancel_total_;
            --sweep_budget;
            if (++orphan_cancel_count_[oid] == kOrphanCancelGiveUp) {
                ++orphan_giveup_total_;
                char gbuf[128];
                std::snprintf(gbuf, sizeof(gbuf),
                    "conf ORPHAN-GIVEUP id=%llu after %d cancels (no terminal; engine reaper/B3/B4 backstop)",
                    (unsigned long long)oid, kOrphanCancelGiveUp);
                ctx->log_warn(gbuf);
            }
        }
        if (orphan_cancel_tick_.size() > 4096) {
            orphan_cancel_tick_.clear();
            orphan_cancel_count_.clear();
        }
    }

    void heartbeat(IStrategyContext* ctx) {
        ++hb_seq_;
        if (s_userdata_) persist_hb_seq_(ctx);
        if (s_probe_cancel_unknown_) { ctx->cancel(kBogusOrderId); ++probe_cancel_unknown_total_; }
        report_count_metrics_(ctx);
        heartbeat_counts_line_(ctx);
        heartbeat_latency_line_(ctx);
    }

    void report_count_metrics_(IStrategyContext* ctx) {
        ctx->report_metric("conf_orders_total",      (double)orders_total_);
        ctx->report_metric("conf_fills_total",       (double)fills_total_);
        ctx->report_metric("conf_cancels_total",     (double)cancels_req_total_);
        ctx->report_metric("conf_terminal_total",    (double)terminal_total_);
        ctx->report_metric("conf_rejects_expected",  (double)rejects_expected_);
        ctx->report_metric("conf_rejects_unexpected",(double)rejects_unexpected_);
        ctx->report_metric("conf_rejects_close_local",(double)rejects_close_local_);
        ctx->report_metric("conf_rejects_gated",   (double)rejects_gated_);
        ctx->report_metric("conf_violation_total",   (double)violations_total_);

        ctx->report_metric("conf_hard_violation_total", (double)hard_violations_total_);
        for (const auto& kv : hard_viol_by_type_)
            ctx->report_metric(kv.first.c_str(), (double)kv.second);
        ctx->report_metric("conf_inflight_now",      (double)inflight_.size());
        ctx->report_metric("conf_net_pos",           order_exec_from_fp(self_pos_fp_));
        ctx->report_metric("conf_unknown_update",    (double)unknown_update_total_);
        ctx->report_metric("conf_unknown_fill",      (double)unknown_fill_total_);
        ctx->report_metric("conf_account_update",    (double)account_update_total_);
        ctx->report_metric("conf_pos_desync_ticks", (double)pos_desync_ticks_);
        ctx->report_metric("conf_max_abs_drift",    order_exec_from_fp(max_abs_drift_fp_));
        ctx->report_metric("conf_orphan_cancels",   (double)orphan_cancel_total_);
        ctx->report_metric("conf_orphan_giveup",    (double)orphan_giveup_total_);
        ctx->report_metric("conf_budget_skips",     (double)budget_skip_total_);
        ctx->report_metric("conf_audit_cycles",     (double)audit_cycles_);
        ctx->report_metric("conf_probe_qty_hits",   (double)probe_qty_hits_);
        ctx->report_metric("conf_probe_pos_hits",   (double)probe_pos_hits_);
        ctx->report_metric("conf_probe_notional_hits", (double)probe_notional_hits_);
        ctx->report_metric("conf_probe_miss",       (double)probe_miss_total_);
        ctx->report_metric("conf_params_updates",   (double)params_update_total_);
        ctx->report_metric("conf_channel_ready",    (double)channel_ready_total_);
        ctx->report_metric("conf_channel_lost",     (double)channel_lost_total_);
        ctx->report_metric("conf_fill_after_cancel",(double)fill_after_cancel_total_);
        ctx->report_metric("conf_overclose_clamped",(double)self_overclose_clamped_);
        ctx->report_metric("conf_terminal_conflict",(double)terminal_conflict_total_);
        ctx->report_metric("conf_md_crossed",       (double)md_crossed_total_);
        ctx->report_metric("conf_md_nonmono",       (double)md_nonmono_total_);
        ctx->report_metric("conf_md_one_sided",     (double)md_one_sided_total_);
        ctx->report_metric("conf_reduce_only_amend_down", (double)reduce_only_amend_down_total_);
        ctx->report_metric("conf_book_amend_down",        (double)book_amend_down_total_);
        ctx->report_metric("conf_recovery_drift_realign", (double)recovery_drift_realign_total_);
        ctx->report_metric("conf_ticks_order",      (double)ticks_order_total_);
        ctx->report_metric("conf_stuck_terminal_soft",    (double)stuck_terminal_soft_total_);
        ctx->report_metric("conf_stuck_cancel_soft",      (double)stuck_cancel_soft_total_);
        ctx->report_metric("conf_stuck_late_terminal",    (double)stuck_late_terminal_total_);
    }

    void heartbeat_counts_line_(IStrategyContext* ctx) {

        char buf[768];
        std::snprintf(buf, sizeof(buf),
            "conf HB tick=%lld ord=%llu fill=%llu cxl=%llu term=%llu "
            "rej[exp=%llu unexp=%llu gated=%llu closeL=%llu] viol=%llu desync=%llu maxdrift_fp=%lld "
            "inflight=%zu pos_fp=%lld drift_fp=%lld orph=%llu bskip=%llu "
            "audit[c=%llu st=%d] probe[q=%llu p=%llu n=%llu miss=%llu] "
            "uf=%llu fac=%llu oc=%llu tc=%llu sess[b=%llu e=%llu] roll=%llu acct=%llu params=%llu ch[r=%llu l=%llu] "
            "md[crossed=%llu nonmono=%llu onesided=%llu] rdamend=%llu rdrift=%llu "
            "lock_fp=%lld",
            (long long)tick_seq_, (unsigned long long)orders_total_, (unsigned long long)fills_total_,
            (unsigned long long)cancels_req_total_, (unsigned long long)terminal_total_,
            (unsigned long long)rejects_expected_, (unsigned long long)rejects_unexpected_,
            (unsigned long long)rejects_gated_, (unsigned long long)rejects_close_local_,
            (unsigned long long)violations_total_, (unsigned long long)pos_desync_ticks_,
            (long long)max_abs_drift_fp_, inflight_.size(),
            (long long)self_pos_fp_, (long long)last_pos_drift_fp_,
            (unsigned long long)orphan_cancel_total_, (unsigned long long)budget_skip_total_,
            (unsigned long long)audit_cycles_, (int)audit_state_,
            (unsigned long long)probe_qty_hits_, (unsigned long long)probe_pos_hits_,
            (unsigned long long)probe_notional_hits_, (unsigned long long)probe_miss_total_,
            (unsigned long long)unknown_fill_total_, (unsigned long long)fill_after_cancel_total_,
            (unsigned long long)self_overclose_clamped_, (unsigned long long)terminal_conflict_total_,
            (unsigned long long)session_begin_total_, (unsigned long long)session_end_total_,
            (unsigned long long)roll_total_, (unsigned long long)account_update_total_,
            (unsigned long long)params_update_total_,
            (unsigned long long)channel_ready_total_, (unsigned long long)channel_lost_total_,
            (unsigned long long)md_crossed_total_, (unsigned long long)md_nonmono_total_,
            (unsigned long long)md_one_sided_total_,
            (unsigned long long)reduce_only_amend_down_total_,
            (unsigned long long)recovery_drift_realign_total_,
            (long long)(self_long_fp_ < self_short_fp_ ? self_long_fp_ : self_short_fp_));
        ctx->log_info(buf);
    }

    void heartbeat_latency_line_(IStrategyContext* ctx) {
        const int64_t now = now_us();
        double ticks_per_s = 0.0, orders_per_s = 0.0;
        if (last_hb_us_ > 0 && now > last_hb_us_) {
            const double win_s = (double)(now - last_hb_us_) / 1e6;
            ticks_per_s  = (double)(tick_seq_ - last_hb_ticks_) / win_s;
            orders_per_s = (double)(orders_total_ - last_hb_orders_) / win_s;
        }
        last_hb_us_ = now; last_hb_ticks_ = tick_seq_; last_hb_orders_ = orders_total_;
        char buf[512];
        std::snprintf(buf, sizeof(buf),
            "conf LAT ack_us[p50=%lld p95=%lld p99=%lld max=%lld n=%llu] "
            "fterm_us[p99=%lld max=%lld n=%llu] cxl_us[p99=%lld max=%lld n=%llu] "
            "gap_us[p99=%lld max=%lld] ontick_us[p99=%lld max=%lld] "
            "rate[tick_s=%.1f ord_s=%.1f]",
            (long long)lat_ack_.pct(50), (long long)lat_ack_.pct(95),
            (long long)lat_ack_.pct(99), (long long)lat_ack_.max_us,
            (unsigned long long)lat_ack_.n,
            (long long)lat_fast_term_.pct(99), (long long)lat_fast_term_.max_us,
            (unsigned long long)lat_fast_term_.n,
            (long long)lat_cancel_.pct(99), (long long)lat_cancel_.max_us,
            (unsigned long long)lat_cancel_.n,
            (long long)lat_tick_gap_.pct(99), (long long)lat_tick_gap_.max_us,
            (long long)lat_on_tick_.pct(99), (long long)lat_on_tick_.max_us,
            ticks_per_s, orders_per_s);
        ctx->log_info(buf);
        ctx->report_metric("conf_lat_ack_p50_us",   (double)lat_ack_.pct(50));
        ctx->report_metric("conf_lat_ack_p99_us",   (double)lat_ack_.pct(99));
        ctx->report_metric("conf_lat_fterm_p99_us", (double)lat_fast_term_.pct(99));
        ctx->report_metric("conf_lat_cancel_p99_us",(double)lat_cancel_.pct(99));
        ctx->report_metric("conf_tick_gap_p99_us",  (double)lat_tick_gap_.pct(99));
        ctx->report_metric("conf_on_tick_p99_us",   (double)lat_on_tick_.pct(99));
        ctx->report_metric("conf_ticks_per_sec",    ticks_per_s);
        ctx->report_metric("conf_orders_per_sec",   orders_per_s);
    }

    void run_userdata_init_checks_(IStrategyContext* ctx) {

        ctx->save_userdata("conf_probe", "v1");
        const char* got = ctx->load_userdata("conf_probe", "");
        if (!got || std::strcmp(got, "v1") != 0) {
            violation(ctx, "userdata", "save_userdata/load_userdata round-trip mismatch");
        }

        const char* prev = ctx->load_userdata("conf_hb_seq", "");
        if (prev && prev[0]) {
            char* end = nullptr;
            const unsigned long long v = std::strtoull(prev, &end, 10);
            if (end == prev || (end && *end != '\0')) {
                violation(ctx, "userdata_corrupt", "conf_hb_seq not a number");
            } else {
                resume_prev_seq_ = (uint64_t)v;
                char buf[96];
                std::snprintf(buf, sizeof(buf), "conf RESUME prev_hb_seq=%llu", v);
                ctx->log_info(buf);
            }
        }
    }

    void persist_hb_seq_(IStrategyContext* ctx) {
        char b[24];
        std::snprintf(b, sizeof(b), "%llu", (unsigned long long)hb_seq_);
        ctx->save_userdata("conf_hb_seq", b);
    }

    static bool is_soft_violation_(const char* inv) noexcept {

        return std::strcmp(inv, "reject_storm") == 0 ||
               std::strcmp(inv, "fill_sum_mismatch_conflict") == 0;
    }

    void violation(IStrategyContext* ctx, const char* inv, const char* detail) {
        ++violations_total_;
        if (!is_soft_violation_(inv)) {
            ++hard_violations_total_;
            ++hard_viol_by_type_[std::string("conf_viol_") + inv];
        }
        char buf[360];
        std::snprintf(buf, sizeof(buf), "CONF VIOLATION [%s] %s", inv, detail);
        ctx->log_warn(buf);
        ctx->report_metric("conf_violation_total", (double)violations_total_);
    }

    bool oscillation_side(int64_t pos) noexcept {

        if (pos >= max_pos_qty_abs_fp_)  return false;
        if (pos <= -max_pos_qty_abs_fp_) return true;
        return (rotor_++ & 1) == 0;
    }

    static int64_t llabs_(int64_t v) noexcept { return v < 0 ? -v : v; }

    void apply_self_fill_(const XtFillEvent& f) noexcept {

        if (!f.is_close()) {
            (f.is_buy ? self_long_fp_ : self_short_fp_) += f.qty_fp;
        } else {
            int64_t& leg = f.is_buy ? self_short_fp_ : self_long_fp_;
            leg = leg > f.qty_fp ? leg - f.qty_fp : 0;
        }
        const int64_t signed_qty = f.is_buy ? f.qty_fp : -f.qty_fp;
        if (!f.is_close()) { self_pos_fp_ += signed_qty; return; }
        const int64_t before = self_pos_fp_;
        const int64_t after  = before + signed_qty;
        const bool crosses = (before > 0 && after < 0) || (before < 0 && after > 0);
        if (crosses || (before == 0 && signed_qty != 0)) {
            ++self_overclose_clamped_;
            self_pos_fp_ = 0;
        } else {
            self_pos_fp_ = after;
        }
    }

    static bool fp_near(int64_t a, int64_t b, int64_t eps) noexcept {
        const int64_t d = a > b ? a - b : b - a;
        return d <= eps;
    }
    int64_t pos_eps_fp() const noexcept {

        const int64_t q = order_exec_to_fp(qty_);
        return q > 1 ? q / 2 : 1;
    }

    bool order_budget_ok(int64_t now_ms) noexcept {
        if (max_orders_per_min_ <= 0) return true;
        const int64_t per_sec = max_orders_per_min_ >= 60 ? max_orders_per_min_ / 60 : 1;
        const int64_t bucket = now_ms / 1000;
        if (bucket != budget_sec_bucket_) { budget_sec_bucket_ = bucket; budget_sec_count_ = 0; }
        return budget_sec_count_ < per_sec;
    }
    void consume_order_budget() noexcept { ++budget_sec_count_; }

    static int64_t packed_hms_to_ms(uint32_t t) {
        const int hh  = static_cast<int>(t / 10000000);
        const int mm  = static_cast<int>((t / 100000) % 100);
        const int ss  = static_cast<int>((t / 1000) % 100);
        const int mmm = static_cast<int>(t % 1000);
        return (static_cast<int64_t>((hh * 60 + mm) * 60 + ss)) * 1000 + mmm;
    }

    static int64_t now_us() noexcept {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    std::string symbol_;
    bool        venue_is_ctp_ = true;
    double      qty_          = 1.0;
    int64_t     warmup_ms_    = 3000;
    int64_t     warmup_ticks_ = 5000;
    int64_t     max_total_orders_ = 200000;
    int         decision_interval_ticks_ = 1;
    int         fire_per_tick_   = 4;
    int         fill_per_tick_   = 0;
    int         flatten_interval_ticks_ = 400;
    int         heartbeat_ticks_ = 2000;
    int         stuck_timeout_ticks_ = 1000;
    int64_t     stuck_viol_grace_ms_ = 40000;
    int64_t     far_offset_ticks_ = 100;
    int64_t     max_orders_per_min_ = 0;
    int64_t     max_pos_qty_abs_fp_ = 0;
    int64_t     flatten_chunk_qty_fp_ = 0;
    int64_t     risk_probe_qty_fp_ = 0;
    int64_t     audit_interval_ticks_ = 4000;
    int64_t     audit_timeout_ticks_ = 3000;
    int64_t     audit_settle_ticks_ = 50;
    int64_t     recent_ttl_ticks_ = 600;
    int64_t     reject_storm_window_ = 200;
    int64_t     reject_storm_pct_ = 50;
    int64_t     risk_probe_pos_qty_fp_ = 0;
    int64_t     risk_probe_notional_qty_fp_ = 0;
    int64_t     probe_every_decisions_ = 32;
    int64_t     cancel_retry_ticks_ = kCancelRetryCooldownTicks;
    bool s_rapid_submit_ = true, s_cancel_storm_ = true, s_market_ioc_ = true, s_flip_ = true;
    bool s_tif_matrix_ = true, s_risk_probe_ = true, s_query_sweep_ = true, s_userdata_ = true;
    bool s_risk_probe_pos_ = false, s_risk_probe_notional_ = false;
    bool s_probe_cancel_unknown_ = true;
    bool s_md_oracle_ = true;
    bool s_book_oracle_ = true;

    int64_t  first_ms_   = -1;
    int64_t  tick_seq_   = 0;
    bool     inside_on_tick_ = false;
    int      scenario_cursor_ = 0;
    int      tif_cursor_ = 0;
    unsigned rotor_ = 0;
    unsigned sweep_rotor_ = 0;
    int64_t  self_pos_fp_ = 0;
    int64_t  self_long_fp_ = 0;
    int64_t  self_short_fp_ = 0;
    int64_t  last_pos_drift_fp_ = 0;
    uint64_t pos_desync_ticks_ = 0;
    int64_t  max_abs_drift_fp_ = 0;
    bool     self_pos_anchored_ = false;
    int64_t  cached_price_tick_fp_ = 0;
    int64_t  budget_sec_bucket_ = -1;
    int      budget_sec_count_  = 0;
    std::unordered_map<uint64_t, int64_t> orphan_cancel_tick_;
    std::unordered_map<uint64_t, int> orphan_cancel_count_;

    AuditState audit_state_ = AuditState::Normal;
    int64_t  audit_active_ticks_ = 0;
    int64_t  settle_left_ = 0;
    int64_t  last_audit_end_tick_ = 0;
    uint64_t audit_cycles_ = 0;
    bool     audit_grace_used_ = false;

    int64_t  window_orders_ = 0;
    int64_t  window_unexp_  = 0;

    unsigned probe_pacer_q_ = 0, probe_pacer_p_ = 0, probe_pacer_n_ = 0;

    int64_t  prev_tick_us_ = 0;
    int64_t  last_hb_us_ = 0;
    int64_t  last_hb_ticks_ = 0;
    uint64_t last_hb_orders_ = 0;
    LatHist  lat_ack_, lat_fast_term_, lat_cancel_, lat_tick_gap_, lat_on_tick_;

    std::unordered_map<uint64_t, Inflight>    inflight_;
    std::unordered_map<uint64_t, TerminalRec> recent_terminal_;

    uint64_t orders_total_ = 0, fills_total_ = 0, cancels_req_total_ = 0, terminal_total_ = 0;
    uint64_t rejects_expected_ = 0, rejects_unexpected_ = 0, rejects_close_local_ = 0, rejects_gated_ = 0;
    uint64_t violations_total_ = 0, orphan_cancel_total_ = 0, budget_skip_total_ = 0;
    uint64_t orphan_giveup_total_ = 0;
    uint64_t hard_violations_total_ = 0;

    std::unordered_map<std::string, uint64_t> hard_viol_by_type_;
    uint64_t unknown_update_total_ = 0, unknown_fill_total_ = 0, account_update_total_ = 0;
    uint64_t fill_after_cancel_total_ = 0, late_update_after_terminal_ = 0;
    uint64_t self_overclose_clamped_ = 0;
    uint64_t stuck_late_terminal_total_ = 0, terminal_conflict_total_ = 0;
    uint64_t stuck_terminal_soft_total_ = 0, stuck_cancel_soft_total_ = 0;
    uint64_t probe_qty_hits_ = 0, probe_pos_hits_ = 0, probe_notional_hits_ = 0;
    uint64_t probe_miss_total_ = 0, probe_cancel_unknown_total_ = 0;
    uint64_t audit_grace_realign_total_ = 0;
    uint64_t session_begin_total_ = 0, session_end_total_ = 0, roll_total_ = 0, params_update_total_ = 0;
    uint64_t channel_ready_total_ = 0, channel_lost_total_ = 0;
    uint64_t hb_seq_ = 0, resume_prev_seq_ = 0;
    uint32_t last_trading_date_ = 0;
    bool viol_once_ticks_order_ = false, viol_once_pos_detail_ = false, viol_once_date_ = false;

    uint64_t md_crossed_total_ = 0, md_nonmono_total_ = 0, md_one_sided_total_ = 0, md_touch_total_ = 0;
    uint64_t ticks_order_total_ = 0;
    uint64_t reduce_only_amend_down_total_ = 0, recovery_drift_realign_total_ = 0;
    uint64_t book_amend_down_total_ = 0;
    bool viol_once_md_crossed_ = false, viol_once_md_nonmono_ = false, viol_once_md_l1_ = false;
    int64_t last_channel_ready_tick_ = -1;

    static constexpr int64_t kSaneFloorFp = 1000000LL * 1000000000LL;
    static constexpr int64_t kQtyEpsFp = 1;
    static constexpr int64_t kCancelRetryCooldownTicks = 500;
    static constexpr int64_t kTicksOrderRecoveryWindowTicks = 2000;
    static constexpr int kOrphanCancelGiveUp = 12;
    static constexpr int64_t kStuckViolGraceTickMult   = 10;
    static constexpr int     kOrphanSweepPerFlatten = 8;
    static constexpr size_t  kOrphanSweepBuf = 64;
    static constexpr int     kMaxFlattenChunks = 64;
    static constexpr int64_t kAuditDrainFlattenTicks = 10;
    static constexpr int64_t kRecentPurgeMask = 63;
    static constexpr unsigned kSweepHeavyMask = 63;
    static constexpr unsigned kSweepDetailMask = 15;
    static constexpr int64_t kTickGapSkipUs = 60LL * 1000000;
    static constexpr uint64_t kBogusOrderId = kReservedProbeCancelOrderId;
};
