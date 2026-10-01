#pragma once
// -----------------------------------------------------------------------------
// HftMarketMakingStrategy — 经典 HFT 双边挂单做市策略（passive maker）
// -----------------------------------------------------------------------------
// [用途]
//   在 hft_tick_revert 之外提供第二个 HFT 策略,用于 control panel
//   多策略切换的端到端验证 + 工业级做市基准。
//
// [信号逻辑]
//   - 每 quote_throttle_ms 检查一次盘口,若与上次挂单价偏离 ≥ requote_ticks
//     或在途单集合为空,则全撤重挂。
//     drift 只由**本周期真正挂出**的一侧驱动: requote 周期开始即把 last_*_px_
//     清零,只有 submit 成功的一侧重新落价位。被 block(reduce-only)或被同步拒
//     (submit 返 0)的一侧 last px 归零 → 该侧 drift 项关闭,陈旧价不会随行情
//     走远而恒 ≥ requote_ticks(否则每个 throttle 周期全撤重挂 = requote 风暴,
//     且会把正常驻留的另一侧一并撤掉重进 match-delay)。
//     已知保留特性: 单侧成交/撤销后,缺失侧要等在场侧 drift 达标或在途集合清空
//     才补挂(修复前同样如此,本处只消除风暴,不改补挂时机)。
//   - bid 挂在 best_bid - bid_offset_ticks * tick_size
//     ask 挂在 best_ask + ask_offset_ticks * tick_size
//   - 库存偏斜（inventory skew）:
//       净多仓 > inventory_target → 收紧 ask 偏移、放宽 bid 偏移（鼓励卖出）
//       净空仓 < -inventory_target → 收紧 bid 偏移、放宽 ask 偏移（鼓励买入）
//   - 风控护栏:
//       |position| ≥ max_position 时停止开仓方向挂单（仅挂 reduce-only 一侧）
//       spread <= 0 或 spread > max_spread_ticks 时静默不挂（保护异常盘口）
//
// [换月]
//   委托 xt_rollover::migrate_gtc_orders + PositionAggregator,与 tick_revert
//   一致。在途 GTC 做市单 cancel + resubmit 到新合约。
//
// [设计原因]
//   - 选择 GTC + tick offset 而不是 IOC: maker 路径才有 queue_model 价值,
//     便于和 backtest queue_model 校准对照
//   - 选择 quote_throttle_ms 而不是逐 tick requote: binance 50-200ms ack
//     下逐 tick requote 会触发 cancel storm + frozen 不释放
//   - 选择 inventory skew 三档线性而不是连续函数: 在 hot path 内运行,
//     避免浮点 mul/div,所有偏移用 int 加减完成
// -----------------------------------------------------------------------------

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
        // tick_size_fp 显式注入: 不依赖 contract_info（部分 venue 缺失元数据）
        tick_size_fp_        = cfg.get_int   ("tick_size_fp",        100);  // 0.0001 for BTCUSDT
        // tick_size_fp_ 是 on_tick 三处整数除数（spread/drift ticks）；0/负配置 → 整数除零 SIGFPE
        // 或反相报价。init-only（on_params 不复钳），故此处一次性钳到正值，回退默认粒度。
        if (tick_size_fp_ <= 0) tick_size_fp_ = 100;
        debug_first_quotes_  = cfg.get_int   ("debug_first_quotes",  0);
    }

    void on_init(IStrategyContext* ctx) override {
        if (!symbol_.empty()) {
            ctx->sub_tick(symbol_.c_str());
            // B: seed positions_ with CONFIRMED (= projected - in-flight) so it stays
            // single-source vs on_position (confirmed); avoids jump-detector + double-count.
            positions_.on_update(symbol_.c_str(),
                ctx->get_position_fp(symbol_.c_str()) - ctx->projected_inflight_fp(symbol_.c_str()));
        }
    }

    // 热改七项: qty / *_offset_ticks / requote_ticks / quote_throttle_ms /
    //          max_position / inventory_target / max_spread_ticks
    // tick_size_fp_ 跟 symbol_ 是 init-only（价位粒度跟合约身份, 热改会让 in-flight 漂移）。
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

        // sanity check
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

    /// 自报能力位（HFT 做市策略 override on_tick / on_fill /
    /// on_order，需要 engine fan-out 回调；多策略 CTA 下静默失效 → Runner abort）。
    uint64_t capabilities() const noexcept override {
        return to_capability_bit(StrategyCapability::SubscribesTick)
             | to_capability_bit(StrategyCapability::RequiresOnFill)
             | to_capability_bit(StrategyCapability::RequiresOnOrder);
    }

    void on_tick(IStrategyContext* ctx, const XtTick& tk) override {
        if (symbol_.empty() || std::strcmp(tk.code, symbol_.c_str()) != 0) return;
        // on_position push→pull 兜底: 主动 pull 同步仓位 (覆盖 non-fill: 强平/ADL/reconcile)。
        sync_position_(ctx);
        if (!ctx->can_submit()) return;

        const int64_t best_bid = tk.bid_price_fp[0];
        const int64_t best_ask = tk.ask_price_fp[0];
        if (best_bid <= 0 || best_ask <= 0 || best_bid >= best_ask) return;  // 异常盘口

        // spread 护栏
        const int64_t spread_ticks = (best_ask - best_bid) / tick_size_fp_;
        if (spread_ticks > max_spread_ticks_) return;

        // throttle: 距上次 quote 决策不足 quote_throttle_ms,直接 return
        if (quote_throttle_ms_ > 0 && last_quote_at_ != 0) {
            const int32_t delta_ms = action_time_delta_ms(last_quote_at_, tk.action_time);
            if (delta_ms < quote_throttle_ms_) return;
        }

        // ── 敞口投影（挂单计入）──────────────────────────────────────────
        // 口径走 get_open_orders（全执行模式单一语义，含 Submitted 未 ack 与
        // cancel 在途单）分侧聚合 buy/sell leaves。仅在通过 throttle 的 quote
        // 决策 tick 执行（非逐 tick），单次为 book 派生查询 + 栈上 64 槽聚合。
        // @note live 未启用 per-sid 路由时 get_open_orders 返回该 code 全部
        //       在途单（含 panel manual / closeout 残单 / Recovered）——按
        //       账户真实敞口计入护栏，非仅本策略自己的单。
        // @note cancel 在途（已发未 ack）单的 leaves 计入 worst-case →
        //       requote 的 cancel-first 之后、ack 之前不叠挂新单（绝不在旧单
        //       存活时叠加敞口）；throttle 窗口 >> cancel ack 延迟时对报价
        //       节奏无实质影响。
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

        // ── 库存偏斜（inventory skew）─────────────────────────────────────
        // 三档线性偏斜（避免 hot path 浮点除法）:
        //   net_pos > inv_target + qty   → 偏斜 -1: bid offset+2, ask offset 不变
        //   net_pos < inv_target - qty   → 偏斜 +1: ask offset+2, bid offset 不变
        //   |net_pos - inv_target| <= qty → 偏斜  0: 对称报价
        // skew 用净额投影（双边对称挂单 net=0 → 输入与纯持仓一致；单边挂单
        // 残留时 skew 会把在途单计入库存预期——刻意的保守语义变化）。
        const double net_pos    = order_exec_from_fp(
            positions_.logical_fp() + buy_leaves_fp - sell_leaves_fp);
        const double diff       = net_pos - inventory_target_;
        int bid_skew = 0, ask_skew = 0;
        if      (diff >  qty_) bid_skew = 2;     // 多仓 → bid 后撤,鼓励卖出
        else if (diff < -qty_) ask_skew = 2;     // 空仓 → ask 后撤,鼓励买入

        // ── 风控护栏: max_position 触发 reduce-only ───────────────────────
        // block 判定用本方向 worst-case（confirmed + 同向 leaves），而非 net：
        // net 口径下对侧挂单会抵消本侧敞口，异步成交不对称时放行超限
        // （net/gross 语义歧义详见 BACKLOG T2.RISK-14）。
        const double worst_long  = order_exec_from_fp(
            positions_.logical_fp() + buy_leaves_fp);
        const double worst_short = order_exec_from_fp(
            positions_.logical_fp() - sell_leaves_fp);
        const bool block_long   = (worst_long  >=  max_position_);
        const bool block_short  = (worst_short <= -max_position_);

        // ── 计算目标价位 ──────────────────────────────────────────────────
        const int64_t target_bid_px = best_bid - static_cast<int64_t>(bid_offset_ticks_ + bid_skew) * tick_size_fp_;
        const int64_t target_ask_px = best_ask + static_cast<int64_t>(ask_offset_ticks_ + ask_skew) * tick_size_fp_;

        // ── requote 判定: 任何一边偏离 ≥ requote_ticks 或在途单缺失 ──────
        // last_*_px_ == 0 的一侧不参与 drift 判定（本周期未挂出 → 无参考价位）。
        const int64_t bid_drift_ticks = std::abs(target_bid_px - last_bid_px_) / tick_size_fp_;
        const int64_t ask_drift_ticks = std::abs(target_ask_px - last_ask_px_) / tick_size_fp_;
        const bool need_requote =
            in_flight_oids_.empty()                                       ||
            (last_bid_px_ != 0 && bid_drift_ticks >= requote_ticks_)      ||
            (last_ask_px_ != 0 && ask_drift_ticks >= requote_ticks_);
        if (!need_requote) return;

        // ── 全撤在途 maker 单 ────────────────────────────────────────────
        for (const auto oid : in_flight_oids_) ctx->cancel(oid);
        in_flight_oids_.clear();

        // 参考价位随周期作废: 下方只有 submit 成功的一侧重新落值。被 block
        // (reduce-only) 或被同步拒 (submit 返 0, 如 offset_policy close_first
        // 空仓时的 ask 腿) 的一侧保持 0 → 不再以陈旧价参与 drift 判定。
        last_bid_px_ = 0;
        last_ask_px_ = 0;

        // ── 重挂 ─────────────────────────────────────────────────────────
        const int64_t qty_fp = order_exec_to_fp(qty_);
        char reason_buf[64];

        if (!block_long) {
            std::snprintf(reason_buf, sizeof(reason_buf),
                "mm|side=bid|skew=%d|inv=%.4f", bid_skew, net_pos);
            ctx->set_next_order_reason(reason_buf);
            const uint64_t bid_oid = xt_rollover::submit_and_track(
                ctx, orders_, positions_, symbol_.c_str(), /*is_buy=*/true,
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
                ctx, orders_, positions_, symbol_.c_str(), /*is_buy=*/false,
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

    // on_position push→pull 改造 (plan on_position_pull_unification): 仓位通知改 strategy
    // 主动 pull。on_fill = 仓位变主因, fill 后即时 sync — account_store 已 fill-immediate,
    // 与原 on_position 同值同时刻 (backtest byte-identical)。
    void on_fill(IStrategyContext* ctx, const XtFillEvent& /*fill*/) override {
        sync_position_(ctx);
    }

    void on_order(IStrategyContext* ctx,
                  const XtOrderExecUpdate& upd) override {
        orders_.apply_update(upd);
        // 终态从在途集合移除
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

    /// @brief 透明换月: 委托 helper 做 GTC 单迁移 + 跨合约仓位跟踪。
    void on_roll(IStrategyContext* ctx, const char* old_code,
                 const char* new_code, uint32_t /*trading_date*/,
                 int64_t /*old_last_price_fp*/) override {
        xt_rollover::migrate_gtc_orders(ctx, orders_, old_code, new_code);
        positions_.touch(old_code);
        positions_.on_update(new_code,  // CONFIRMED base (= projected - in-flight)
            ctx->get_position_fp(new_code) - ctx->projected_inflight_fp(new_code));
        symbol_ = new_code;
        in_flight_oids_.clear();   // 旧合约 oid 已迁移, 重置等下次 tick 重挂
        last_bid_px_ = 0;
        last_ask_px_ = 0;
        ctx->sub_tick(new_code);
    }

private:
    /// @brief HHMMSSmmm action_time → 当日毫秒偏移。
    static uint32_t action_time_to_ms_of_day(uint32_t at) noexcept {
        const uint32_t hh = at / 10000000u;
        const uint32_t mm = (at / 100000u) % 100u;
        const uint32_t ss = (at / 1000u)   % 100u;
        const uint32_t ms = at             % 1000u;
        return hh * 3600000u + mm * 60000u + ss * 1000u + ms;
    }

    /// @brief 计算两 action_time 的毫秒差,跨午夜回绕处理。
    static int32_t action_time_delta_ms(uint32_t prev, uint32_t cur) noexcept {
        const int64_t cur_ms  = static_cast<int64_t>(action_time_to_ms_of_day(cur));
        const int64_t prev_ms = static_cast<int64_t>(action_time_to_ms_of_day(prev));
        int64_t delta = cur_ms - prev_ms;
        if (delta < 0) delta += 86'400'000;
        return static_cast<int32_t>(delta);
    }

    /// @brief on_position push→pull: 主动 pull 仓位 view (= get_position_fp − in-flight,
    /// 同 on_init/on_roll seed 口径)。替代原 on_position push。挂 on_fill (主) + on_tick (兜底)。
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
    int64_t tick_size_fp_       = 100;       ///< 显式注入,避免依赖 contract_info 元数据

    uint32_t last_quote_at_     = 0;         ///< 上次 quote 决策的 action_time
    int64_t  last_bid_px_       = 0;         ///< 最近一个 requote 周期成功挂出的 bid 价位 fp；0 = 未挂出，不参与 drift
    int64_t  last_ask_px_       = 0;         ///< 最近一个 requote 周期成功挂出的 ask 价位 fp；0 = 未挂出，不参与 drift
    std::unordered_set<uint64_t> in_flight_oids_;  ///< 在途 maker 订单集合

    int debug_first_quotes_     = 0;
    int debug_quotes_emitted_   = 0;
    int quotes_emitted_         = 0;
};
