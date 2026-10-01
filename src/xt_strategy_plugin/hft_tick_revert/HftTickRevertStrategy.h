#pragma once
// -----------------------------------------------------------------------------
// HftTickRevertStrategy — 最简 HFT 反转策略
// -----------------------------------------------------------------------------
// [用途]
//   最简 HFT smoke 策略：单合约、净仓 ±1 手上限（换月/跳空窗口可能短暂 ±2）。
//   示范两条正确性纪律——正确敞口核算（确认仓 + 本策略自有在途单）与反转撤单
//   （换方向前先撤对侧在途单，防旧单在反转后意外成交叠仓）。
//
// [信号逻辑]
//   - 每 tick_window (默认 100) 个 tick 触发一次决策
//   - 比较窗口首 tick 价 vs 窗口末 tick 价：
//       end > start  → 趋势向上 → 反转：开空 / 平多
//       end < start  → 趋势向下 → 反转：开多 / 平空
//       end == start → 不动作
//   - 单合约、long-only-or-short-only、固定 1 手
//
// [下单模式]
//   use_ioc=1（默认，binance/CTP live）：IOC 穿越盘口，同 tick 成交或撤，不留挂单，
//     敞口天然收敛。use_ioc=0（回测/GTC）：限价挂单 rest 到成交，靠敞口核算 +
//     反转撤单守净仓上限。
//
// [换月]
//   项目规范 "strategy-driven rollover"：策略在 on_roll 中自己处理仓位 / 订单
//   迁移。本策略采用"透明换月"语义——复用公共 helper xt_rollover:: 维护跨合约
//   逻辑仓位、in-flight GTC 单元数据，并委托 migrate_gtc_orders 做 cancel+resubmit。
// -----------------------------------------------------------------------------

#include "xt_strategy_engine/XtStrategyBase.h"
#include "xt_strategy_engine/XtStrategyInitConfig.h"
#include "xt_strategy_engine/util/XtRolloverHelper.h"
#include "xt_strategy_engine/util/XtNotionalSizing.h"  // quote-notional open sizing
#include "api/IStrategyContext.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtOrderTypes.h"

#include <cstring>
#include <cstdio>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

/// @warning GTC 模式（use_ioc=0）close 追价语义：持仓下若某 GTC close 单落在当前市价的
///   不可成交侧，累计 close_chase_ticks 个 on_tick 后 cancel 该单，待其终态回报（Canceled）
///   再按当时 last 价重挂一次（wait-for-terminal，逐 K 周期追至成交；重挂前旧单必已终态出账，
///   不存在旧/新 close 同时在簿的漏算窗）。close_chase_ticks<=0 = disabled（不追价）。
///   CTP 上启用需留意交易所撤单考核（稳态可能每若干 K 一撤一挂）。IOC 模式不触发（IOC 单不留挂）。
/// @warning 换月迁移窗内 per-symbol 净仓可能短暂 +2：migrate_gtc_orders 先 cancel 旧合约在途
///   单再 resubmit 到新合约，旧单 cancel 终态回报与新单成交之间，旧/新合约瞬时各持一腿。
///   aggregate 逻辑净仓仍守 ±1，仅单合约维度出现瞬时 +2（roll/holiday 场景实测可见，已披露）。
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
        // close_throttle_ms: 高频 venue (binance/REST 50-200ms ack) 防止 close burst
        // 累积 outstanding 触发 RISK InsufficientAvailablePosition 级联 reject。
        // CTP venue 同步 ack µs 级，可设 0 关闭。默认 200ms 覆盖 binance demo p99 ack。
        close_throttle_ms_ = cfg.get_int("close_throttle_ms", 200);
        // close_reject_backoff_ms: after a CLOSE order gets a TERMINAL
        // reject, hold off re-firing the same close for this many ms (or until the
        // position view moves) — bounds a venue reject-loop (binance reduceOnly -2022
        // on a venue-stuck position) instead of the prior erase-and-immediately-rearm.
        // 0 = off. Default 3000ms.
        close_reject_backoff_ms_ = cfg.get_int("close_reject_backoff_ms", 3000);
        // close_expire_backoff_ms: SHORT hold-off after a CLOSE returns terminal with NO
        // fill (binance IOC reduceOnly EXPIRED → mapped to Canceled). Unlike a hard reject
        // (-2022, 3000ms latch) an EXPIRED no-fill is a normal transient (fillable next
        // tick), so this is a much shorter throttle that just bounds the ~200ms no-fill
        // re-fire hot-loop. DEFAULT 0 (off) → zero behavior change in backtest (bit-exact)
        // / CTP / existing configs; enabled only via live binance config. Cleared on
        // pos-move (close had effect) or window expiry.
        close_expire_backoff_ms_ = cfg.get_int("close_expire_backoff_ms", 0);
        // close_chase_ticks: GTC-only（use_ioc=0）stale close 单追价阈值。持仓时按旧价
        // 挂的 GTC close 单若落到当前市价的不可成交侧（SELL close: last < limit；BUY close:
        // last > limit，跳空/假日缺口后常见），仓位卡住无法平 → 确认仓非 0 → 开仓门
        // （logical_fp()==0）不许再开 → 策略失活。追价：该单累计 close_chase_ticks 个 on_tick
        // 仍不可成交 → cancel 并按当前 last 价重挂同量同 tag GTC close（有界 churn，直到成交）。
        // 默认 20。<=0 显式定义为 disabled（不追价）——阈值 <=0 时 chase 计数（从 0 起增）在首个
        // on_tick 即达标 → 每 tick 撤挂风暴，故 <=0 时不登记 chase 跟踪、不扫描（登记侧已 gate）。
        // use_ioc=1 路径零触碰（IOC 单不 rest，无 stale close，追价永不触发）。
        close_chase_ticks_ = cfg.get_int("close_chase_ticks", 20);
        debug_first_ticks_ = cfg.get_int("debug_first_ticks", 0);
        debug_first_decisions_ = cfg.get_int("debug_first_decisions", 0);
        // passive_depth_level：use_ioc=0 + 该参数 >= 0 时启用 passive maker 路径
        // 用 depth level 索引 (0=best, 1=bid[1]/ask[1], ...) 直接访问 XtTick 5 档 fp 数组
        // -1 = 关闭（默认，bit-exact 兼容老行为）
        passive_depth_level_ = cfg.get_int("passive_depth_level", -1);
        // maker_ttl_ms：GTC maker 单最长挂龄（ms，按 tick action_time 计）。>0 且
        // use_ioc=0 时启用：on_tick 扫描在途 GTC 单，超龄发一次 cancel。给 queue
        // 校准产生 no_fill 样本（挂到必成交会让 fill_rate 饱和无法拟合 k），也给
        // maker 模式兼做挂单兑底。init-only；0 = 关（默认，存量 config/golden bit-exact）。
        // ⚠️ CTP 上启用需留意交易所撤单考核（稳态 ≈ 每单一撤）。
        maker_ttl_ms_ = cfg.get_int("maker_ttl_ms", 0);
        // quote-notional OPEN sizing — BINANCE/crypto ONLY. order_notional_usdt
        // > 0 → size opens so notional ≈ target USDT (qty = target/price, quantized to qty_step,
        // then bumped up to clear min_order_notional) instead of the fixed base qty_ → immune to
        // the "fixed base × falling price < MIN_NOTIONAL → opens rejected → strategy stops"
        // fragility (retires the live `qty: 0.002` hack). Default 0 = OFF → opens use qty_ →
        // backtest bit-exact. Closes unaffected.
        //   ⚠️ CTP: leave 0. CTP trades INTEGER lots (qty_step=0.001 is a BTC LOT_SIZE); a USDT
        //      notional on a CNY contract is meaningless and truncates to 0 (silently stops) or a
        //      wrong contract count. This strategy runs on CTP too — notional mode is binance-only.
        //   ⚠️ Keep order_notional_usdt / min_expected_price ≤ runtime.risk.max_order_qty, else
        //      every open is MaxOrderQty-rejected. These three knobs are init-only (restart to
        //      retune); in notional mode the hot `qty` param is ignored for opens.
        order_notional_usdt_ = cfg.get_double("order_notional_usdt", 0.0);
        qty_step_            = cfg.get_double("qty_step", 0.001);          // LOT_SIZE step (BTCUSDT)
        min_order_notional_  = cfg.get_double("min_order_notional", 0.0);  // floor to bump opens to (mirror risk's; 0 = no bump)
    }

    void on_init(IStrategyContext* ctx) override {
        if (!symbol_.empty()) {
            ctx->sub_tick(symbol_.c_str());
            // B: seed positions_ with CONFIRMED (= projected - in-flight) so it stays
            // single-source vs on_position (confirmed); avoids jump-detector + double-count.
            positions_.on_update(symbol_.c_str(),
                ctx->get_position_fp(symbol_.c_str()) - ctx->projected_inflight_fp(symbol_.c_str()));
        }
        // 误配可见性：TTL 只作用于 GTC maker 模式，IOC 下静默无效会让用户误以为生效。
        if (maker_ttl_ms_ > 0 && use_ioc_) {
            ctx->log_warn("hft_tick_revert maker_ttl_ms set but use_ioc=1 — TTL only applies to GTC maker mode (ignored)");
        }
    }

    // 热改五项：tick_window / qty / close_throttle_ms / passive_depth_level / debug_*
    // symbol_ 跟 use_ioc_ 在 init() 之后不可改（in-flight 单跨合约 / close-offset 边界）。
    // bare key 由 XtStrategyContext::find_param_value 自动 namespace 成 `strategy.<id>.<key>`。
    void on_params(IStrategyContext* ctx) override {
        if (!ctx) return;
        // 数值类参数：用现值作 default,key 不存在时保留原值
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

        // sanity check：tick_window 必须 > 0,否则窗口决策路径死锁
        const int safe_tick_window = (new_tick_window > 0) ? new_tick_window : tick_window_;
        // qty 必须 > 0
        const double safe_qty = (new_qty > 0.0) ? new_qty : qty_;
        // close_throttle_ms ≥ 0 (0 = 关闭 backstop throttle)
        const int safe_throttle = (new_close_throttle_ms >= 0) ? new_close_throttle_ms : close_throttle_ms_;
        // passive_depth_level ∈ [-1, 4] (-1=关闭,0..4=bid/ask 5 档索引)
        const int safe_passive =
            (new_passive_depth_level >= -1 && new_passive_depth_level <= 4)
                ? new_passive_depth_level : passive_depth_level_;

        // 仅 log diff,避免每次 on_params 刷屏
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

    /// 自报能力位。HFT 策略 override on_tick / on_fill /
    /// on_order，需要 engine fan-out 这些回调。多策略 CTA 部署下 exec sink callback path 不可用
    /// 会让这些回调静默失效 → Runner 启动期 abort。
    uint64_t capabilities() const noexcept override {
        return to_capability_bit(StrategyCapability::SubscribesTick)
             | to_capability_bit(StrategyCapability::RequiresOnFill)
             | to_capability_bit(StrategyCapability::RequiresOnOrder);
    }

    void on_tick(IStrategyContext* ctx, const XtTick& tk) override {
        if (symbol_.empty() || std::strcmp(tk.code, symbol_.c_str()) != 0) return;
        // on_position push→pull 兜底: 主动 pull 同步仓位 + 清 reject-backoff。覆盖 non-fill
        // 仓位变 (强平/ADL/reconcile flat, 无 on_fill)。on_fill 是主路, on_tick 兜底。
        sync_position_and_clear_backoff_(ctx);
        // ── maker TTL 扫描（use_ioc=0 且 maker_ttl_ms>0 才有跟踪项）──
        // 超龄 GTC 单发一次 cancel 并停止跟踪；终态经 on_order 收敛（Canceled →
        // 校准 no_fill 样本；cancel/fill race 出 Filled 也正常收敛）。放在 can_submit
        // gate 之前：cancel 不受 submit gate 约束（见 IStrategyContext::can_submit @note）。
        if (!ttl_orders_.empty()) {
            for (auto it = ttl_orders_.begin(); it != ttl_orders_.end();) {
                const int32_t age_ms = action_time_delta_ms(it->second, tk.action_time);
                // 上界护栏：乱序 tick（cur < submit）经跨日回绕会得 ~86.4M ms → 误判
                // 超龄立即撤刚挂的单，污染校准样本；真实 session gap（CTP 隔夜 ≤9h）
                // 远小于该界，跳过后下个正序 tick 给出真挂龄。
                if (age_ms >= maker_ttl_ms_ && age_ms < 86'000'000) {
                    ctx->cancel(it->first);
                    // 一次性 cancel：cancel 已出网时残径由终态兜（Canceled/Filled race
                    // 均经 on_order 收敛）；cancel 被 gateway 门拦下（限流/TD 未 ready）
                    // 则该单回到无 TTL 的 baseline 行为（rest 到成交），不重发。
                    it = ttl_orders_.erase(it);
                } else {
                    ++it;
                }
            }
        }
        // 综合 submit gate 查询, 替换单一 is_in_trading_section.
        // can_submit() AND 检查: section + trading_ready + !kill_switch + !closeout_no_open.
        // 单查 section 不查 trading_ready 时,策略无法感知 TD 未 ready 就继续发单,
        // 实测会触发大量 TRADING_NOT_READY 无效 submit。
        if (!ctx->can_submit()) return;

        // ── GTC close 追价（chase）：持仓 + stale close 单永不成交时按当前 last 重挂 ──
        // 场景：跳空/假日缺口后早先按旧价挂的 GTC close 单落在当前市价不可成交侧，仓位
        // 卡住无法平 → 确认仓非 0 → 开仓门（logical_fp()==0）不许再开 → 策略后续零成交
        // 失活。追价：该 close 单累计 close_chase_ticks 个 on_tick 仍不可成交 → cancel、按当前
        // last 价重挂同量同 tag GTC close、计数归零；仍不成交则下个 K 周期再追（有界 churn）。
        // 仅动 close 腿、开仓门与 D1/D2 逻辑不变。IOC 路径 close_chase_age_ 恒空 → 零触碰。
        // close_chase_ticks<=0 = disabled（登记侧已 gate，close_chase_age_ 恒空，此处天然跳过）。
        if (!use_ioc_ && close_chase_ticks_ > 0 && !close_chase_age_.empty()) {
            maybe_chase_close_(ctx, tk);
        }

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

        // 窗口起点：第 1 个 tick 记 start_price
        if (count_ == 1) {
            start_price_fp_ = ref_price_fp;
            return;
        }

        // 窗口未满：累积
        if (count_ < tick_window_) return;

        // 窗口末：决策 + 重置
        const int64_t end_price_fp = ref_price_fp;
        const int64_t qty_fp       = order_exec_to_fp(qty_);
        // OPEN qty. notional mode → quote-notional sized (target/price, quantized);
        // else the base qty_. Default OFF → open_qty_fp == qty_fp → bit-exact. Only opens
        // (rev_short/rev_long) use this; closes keep cur_pos (IOC) / qty_fp (GTC).
        const int64_t open_qty_fp = (order_notional_usdt_ > 0.0)
            ? order_exec_to_fp(xt_sizing::notional_to_qty(
                  order_notional_usdt_, order_exec_from_fp(end_price_fp), qty_step_,
                  min_order_notional_))
            : qty_fp;
        // #3 (F3): notional mode produced qty 0 (target too small for price × qty_step) → opens
        // skip silently below; WARN once so the no-trade is observable (not mistaken for a halt).
        if (order_notional_usdt_ > 0.0 && open_qty_fp <= 0 && !notional_zero_warned_) {
            notional_zero_warned_ = true;
            char nzbuf[208];
            std::snprintf(nzbuf, sizeof(nzbuf),
                "hft_tick_revert NOTIONAL-SIZING produced qty=0 (order_notional_usdt=%.2f, "
                "qty_step=%.6f) — opens skipped; lower the target or check qty_step",
                order_notional_usdt_, qty_step_);
            ctx->log_warn(nzbuf);
        }
        // 敞口 = 确认净仓（logical_fp，含换月跨合约结转）+ 在途签名 leaves，两口径按模式分工：
        //   - GTC（回测/use_ioc=0）：用本策略自有 WorkingOrderRegistry（buy +leaves / sell −leaves）。
        //     回测下 ctx->projected_inflight_fp 恒 0，会漏计 resting GTC 单 → 反复叠开，故必须用自有账本。
        //   - IOC（live binance/CTP，默认）：用 ctx->projected_inflight_fp（网关权威投影）。IOC 单不
        //     rest、registry 稳态空，但 recovery / 丢终态场景下自有 registry 依赖回调投递会与网关投影
        //     分歧，网关投影才是权威真值源。此分工让"IOC 路径零触碰自有 registry"名副其实（live 行为
        //     恢复到 self_inflight 引入前的 projected_inflight 口径，按字面不变）。
        const int64_t cur_pos      = positions_.logical_fp() +
            (use_ioc_ ? ctx->projected_inflight_fp(symbol_.c_str())
                      : self_inflight_signed_fp_());

        // v3 gross-aware (CTP 锁仓修复): 净仓 cur_pos 区分不了真空仓(0,0)与锁仓(1,1)。
        // 一次快照读 broker 真实多/空腿,开/平决策按"腿量"走 —— 持反向腿先平那条腿、
        // 仅双腿皆 0(且无在途 cur_pos==0)才开 → 永不在持反向腿时叠开 → 不锁仓。
        // 恢复/未知期分腿可能读 0(看似空仓),由风控 Open 门控兜底(见 XtLiveRiskGuardContext)。
        xt_runtime::XtStraPositionDetail pos_det{};
        ctx->get_position_detail(symbol_.c_str(), &pos_det, 1);  // flat→不写,zero-init 读到 0
        // 锁仓修复(最小行为变更):仅当 broker 真实多/空腿**同时 >0**(CTP 锁仓,净仓 L−S
        // 掩盖真实毛敞口)才按真实分腿决策(平反向腿、不再叠开);其余一律净仓口径 == 旧版。
        // 回测自动净额永不双腿并存 → 永走净仓分支 → byte-identical;仅锁仓这一 bug 态行为改变。
        // (换月连续码下 per-symbol 腿与 logical 净仓不对齐,也因走净仓分支而不受影响。)
        const int64_t real_long_fp  = order_exec_to_fp(pos_det.long_side.position);
        const int64_t real_short_fp = order_exec_to_fp(pos_det.short_side.position);
        int64_t long_leg_fp, short_leg_fp;
        if (real_long_fp > 0 && real_short_fp > 0) {   // 真实锁仓 → 用真实分腿
            long_leg_fp  = real_long_fp;
            short_leg_fp = real_short_fp;
        } else {                                       // 正常 → 净仓口径(legacy 等价)
            long_leg_fp  = (cur_pos > 0) ?  cur_pos : 0;
            short_leg_fp = (cur_pos < 0) ? -cur_pos : 0;
        }

        // use_ioc=true (默认): IOC + best_bid/ask 主动穿越盘口，同 tick 必成交
        // use_ioc=false: GTC + last_price 限价挂单，rest 到成交
        const auto tif = use_ioc_ ? IStrategyContext::TIF_IOC : IStrategyContext::TIF_GTC;
        int64_t buy_px, sell_px;
        if (use_ioc_) {
            const XtTick* snap = ctx->get_last_tick(symbol_.c_str());
            buy_px  = (snap && snap->ask_price_fp[0] > 0) ? snap->ask_price_fp[0]
                     : (tk.ask_price_fp[0] > 0 ? tk.ask_price_fp[0] : end_price_fp);
            sell_px = (snap && snap->bid_price_fp[0] > 0) ? snap->bid_price_fp[0]
                     : (tk.bid_price_fp[0] > 0 ? tk.bid_price_fp[0] : end_price_fp);
        } else if (passive_depth_level_ >= 0 && passive_depth_level_ < 5) {
            // Passive maker 路径：BUY 挂在 bid 档（被动等卖压成交），SELL 挂在 ask 档
            // (被动等买压成交)。is_aggressive 检测：BUY price < best_ask & SELL price >
            // best_bid → passive → 触发 XtOrderManager queue_model_->estimate_queue_ahead。
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

        // GTC 模式全部用 Open offset（避免 close_outstanding 风控拒单；平仓信号靠反向 Open 单对冲）
        const auto close_off = use_ioc_ ? XtOffset::Close : XtOffset::Open;

        // ── close throttle: outstanding-count（仅 IOC 模式）
        // 仅在 use_ioc=1 启用 — 对应 binance / 真 IOC venue 的 close 发 Close offset，
        // 走 RISK avail_closeable 检查，outstanding 累积 → 级联 reject。
        //
        // GTC 模式（use_ioc=0，仅回测；⚠️ live binance & CTP 生产默认 IOC=1，
        //   见 init() get_int("use_ioc",1)，live_hft_verify.yaml 不设此键）：close 信号实际发 Open offset（见下面
        // close_off = use_ioc_ ? Close : Open），不走 close pre-check，没有
        // outstanding bug；regression 数据集均 use_ioc=0，throttle 关闭保 bit-exact。
        //
        // 单纯时间窗 throttle 在 binance tick_window=10 + 20 ticks/s = 500ms/decision
        // 下永不触发，需要 outstanding-count 主防御 + 时间窗 backstop 双层。
        // will_close: 反向信号时是否在平腿(上行平多腿 / 下行平空腿)——按腿量,不按净仓。
        const bool will_close = use_ioc_ &&
                                ((end_price_fp > start_price_fp_ && long_leg_fp > 0) ||
                                 (end_price_fp < start_price_fp_ && short_leg_fp > 0));
        if (will_close) {
            // (a) outstanding-count: 任一 in-flight close 单 → skip
            if (!close_oids_.empty()) {
                count_ = 0;
                start_price_fp_ = 0;
                return;
            }
            // (b) backstop time-window throttle (=0 关闭)
            if (close_throttle_ms_ > 0 && last_close_at_ != 0) {
                const int32_t delta_ms = action_time_delta_ms(last_close_at_, tk.action_time);
                if (delta_ms < close_throttle_ms_) {
                    count_ = 0;
                    start_price_fp_ = 0;
                    return;
                }
            }
            // (c) reject-backoff: after a terminal close reject, hold off
            //     re-firing the SAME close until the window expires OR the position view
            //     moves (on_position clears last_close_reject_at_). Bounds a venue
            //     reject-loop (e.g. binance reduceOnly -2022 on a venue-stuck position)
            //     WITHOUT permanently latching: time-expiry auto-retries a transient/
            //     healed reject; a pos-move re-evaluates immediately. Replaces the old
            //     erase-and-immediately-rearm that produced the 208× -2022 loop.
            if (close_reject_backoff_ms_ > 0 && last_close_reject_at_ != 0) {
                const int32_t delta_ms = action_time_delta_ms(last_close_reject_at_, tk.action_time);
                if (delta_ms < close_reject_backoff_ms_) {
                    count_ = 0;
                    start_price_fp_ = 0;
                    return;
                }
                last_close_reject_at_ = 0;  // window expired → allow one probe re-fire
            }
            // (d) expire-backoff: after a CLOSE returns with NO fill (IOC EXPIRED →
            //     Canceled), hold off re-firing the SAME close for a SHORT window —
            //     bounds the no-fill re-fire hot-loop. Shorter than reject-backoff
            //     (EXPIRED is a normal transient, fillable next tick). Same clear
            //     semantics (pos-move clears it; window-expiry allows one probe re-fire).
            //     Default off (close_expire_backoff_ms_=0) → this whole block is skipped.
            if (close_expire_backoff_ms_ > 0 && last_close_expire_at_ != 0) {
                const int32_t delta_ms = action_time_delta_ms(last_close_expire_at_, tk.action_time);
                if (delta_ms < close_expire_backoff_ms_) {
                    count_ = 0;
                    start_price_fp_ = 0;
                    return;
                }
                last_close_expire_at_ = 0;  // window expired → allow one probe re-fire
            }
        }

        uint64_t oid = 0;
        const char* tag = nullptr;
        // (I L2) 决策原因: tick_revert 核心信号 = 窗口末价相对窗口首价的方向与幅度.
        //        格式 "tick_revert|dir=<dir>|delta_ticks=<N>" — reader 可直接解析归因.
        char reason_buf[48] = {};
        const double delta_ticks = order_exec_from_fp(end_price_fp - start_price_fp_);

        // -----------------------------------------------------------------
        // close qty: IOC 模式平"整条 broker 腿量",不用 abs(净仓)
        // -----------------------------------------------------------------
        // v3: 锁仓时 net ≠ 腿量,abs(cur_pos) 会平不干净(残腿);改用实际多/空腿量,
        // 网关 build_close_slices 再拆平今/平昨。GTC 回测路径保持 qty_fp 不漂移。
        const int64_t close_long_qty_fp  = use_ioc_ ? long_leg_fp  : qty_fp;
        const int64_t close_short_qty_fp = use_ioc_ ? short_leg_fp : qty_fp;

        // v3 分支按"腿量"选(锁仓修复核心):上行反转做空 → 持多腿先平多(close_long_qty=多腿)、
        // 仅双腿皆 0 且无在途才开空;下行对称。锁仓 (L>0,S>0) 时净=0 旧码会走"开"分支(bug),
        // 现 long_leg>0 → 走平多,永不在持反向腿时叠开。
        if (end_price_fp > start_price_fp_) {
            if (long_leg_fp > 0) {
                // 反转撤单纪律（GTC）：换向发 SELL 前先撤在途 BUY，防旧买单在反转后成交叠多。
                if (!use_ioc_) cancel_opposite_resting_(ctx, /*want_is_buy=*/false);
                tag = "rev_close_long";
                std::snprintf(reason_buf, sizeof(reason_buf),
                              "tick_revert|dir=down|delta=%+.4f", delta_ticks);
                ctx->set_next_order_reason(reason_buf);
                oid = xt_rollover::submit_and_track(ctx, orders_, positions_, symbol_.c_str(), /*is_buy=*/false,
                                              sell_px, close_long_qty_fp, tif, tag, close_off);
                if (oid != 0 && use_ioc_) {                // 仅 IOC venue 启用 throttle 跟踪
                    last_close_at_ = tk.action_time;
                    close_oids_.insert(oid);
                }
            } else if (long_leg_fp == 0 && short_leg_fp == 0 && cur_pos == 0 &&
                       positions_.logical_fp() == 0 && open_qty_fp > 0) {
                // 开仓门收紧：仅 cur_pos==0（净敞口）不够——确认多仓被在途平仓单抵消也会净 0，
                // 若此时开新仓并撤掉那张平仓单，确认仓会叠到 ±2/±3。追加 logical_fp()==0（确认仓
                // 真 flat）才开，杜绝持仓时叠开。IOC 下 orders_ 稳态空 → 与 cur_pos==0 等价，不改实盘行为。
                if (!use_ioc_) cancel_opposite_resting_(ctx, /*want_is_buy=*/false);
                tag = "rev_short";
                std::snprintf(reason_buf, sizeof(reason_buf),
                              "tick_revert|dir=down|delta=%+.4f", delta_ticks);
                ctx->set_next_order_reason(reason_buf);
                oid = xt_rollover::submit_and_track(ctx, orders_, positions_, symbol_.c_str(), /*is_buy=*/false,
                                              sell_px, open_qty_fp, tif, tag, XtOffset::Open);
            }
        } else if (end_price_fp < start_price_fp_) {
            if (short_leg_fp > 0) {
                // 反转撤单纪律（GTC）：换向发 BUY 前先撤在途 SELL，防旧卖单在反转后成交叠空。
                if (!use_ioc_) cancel_opposite_resting_(ctx, /*want_is_buy=*/true);
                tag = "rev_close_short";
                std::snprintf(reason_buf, sizeof(reason_buf),
                              "tick_revert|dir=up|delta=%+.4f", delta_ticks);
                ctx->set_next_order_reason(reason_buf);
                oid = xt_rollover::submit_and_track(ctx, orders_, positions_, symbol_.c_str(), /*is_buy=*/true,
                                              buy_px, close_short_qty_fp, tif, tag, close_off);
                if (oid != 0 && use_ioc_) {                // 仅 IOC venue 启用 throttle 跟踪
                    last_close_at_ = tk.action_time;
                    close_oids_.insert(oid);
                }
            } else if (long_leg_fp == 0 && short_leg_fp == 0 && cur_pos == 0 &&
                       positions_.logical_fp() == 0 && open_qty_fp > 0) {
                if (!use_ioc_) cancel_opposite_resting_(ctx, /*want_is_buy=*/true);
                tag = "rev_long";
                std::snprintf(reason_buf, sizeof(reason_buf),
                              "tick_revert|dir=up|delta=%+.4f", delta_ticks);
                ctx->set_next_order_reason(reason_buf);
                oid = xt_rollover::submit_and_track(ctx, orders_, positions_, symbol_.c_str(), /*is_buy=*/true,
                                              buy_px, open_qty_fp, tif, tag, XtOffset::Open);
            }
        }
        // maker TTL 跟踪：GTC maker 模式记 submit 时刻（action_time）供超龄扫描。
        if (oid != 0 && maker_ttl_ms_ > 0 && !use_ioc_) {
            ttl_orders_.emplace(oid, tk.action_time);
        }
        // chase 跟踪：GTC close 单登记进 chase age map（初始计数 0），供后续超龄追价扫描。
        // 只登记 close 腿（rev_close_long/rev_close_short），开仓单不追价。
        // close_chase_ticks<=0 = disabled → 不登记（map 恒空 → on_tick 追价扫描天然跳过）。
        if (oid != 0 && !use_ioc_ && close_chase_ticks_ > 0 && tag != nullptr &&
            (std::strcmp(tag, "rev_close_long") == 0 ||
             std::strcmp(tag, "rev_close_short") == 0)) {
            close_chase_age_[oid] = 0;
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

        // 重置窗口
        count_          = 0;
        start_price_fp_ = 0;
    }

    // on_position push→pull 改造 (plan on_position_pull_unification): 仓位通知不再靠
    // engine push on_position, 改 strategy 主动 pull (get_position_fp)。on_fill = 仓位变
    // 主因 (close/open 成交), fill 后即时 sync — account_store 已 fill-immediate, 与原
    // on_position 同值同时刻 (backtest byte-identical)。
    void on_fill(IStrategyContext* ctx, const XtFillEvent& /*fill*/) override {
        sync_position_and_clear_backoff_(ctx);
    }

    void on_order(IStrategyContext* ctx,
                  const XtOrderExecUpdate& upd) override {
        orders_.apply_update(upd);

        // chase age map 与 registry 生命周期对齐：order 离开 registry（终态或 leaves<=0）
        // 即从 chase 跟踪移除，防 orphan 计数无限累积（apply_update 已做 registry erase）。
        if (!orders_.find(upd.id)) close_chase_age_.erase(upd.id);

        // capture close-membership BEFORE the terminal erase below.
        const bool tracked_close = (close_oids_.count(upd.id) > 0);

        // Outstanding-count throttle 解锁：close 单进入终态 → 从 close_oids_ 移除
        // 终态：Filled / Canceled / Rejected。Submitted/Accepted/PartiallyFilled
        // 仍是 in-flight（PartiallyFilled 后续可能继续 fill 或 cancel，保守不解锁）。
        const bool is_terminal =
            upd.status == XtOrderStatus::Filled   ||
            upd.status == XtOrderStatus::Canceled ||
            upd.status == XtOrderStatus::Rejected;
        if (is_terminal) {
            close_oids_.erase(upd.id);  // erase no-op if not a close oid
            ttl_orders_.erase(upd.id);  // maker TTL 跟踪同步释放（no-op if untracked）
        }
        // ── P1-1 chase wait-for-terminal 状态机：pending 的 cancel 终态到达后决定重挂/放弃 ──
        // maybe_chase_close_ 只发 cancel + 标记 pending（不 erase registry、不立即重挂）；旧单在
        // cancel latency 窗内仍留 registry 照常计敞口 → 无"旧单未计敞口却仍可成交"的漏算窗。
        // 终态到达：Canceled 且无成交（filled_fp==0，仓位仍卡）→ 按当时 last 价重挂同量同 tag close；
        // Filled 或部分成交后 Canceled（filled_fp>0，仓位已减）→ 放弃重挂让正常逻辑接管。重挂前旧单必已
        // 终态（apply_update 已从 registry erase），天然无旧/新 close 同向双计翻符号问题。
        if (chase_pending_oid_ != 0 && upd.id == chase_pending_oid_ && is_terminal) {
            const int64_t qty_fp = chase_pending_qty_fp_;
            const bool    is_buy = chase_pending_is_buy_;
            const char*   tag_p  = chase_pending_tag_;
            chase_pending_oid_    = 0;   // 清 pending（无论重挂与否；单 symbol 单 close 至多一个）
            chase_pending_qty_fp_ = 0;
            chase_pending_tag_    = nullptr;
            // P1-a 重挂前方向/数量校验：cancel→终态窗内别的单可能成交，仓位可能翻向或缩小，
            //   按旧方向/量重挂会变加仓/过度反转。方向仍是平仓向才重挂——BUY close 需仍持空
            //   (logical<0)、SELL close 需仍持多 (logical>0)，方向翻转则放弃（pending 已清）；
            //   重挂量 clamp 到 min(pending_qty, |logical|)，仓位缩小时不按旧量过度反转。
            //   logical==0（已平）两侧判据皆假 → 天然放弃，等价旧 logical_fp()!=0 gate。
            const int64_t logical_fp = positions_.logical_fp();
            const bool close_dir_ok  = is_buy ? (logical_fp < 0) : (logical_fp > 0);
            if (upd.status == XtOrderStatus::Canceled && upd.filled_fp == 0 &&
                close_dir_ok && qty_fp > 0 && tag_p != nullptr) {
                const int64_t abs_logical_fp = (logical_fp < 0) ? -logical_fp : logical_fp;
                const int64_t requeue_qty_fp = (qty_fp < abs_logical_fp) ? qty_fp : abs_logical_fp;
                const XtTick* snap = ctx->get_last_tick(symbol_.c_str());
                const int64_t last_fp = snap ? decision_price_fp(*snap) : 0;
                if (requeue_qty_fp > 0 && last_fp > 0) {
                    ctx->set_next_order_reason("tick_revert|chase_close");
                    const uint64_t new_oid = xt_rollover::submit_and_track(
                        ctx, orders_, positions_, symbol_.c_str(), is_buy,
                        last_fp, requeue_qty_fp, IStrategyContext::TIF_GTC, tag_p, XtOffset::Open);
                    if (new_oid != 0 && close_chase_ticks_ > 0) close_chase_age_[new_oid] = 0;
                }
            }
        }
        // a terminal REJECT on a tracked CLOSE → start the reject-backoff
        // (keyed off the close's submit action_time) so will_close holds off re-firing
        // the same close until the window expires OR the position view moves. Replaces the
        // prior erase-and-immediately-rearm (close_oids_ empty → re-fire next tick) that
        // produced the binance reduceOnly -2022 208× loop. (Any close Rejected triggers it;
        // a transient reject simply auto-retries after the time window — safe.)
        if (tracked_close && upd.status == XtOrderStatus::Rejected && last_close_at_ != 0) {
            last_close_reject_at_ = last_close_at_;
        }
        // a terminal CLOSE that came back with NO fill (IOC EXPIRED → Canceled) → arm the
        // SHORT expire-backoff. filled_fp==0 distinguishes a pure no-fill (position unmoved
        // → would hot-loop re-firing the same close) from a partial-fill-then-cancel
        // (position moved → self-heals via the on_fill/on_tick pos-move clear, no backoff
        // needed). Default off (close_expire_backoff_ms_=0) makes will_close ignore this.
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

    // 透明换月：委托 helper 做 GTC 单迁移 + 跨合约仓位跟踪。
    // tick window 计数不重置（HOT 语义：换月不打断窗口累积）。
    void on_roll(IStrategyContext* ctx, const char* old_code,
                 const char* new_code, uint32_t /*trading_date*/,
                 int64_t /*old_last_price_fp*/) override {
        // P1-b 迁移前先清 chase pending/age：migrate_gtc_orders 内 cancel() 可能同步回灌终态
        //   on_order，此时若旧 pending 尚存会在旧合约按旧方向/量重挂（P1-a 重挂路径），而迁移
        //   helper 又 resubmit 新单 → 双单。迁移前清空 → 同步回灌落到「无 pending」路径天然不重挂；
        //   迁移后再按现存 close 单重建 chase 跟踪。旧 close_chase_age_/pending 键随迁移作废。
        chase_pending_oid_    = 0;
        chase_pending_qty_fp_ = 0;
        chase_pending_tag_    = nullptr;
        close_chase_age_.clear();

        xt_rollover::migrate_gtc_orders(ctx, orders_, old_code, new_code);
        // migrate_gtc_orders cancel+resubmit 产生新 id（不经本策略 submit 路径），
        // 旧 id 跟踪作废；迁移后的单不再受 TTL 管控（可接受：校准盘 binance 无 roll）。
        ttl_orders_.clear();
        // chase map 重建：迁移产生的新 close 单用新 oid，按迁移后 registry 现存单重建
        // （close 腿计数清零起算，非 close 单不入）；close_chase_ticks<=0 = disabled 时保持空。
        if (!use_ioc_ && close_chase_ticks_ > 0) {
            for (const auto& kv : orders_.all()) {
                const auto& ord = kv.second;
                if (ord.leaves_fp <= 0) continue;
                if (ord.tag == "rev_close_long" || ord.tag == "rev_close_short") {
                    close_chase_age_[ord.id] = 0;
                }
            }
        }
        positions_.touch(old_code);
        positions_.on_update(new_code,  // CONFIRMED base (= projected - in-flight)
            ctx->get_position_fp(new_code) - ctx->projected_inflight_fp(new_code));
        symbol_ = new_code;
        ctx->sub_tick(new_code);  // 重新订阅新合约（dispatch_tick 按订阅过滤）
    }

private:
    /// @brief HHMMSSmmm action_time → 当日毫秒偏移（HH*3600000 + MM*60000 + SS*1000 + ms）
    static uint32_t action_time_to_ms_of_day(uint32_t at) noexcept {
        const uint32_t hh = at / 10000000u;
        const uint32_t mm = (at / 100000u) % 100u;
        const uint32_t ss = (at / 1000u)   % 100u;
        const uint32_t ms = at             % 1000u;
        return hh * 3600000u + mm * 60000u + ss * 1000u + ms;
    }

    /// @brief 计算两 action_time 的毫秒差（current - prev），跨午夜回绕处理。
    static int32_t action_time_delta_ms(uint32_t prev, uint32_t cur) noexcept {
        const int64_t cur_ms  = static_cast<int64_t>(action_time_to_ms_of_day(cur));
        const int64_t prev_ms = static_cast<int64_t>(action_time_to_ms_of_day(prev));
        int64_t delta = cur_ms - prev_ms;
        if (delta < 0) delta += 86'400'000;  // 跨日（binance perp 24h，CTP 跨夜盘）
        return static_cast<int32_t>(delta);
    }

    /// @brief on_position push→pull: 主动 pull 仓位 view + 仓位变时清 reject-backoff。
    /// view = CONFIRMED (= get_position_fp − in-flight, 同 on_init/on_roll seed 口径)。
    /// 替代原 on_position push 回调里的 positions_.on_update + backoff 清除 (rev.5 Fix A
    /// 语义保留: 仓位 view 变 → 清退避立即重评估)。挂 on_fill (主) + on_tick (兜底)。
    void sync_position_and_clear_backoff_(IStrategyContext* ctx) {
        if (!ctx || symbol_.empty()) return;
        const int64_t old_pos = positions_.logical_fp();
        positions_.on_update(symbol_.c_str(),
            ctx->get_position_fp(symbol_.c_str()) - ctx->projected_inflight_fp(symbol_.c_str()));
        const int64_t new_pos = positions_.logical_fp();
        if (old_pos != new_pos) {
            last_close_reject_at_ = 0;
            last_close_expire_at_ = 0;  // pos moved → close had effect → drop expire-backoff
        }
    }

    /// @brief 本策略自有在途单的签名 leaves 之和（买 +leaves / 卖 −leaves）。
    /// orders_（WorkingOrderRegistry）只含非终态单（apply_update 终态即 erase），故等于
    /// 未成交在途敞口。与 logical_fp()（确认净仓）相加得本策略真实敞口，回测/实盘同口径——
    /// 取代 ctx->projected_inflight_fp（回测恒 0，会漏计 resting GTC 单致反复叠开）。
    int64_t self_inflight_signed_fp_() const {
        int64_t total = 0;
        for (const auto& kv : orders_.all()) {
            const auto& ord = kv.second;
            total += ord.is_buy ? ord.leaves_fp : -ord.leaves_fp;
        }
        return total;
    }

    /// @brief 反转撤单纪律：撤掉方向与新决策相反的在途 resting 单，防旧单在反转后成交叠仓。
    /// 先拷 id 再撤：ctx->cancel 可能同步回灌 on_order → registry erase，避免迭代器失效。
    /// 仅 GTC 模式调用（IOC 单不 rest，orders_ 稳态为空，无对侧可撤）。
    void cancel_opposite_resting_(IStrategyContext* ctx, bool want_is_buy) {
        std::vector<uint64_t> ids;
        for (const auto& kv : orders_.all()) {
            if (kv.second.is_buy != want_is_buy) ids.push_back(kv.first);
        }
        for (uint64_t id : ids) ctx->cancel(id);
    }

    /// @brief GTC close 追价扫描（wait-for-terminal）：对确认仓方向的 stale close 单累计超龄后
    /// 只发 cancel + 标记 pending；重挂延后到 on_order 收到该单终态时进行（Canceled 无成交 → 按当时
    /// last 重挂同量同 tag；Filled/部分成交 → 放弃）。仅 GTC 模式（!use_ioc_）调用。每次调用先递增所有
    /// tracked close 单的 chase 计数（确定性，按 on_tick 数，不用墙钟）；命中触发（确认仓非 0、方向匹配、
    /// 当前不可成交、计数 >= close_chase_ticks）则 cancel 旧单、标记 pending，本轮不重挂。pending 期间
    /// 不再触发新一轮（单 symbol 单 close 至多一张在追）。旧单保留在 registry 直到终态出账，cancel
    /// latency 窗内照常计敞口 → 无双平漏算窗（P1-1）。
    void maybe_chase_close_(IStrategyContext* ctx, const XtTick& tk) {
        // 已有 pending chase（cancel 已发、等终态）时不再触发新一轮：单 symbol 单 close 语义下
        // 至多一张 close 在追，等其终态经 on_order 收敛（Canceled → 重挂 / Filled → 放弃）。
        if (chase_pending_oid_ != 0) return;
        // 递增所有 tracked close 单的 chase 计数（结构不变，仅改值，遍历安全）。
        for (auto& kv : close_chase_age_) ++kv.second;
        const int64_t confirmed_fp = positions_.logical_fp();
        if (confirmed_fp == 0) return;      // 已平 flat：无需追价（终态回报会清 age map）
        const int64_t last_fp = decision_price_fp(tk);
        if (last_fp <= 0) return;
        // 确认仓方向 → 平仓单方向/tag：long → SELL close(rev_close_long)；short → BUY close(rev_close_short)
        const bool  want_is_buy = (confirmed_fp < 0);
        const char* want_tag    = (confirmed_fp > 0) ? "rev_close_long" : "rev_close_short";

        // 找一张匹配方向、当前不可成交、且计数达阈值的 stale close 单。
        uint64_t stale_id     = 0;
        int64_t  stale_qty_fp = 0;
        for (const auto& kv : orders_.all()) {
            const auto& ord = kv.second;
            if (ord.is_buy != want_is_buy) continue;
            if (ord.tag    != want_tag)    continue;
            if (ord.leaves_fp <= 0)        continue;
            const auto ait = close_chase_age_.find(ord.id);
            if (ait == close_chase_age_.end()) continue;
            if (ait->second < close_chase_ticks_) continue;
            // 不可成交判据：SELL close 需 last >= limit 才成交 → last < limit 卡住；BUY close 对称。
            const bool unfillable = want_is_buy ? (last_fp > ord.price_fp)   // BUY close: last > limit
                                                : (last_fp < ord.price_fp);  // SELL close: last < limit
            if (!unfillable) continue;
            stale_id     = ord.id;
            stale_qty_fp = ord.leaves_fp;
            break;
        }
        if (stale_id == 0) return;

        // P1-1 wait-for-terminal 状态机：只发 cancel + 标记 pending，不 erase registry、不立即重挂。
        // 旧单在 cancel latency 窗内仍留 registry，self_inflight_signed_fp_() 照常把它计入敞口 →
        // 消除旧实现"cancel 生效前 immediate-erase → 旧单不在簿计敞口却仍可成交 → 双平漏算窗"。
        // 旧单终态经 on_order 收敛后再决定重挂（Canceled 无成交）或放弃（Filled/部分成交），见 on_order。
        // 重挂前旧单必已终态出账，天然无旧/新 close 同向双计翻符号问题。
        close_chase_age_.erase(stale_id);   // 停止追这张（进入 pending 终态等待，close_chase_age_ 转空）
        chase_pending_oid_    = stale_id;
        chase_pending_qty_fp_ = stale_qty_fp;
        chase_pending_is_buy_ = want_is_buy;
        chase_pending_tag_    = want_tag;
        ctx->cancel(stale_id);
    }

    std::string symbol_;
    xt_rollover::PositionAggregator   positions_;
    xt_rollover::WorkingOrderRegistry orders_;
    int         tick_window_   = 100;
    double      qty_           = 1.0;
    double      order_notional_usdt_ = 0.0;     ///< quote-notional OPEN target (USDT); 0 = off → use qty_ (base). BINANCE-only. Retires the price-fragile fixed-qty hack.
    double      qty_step_      = 0.001;         ///< #3: LOT_SIZE step for open-qty quantization (BTCUSDT=0.001)
    double      min_order_notional_ = 0.0;      ///< #3: floor to bump notional-sized opens up to (mirror runtime.risk.min_order_notional); 0 = no bump
    bool        notional_zero_warned_ = false;  ///< #3 (F3): one-shot guard for the "notional sizing → qty 0" warn
    bool        use_ioc_       = true;
    int         passive_depth_level_ = -1;       ///< -1 = 关（默认）；0..4 = 用 bid/ask depth 第 N 档作为 passive 限价，触发 queue_model 路径
    int         maker_ttl_ms_ = 0;             ///< GTC maker 单最长挂龄 ms；0=关（默认，bit-exact）。use_ioc=0 时生效，超龄发一次 cancel
    std::unordered_map<uint64_t, uint32_t> ttl_orders_;  ///< oid → submit action_time（maker TTL 跟踪；cancel 后即移除，终态同步释放）
    int         close_throttle_ms_ = 200;       ///< CLOSE 时间窗 backstop（ms），0 = 关；主防御是 close_oids_
    int         close_reject_backoff_ms_ = 3000;///< 平仓单终态被拒后 hold-off 重发的 ms 窗口（或直到仓位变动）；0 = 关。防 venue reject-loop（binance reduceOnly -2022）
    uint32_t    last_close_at_ = 0;             ///< 上次成功 submit 的 close 单 action_time HHMMSSmmm
    uint32_t    last_close_reject_at_ = 0;      ///< 触发退避的那张 close 的 action_time；0 = 无。清除 = 退避到期 OR on_position 仓位变动
    int         close_expire_backoff_ms_ = 0;   ///< CLOSE 无成交(IOC EXPIRED→Canceled)后短退避 ms；0=关(默认,bit-exact)；仅 live binance 开。防 no-fill 再发热循环
    uint32_t    last_close_expire_at_ = 0;      ///< 触发 expire-backoff 的 close action_time；0=无。清除 = pos-move OR 窗口到期
    std::unordered_set<uint64_t> close_oids_;   ///< outstanding close orders（in-flight close 单 id 集合，主 throttle 机制）
    int         close_chase_ticks_ = 20;        ///< GTC close 追价阈值：stale close 单累计该多个 on_tick 仍不可成交则 cancel+按 last 重挂；默认 20。use_ioc=0 生效
    std::unordered_map<uint64_t, int> close_chase_age_;  ///< GTC close oid → 自 submit/上次重挂累计 on_tick 数（chase 计数；终态/离开 registry 时移除）
    uint64_t    chase_pending_oid_    = 0;      ///< P1-1: chase 已 cancel、等终态的 close oid；0=无。单 symbol 单 close → 至多一个
    int64_t     chase_pending_qty_fp_ = 0;      ///< P1-1: pending chase 重挂用的 close 量（原 stale leaves_fp）
    bool        chase_pending_is_buy_ = false;  ///< P1-1: pending chase 重挂方向（confirmed<0 → BUY close）
    const char* chase_pending_tag_    = nullptr;///< P1-1: pending chase 重挂 tag（静态串 rev_close_long/short；nullptr=无 pending）
    int         debug_first_ticks_ = 0;
    int         debug_first_decisions_ = 0;
    int         debug_tick_emitted_ = 0;
    int         debug_decision_emitted_ = 0;
    int         count_         = 0;
    int64_t     start_price_fp_ = 0;
};
