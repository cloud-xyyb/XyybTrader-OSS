#pragma once
// -----------------------------------------------------------------------------
// HftConformanceStrategy — HFT 全功能高压自检策略（conformance / stress oracle）
// -----------------------------------------------------------------------------
// [用途]
//   **非盈利**。目的是在高压下驱动当前 HFT 引擎的全部功能面（下单/撤单/市价/
//   各 TIF/各 offset/风控/查询视图/持久化/生命周期回调），并由策略自身做
//   oracle 断言，验证各组件行为是否符合预期。一套 .so 三处同跑：
//     - HFT 回测 XtBackTestRunner（确定性，主验证手段）
//     - CTP SimNow live（cloud，期货 demo，主）
//     - binance demo live（BTCUSDT）
//
// [自检哲学：必要条件断言]
//   所有自检写成"若 X 发生则 Y 必成立"（if X then Y），X 不发生不报错。
//   这样回测/CTP/binance 三路引擎差异被自然吸收；live-only 行为（per-strategy
//   风控 reject、set_next_order_reason 生效）只做软计数，不硬断言。
//
// [oracle 体系]
//   1. per-order 消账：on_fill 按 order_id 累计，与终态 filled_fp 双源交叉
//      （overfill / fok_partial / filled_qty_mismatch / fill_sum_mismatch）。
//      终态单进 bounded TTL recent map 吸收 fill/terminal 乱序，TTL 后定稿检查。
//   2. audit cycle：周期进入静默审计（停发场景单 → flatten 排空 → quiesce 断言
//      projected==0 / |pos|≈0 / confirmed==self 累计），排空超时 = audit_quiesce_timeout
//      violation（flatten 死锁类故障的结构性探测器）。每 cycle 末重锚 self_pos。
//   3. reject-storm 门：滚动窗口 unexpected reject 占比超阈 → reject_storm
//      violation（拒单风暴自报警，不依赖人工读 HB）。
//   4. 终态唯一/超时：double_terminal / stuck_terminal / stuck_cancel / view 因果 / session 单调。
//
// [延迟/效率报告]
//   策略席位 steady-clock 直方图：submit→first-ack / submit→fast-terminal /
//   cancel→canceled / tick 间隔 / on_tick 自身耗时；HB 同步输出 p50/p95/p99/max
//   与 ticks/s、orders/s。live = 真实链路 RTT；backtest = 引擎处理耗时
//   （sim 时间轴上的延迟由 latency_model 决定，与本测量无关）。
//
// [venue 无关]
//   "市价"不发 price_fp=0（回测撮合层会按 nonpos_price 直接拒），而是策略自算
//   aggressive limit（buy=对手价/涨停，sell=对手价/跌停）+ IOC，三处行为一致。
//   offset 统一发 Open/Close，今昨拆分交给 venue 层。binance 无 offset 概念，
//   gateway 自行忽略。
//
// [安全边界]（压测必须 fail-safe，不能跑飞）
//   - max_total_orders 每交易日硬上限：到顶只平不开；交易日轮转复位（per-tick
//     get_trading_date 变化检测，CTP 21:00 / binance UTC 日切由引擎统一推导）。
//     首次撞顶 loud journal + HB capped 标志 + conf_capped/conf_cap_hit_days metric。
//   - max_orders_per_min 限速预算：按秒折算 pacing，开仓类场景超额即停（flatten/cancel
//     豁免），避免超 venue 下单限速（binance -1015）反向制造海量拒单；滚动窗永不失效，
//     跑飞防护由它承担（日额度复位不削弱 fail-safe）。H3：按**每次 attempt**消费（含本地
//     拒），使失控拒因（日损/kill 等永久拒因）的本地拒循环有界，不再无限烧尝试。
//   - 周期 flatten：撤掉所有挂单（含引擎可见但非本进程提交的孤儿单）+ 反向平掉净仓 → 回到 0。
//   - 开仓仅在 ctx->can_submit() 为 true 时发；撤单/平仓不受此 gate。
//   - max_pos_qty_abs：净仓达上限强制只发减仓方向（含自身在途同向开仓敞口）。
//   - per-symbol latched-halt（T2.CONF-1）：非自愈闩锁类本地拒（SymbolBlacklisted /
//     SelfMatch{PostTrade,Detected} / StrategyKillSwitch / StrategyBlockedByWatchdog /
//     AnnualLoss）对同一 symbol 连续 kSymbolHaltStreakThreshold 次 → halt 该品种开仓场景
//     直到交易日轮转（同 capped 语义，flatten/cancel 不受影响）；防事故态（黑名单/kill）下
//     无效开仓刷爆日额度。首次 halt loud warn + conf_symbol_halted metric。
//   - marketable 腿序列化（T2.CONF-1）：market_ioc / flip / fill_storm 的激进 IOC 单腿
//     同一时刻至多一个在途（等前一腿终态才发下一腿），避免两激进反向腿在 venue RTT 窗口内
//     互相撮合触发自成交（STP 上膛后本地拒 / 未上膛时 venue 黑名单，07-21 事故形态）。
//   - 小 qty；仅 demo/testnet 运行。
//
// [场景开关]（YAML runtime.strategies[].<key>，默认全开；probe 扩维默认关，
//   开启即代表 operator 保证配置使其可触发）
//   s_rapid_submit / s_cancel_storm / s_market_ioc / s_flip / s_tif_matrix /
//   s_risk_probe / s_query_sweep / s_userdata / s_risk_probe_pos /
//   s_risk_probe_notional / s_probe_cancel_unknown
//
// [ABI] XtStrategyBase 子类 + XT_STRATEGY_ABI_EXPORTS（见 .cpp），与 hft_tick_revert 同。
// -----------------------------------------------------------------------------

#include "xt_strategy_engine/XtStrategyBase.h"
#include "xt_strategy_engine/XtStrategyInitConfig.h"
#include "api/IStrategyContext.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/market/XtBarStruct.h"
#include "xt_core/order/XtOrderTypes.h"   // XtTimeInForce / XtOrderStatus / XtOrderExecUpdate / order_exec_to_fp
#include "xt_core/order/XtOffset.h"       // XtOffset / infer_offset_from_pos
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
    // ── 初始化 ──────────────────────────────────────────────────────────
    void init(const XtStrategyInitConfig& cfg) override {
        symbol_       = cfg.get_string("symbol", "");
        const std::string venue = cfg.get_string("venue", "ctp");
        venue_is_ctp_ = (venue != "binance");   // 非 binance 一律按 ctp（含 xtp 期货语义）
        qty_          = cfg.get_double("qty", 1.0);
        warmup_ms_    = cfg.get_int("warmup_ms", 3000);
        warmup_ticks_ = cfg.get_int("warmup_ticks", 5000);   // clock 不可用时的兜底

        // 强度 / 安全
        max_total_orders_        = cfg.get_int("max_total_orders", 200000);
        decision_interval_ticks_ = static_cast<int>(cfg.get_int("decision_interval_ticks", 1));
        fire_per_tick_           = static_cast<int>(cfg.get_int("fire_per_tick", 4));
        fill_per_tick_           = static_cast<int>(cfg.get_int("fill_per_tick", 0));   // fill-storm aggressive IOC（默认 0=关）
        flatten_interval_ticks_  = static_cast<int>(cfg.get_int("flatten_interval_ticks", 400));
        heartbeat_ticks_         = static_cast<int>(cfg.get_int("heartbeat_ticks", 2000));
        stuck_timeout_ticks_     = static_cast<int>(cfg.get_int("stuck_timeout_ticks", 1000));
        // stuck 分级：evict（占用清出）仍按 stuck_timeout，viol 升级押后到 grace —
        // venue 偶发漏推终态由 ghost-REST 收敛（~31s SLA），抢先报 viol 全是怪癖假阳。
        // wall 门（live 对齐 ghost SLA）OR tick 门（BT 重放 wall 压缩，防尾窗漏报）。
        stuck_viol_grace_ms_     = cfg.get_int("stuck_viol_grace_ms", 40000);
        far_offset_ticks_        = cfg.get_int("far_offset_ticks", 100);   // rapid_submit 远盘口偏移
        max_orders_per_min_      = cfg.get_int("max_orders_per_min", 0);     // venue 限速预算（0=不限）
        halt_reprobe_decisions_  = cfg.get_int("halt_reprobe_decisions", 600); // F-C halt 自愈探针间隔（<=0 关）
        max_pos_qty_abs_fp_      = order_exec_to_fp(cfg.get_double("max_pos_qty_abs", qty_ * 5.0));
        // 平仓切片上限：默认 = 单笔 qty_（结构上必 ≤ runner risk.max_order_qty，故平仓单必过风控）。
        flatten_chunk_qty_fp_    = order_exec_to_fp(cfg.get_double("flatten_chunk_qty", qty_));
        // risk_probe：故意超 runner risk.max_order_qty 的巨量，期望被拒。
        risk_probe_qty_fp_       = order_exec_to_fp(cfg.get_double("risk_probe_qty", 1.0e9));

        // audit cycle / oracle 强度
        audit_interval_ticks_    = cfg.get_int("audit_interval_ticks", 4000);  // 0 = 关闭 audit cycle
        audit_timeout_ticks_     = cfg.get_int("audit_timeout_ticks", 3000);
        audit_settle_ticks_      = cfg.get_int("audit_settle_ticks", 50);
        recent_ttl_ticks_        = cfg.get_int("recent_terminal_ttl_ticks", 600);
        reject_storm_window_     = cfg.get_int("reject_storm_window", 200);
        // reject_storm_pct=50：in-strategy LIVE SOFT 自检，over rolling reject_storm_window(~200 单)。
        // 刻意比 OFFLINE analyzer gate `conformance_verdict.py --max-unexp-pct=20`（per-RUN 累计 HARD
        // gate）宽：live 容忍瞬时 burst（operator 可介入），offline G4 守整 run 的 bar。不同 window +
        // scope + severity → 非同口径，勿并值（health_signal_ssot Phase 4）。reject_storm 经
        // is_soft_violation_ 不计入 sweep 读的 conf_hard_violation_total（reject_storm 单独不触发 sweep；
        // 同 run 若另有 HARD viol 仍由那个 viol 触发，与本 SOFT 信号无关）。
        reject_storm_pct_        = cfg.get_int("reject_storm_pct", 50);

        // probe 扩维（默认关；开启 = operator 保证 risk 配置可触发）
        s_risk_probe_pos_        = cfg.get_int("s_risk_probe_pos", 0) != 0;
        risk_probe_pos_qty_fp_   = order_exec_to_fp(cfg.get_double("risk_probe_pos_qty", 0.0));
        s_risk_probe_notional_   = cfg.get_int("s_risk_probe_notional", 0) != 0;
        risk_probe_notional_qty_fp_ = order_exec_to_fp(cfg.get_double("risk_probe_notional_qty", 0.0));
        s_probe_cancel_unknown_  = cfg.get_int("s_probe_cancel_unknown", 1) != 0;

        // 场景开关
        s_rapid_submit_ = cfg.get_int("s_rapid_submit", 1) != 0;
        s_cancel_storm_ = cfg.get_int("s_cancel_storm", 1) != 0;
        s_market_ioc_   = cfg.get_int("s_market_ioc",   1) != 0;
        s_flip_         = cfg.get_int("s_flip",         1) != 0;
        s_tif_matrix_   = cfg.get_int("s_tif_matrix",   1) != 0;
        s_risk_probe_   = cfg.get_int("s_risk_probe",   1) != 0;
        s_query_sweep_  = cfg.get_int("s_query_sweep",  1) != 0;
        s_userdata_     = cfg.get_int("s_userdata",     1) != 0;
        s_md_oracle_    = cfg.get_int("s_md_oracle",    1) != 0;   // 行情正确性自检（bid<ask/真实档单调/缺档终止）
        s_book_oracle_  = cfg.get_int("s_book_oracle",  1) != 0;   // book leaves 一致性自检（回报流独立重建 vs get_open_orders）

        // probe 节流：每 N 个轮转机会才发一次，避免 probe 拒单淹没真实订单流。
        probe_every_decisions_ = cfg.get_int("probe_every_decisions", 32);
        if (probe_every_decisions_ < 1) probe_every_decisions_ = 1;
        // cancel 重试窗必须 < stuck_timeout：首发 cancel 丢失（如 sim 中撤单先于
        // 进簿被丢）时重试要赶在 stuck 误报前；live 上限 500 防 IP 预算烧穿。
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

        // 参数通道 exercise：typed getter 路径不崩即可（值无 contract，不断言）。
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

    /// 压测要 engine fan-out on_tick / on_fill / on_order 三通道。
    uint64_t capabilities() const noexcept override {
        return to_capability_bit(StrategyCapability::SubscribesTick)
             | to_capability_bit(StrategyCapability::RequiresOnFill)
             | to_capability_bit(StrategyCapability::RequiresOnOrder);
    }

    // ── 主热路径 ────────────────────────────────────────────────────────
    void on_tick(IStrategyContext* ctx, const XtTick& tk) override {
        if (symbol_.empty() || std::strcmp(tk.code, symbol_.c_str()) != 0) return;
        const int64_t t0 = now_us();
        // tick 间隔（事件循环停滞探测）；超 60s 视为 session 边界不入样。
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

    // ── 回报回调（消账 + oracle）────────────────────────────────────────
    void on_order(IStrategyContext* ctx, const XtOrderExecUpdate& u) override {
        auto it = inflight_.find(u.id);
        if (it == inflight_.end()) { on_order_not_inflight_(ctx, u); return; }
        Inflight& o = it->second;
        if (o.first_ack_us == 0) {
            o.first_ack_us = now_us();
            lat_ack_.add(o.first_ack_us - o.submit_us);
        }
        // view 因果性：本 tick 提交的单，其回报不应在同一 on_tick 内被处理。
        if (u.exch_ts_us == 0 && o.submit_tick_seq == tick_seq_ && inside_on_tick_) {
            violation(ctx, "view_causality", "order update observed within same on_tick as submit");
        }
        if (u.filled_fp > o.cum_upd_fill_fp) o.cum_upd_fill_fp = u.filled_fp;
        // venue 缩量跟踪（binance reduceOnly amend-down 只缩不涨）：min-merge
        // 免疫乱序旧 update 携带原始 qty 的回抬。
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
        // 终态单移入 recent map：吸收 fill/terminal 乱序，TTL 后定稿检查（见 finalize_terminal_）。
        TerminalRec rec;
        rec.qty_fp = o.qty_fp; rec.cum_onfill_fp = o.cum_fill_fp;
        rec.terminal_filled_fp = u.filled_fp; rec.terminal_tick = tick_seq_;
        rec.tif = o.tif; rec.offset = o.offset; rec.status = u.status;
        rec.expect_reject = o.expect_reject; rec.overfill_flagged = o.overfill_flagged;
        recent_terminal_[u.id] = rec;
        inflight_.erase(it);
        // T2.CONF-1：在途 marketable 腿终态 → 放行下一激进腿。
        if (u.id == pending_marketable_id_) pending_marketable_id_ = 0;
        check_reject_storm_(ctx);
    }

    void on_fill(IStrategyContext* ctx, const XtFillEvent& f) override {
        ++fills_total_;
        apply_self_fill_(f);   // close 越零截断后累加自记净仓；quiesce 点做硬断言
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
                ++fill_after_cancel_total_;   // 撤单 ack 与成交跨线 race，live 合法 → 软计数
            }
            return;
        }
        ++unknown_fill_total_;   // 非本策略登记单的成交（孤儿/TTL 过期），软计数
    }

    void on_account(IStrategyContext* /*ctx*/, const XtAccountUpdate& /*a*/) override {
        ++account_update_total_;   // live-only 通道：仅计数，sanity 在 query_sweep 做。
    }

    // ── 生命周期回调（计数 + 单调自检）──────────────────────────────────
    void on_session_begin(IStrategyContext* ctx, uint32_t trading_date) override {
        ++session_begin_total_;
        if (last_trading_date_ != 0 && trading_date < last_trading_date_) {
            violation(ctx, "session_monotonic", "session_begin trading_date went backwards");
        }
        last_trading_date_ = trading_date;
    }

    void on_session_end(IStrategyContext* /*ctx*/, uint32_t /*trading_date*/) override {
        ++session_end_total_;
    }

    void on_roll(IStrategyContext* ctx, const char* old_code, const char* new_code,
                 uint32_t /*trading_date*/, int64_t /*old_last_price_fp*/) override {
        ++roll_total_;
        // 透明换月：把后续下单/仓位口径切到新主力合约。
        // dispatch_tick 按订阅过滤，必须重订阅新合约，否则 roll 后策略收不到任何 tick
        //（同 hft_tick_revert / hft_market_making 的 on_roll 契约）。
        if (new_code && new_code[0]) {
            symbol_ = new_code;
            ctx->sub_tick(new_code);
            self_pos_fp_ = confirmed_pos_(ctx);   // 以新合约 confirmed 真值重锚
            self_pos_anchored_ = true;
        }
        char buf[160];
        std::snprintf(buf, sizeof(buf), "conf ROLL %s -> %s (retarget)",
                      old_code ? old_code : "", new_code ? new_code : "");
        ctx->log_info(buf);
    }

    void on_params(IStrategyContext* ctx) override {
        ++params_update_total_;
        // 热更新通道 exercise：重读一个 typed param（值无 contract，不断言）。
        (void)ctx->get_param_int("fire_per_tick", -1);
    }

    // ── 通道状态（live-only；回测不触发，纯计数 + 可见性）────────────────
    void on_channel_ready(IStrategyContext* ctx, const char* channel) override {
        ++channel_ready_total_;
        last_channel_ready_tick_ = tick_seq_;   // recovery 窗口标记：见 audit_settle_asserts_ drift gate
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
    // ── 场景枚举（轮转用）──
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
        int64_t       cum_fill_fp   = 0;       ///< on_fill 累计（消账）
        int64_t       cum_upd_fill_fp = 0;     ///< on_order filled_fp 累计 max（book oracle 双源之一）
        int64_t       venue_qty_fp  = 0;       ///< venue 确认口径 qty（amend-down min-merge 只缩不涨；0=未初始化按 qty_fp）
        bool          book_viol_flagged = false; ///< book oracle 每单只报一次
        XtTimeInForce tif           = XtTimeInForce::GTC;
        XtOffset      offset        = XtOffset::Open;
        int64_t       submit_tick_seq = 0;
        int64_t       submit_us     = 0;       ///< steady-clock 提交时刻
        int64_t       first_ack_us  = 0;       ///< 首个 on_order 回报时刻（0=未到）
        int64_t       cancel_req_us = 0;       ///< 首次 cancel 请求时刻（0=未撤）
        int64_t       cancel_req_tick = -1;    ///< 最近一次 cancel 请求 tick（冷却去重）
        int64_t       cancel_first_tick = -1;  ///< 首次 cancel 请求 tick（cancel 无应答驱逐基准）
        bool          expect_reject = false;   // 各 risk probe 的单
        bool          expect_fast_terminal = false;  // IOC/FOK/已撤的单：应快速终态
        bool          overfill_flagged = false;
    };

    /// 终态后的消账暂存：等 TTL 内的晚到 fill 归账后定稿检查。
    struct TerminalRec {
        int64_t       qty_fp             = 0;
        int64_t       cum_onfill_fp      = 0;
        int64_t       terminal_filled_fp = 0;   ///< 终态回报自带的累计成交（双源之二）
        int64_t       terminal_tick      = 0;
        XtTimeInForce tif                = XtTimeInForce::GTC;
        XtOffset      offset             = XtOffset::Open;   ///< 提交口径 offset（reduceOnly amend-down 收窄用）
        XtOrderStatus status             = XtOrderStatus::Canceled;
        bool          expect_reject      = false;
        bool          overfill_flagged   = false;
        bool          stuck              = false;   ///< stuck 清出的单：只查 overfill，不定稿比对
        bool          stuck_fast         = false;   ///< stuck 类型：true=fast_terminal / false=cancel（升级 viol 名用）
        bool          got_late_terminal  = false;
        int64_t       stuck_evict_us     = 0;       ///< stuck 清出时刻（grace 升级判定基准）
        bool          had_terminal_conflict = false; ///< 双通道矛盾终态(ws/UDS)出现过 → fill_sum_mismatch 降 soft
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
        int64_t pct(int p) const noexcept {   // p ∈ [1,100]，返回该分位所在桶的上界
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

    // ── 主循环体 ────────────────────────────────────────────────────────
    void on_tick_impl_(IStrategyContext* ctx, const XtTick& tk) {
        ++tick_seq_;
        // 交易日轮转（T1.152）：get_trading_date 前进即复位日额度与撞顶闩。CTP 21:00 /
        // binance UTC 日切由引擎侧统一推导；warmup 期也检测防跨日重启窗漏切；td=0
        //（view 未就绪）不切；**仅向前切**——binance 多流 event-time 倒挂（见
        // check_md_tick_ 注释）午夜窗口会短暂回跳，回跳视为 stale 不复位，防额度
        // 翻倍与闩抖动（盲审 fold）。
        const uint32_t td_now = ctx->get_trading_date();
        if (td_now != 0 && td_now > cap_day_) {
            if (cap_day_ != 0) {
                char rbuf[144];
                std::snprintf(rbuf, sizeof(rbuf),
                              "conf CAP day rollover %u -> %u ord_day=%llu capped=%d (quota reset)",
                              cap_day_, td_now, (unsigned long long)orders_day_, (int)capped_);
                ctx->log_info(rbuf);
            }
            cap_day_ = td_now;
            orders_day_ = 0;
            capped_ = false;
            // T2.CONF-1：日切复位品种 halt 闩 + 拒因连续计数（与 cap 同 rollover）。
            symbol_open_halted_.clear();
            symbol_reject_streak_.clear();
        }
        // 预热：等 td 就绪（账户快照）再发单，clock 不可用时按 tick 数兜底。
        const int64_t now_ms = packed_hms_to_ms(ctx->get_time_ms());
        if (first_ms_ < 0) first_ms_ = now_ms;
        const bool warmed = (now_ms - first_ms_ >= warmup_ms_) || (tick_seq_ >= warmup_ticks_);
        if (!warmed) { ++skip_warmup_total_; return; }

        // 冷启动锚定：重启后 recovery 仓位走 snapshot 覆盖（Recovering 态不 fan-out
        // on_fill），self 累加起点必须取引擎真值；锚定前不做 drift 观测、不发单。
        if (!self_pos_anchored_) {
            if (!ctx->can_submit()) { ++skip_gate_total_; return; }
            self_pos_fp_ = confirmed_pos_(ctx);
            self_pos_anchored_ = true;
            char abuf[96];
            std::snprintf(abuf, sizeof(abuf), "conf ANCHOR self_pos_fp=%lld",
                          (long long)self_pos_fp_);
            ctx->log_info(abuf);
        }

        // ── 周期性 always-on oracle / 维护 ──
        observe_drift_(ctx);
        if (s_md_oracle_) check_md_tick_(ctx, tk);      // 行情正确性自检（venue 条件化）
        if (s_query_sweep_) run_query_sweep(ctx, tk);   // 只读视图压测（无副作用）
        check_stuck_orders(ctx);                        // 终态超时自检（应快速终态的单）
        if ((tick_seq_ & kRecentPurgeMask) == 0) purge_recent_terminals_(ctx);
        if (audit_state_ == AuditState::Normal &&
            flatten_interval_ticks_ > 0 && (tick_seq_ % flatten_interval_ticks_) == 0)
            flatten(ctx, tk, "periodic");               // 周期净仓归零 + 撤挂单
        if (heartbeat_ticks_ > 0 && (tick_seq_ % heartbeat_ticks_) == 0)
            heartbeat(ctx);

        // ── audit cycle：静默排空 + quiesce 硬断言（排空超时 = 死锁探测）──
        if (audit_interval_ticks_ > 0) run_audit_machine_(ctx, tk);
        if (audit_state_ != AuditState::Normal) return;   // 审计期不发场景单

        // ── 发单类场景：受 can_submit gate + 安全上限 + venue 限速预算 ──
        if (!ctx->can_submit()) { ++skip_gate_total_; return; }
        if (decision_interval_ticks_ > 1 && (tick_seq_ % decision_interval_ticks_) != 0) return;
        if (orders_day_ >= (uint64_t)max_total_orders_) {  // 每交易日硬上限：只剩 flatten/cancel 维护
            if (!capped_) {   // 首次撞顶 loud（每交易日一次）；日切复位后重挂
                capped_ = true;
                ++cap_hit_days_total_;
                char cbuf[176];
                std::snprintf(cbuf, sizeof(cbuf),
                              "conf CAP HIT day=%u ord_day=%llu max=%lld "
                              "(open scenarios halted until day rollover; flatten/cancel unaffected)",
                              cap_day_, (unsigned long long)orders_day_,
                              (long long)max_total_orders_);
                ctx->log_warn(cbuf);
            }
            return;
        }
        // T2.CONF-1：品种 halt（连续非自愈拒因）→ 跳过全部开仓场景（同 capped 语义）。
        // flatten/heartbeat 已在本 tick 上方执行，不受影响，仍会把仓位平回 0。
        // F-C re-probe 自愈：每 halt_reprobe_decisions_ 放行一次探针 submit；成功即在 submit()
        // 内清 halt（解 kill-switch resume / open-halt 解除后 conformance 静默死到日切）。
        if (is_open_halted_(symbol_)) {
            if (halt_reprobe_decisions_ <= 0) return;                 // 关闭 re-probe = 死到日切
            if (++halt_reprobe_counter_ < halt_reprobe_decisions_) return;
            halt_reprobe_counter_ = 0;
            // H4：显式标记本次为 Open 探针——只有它成功才清 halt（submit 内 halt_probe_active_
            // gate），普通 Close/flatten 成功不得伪造清闩。
            halt_probe_active_ = true;
            fire_rapid_submit(ctx, tk);   // 单探针（far GTC，Open offset）；成功清 halt，失败保持
            halt_probe_active_ = false;
            return;                       // 本 tick 只发探针，不跑其余场景
        }
        if (!order_budget_ok(now_ms)) { ++budget_skip_total_; return; }

        // rapid_submit：每 decision tick 灌 fire_per_tick 个远盘口 GTC（压 submit/orderbook/risk）。
        // F-B：循环体内逐次查 is_open_halted_ —— latch 后同 tick 不再追加提交（修「同 tick 继续烧」）。
        if (s_rapid_submit_) {
            for (int i = 0; i < fire_per_tick_ && orders_day_ < (uint64_t)max_total_orders_
                            && order_budget_ok(now_ms) && !is_open_halted_(symbol_); ++i)
                fire_rapid_submit(ctx, tk);
        }

        // fill-storm：每 decision tick 灌 fill_per_tick 个 aggressive IOC，专压 fill/撮合/仓位/记账/PnL（默认 0=关）。
        if (fill_per_tick_ > 0) {
            int64_t fs_proj_fp = 0;   // 本 tick fill-storm 乐观净仓投影（fill 跨 tick 才落 confirmed_pos_）
            for (int i = 0; i < fill_per_tick_ && orders_day_ < (uint64_t)max_total_orders_
                            && order_budget_ok(now_ms) && !is_open_halted_(symbol_); ++i)
                fire_fill_storm(ctx, tk, fs_proj_fp);
        }

        // 其余场景轮转执行（一次一个，分散压力 + 全覆盖）。
        // 日额度同时 gate rotation：撞顶边界 tick 上 rapid/fill 循环耗尽余额后，
        // rotation 不得再超发（盲审 fold，两席独立命中）。
        if (orders_day_ < (uint64_t)max_total_orders_ && order_budget_ok(now_ms))
            dispatch_rotating_scenario(ctx, tk);
    }

    // ── 价格 helper（venue 无关 aggressive 价）──
    int64_t aggressive_buy_px(const XtTick& tk) const noexcept {
        // binance: 返回 0 → ctx 解析市价(best-opposite±1% IOC)，成交率高于自算 best-price limit。
        // ctp/backtest: 涨停/对手价 limit（回测撞合拒 price_fp=0，故仅非 binance 走 limit）。
        if (!venue_is_ctp_) return 0;
        if (tk.upper_limit_fp > 0) return tk.upper_limit_fp;   // 涨停确保成交
        if (tk.ask_price_fp[0] > 0) return tk.ask_price_fp[0]; // 对手价
        return tk.price_fp;
    }
    int64_t aggressive_sell_px(const XtTick& tk) const noexcept {
        if (!venue_is_ctp_) return 0;                          // binance: 市价（见 buy 注）
        if (tk.lower_limit_fp > 0) return tk.lower_limit_fp;   // 跌停确保成交
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
        // 优先 catalog 真值；无 contract_info 时用 price 的 1e-4 兜底，至少 1 fp。
        if (cached_price_tick_fp_ > 0) return cached_price_tick_fp_;
        if (const auto* ci = ctx->get_contract_info(symbol_.c_str())) {
            const int64_t t = order_exec_to_fp(ci->price_tick);
            if (t > 0) { cached_price_tick_fp_ = t; return t; }
        }
        const int64_t mid = ref_mid_px(tk);
        int64_t t = mid / 10000;
        return t > 0 ? t : 1;
    }

    // ── 行情正确性自检（venue 条件化必要条件断言）──
    /// 真实档前缀严格单调 + 缺档终止（缺档之后不得再出现真实档）。
    /// 档位存在性 = price>0 且 qty>0：BT CSV reader 对缺档做上一档价格继承
    /// （TickCsvUtil L2-L5 缺价继承前档 + L6+ 延续 L5 清零量），继承档价格非零但
    /// qty==0 → 以 qty 判存在性让填充档与 venue 缺档（price==0）统一跳过；
    /// 真实 venue 档位 qty 恒 >0，binance 满档严格单调断言力度不变。
    /// @param px/@param qty DEPTH 档价格/数量数组；@param descending true=bid(严格降) / false=ask(严格升)。
    /// @return true=结构破坏（真实档前缀非单调，或缺档后又出现真实档）。
    static bool md_prefix_bad_(const int64_t* px, const int64_t* qty, bool descending) noexcept {
        bool gap_seen = false;
        int64_t prev = 0;
        for (int i = 0; i < XtTick::DEPTH; ++i) {
            const bool present = px[i] > 0 && qty[i] > 0;
            if (!present) { gap_seen = true; continue; }
            if (gap_seen) return true;                  // 缺档之后又出现真实档 = 串档
            if (prev != 0) {
                if (descending && px[i] >= prev) return true;   // bid 必须严格降
                if (!descending && px[i] <= prev) return true;  // ask 必须严格升
            }
            prev = px[i];
        }
        return false;
    }

    /// 行情盘口必要条件断言：L1 存在性 / 不交叉 / 真实档深度单调 / 缺档终止。
    /// binance 实测带满 DEPTH 档非零盘口（push 被 book-ready 门控）→ 全硬断言；
    /// CTP 涨跌停单边价合法为 0、SimNow 仅 1 档 → L1 缺失与触碰软计数，仅严格交叉硬断言。
    /// 所有断言以"真实档存在"（price>0 且 qty>0）为前提（if X then Y），
    /// BT 数据无 depth / CSV reader 价格继承填充档自然跳过。
    void check_md_tick_(IStrategyContext* ctx, const XtTick& tk) {
        const int64_t b0 = tk.bid_price_fp[0], a0 = tk.ask_price_fp[0];
        const bool both_l1 = (b0 > 0 && a0 > 0);

        // L1 存在性：binance 必有（硬，首发限流）；CTP 单边合法 → 软计数。
        if (!both_l1) {
            ++md_one_sided_total_;
            if (!venue_is_ctp_ && (b0 == 0 || a0 == 0) && !viol_once_md_l1_) {
                viol_once_md_l1_ = true;
                violation(ctx, "md_l1_absent", "binance tick missing L1 bid/ask");
            }
        }
        // 交叉 / 触碰：严格 bid>ask 两 venue 硬；bid==ask binance 硬、CTP 软（集合竞价）。
        if (both_l1) {
            if (b0 > a0) {
                ++md_crossed_total_;
                md_viol_crossed_(ctx, b0, a0);
            } else if (b0 == a0) {
                if (!venue_is_ctp_) { ++md_crossed_total_; md_viol_crossed_(ctx, b0, a0); }
                else                  ++md_touch_total_;
            }
        }
        // 深度单调 + 缺档终止：真实档（price>0 且 qty>0）存在才查（BT 无 depth 时空真）。
        if ((b0 > 0 || a0 > 0) &&
            (md_prefix_bad_(tk.bid_price_fp, tk.bid_qty_fp, /*descending=*/true) ||
             md_prefix_bad_(tk.ask_price_fp, tk.ask_qty_fp, /*descending=*/false))) {
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

    /// @param pos confirmed 口径净仓（caller 用 confirmed_pos_ 取；勿传 get_position_fp：
    ///            live 投影口径与本函数 outstanding 双计，且僵尸在途投影会使方向判断失真）。
    bool can_open_side(int64_t pos, bool is_buy) const noexcept {
        // 净仓达上限时禁止继续加仓（只允许减仓方向）；计入自身在途同向开仓敞口，
        // 避免 risk 在途预占先于真仓位命中 max_pos 造成可预见的本地拒单。
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

    // ── T2.CONF-1：per-symbol 非自愈拒因 halt + marketable 腿序列化 ──
    bool is_open_halted_(const std::string& code) const {
        auto it = symbol_open_halted_.find(code);
        return it != symbol_open_halted_.end() && it->second;
    }

    /// 记录一次本地开仓拒（非 expect_reject）：latch_class 累加连续计数，非 latch 归零；
    /// 连续达阈值首次置 halt 闩（loud journal 去重）。halt 闩留至日切复位。
    void note_open_reject_(IStrategyContext* ctx, bool latch_class) {
        int& streak = symbol_reject_streak_[symbol_];
        if (!latch_class) { streak = 0; return; }
        if (++streak < kSymbolHaltStreakThreshold) return;
        bool& halted = symbol_open_halted_[symbol_];
        if (halted) return;   // 已 halt → 去重 loud
        halted = true;
        ++symbol_halt_events_total_;
        char hbuf[192];
        std::snprintf(hbuf, sizeof(hbuf),
            "conf SYMBOL HALT sym=%s streak=%d "
            "(non-self-healing latch reject; open scenarios halted until day rollover; "
            "flatten/cancel unaffected)",
            symbol_.c_str(), streak);
        ctx->log_warn(hbuf);
    }

    /// 是否有在途 marketable（激进 IOC）腿未终态。终态回报清零 pending_marketable_id_；
    /// 兜底：id 已不在 inflight_（终态已处理）亦视为无在途。
    bool marketable_pending_() const {
        return pending_marketable_id_ != 0 &&
               inflight_.find(pending_marketable_id_) != inflight_.end();
    }

    /// F-B（修 C4）+ H2/H5：激进价腿前置栅栏 —— 己方对侧不得有任何未终态订单方可发激进腿
    /// （激进价按构造穿越己方任一对侧 resting → venue RTT 窗口互撮 / 穿越 recovered far GTC
    /// 触发真实自成交）。对侧扫描 = 策略 inflight_ ∪ ctx->get_open_orders()（book 派生，含
    /// cold-start Recovered / stuck-erase 离 inflight_ 的对侧单——inflight_ 单独看只是
    /// happy-path only 栅栏）。inflight_ 对侧单走 request_cancel_（去重簿记）；book 派生但不在
    /// inflight_ 的对侧单幂等 ctx->cancel。存在任一 → return false 让本 decision 跳过激进腿
    /// （软计数 aggr_gate_skips_，不算拒、不进 streak）+ 记录被阻塞侧（H5：fire_rapid_submit
    /// 据此 side-steer，让对侧清空，下一 decision 激进腿放行）；对侧清 → 恢复交替。
    bool aggressive_gate_ready_(IStrategyContext* ctx, bool is_buy) {
        bool blocked = false;
        // (a) 策略 inflight_ 簿记（cancel 去重 / 延迟打点归此）。
        for (auto& kv : inflight_) {
            Inflight& o = kv.second;
            if (o.is_buy == is_buy) continue;   // 只看对侧（激进 buy 会穿越己方 sell/ask）
            blocked = true;
            request_cancel_(ctx, kv.first, o);  // 未终态对侧单补撤
        }
        // (b) H2：book 派生 open-order 并集——含 recovered / stuck-erase 离 inflight_ 的对侧单。
        // K3（B-3 披露）：sid routing 下 get_open_orders 下传本策略 sid_filter + 本次扫描
        // kOrphanSweepBuf(64) 截断 → 看不到 sid=0 的 recovered-unowned / 他 sid 的对侧单。真实
        // 自成交由账户级 STP（seed 全量不分 sid）兜住不落 venue；后果 = 激进腿撞 SelfTradeDetected
        // （transient 软计数不 latch）直到 runner engine orphan_reap / B3/B4 收敛该对侧单——duty-cycle
        // 非安全，非 correctness 洞。裁决不改 get_open_orders 契约（见 fold_notes R3 §K3）。
        XtOrderExecUpdate ob[kOrphanSweepBuf];
        const size_t n = ctx->get_open_orders(symbol_.c_str(), ob, kOrphanSweepBuf);
        for (size_t i = 0; i < n; ++i) {
            const uint64_t oid = ob[i].id;
            if (oid == 0 || ob[i].is_buy == is_buy) continue;  // 只看对侧
            if (inflight_.count(oid) != 0) continue;           // 已在 (a) 处理（去重）
            blocked = true;   // 对侧 book 单存在即阻塞激进腿（无论本 tick 是否重撤）
            // K2（修 B-2）：per-id 冷却/give-up（复用 orphan 表）——同 tick fill_per_tick 循环
            // 每迭代重撤同一 recovered/stuck 单会烧 gateway cancel limiter，冷却窗内不重发。
            try_cancel_orphan_(ctx, oid);
        }
        if (blocked) {
            ++aggr_gate_skips_;
            steer_rapid_valid_  = true;    // H5：记被阻塞的激进腿侧 → rapid 同侧 re-arm，对侧留空
            steer_rapid_is_buy_ = is_buy;
        } else {
            steer_rapid_valid_ = false;    // 对侧已清 → 恢复交替
        }
        return !blocked;
    }

    // ── F-C：单一表驱动 reject 分类（替换散落的 infra_gated / latch_class 双表）──
    // kSticky      = 当日持续、非自愈拒因 → 进 streak/halt 防烧 cap（+re-probe 自愈）。
    // kTransient   = 瞬态拒因 → 不 latch、软计数（SelfTradeDetected 经 F-B gate 后应≈0）。
    // kCloseFriction = 平仓预占摩擦（高压预期）→ close_local 桶。
    // kInfraSoft   = runner 状态软拒（已知码）→ gated 桶，不 latch。
    // kUnexpected  = 未归类真拒 → unexp 桶（喂 reject_storm）。
    // kExpectedProbe（探针预期拒）由 expect_reject flag 驱动，不经本表。
    enum class RejectClass { kSticky, kTransient, kCloseFriction, kInfraSoft, kUnexpected };

    static RejectClass classify_reject_(xt_runtime::XtRejectCode rc, XtOffset offset) {
        using RC = xt_runtime::XtRejectCode;
        switch (rc) {
            // sticky：当日持续、非自愈 → latch/halt（C5 修：补入 OpenHalted /
            // KillSwitchActive / StrategyDailyLossExceeded；H3 补入全局 DailyLossExceeded
            // ——否则永久拒因逐 attempt 烧 cap / per-min 预算）。
            case RC::SymbolBlacklisted:
            case RC::SelfMatchPostTrade:
            case RC::StrategyKillSwitchActive:
            case RC::StrategyBlockedByWatchdog:
            case RC::AnnualLossExceeded:
            case RC::OpenHalted:
            case RC::KillSwitchActive:
            case RC::StrategyDailyLossExceeded:
            case RC::DailyLossExceeded:      // H3：全局账户级日损（vs per-sid StrategyDailyLossExceeded）
                return RejectClass::kSticky;
            // transient：瞬态，不 latch（C5 修：SelfTradeDetected 从 latch 移出——F-B gate
            // 后应≈0，锁到日切是错位）。
            case RC::SelfTradeDetected:
            case RC::OrderRateExceeded:
                return RejectClass::kTransient;
            // close friction：平仓预占摩擦（高压预期）→ close_local。
            case RC::CloseSyncPending:
            case RC::InsufficientCloseToday:
            case RC::InsufficientCloseYesterday:
            case RC::PositionSyncStale:
            case RC::OppositeOpenPending:
                return RejectClass::kCloseFriction;
            // infra soft：已知 runner 状态软拒（组件正确 gating）→ gated。
            case RC::CloseoutNoOpen:
            case RC::Gated:
            case RC::TradingNotReady:
            case RC::OrderCountExceeded:
            case RC::StrategyMaxPosQtyExceeded:
                return RejectClass::kInfraSoft;
            case RC::PositionLimit:
                // overloaded：!Open = 可平不足（close friction）；Open = 真硬顶（unexp）。
                return offset != XtOffset::Open ? RejectClass::kCloseFriction
                                                : RejectClass::kUnexpected;
            case RC::Unknown:
                // offset 启发式：Unknown-close = close friction 兜底；Unknown-open = unexp。
                return offset != XtOffset::Open ? RejectClass::kCloseFriction
                                                : RejectClass::kUnexpected;
            default:
                return RejectClass::kUnexpected;
        }
    }

    // ── 下单封装（统一登记 in-flight + 统计）──
    uint64_t submit(IStrategyContext* ctx, int scenario, bool is_buy, int64_t price_fp,
                    int64_t qty_fp, XtTimeInForce tif, XtOffset offset,
                    const char* tag, bool expect_reject, bool expect_fast_terminal) {
        ctx->set_next_order_reason(tag);   // live-only：进 ORDER_SUBMIT 录制（回测 noop）
        const int64_t t_sub = now_us();
        const uint64_t id = is_buy
            ? ctx->buy_fp (symbol_.c_str(), price_fp, qty_fp, tif, tag, offset)
            : ctx->sell_fp(symbol_.c_str(), price_fp, qty_fp, tif, tag, offset);
        ++orders_total_;
        ++window_orders_;
        // H3：per-min 预算按**每次 attempt**消费（submit 入口即消费，含本地拒）——兑现
        // 「失控拒因护栏」：本地拒循环有界（失控日损/kill 等永久拒因不会无限烧尝试）。
        // day cap（orders_day_）仍守 accepted-only（venue 负载语义不变），两口径分离。
        consume_order_budget();
        if (id == 0) {
            // 本地拒（id==0，未发出）。F-C 单一表分类（classify_reject_）：kSticky 进 streak/halt；
            // kTransient/kInfraSoft → gated 桶；kCloseFriction → close_local；kUnexpected → unexp
            // （喂 reject_storm）。cap 计 accepted-only → 本地拒不烧 orders_day_（下方成功才计）。
            const auto rc = ctx->last_reject_code();
            using RC = xt_runtime::XtRejectCode;
            if (rc == RC::SelfTradeDetected) ++selftrade_detected_total_;  // transient 独立可见
            if (expect_reject) { ++rejects_expected_; probe_hit_(scenario); }
            else {
                const RejectClass cls = classify_reject_(rc, offset);
                note_open_reject_(ctx, cls == RejectClass::kSticky);  // 仅 sticky 进 streak/halt
                switch (cls) {
                    case RejectClass::kSticky:
                    case RejectClass::kTransient:
                    case RejectClass::kInfraSoft:
                        ++rejects_gated_; break;
                    case RejectClass::kCloseFriction:
                        ++rejects_close_local_; break;
                    case RejectClass::kUnexpected:
                        ++rejects_unexpected_; ++window_unexp_; break;
                }
            }
            check_reject_storm_(ctx);
            return 0;
        }
        // F-C：cap 计 accepted-only —— 成功提交才计日额度（本地拒不烧，失控拒因由
        // latch/re-probe + per-min 护栏兜底）。
        ++orders_day_;
        // H4：streak「连续」判据只由**开仓**成功归零——Close/flatten 成功不清 streak
        //（Close 被 OpenHalted 放行，不代表 Open 拒因已解除）。
        if (offset == XtOffset::Open) symbol_reject_streak_[symbol_] = 0;
        // H4 re-probe 自愈：halt 清闩**只认显式 Open 探针成功**（halt_probe_active_ 由 on_tick
        // halted-reprobe 分支贯穿 submit）——普通 Close/flatten 成功不得伪造清闩（OpenHalted
        // 只拒 Open、放 Close，任何 accepted 都清 halt 会被 flatten Close 击穿 → 拒因仍在却清闩
        // → 同 tick 重开又被拒又 relatch 的 churn）。
        if (halt_probe_active_) {
            auto halt_it = symbol_open_halted_.find(symbol_);
            if (halt_it != symbol_open_halted_.end() && halt_it->second) {
                halt_it->second = false;
                halt_reprobe_counter_ = 0;
                ++symbol_halt_clears_total_;
                ctx->log_info("conf SYMBOL HALT CLEARED (reprobe Open probe accepted)");
            }
        }
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

    /// cancel 请求（冷却去重 + 延迟打点）。首次必发，重试受冷却窗约束。
    void request_cancel_(IStrategyContext* ctx, uint64_t id, Inflight& o) {
        if (o.cancel_req_tick >= 0 &&
            (tick_seq_ - o.cancel_req_tick) <= cancel_retry_ticks_) return;
        ctx->cancel(id);
        o.cancel_req_tick = tick_seq_;
        if (o.cancel_first_tick < 0) o.cancel_first_tick = tick_seq_;
        if (o.cancel_req_us == 0) o.cancel_req_us = now_us();
        ++cancels_req_total_;
    }

    // ── 场景：rapid_submit（远盘口 GTC，纯压 submit/orderbook，不期望成交）──
    void fire_rapid_submit(IStrategyContext* ctx, const XtTick& tk) {
        const int64_t tickfp = price_tick_fp(ctx, tk);
        const int64_t off = tickfp * far_offset_ticks_;
        // H5：aggressive gate 阻塞待清场时（steer_rapid_valid_），与 pending 激进腿同侧 re-arm
        // far GTC —— 同侧 passive 永不交叉、对侧保持干净 → 下一 decision 激进腿放行（恢复任意
        // fire_per_tick 下 fills>0，修 fire_per_tick>=2 双侧每 tick re-arm 致激进腿永久 block）；
        // 无 pending 时维持交替（远离盘口 → 挂着不成交，靠 flatten/cancel_storm 回收）。
        const bool is_buy = steer_rapid_valid_ ? steer_rapid_is_buy_ : ((rotor_++ & 1) == 0);
        if (!can_open_side(confirmed_pos_(ctx), is_buy)) return;
        const int64_t mid = ref_mid_px(tk);
        if (mid <= 0) return;
        const int64_t px = is_buy ? (mid - off > 0 ? mid - off : tickfp) : (mid + off);
        submit(ctx, -1 /*rapid*/, is_buy, px, order_exec_to_fp(qty_),
               XtTimeInForce::GTC, XtOffset::Open, "rapid_submit",
               /*expect_reject=*/false, /*fast_terminal=*/false);
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
            return;  // 一次只跑一个场景
        }
    }

    // ── 场景：cancel_storm（发远盘口单立即撤）──
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
                                   false, /*fast_terminal=*/true);
        if (id != 0) {
            auto it = inflight_.find(id);
            if (it != inflight_.end()) request_cancel_(ctx, id, it->second);
        }
    }

    // ── 场景：market_ioc（aggressive limit + IOC，验证 IOC 语义 + fill 因果）──
    void fire_market_ioc(IStrategyContext* ctx, const XtTick& tk) {
        if (marketable_pending_()) return;   // T2.CONF-1：等前一激进腿终态（防两腿 RTT 窗口互撮）
        const int64_t pos = confirmed_pos_(ctx);
        const bool is_buy = oscillation_side(pos);
        const XtOffset offset = infer_offset_from_pos(pos, is_buy);
        if (offset == XtOffset::Open && !can_open_side(pos, is_buy)) return;
        if (!aggressive_gate_ready_(ctx, is_buy)) return;  // F-B：己方对侧无未终态单方可发
        const int64_t px = is_buy ? aggressive_buy_px(tk) : aggressive_sell_px(tk);
        if (venue_is_ctp_ && px <= 0) return;  // binance px=0=市价合法
        const uint64_t id = submit(ctx, SC_MARKET_IOC, is_buy, px, order_exec_to_fp(qty_),
               XtTimeInForce::IOC, offset, "market_ioc", false, /*fast_terminal=*/true);
        if (id != 0) pending_marketable_id_ = id;
    }

    // ── 场景：fill_storm（aggressive IOC，高频成交压 fill/撮合/仓位 reducer/记账/PnL；config-gated）──
    void fire_fill_storm(IStrategyContext* ctx, const XtTick& tk, int64_t& proj_fp) {
        if (marketable_pending_()) return;   // T2.CONF-1：至多一个激进腿在途（本 tick 多发即被此 gate 序列化）
        // 有效净仓 = confirmed + 本 tick fill-storm 乐观投影；单 tick 多发须本地累加，
        // 因 fill 跨 tick 才落 confirmed_pos_，否则同侧连发冲过 max_pos_qty_abs。
        const int64_t eff_pos = confirmed_pos_(ctx) + proj_fp;
        const bool is_buy = oscillation_side(eff_pos);
        const XtOffset offset = infer_offset_from_pos(eff_pos, is_buy);
        if (offset == XtOffset::Open && !can_open_side(eff_pos, is_buy)) return;
        if (!aggressive_gate_ready_(ctx, is_buy)) return;  // F-B：己方对侧无未终态单方可发
        const int64_t px = is_buy ? aggressive_buy_px(tk) : aggressive_sell_px(tk);
        if (venue_is_ctp_ && px <= 0) return;   // binance px=0=市价合法
        const int64_t qfp = order_exec_to_fp(qty_);
        const uint64_t id = submit(ctx, SC_MARKET_IOC, is_buy, px, qfp,
                                   XtTimeInForce::IOC, offset, "fill_storm",
                                   /*expect_reject=*/false, /*fast_terminal=*/true);
        if (id != 0) {
            proj_fp += is_buy ? qfp : -qfp;   // net：buy=+ / sell=−（与 offset 无关）
            pending_marketable_id_ = id;      // T2.CONF-1：序列化下一激进腿
        }
    }

    // ── 场景：flip（持续翻仓，压 offset 路由 + CTP 平今/平昨 + close 拆分）──
    void fire_flip(IStrategyContext* ctx, const XtTick& tk) {
        if (marketable_pending_()) return;   // T2.CONF-1：等前一激进腿终态（防两腿 RTT 窗口互撮）
        // 净空则买、净多则卖、净平则交替开仓 → 仓位在 0 附近持续翻转。
        const int64_t pos = confirmed_pos_(ctx);
        bool is_buy;
        if (pos > 0)      is_buy = false;
        else if (pos < 0) is_buy = true;
        else              is_buy = (rotor_++ & 1) == 0;
        const XtOffset offset = infer_offset_from_pos(pos, is_buy);
        if (offset == XtOffset::Open && !can_open_side(pos, is_buy)) return;
        if (!aggressive_gate_ready_(ctx, is_buy)) return;  // F-B：己方对侧无未终态单方可发
        const int64_t px = is_buy ? aggressive_buy_px(tk) : aggressive_sell_px(tk);
        if (venue_is_ctp_ && px <= 0) return;  // binance px=0=市价合法
        const uint64_t id = submit(ctx, SC_FLIP, is_buy, px, order_exec_to_fp(qty_),
               XtTimeInForce::IOC, offset, "flip", false, /*fast_terminal=*/true);
        if (id != 0) pending_marketable_id_ = id;
    }

    // ── 场景：tif_matrix（轮发 GTC/IOC/FOK，验证各 TIF 终态语义）──
    void fire_tif_matrix(IStrategyContext* ctx, const XtTick& tk) {
        const XtTimeInForce tifs[3] = { XtTimeInForce::GTC, XtTimeInForce::IOC, XtTimeInForce::FOK };
        const XtTimeInForce tif = tifs[(tif_cursor_++) % 3];
        const bool is_buy = (rotor_++ & 1) == 0;
        if (!can_open_side(confirmed_pos_(ctx), is_buy)) return;
        // GTC 用远盘口（挂着，passive），IOC/FOK 用 aggressive（即时撮合）。
        const bool aggressive = (tif != XtTimeInForce::GTC);
        // F-B：IOC/FOK 激进腿补进 marketable 序列化 + 对侧栅栏（GTC passive 不穿越，不需）。
        if (aggressive) {
            if (marketable_pending_()) return;
            if (!aggressive_gate_ready_(ctx, is_buy)) return;
        }
        int64_t px;
        bool fast_terminal;
        if (!aggressive) {
            const int64_t mid = ref_mid_px(tk);
            if (mid <= 0) return;
            px = is_buy ? (mid - price_tick_fp(ctx, tk) * far_offset_ticks_)
                        : (mid + price_tick_fp(ctx, tk) * far_offset_ticks_);
            fast_terminal = false;
        } else {
            px = is_buy ? aggressive_buy_px(tk) : aggressive_sell_px(tk);
            fast_terminal = true;   // IOC/FOK 当 tick 必出终态
        }
        if (venue_is_ctp_ && px <= 0) return;  // binance IOC/FOK px=0=市价；GTC 走远盘口>0
        const uint64_t id = submit(ctx, SC_TIF_MATRIX, is_buy, px, order_exec_to_fp(qty_),
               tif, XtOffset::Open, "tif_matrix", false, fast_terminal);
        if (aggressive && id != 0) pending_marketable_id_ = id;  // 序列化下一激进腿
    }

    // ── 场景：risk_probe（故意超 max_order_qty 巨单，期望被风控拒 id==0）──
    void fire_risk_probe(IStrategyContext* ctx, const XtTick& tk) {
        if ((probe_pacer_q_++ % probe_every_decisions_) != 0) return;
        // 远离盘口的极端限价 + 巨量：即便风控未拦（id>0），远盘口也不会成交，
        // 且随即撤掉，双保险不打爆账户。
        const int64_t mid = ref_mid_px(tk);
        if (mid <= 0) return;
        const bool is_buy = (rotor_++ & 1) == 0;
        const int64_t px = is_buy ? (mid / 2 > 0 ? mid / 2 : 1) : (mid * 2);
        const uint64_t id = submit(ctx, SC_RISK_PROBE, is_buy, px, risk_probe_qty_fp_,
                                   XtTimeInForce::GTC, XtOffset::Open, "risk_probe",
                                   /*expect_reject=*/true, /*fast_terminal=*/true);
        if (id != 0) { ++probe_miss_total_; probe_cancel_(ctx, id); }
    }

    // ── 场景：risk_probe_pos（单笔 ≤ max_order_qty 但 pos+qty 超 max_pos_qty，期望拒）──
    void fire_risk_probe_pos(IStrategyContext* ctx, const XtTick& tk) {
        if ((probe_pacer_p_++ % probe_every_decisions_) != 0) return;
        const int64_t mid = ref_mid_px(tk);
        if (mid <= 0) return;
        // 顺仓方向加仓（|pos+qty| 最大化），operator 保证 qty 配置满足
        // max_pos_qty + max_pos_qty_abs ≤ qty ≤ max_order_qty。
        const bool is_buy = confirmed_pos_(ctx) >= 0;
        const int64_t px = is_buy ? (mid / 2 > 0 ? mid / 2 : 1) : (mid * 2);
        const uint64_t id = submit(ctx, SC_RISK_PROBE_POS, is_buy, px, risk_probe_pos_qty_fp_,
                                   XtTimeInForce::GTC, XtOffset::Open, "risk_probe_pos",
                                   /*expect_reject=*/true, /*fast_terminal=*/true);
        if (id != 0) { ++probe_miss_total_; probe_cancel_(ctx, id); }
    }

    // ── 场景：risk_probe_notional（名义额 < risk.min_order_notional，期望拒）──
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
                                   /*expect_reject=*/true, /*fast_terminal=*/true);
        if (id != 0) { ++probe_miss_total_; probe_cancel_(ctx, id); }
    }

    void probe_cancel_(IStrategyContext* ctx, uint64_t id) {
        auto it = inflight_.find(id);
        if (it != inflight_.end()) request_cancel_(ctx, id, it->second);
    }

    // ── 场景：query_sweep（每 tick 扫所有只读视图，验证不崩 + sane + 自洽）──
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
        // 资金 sanity：equity 不应为荒谬负值（demo 起始资金为正）。
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

    /// 仓位明细自洽：net(long-short) 应与 confirmed pos 一致。
    void sweep_pos_detail_(IStrategyContext* ctx) {
        xt_runtime::XtStraPositionDetail pd[4];
        const size_t n = ctx->get_position_detail(symbol_.c_str(), pd, 4);
        if (n == 0) return;   // venue/engine 不供明细 → 必要条件不触发
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

    /// 历史视图：ring buffer 读路径不崩 + 计数 ≤ max + tick 时序单调。
    void sweep_history_(IStrategyContext* ctx) {
        XtTick tb[32];
        const size_t nt = ctx->get_ticks(symbol_.c_str(), tb, 32);
        if (nt > 32) { violation(ctx, "ticks_overflow", "get_ticks count > max"); return; }
        // 跨 tick 严格单调是 CTP 单时钟 feed 假设；binance 合并多流（@aggTrade +
        // @bookTicker，到达序 ring）跨流 event-time 倒挂是固有性质、非排序 bug → binance
        // 永不硬违例（仅软计数 conf_ticks_order）；仅 CTP/XTP 单时钟才硬违例（真排序 bug
        // tripwire）。CTP 下另豁免 channel (re)connect catch-up 突发期（recovery_window）。
        // 2026-06-24: 原仅靠 recovery_window 拦 binance reconnect 突发，但 binance 多流倒挂在
        // 窗口外也常驻（~4-5min 后跨 2000-tick 窗口）→ 改 venue-aware：binance 一律软计数。
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

    /// 日期视图 sanity：yyyymmdd 形状 + 与 session 回调一致性（软，一次性）。
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

    // ── always-on：终态超时检查（应快速终态/已请求 cancel 的单卡住 = 可能丢单）──
    void check_stuck_orders(IStrategyContext* ctx) {
        if (stuck_timeout_ticks_ <= 0) return;
        for (auto it = inflight_.begin(); it != inflight_.end(); ) {
            const Inflight& o = it->second;
            const bool fast_stuck = o.expect_fast_terminal &&
                (tick_seq_ - o.submit_tick_seq) > stuck_timeout_ticks_;
            // cancel 已请求却长期无终态 = cancel→terminal 链路断（venue 僵尸单）。
            // 不驱逐会永久占用 can_open_side / close_outstanding / audit drain 的
            // 在途记账，钉死整个策略状态机，必须按 TTL 清出。
            const bool cancel_stuck = !fast_stuck && o.cancel_first_tick >= 0 &&
                (tick_seq_ - o.cancel_first_tick) > stuck_timeout_ticks_;
            if (fast_stuck || cancel_stuck) {
                // 分级第一档：清出即软计数（占用释放不延迟），viol 押后到 grace 升级 —
                // venue 漏推终态由 ghost-REST ~31s SLA 收敛，晚到终态在 grace 窗口内归账。
                if (fast_stuck) ++stuck_terminal_soft_total_; else ++stuck_cancel_soft_total_;
                char buf[176];
                std::snprintf(buf, sizeof(buf),
                    "conf STUCK-EVICT id=%llu sc=%d age_ticks=%lld (%s; soft, viol after grace)",
                    (unsigned long long)o.id, o.scenario,
                    (long long)(tick_seq_ - o.submit_tick_seq),
                    fast_stuck ? "IOC/FOK/cancelled never terminalized"
                               : "cancel requested, never terminalized");
                ctx->log_warn(buf);
                // 一次性诊断 dump：把「为何卡住」直接摆出，省掉从撤单日志反推。
                char dump[320];   // 宽裕：id + 3 个 steady-µs 时间戳 + 3 个 fp，防截断丢尾字段
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
                // 移入 recent map（stuck 标记）：晚到终态/成交仍可归账，不再当 unknown。
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

    // ── 终态消账定稿（TTL 到期）：双源交叉 + TIF 语义 ──
    // stuck 且终态未到的 rec 额外押 grace（wall OR tick 双门）再定稿：给 venue
    // 漏推终态留 ghost-REST 收敛窗口，晚到终态在窗口内归账即不升级 viol。
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
            // 分级第二档：grace 内终态始终没到 → 升级硬 viol（真丢终态/真僵尸）。
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
            // Binance UM reduceOnly amend-down：平仓单 qty 超过落地时仓位 → venue 自动
            // 缩量到仓位大小后整单 FILLED（filled<qty）。是 venue 怪癖非记账 bug；
            // 真正的丢/重 fill 由 fill_sum_mismatch（双源交叉）兜底硬断言。
            // overfill 方向（filled>qty）仍硬断言 filled_qty_mismatch。
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
        // 双源交叉：on_fill 累计 vs 终态自带 filled_fp（任何一边丢/重 fill 都会暴露）。
        if (!fp_near(rec.cum_onfill_fp, rec.terminal_filled_fp, kQtyEpsFp)) {
            std::snprintf(buf, sizeof(buf), "id=%llu onfill_sum_fp=%lld terminal_filled_fp=%lld",
                          (unsigned long long)id, (long long)rec.cum_onfill_fp,
                          (long long)rec.terminal_filled_fp);
            // 双通道矛盾终态(ws EXPIRED + UDS FILLED)定稿取 filled 较大一路(@on_order_not_inflight_)，
            // fill-event 累计与之 legitimately 不等 = 双通道仲裁产物、非丢/重 fill。降 soft + 独立命名；
            // 真正的丢/重 fill 安全网是 quiesce 的 pos/drift 硬断言 + clean-terminal 的本检查仍 hard。
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

    /// book-view leaves 一致性 oracle：从回报流独立重建剩余量
    /// expected = qty - max(on_order filled 累计, on_fill 累计)，与
    /// ctx->get_open_orders() 的 book 视图比对。钉住「update 绝对 SET + trade
    /// 相对减」双驱动的合并语义：活单从 book 消失（expected>0）或 leaves 漂移
    /// 都是 violation。多切片单（flatten 超 max_order_qty 会切）经
    /// translate_open_orders 合并为逻辑行（id=clord、qty=策略总量）后同口径
    /// 比对；engine 派发回调前 book 已更新（同 worker 线程顺序处理），
    /// 比对无时序窗口。
    void check_book_leaves_(IStrategyContext* ctx, Inflight& o) {
        if (!s_book_oracle_ || o.qty_fp <= 0 || o.book_viol_flagged) return;
        const int64_t cum = std::max(o.cum_upd_fill_fp, o.cum_fill_fp);
        // expected 基准取 venue 已确认口径（amend-down 后 = 缩后 qty，on_order
        // min-merge 跟踪）：正常 amend-down 不再 mismatch，硬比对全强度保留。
        const int64_t basis = o.venue_qty_fp > 0 ? o.venue_qty_fp : o.qty_fp;
        const int64_t expected = basis > cum ? basis - cum : 0;
        if (expected <= 0) return;  // 应已终态：消账由 finalize_terminal_ 双源交叉负责
        XtOrderExecUpdate ob[kOrphanSweepBuf];
        const size_t n = ctx->get_open_orders(symbol_.c_str(), ob, kOrphanSweepBuf);
        if (n >= kOrphanSweepBuf) return;  // 枚举截断时放弃本次比对，避免假阳
        for (size_t i = 0; i < n; ++i) {
            if (ob[i].id != o.id) continue;
            if (ob[i].leaves_fp != expected) {
                // 残余软分支（expected 已按 venue_qty 重建，正常 amend-down 走硬
                // 比对不再进这里）：book 行 qty（= filled + leaves，venue 真值派生）
                // 仍 < oracle 已知 venue 口径 = book 与策略侧 venue-qty 视图分歧的
                // 过渡窗（如 fill 事件先于携带缩后 qty 的 on_order 到达），软计数
                // 观测；持续增长值得人工看。派生 qty 缩水类 book bug（如 leaves
                // 双扣）与本签名同形会被软吞，该子集的丢/重 fill 安全网由
                // fill_sum_mismatch 双源交叉兜底。
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

    /// 非 inflight 的 on_order：recent 命中分流同状态重复 / 矛盾终态 / 晚到终态 / 乱序更新。
    void on_order_not_inflight_(IStrategyContext* ctx, const XtOrderExecUpdate& u) {
        auto rt = recent_terminal_.find(u.id);
        if (rt == recent_terminal_.end()) { ++unknown_update_total_; return; }
        TerminalRec& rec = rt->second;
        if (order_status_is_final(u.status)) {
            if (rec.stuck && !rec.got_late_terminal) {
                // stuck 报过之后终态终于到了：归账供定稿检查，单独软计数。
                rec.got_late_terminal = true;
                rec.status = u.status;
                rec.terminal_filled_fp = u.filled_fp;
                ++stuck_late_terminal_total_;
            } else if (u.status == rec.status) {
                // 同状态重复：引擎 rank dedup 应吞同 rank 重放，到达策略 = dedup 失效。
                char buf[120];
                std::snprintf(buf, sizeof(buf), "id=%llu status=%d repeated",
                              (unsigned long long)u.id, (int)u.status);
                violation(ctx, "double_terminal", buf);
            } else {
                // 矛盾终态对（如 ws EXPIRED + UDS FILLED）：binance 双通道 + 引擎
                // order_status_rank 明文放行冲突终态 → 预期可见，软计数。
                // 定稿依据取 filled 较大的一路（真成交量由 fill_sum_mismatch 裁决）。
                ++terminal_conflict_total_;
                rec.had_terminal_conflict = true;   // fill_sum_mismatch 由本冲突派生 → 定稿降 soft
                if (u.filled_fp > rec.terminal_filled_fp) {
                    rec.terminal_filled_fp = u.filled_fp;
                    rec.status = u.status;
                }
            }
        } else {
            ++late_update_after_terminal_;   // 终态后乱序非终态更新（venue 重放），软计数
        }
    }

    // ── reject-storm 门：滚动窗口 unexpected 拒单占比超阈 → violation ──
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

    // ── audit cycle：静默排空 → quiesce 断言 → 重锚（排空超时 = 死锁探测）──
    void run_audit_machine_(IStrategyContext* ctx, const XtTick& tk) {
        switch (audit_state_) {
        case AuditState::Normal:
            if (tick_seq_ - last_audit_end_tick_ < audit_interval_ticks_) return;
            audit_state_ = AuditState::Drain;
            audit_active_ticks_ = 0;
            flatten(ctx, tk, "audit");
            return;
        case AuditState::Drain: {
            if (!ctx->can_submit()) return;   // operator halt / 非交易时段不计入超时
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
                audit_state_ = AuditState::Drain;   // 晚到 fill 把仓位顶回来 → 重新排空
                return;
            }
            if (--settle_left_ > 0) { audit_timeout_check_(ctx, confirmed); return; }
            audit_settle_asserts_(ctx);
            audit_end_(/*completed=*/true);
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
        audit_end_(/*completed=*/false);
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
            // recovery 窗口豁免：本 audit 观测窗口内（上一 cycle 末 → 现在；此处
            // last_audit_end_tick_ 仍是上一 cycle 终值，audit_end_ 在断言之后才更新）
            // 发生过 channel recovery → UDS 丢的 ORDER_TRADE_UPDATE 经 Recovering 重放
            // 不 fan-out 给策略，self 账本漏账而引擎真值正确，drift 是预期连带，软计数。
            const bool recovery_in_window = last_channel_ready_tick_ > last_audit_end_tick_;
            if (audit_cycles_ == 0 && !audit_grace_used_) {
                // 重启后首个 cycle：孤儿单成交等外部仓位变动允许一次重锚（软计数）。
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
        self_pos_fp_ = confirmed;   // 周期重锚：每 cycle 独立检测新 drift
    }

    void audit_end_(bool completed) {
        if (completed) ++audit_cycles_;
        audit_state_ = AuditState::Normal;
        last_audit_end_tick_ = tick_seq_;
    }

    /// 已确认仓位（剥离 in-flight 投影；回测投影恒 0）。
    int64_t confirmed_pos_(IStrategyContext* ctx) {
        return ctx->get_position_fp(symbol_.c_str()) -
               ctx->projected_inflight_fp(symbol_.c_str());
    }

    /// fill→position drift 持续观测（confirmed 口径，剥离 in-flight 投影噪声）。
    void observe_drift_(IStrategyContext* ctx) {
        const int64_t real_pos = confirmed_pos_(ctx);
        last_pos_drift_fp_ = real_pos - self_pos_fp_;
        const int64_t ad = llabs_(last_pos_drift_fp_);
        if (ad > max_abs_drift_fp_) max_abs_drift_fp_ = ad;
        if (ad > pos_eps_fp()) ++pos_desync_ticks_;
    }

    // ── always-on：周期 flatten（撤所有挂单 + 反向平净仓 → 0）──
    void flatten(IStrategyContext* ctx, const XtTick& tk, const char* why) {
        // 平仓决策用 confirmed 口径：live 的 get_position_fp 含 signed in-flight 投影，
        // venue 已死但本地仍 alive 的僵尸在途单会把投影口径钉在 0，flatten 误判已平
        // 而永久 no-op；在途重复平仓由下方 close_outstanding 抵扣防护，不靠投影。
        const int64_t pos = confirmed_pos_(ctx);
        const bool close_is_buy = pos < 0;   // 净空买回 / 净多卖出
        // 1) 撤掉所有仍在途的单（冷却去重：重复 cancel 只烧 venue/IP 请求预算）。
        int64_t close_outstanding_fp = 0;
        for (auto& kv : inflight_) {
            Inflight& o = kv.second;
            if (o.scenario == -2 && o.is_buy == close_is_buy) close_outstanding_fp += o.qty_fp;
            request_cancel_(ctx, kv.first, o);
        }
        // 1b) 孤儿挂单清扫：引擎 open-order 枚举含 reverse-sync 回灌的 Recovered 单。
        //    不撤会堆满 venue max-open-order 上限（binance -2025）→ 之后所有新单必拒。
        sweep_orphan_orders_(ctx);
        // 2) 反向平掉净仓（aggressive IOC，Close offset），按 flatten_chunk_qty 切片：
        //    每笔 ≤ runner risk.max_order_qty；扣除在途同向 close 后再发。
        // F-B：激进平仓腿同接对侧栅栏（穿越己方对侧 resting 会触发 STP 拒/自成交）。cancel-all
        //    已在上方发出，对侧终态后本腿放行；audit drain 期 rapid 停发 → 对侧终态 → 收敛
        //    （periodic 期 rapid re-arm 对侧则本腿延后，仓位由 max_pos_qty_abs 上界 + audit 收敛）。
        if (pos != 0 && aggressive_gate_ready_(ctx, close_is_buy)) {
            const int64_t px = close_is_buy ? aggressive_buy_px(tk) : aggressive_sell_px(tk);
            if (!venue_is_ctp_ || px > 0) {    // binance px=0=市价 reduceOnly 平仓
                const int64_t total_abs = pos < 0 ? -pos : pos;
                const int64_t to_close = total_abs - close_outstanding_fp;
                const int64_t chunk = flatten_chunk_qty_fp_ > 0 ? flatten_chunk_qty_fp_ : to_close;
                int64_t remaining = to_close;
                for (int n2 = 0; n2 < kMaxFlattenChunks && remaining > 0; ++n2) {
                    const int64_t q = remaining < chunk ? remaining : chunk;
                    submit(ctx, -2 /*flatten*/, close_is_buy, px, q,
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

    /// 幂等撤一笔 book 派生 orphan/recovered 单：per-id 冷却 + give-up（复用
    /// orphan_cancel_tick_/orphan_cancel_count_ 表）。sweep_orphan_orders_（flatten 路径）与
    /// aggressive_gate_ready_ 的 book 并集分支共用，同 tick/冷却窗内不对同一 id 重撤（防
    /// fill_per_tick 循环烧 gateway cancel limiter）。
    /// @return true = 本次真发了 cancel（供 caller 计 budget）；false = 冷却窗内 / 已 give-up 跳过。
    bool try_cancel_orphan_(IStrategyContext* ctx, uint64_t oid) {
        // [P2] per-id give-up：重撤达上限的死幽灵不再刷（止 no_cancel_target 风暴日志）；
        // venue 真值清理由 engine orphan_reap + B3/B4 lost-terminal reconcile 兜底。
        if (orphan_cancel_count_[oid] >= kOrphanCancelGiveUp) return false;
        auto it = orphan_cancel_tick_.find(oid);
        if (it != orphan_cancel_tick_.end() &&
            (tick_seq_ - it->second) <= kCancelRetryCooldownTicks) return false;
        ctx->cancel(oid);
        orphan_cancel_tick_[oid] = tick_seq_;
        ++cancels_req_total_;
        ++orphan_cancel_total_;
        if (++orphan_cancel_count_[oid] == kOrphanCancelGiveUp) {
            ++orphan_giveup_total_;
            char gbuf[128];
            std::snprintf(gbuf, sizeof(gbuf),
                "conf ORPHAN-GIVEUP id=%llu after %d cancels (no terminal; engine reaper/B3/B4 backstop)",
                (unsigned long long)oid, kOrphanCancelGiveUp);
            ctx->log_warn(gbuf);
        }
        return true;
    }

    void sweep_orphan_orders_(IStrategyContext* ctx) {
        XtOrderExecUpdate ob[kOrphanSweepBuf];
        const size_t n = ctx->get_open_orders(symbol_.c_str(), ob, kOrphanSweepBuf);
        int sweep_budget = kOrphanSweepPerFlatten;
        for (size_t i = 0; i < n && sweep_budget > 0; ++i) {
            const uint64_t oid = ob[i].id;
            if (oid == 0 || inflight_.count(oid) != 0) continue;
            if (try_cancel_orphan_(ctx, oid)) --sweep_budget;
        }
        if (orphan_cancel_tick_.size() > 4096) {  // 有界即可（give-up 计数随之清零 → best-effort，越界后死幽灵会重新累计到上限，仍由 B3/B4 兜底真清）
            orphan_cancel_tick_.clear();
            orphan_cancel_count_.clear();
        }
    }

    // ── 周期心跳：统计汇总 + metric 上报（两行：计数 + 延迟/效率）──
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
        // 硬不变量违规单列（排除 reject_storm 软噪声）→ 接 XyybtraderConformanceHardViolation
        // alert → triage；per-type series 仅在该类型真触发后出现（cardinality 受限）。
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
        // T1.152 每交易日额度 + not-submitting reason 面。conf_capped 用 0/1 latch
        // 语义（免 emit-on-increment counter 的 series-birth 告警盲区，见 T1.145）。
        ctx->report_metric("conf_orders_day",       (double)orders_day_);
        ctx->report_metric("conf_capped",           capped_ ? 1.0 : 0.0);
        ctx->report_metric("conf_cap_hit_days",     (double)cap_hit_days_total_);
        // T2.CONF-1：品种 halt（0/1，本策略单 symbol）+ halt 触发累计。
        ctx->report_metric("conf_symbol_halted",     is_open_halted_(symbol_) ? 1.0 : 0.0);
        ctx->report_metric("conf_symbol_halt_events",(double)symbol_halt_events_total_);
        // F-B/F-C：激进腿栅栏跳过 + SelfTradeDetected（transient，应≈0）+ re-probe 清 halt。
        ctx->report_metric("conf_aggr_gate_skips",   (double)aggr_gate_skips_);
        ctx->report_metric("conf_selftrade_detected",(double)selftrade_detected_total_);
        ctx->report_metric("conf_symbol_halt_clears",(double)symbol_halt_clears_total_);
        ctx->report_metric("conf_skip_warmup",      (double)skip_warmup_total_);
        ctx->report_metric("conf_skip_gate",        (double)skip_gate_total_);
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
        // 1280：45 个 specifier，全字段极值理论宽 ~1200 全覆盖（截断会让 verdict
        // 正则静默丢尾部 optional 组，盲审 fold 把“实用宽度留余量”改为绝对覆盖）。
        char buf[1280];
        std::snprintf(buf, sizeof(buf),
            "conf HB tick=%lld ord=%llu fill=%llu cxl=%llu term=%llu "
            "rej[exp=%llu unexp=%llu gated=%llu closeL=%llu] viol=%llu desync=%llu maxdrift_fp=%lld "
            "inflight=%zu pos_fp=%lld drift_fp=%lld orph=%llu bskip=%llu "
            "audit[c=%llu st=%d] probe[q=%llu p=%llu n=%llu miss=%llu] "
            "uf=%llu fac=%llu oc=%llu tc=%llu sess[b=%llu e=%llu] roll=%llu acct=%llu params=%llu ch[r=%llu l=%llu] "
            "md[crossed=%llu nonmono=%llu onesided=%llu] rdamend=%llu rdrift=%llu "
            "lock_fp=%lld "
            "cap[day=%u dord=%llu capped=%d] halt[on=%d ev=%llu] skip[warm=%llu gate=%llu]",
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
            (long long)(self_long_fp_ < self_short_fp_ ? self_long_fp_ : self_short_fp_),
            cap_day_, (unsigned long long)orders_day_, (int)capped_,
            is_open_halted_(symbol_) ? 1 : 0, (unsigned long long)symbol_halt_events_total_,
            (unsigned long long)skip_warmup_total_, (unsigned long long)skip_gate_total_);
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

    // ── userdata 持久化自检 ──
    void run_userdata_init_checks_(IStrategyContext* ctx) {
        // 即时往返：写后读回必须一致（持久化层 API 路径）。
        ctx->save_userdata("conf_probe", "v1");
        const char* got = ctx->load_userdata("conf_probe", "");
        if (!got || std::strcmp(got, "v1") != 0) {
            violation(ctx, "userdata", "save_userdata/load_userdata round-trip mismatch");
        }
        // 跨重启 seq：上次进程 heartbeat 持续写入的递增值，重启后应可读回且可解析。
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

    // ── 工具 ──
    // reject_storm 是 margin/限速驱动、排干 demo 账户下预期出现的软噪声；其余全为硬不变量，
    // 触发即代表组件真出问题 → 单列 conf_hard_violation_total 接 alert（不被 reject_storm 淹没）。
    static bool is_soft_violation_(const char* inv) noexcept {
        // reject_storm：margin/限速驱动的预期软噪声。fill_sum_mismatch_conflict：双通道
        // 矛盾终态的仲裁产物(非丢/重 fill；clean-terminal 的 fill_sum_mismatch 仍硬)。
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
        // 仓位摆动：达上限时强制减仓方向，否则交替。
        if (pos >= max_pos_qty_abs_fp_)  return false;  // 太多多头 → 卖
        if (pos <= -max_pos_qty_abs_fp_) return true;   // 太多空头 → 买
        return (rotor_++ & 1) == 0;
    }

    static int64_t llabs_(int64_t v) noexcept { return v < 0 ? -v : v; }

    /// 自记净仓应用一笔成交：close 不允许越过 0（引擎 over-close 零轴截断契约：
    /// over-close 成交全量上报但仓位效果截断；live venue 层 CTP 可平校验 /
    /// binance reduceOnly 使 close 成交天然不越零，截断不触发）。
    void apply_self_fill_(const XtFillEvent& f) noexcept {
        // 双腿账本：open 加同向腿，close 减对侧腿（下限 0）。lock 观测用，
        // 不参与任何 oracle 硬断言 — offset 路由灰度的对照证据源。
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
            self_pos_fp_ = after;   // 含 after==0 的精确平仓
        }
    }

    static bool fp_near(int64_t a, int64_t b, int64_t eps) noexcept {
        const int64_t d = a > b ? a - b : b - a;
        return d <= eps;
    }
    int64_t pos_eps_fp() const noexcept {
        // 半个最小手数容差，吸收定点舍入。
        const int64_t q = order_exec_to_fp(qty_);
        return q > 1 ? q / 2 : 1;
    }

    // ── venue 限速预算（按秒折算 pacing；0=不限）──
    // H3：按**每次 submit attempt**消费（consume_order_budget 在 submit 入口调，含本地拒），
    // 非 accepted-only —— 兑现失控拒因护栏（本地拒循环有界）。day cap（orders_day_）另守 accepted。
    bool order_budget_ok(int64_t now_ms) noexcept {
        if (max_orders_per_min_ <= 0) return true;
        const int64_t per_sec = max_orders_per_min_ >= 60 ? max_orders_per_min_ / 60 : 1;
        const int64_t bucket = now_ms / 1000;
        if (bucket != budget_sec_bucket_) { budget_sec_bucket_ = bucket; budget_sec_count_ = 0; }
        return budget_sec_count_ < per_sec;
    }
    void consume_order_budget() noexcept { ++budget_sec_count_; }

    // get_time_ms() 返回 HHMMSSmmm 打包十进制 → 当日绝对毫秒（wall-clock 预热用）。
    static int64_t packed_hms_to_ms(uint32_t t) {
        const int hh  = static_cast<int>(t / 10000000);
        const int mm  = static_cast<int>((t / 100000) % 100);
        const int ss  = static_cast<int>((t / 1000) % 100);
        const int mmm = static_cast<int>(t % 1000);
        return (static_cast<int64_t>((hh * 60 + mm) * 60 + ss)) * 1000 + mmm;
    }

    /// steady-clock µs：延迟/效率测量统一时基。live=真实 RTT；回测=引擎处理耗时。
    static int64_t now_us() noexcept {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    // ── 配置 ──
    std::string symbol_;
    bool        venue_is_ctp_ = true;
    double      qty_          = 1.0;
    int64_t     warmup_ms_    = 3000;
    int64_t     warmup_ticks_ = 5000;
    int64_t     max_total_orders_ = 200000;
    int         decision_interval_ticks_ = 1;
    int         fire_per_tick_   = 4;
    int         fill_per_tick_   = 0;       ///< fill-storm 每 tick aggressive IOC 数（0=关，压 fill/撮合/记账 money-path）
    int         flatten_interval_ticks_ = 400;
    int         heartbeat_ticks_ = 2000;
    int         stuck_timeout_ticks_ = 1000;
    int64_t     stuck_viol_grace_ms_ = 40000;   ///< stuck viol 升级 wall 门（0=禁 wall 门，仅 tick 门）
    int64_t     far_offset_ticks_ = 100;
    int64_t     max_orders_per_min_ = 0;     ///< venue 下单限速预算（0=不限）
    int64_t     max_pos_qty_abs_fp_ = 0;
    int64_t     flatten_chunk_qty_fp_ = 0;   ///< 单笔平仓最大 qty（必须 ≤ runner risk.max_order_qty）
    int64_t     risk_probe_qty_fp_ = 0;
    int64_t     audit_interval_ticks_ = 4000;   ///< quiesce audit 周期（0=关）
    int64_t     audit_timeout_ticks_ = 3000;    ///< 排空超时（active tick 计）
    int64_t     audit_settle_ticks_ = 50;       ///< 排空后静置观察窗
    int64_t     recent_ttl_ticks_ = 600;        ///< 终态消账暂存 TTL
    int64_t     reject_storm_window_ = 200;     ///< 拒单风暴滚动窗口（submit 数）
    int64_t     reject_storm_pct_ = 50;         ///< 风暴阈值（unexp 占比 %）
    int64_t     risk_probe_pos_qty_fp_ = 0;
    int64_t     risk_probe_notional_qty_fp_ = 0;
    int64_t     probe_every_decisions_ = 32;   ///< probe 节流：每 N 轮转发一次
    int64_t     cancel_retry_ticks_ = kCancelRetryCooldownTicks;   ///< 同单 cancel 重试窗（init 按 stuck_timeout 收紧）
    bool s_rapid_submit_ = true, s_cancel_storm_ = true, s_market_ioc_ = true, s_flip_ = true;
    bool s_tif_matrix_ = true, s_risk_probe_ = true, s_query_sweep_ = true, s_userdata_ = true;
    bool s_risk_probe_pos_ = false, s_risk_probe_notional_ = false;
    bool s_probe_cancel_unknown_ = true;
    bool s_md_oracle_ = true;   ///< 行情正确性自检开关
    bool s_book_oracle_ = true; ///< book leaves 一致性自检开关

    // ── 运行态 ──
    int64_t  first_ms_   = -1;
    int64_t  tick_seq_   = 0;
    bool     inside_on_tick_ = false;   // view 因果性检测用（同步撞合回报泄漏）
    int      scenario_cursor_ = 0;
    int      tif_cursor_ = 0;
    unsigned rotor_ = 0;
    unsigned sweep_rotor_ = 0;
    int64_t  self_pos_fp_ = 0;
    int64_t  self_long_fp_ = 0;   // 双腿账本（lock 观测）：fill 流自维护 long 腿
    int64_t  self_short_fp_ = 0;  // 同上 short 腿；min(两腿)>0 = 锁仓存量
    int64_t  last_pos_drift_fp_ = 0;
    uint64_t pos_desync_ticks_ = 0;   // fill→position drift 超容差的 tick 数（观测）
    int64_t  max_abs_drift_fp_ = 0;   // 观测期内 |confirmed-self| 最大值
    bool     self_pos_anchored_ = false;  // 冷启动锚定完成（见 on_tick 锚定块）
    int64_t  cached_price_tick_fp_ = 0;   // contract_info.price_tick 缓存（0=未取到）
    int64_t  budget_sec_bucket_ = -1;
    int      budget_sec_count_  = 0;
    std::unordered_map<uint64_t, int64_t> orphan_cancel_tick_;   // 孤儿单 cancel 冷却
    std::unordered_map<uint64_t, int> orphan_cancel_count_;  // [P2] 孤儿单累计重撤次数（per-id give-up）

    AuditState audit_state_ = AuditState::Normal;
    int64_t  audit_active_ticks_ = 0;
    int64_t  settle_left_ = 0;
    int64_t  last_audit_end_tick_ = 0;
    uint64_t audit_cycles_ = 0;
    bool     audit_grace_used_ = false;

    int64_t  window_orders_ = 0;   // reject-storm 滚动窗口
    int64_t  window_unexp_  = 0;
    // probe 节流计数器：每 probe 独立，避免共享 pacer 在轮转序下相位锁死。
    unsigned probe_pacer_q_ = 0, probe_pacer_p_ = 0, probe_pacer_n_ = 0;

    int64_t  prev_tick_us_ = 0;
    int64_t  last_hb_us_ = 0;
    int64_t  last_hb_ticks_ = 0;
    uint64_t last_hb_orders_ = 0;
    LatHist  lat_ack_, lat_fast_term_, lat_cancel_, lat_tick_gap_, lat_on_tick_;

    std::unordered_map<uint64_t, Inflight>    inflight_;
    std::unordered_map<uint64_t, TerminalRec> recent_terminal_;

    // ── 统计 ──
    uint64_t orders_total_ = 0, fills_total_ = 0, cancels_req_total_ = 0, terminal_total_ = 0;
    uint64_t rejects_expected_ = 0, rejects_unexpected_ = 0, rejects_close_local_ = 0, rejects_gated_ = 0;
    uint64_t violations_total_ = 0, orphan_cancel_total_ = 0, budget_skip_total_ = 0;
    // T1.152：每交易日额度（cap_day_ = 额度所属 trading_date，日切复位）+ 静默路径观测
    uint64_t orders_day_ = 0;                  ///< 本交易日已发单数（cap 判据）
    uint32_t cap_day_ = 0;                     ///< 日额度所属交易日（0=未初始化）
    bool     capped_ = false;                  ///< 本交易日撞顶闩（loud journal 去重）
    uint64_t cap_hit_days_total_ = 0;          ///< 撞顶事件累计（进程生命周期）
    // T2.CONF-1：per-symbol 非自愈闩锁类拒因连续计数 → halt 开仓场景（日切复位，同 cap rollover）。
    std::unordered_map<std::string, int>  symbol_reject_streak_;  ///< code → 连续 latch-class 本地拒次数
    std::unordered_map<std::string, bool> symbol_open_halted_;    ///< code → 开仓 halt 闩
    uint64_t symbol_halt_events_total_ = 0;                       ///< halt 触发累计（进程生命周期）
    // T2.CONF-1：marketable（激进 IOC）单腿序列化——同一时刻至多一个在途，避免两激进反向腿
    // 在 venue RTT 窗口内互相撮合（07-21 自成交窗口）。0 = 无在途，终态回报清零。
    uint64_t pending_marketable_id_ = 0;
    // F-B：激进腿对侧栅栏跳过计数（软观测，不算拒、不进 streak）。
    uint64_t aggr_gate_skips_ = 0;
    // H5：aggressive gate 阻塞时记录被阻塞的激进腿侧 → fire_rapid_submit 同侧 re-arm（对侧留空）。
    bool steer_rapid_valid_  = false;   ///< true=side-steer（rapid 发 steer_rapid_is_buy_ 侧），false=交替
    bool steer_rapid_is_buy_ = false;   ///< steer 侧（= 上次被阻塞的激进腿方向）
    // F-C：SelfTradeDetected 独立可见计数（transient，F-B 后应≈0，非零即 gate bug 信号）。
    uint64_t selftrade_detected_total_ = 0;
    // F-C：halt re-probe 自愈——halted symbol 每 halt_reprobe_decisions_ 放行一次探针，
    // 成功即清 halt（解 kill-switch resume / open-halt 解除后静默死到日切）。
    int64_t  halt_reprobe_decisions_ = 600;  ///< 探针间隔（decisions；<=0 关闭 re-probe）
    int64_t  halt_reprobe_counter_ = 0;      ///< halt 期间 decision 计数
    bool     halt_probe_active_ = false;     ///< H4：本次 submit 是 halted-reprobe 显式 Open 探针（唯一清闩来源）
    uint64_t symbol_halt_clears_total_ = 0;  ///< re-probe 成功清 halt 累计
    uint64_t skip_warmup_total_ = 0, skip_gate_total_ = 0;  ///< not-submitting：warmup / can_submit gate
    uint64_t orphan_giveup_total_ = 0;  // [P2] 达重撤上限放弃的死幽灵计数
    uint64_t hard_violations_total_ = 0;  // 硬不变量违规累计（排除 reject_storm 软噪声）→ 接 alert
    // 已触发的硬违规按完整 metric 名累计；key 即 "conf_viol_<type>"。XtStrategyMetricsAggregator
    // 以 string_view 存 name 不拷贝 → 本 map 只增不 erase/clear，保 report_metric 传入的 c_str() 不悬空。
    std::unordered_map<std::string, uint64_t> hard_viol_by_type_;
    uint64_t unknown_update_total_ = 0, unknown_fill_total_ = 0, account_update_total_ = 0;
    uint64_t fill_after_cancel_total_ = 0, late_update_after_terminal_ = 0;
    uint64_t self_overclose_clamped_ = 0;
    uint64_t stuck_late_terminal_total_ = 0, terminal_conflict_total_ = 0;
    uint64_t stuck_terminal_soft_total_ = 0, stuck_cancel_soft_total_ = 0;   ///< stuck 分级第一档（evict 即计）
    uint64_t probe_qty_hits_ = 0, probe_pos_hits_ = 0, probe_notional_hits_ = 0;
    uint64_t probe_miss_total_ = 0, probe_cancel_unknown_total_ = 0;
    uint64_t audit_grace_realign_total_ = 0;
    uint64_t session_begin_total_ = 0, session_end_total_ = 0, roll_total_ = 0, params_update_total_ = 0;
    uint64_t channel_ready_total_ = 0, channel_lost_total_ = 0;
    uint64_t hb_seq_ = 0, resume_prev_seq_ = 0;
    uint32_t last_trading_date_ = 0;
    bool viol_once_ticks_order_ = false, viol_once_pos_detail_ = false, viol_once_date_ = false;
    // MD oracle 计数 + 首发限流 + recovery 窗口标记
    uint64_t md_crossed_total_ = 0, md_nonmono_total_ = 0, md_one_sided_total_ = 0, md_touch_total_ = 0;
    uint64_t ticks_order_total_ = 0;          ///< [ticks_order] 软计数（含 recovery 窗口内合法跨流乱序）
    uint64_t reduce_only_amend_down_total_ = 0, recovery_drift_realign_total_ = 0;
    uint64_t book_amend_down_total_ = 0;   ///< book oracle 的 amend-down 收窄（软，与定稿 rdamend 分开计）
    bool viol_once_md_crossed_ = false, viol_once_md_nonmono_ = false, viol_once_md_l1_ = false;
    int64_t last_channel_ready_tick_ = -1;   ///< 最近 channel recovery tick（pos_drift_quiesce 豁免窗口）

    static constexpr int64_t kSaneFloorFp = 1000000LL * 1000000000LL;  // -1e9 元/USDT 视为荒谬
    static constexpr int64_t kQtyEpsFp = 1;                    ///< 消账容差（1e-6 手）
    static constexpr int64_t kCancelRetryCooldownTicks = 500;  ///< 同单重复 cancel 冷却（tick）
    static constexpr int64_t kTicksOrderRecoveryWindowTicks = 2000;  ///< channel recovery 后 ticks_order 硬违例豁免窗口（tick）
    static constexpr int kOrphanCancelGiveUp = 12;  ///< [P2] 同一孤儿单重撤上限，超过即放弃止噪（B3/B4 兜底真清）
    static constexpr int kSymbolHaltStreakThreshold = 2;  ///< T2.CONF-1：连续 latch-class 拒达此值 → halt 开仓
    static constexpr int64_t kStuckViolGraceTickMult   = 10;   ///< stuck viol 升级 tick 门 = N × stuck_timeout
    static constexpr int     kOrphanSweepPerFlatten = 8;       ///< 每轮 flatten 孤儿撤单上限
    static constexpr size_t  kOrphanSweepBuf = 64;             ///< 孤儿清扫 open-order 枚举缓冲
    static constexpr int     kMaxFlattenChunks = 64;           ///< 单轮平仓切片上限（防下单突发）
    static constexpr int64_t kAuditDrainFlattenTicks = 10;     ///< audit 排空期 flatten 节拍
    static constexpr int64_t kRecentPurgeMask = 63;            ///< 终态暂存定稿扫描节拍
    static constexpr unsigned kSweepHeavyMask = 63;            ///< 历史/日期 sweep 节拍
    static constexpr unsigned kSweepDetailMask = 15;           ///< 仓位明细 sweep 节拍
    static constexpr int64_t kTickGapSkipUs = 60LL * 1000000;  ///< 超 60s 视为 session 边界
    static constexpr uint64_t kBogusOrderId = kReservedProbeCancelOrderId;  ///< cancel 负路径探针（复用 xt_core 保留哨兵，网关侧识别为 probe_unknown）
};
