#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <prometheus/metric_family.h>

namespace prometheus {
class Registry;
class Exposer;
class Histogram;
class Counter;
}  // namespace prometheus

namespace ct::metrics {

/// @brief runner 进程里的 Prometheus metrics 总入口
///
/// runner 启动时用 XtMetricRegistry 开一个 /metrics HTTP endpoint，Prometheus 来 scrape；
/// 运行中各模块把已经发生的事实打进去，例如下单数、成交数、拒单数、行情 tick 数、latency histogram 等
class XtMetricRegistry {
public:
    /// @brief 单例访问器。线程安全（C++11 magic statics）。
    static XtMetricRegistry& instance();

    XtMetricRegistry(const XtMetricRegistry&) = delete;
    XtMetricRegistry& operator=(const XtMetricRegistry&) = delete;

    /// @brief 启动 HTTP /metrics endpoint。
    /// @param port 监听端口；默认 9101。
    /// @param bind_addr 监听地址；生产默认只绑定 127.0.0.1，由上游反代或 scrape agent 接入。
    /// @return true 表示启动成功；false 表示已启动或 bind 失败。
    bool start_exposer(uint16_t port = 9101,
                       std::string_view bind_addr = "127.0.0.1");

    /// @brief 停止 HTTP exposer。幂等，析构时也会自动调。
    ///
    /// Runner 应在 runtime 线程停止之后、日志系统关闭之前显式调用。
    /// 这样 civetweb worker 不会在静态单例析构阶段继续 serve `/metrics`，
    /// 也能降低重启时端口短暂占用的概率。
    void stop_exposer();

    /// @brief 是否已启动 exposer。
    bool is_running() const noexcept;

    /// @brief 不启动 HTTP exposer 时采集 metric family；用于测试和诊断。
    std::vector<prometheus::MetricFamily> collect_metric_families_for_test() const;

    // ── emit helpers (事件类型) ─────────────────────────────────────────────
    //
    // 全部 thread-safe (prometheus-cpp 内部 atomic / 锁)，hot path 直接调。

    /// @brief 订单提交事件。计 xt_submits_total 计数器。
    void on_order_submit(std::string_view strategy,
                         std::string_view book,
                         std::string_view venue,
                         std::string_view symbol,
                         std::string_view side);

    // ── 市场数据 tick counter ────────────────────────────────────────────

    /// @brief 行情 tick 事件 — 计 xt_md_ticks_total{venue, symbol} counter.
    ///
    /// XtLiveRecorder::emit_md_tick 是跨 venue 单点调用路径。
    /// panel 查 `rate(xt_md_ticks_total{venue=X}[1m])` 获取真实 tick rate。
    /// @param venue   "ctp" / "binance_u" / "xtp" (跟 venue_kind_to_id 一致)
    /// @param symbol  instrument code (BTCUSDT / ag2606 / 600000.SH 等)
    void on_md_tick(std::string_view venue,
                    std::string_view symbol);

    /// @brief 订单状态回报事件。计 xt_order_rtns_total{status=...}。
    void on_order_rtn(std::string_view strategy,
                      std::string_view book,
                      std::string_view venue,
                      std::string_view symbol,
                      std::string_view status);

    /// @brief 成交回报事件。
    /// 计 xt_fills_total + xt_today_fills_total + positive fee/PnL counters +
    /// rebate/loss counters。Today PnL 由 baseline-aware snapshot path 发出。
    /// @param qty_fp           成交量 ×1e6
    /// @param price_fp         成交价 ×1e6（用于 notional gauge）
    /// @param fee_fp           成交手续费 ×1e6（signed: 返佣负值）
    /// @param realized_pnl_fp  该笔实现 PnL ×1e6（close 单非零）
    /// @param is_maker         maker / taker 区分
    void on_trade(std::string_view strategy,
                  std::string_view book,
                  std::string_view venue,
                  std::string_view symbol,
                  std::string_view side,
                  int64_t qty_fp,
                  int64_t price_fp,
                  int64_t fee_fp,
                  int64_t realized_pnl_fp,
                  bool is_maker);

    /// @brief 成交 route triage：route_status 漏斗 + routed per-fill 计数。
    ///
    /// `route_status != routed` 只增 triage counter。today_* gauge 由
    /// set_strategy_ledger_today() 的 frame-loop emit 负责。`strategy_accounted`
    /// 由 recorder 从 event 字段直接判定（route + 非空 strategy/symbol + qty/price>0）。
    void on_normalized_trade(std::string_view strategy,
                             std::string_view book,
                             std::string_view venue,
                             std::string_view symbol,
                             std::string_view side,
                             std::string_view route_status,
                             bool strategy_accounted);

    /// @brief 权威 today gauge emit（baseline-aware；来自 XtStrategyPnlAggregator − baseline）。
    ///
    /// 写 xt_strategy_ledger_today_{realized_pnl_fp,fees_fp,fills_total}
    /// {strategy,book,venue,symbol}。Runner 的 strategy-PnL frame-loop 调用本接口；
    /// recorder per-trade 路径不写 today gauge。
    void set_strategy_ledger_today(std::string_view strategy,
                                   std::string_view book,
                                   std::string_view venue,
                                   std::string_view symbol,
                                   int64_t today_realized_pnl_fp,
                                   int64_t today_fees_fp,
                                   uint64_t today_fills);

    /// @brief 风控拒单事件。计 xt_risk_rejects_total{reason=...}。
    void on_risk_reject(std::string_view strategy,
                        std::string_view book,
                        std::string_view venue,
                        std::string_view symbol,
                        std::string_view reason);

    /// @brief runner-owned pre-trade reject 累计 delta emit（1Hz，cumulative→delta）。
    /// @note 三 runner（HFT/CTA/SEL）用 gateway `RiskStatsSnapshot::total_rejects()`
    ///       的单调总数按帧算增量，聚合 label `{strategy=_runtime,venue,symbol=_all,
    ///       reason=_all}`（不带 book——runner 级聚合恒 _runtime 无信息量，见 .cpp 注释）。为
    ///       `venue:xt_risk_block_rate_5m:per_minute` 提供真数据源（旧 emit_risk_reject
    ///       死链已绕开）。每帧调用即 Add 取引用，首帧 eager 创建 0 值 series 保 rate()
    ///       基线（惰性 series 会让首个 reject burst 逃出 rate()）；仅 delta>0 时才递增。
    void inc_risk_rejects_total(std::string_view venue, uint64_t delta);

    /// @brief 交易所业务拒单事件。计 xt_order_rejects_total{err_code=...}。
    ///
    /// venue 网关侧 kRejected ack 调用。err_code 必须来自有界集合；
    /// err_msg 不得作为 label。panel 用 err_code 维度聚合拒因。
    void on_order_reject(std::string_view venue,
                         std::string_view err_code);

    // ── Binance MD single-trip latency + clock skew ─────────────────────
    //
    // XtClockSkewEstimator 通过本组接口发布 MD 单程延迟和 clock offset。
    // channel 为 bookTicker/aggTrade/depth/kline；venue 由 prom-agent
    // external_labels 补齐，不作为 label 参数传入。

    void observe_md_event_to_recv_us(std::string_view channel,
                                      uint64_t latency_us);

    void observe_md_internal_queue_us(std::string_view channel,
                                       uint64_t queue_us);

    /// @brief 设置 clock offset 估计值（μs）。
    /// @note 传 NaN 表示 estimator 处于 STEPPED 等不可信状态，dashboard 不应当作 0 offset。
    void set_md_clock_offset_us(std::string_view channel, double offset_us);

    /// @brief 设置 clock offset confidence flag。
    /// @note INIT/WARMING/STEPPED 为 false；LOCKED 且样本足够时才为 true。
    void set_md_clock_offset_confidence(std::string_view channel, bool locked);

    void set_md_clock_offset_window_samples(std::string_view channel,
                                              uint64_t samples);

    void set_md_clock_offset_window_min_age_seconds(std::string_view channel,
                                                      double age_s);

    /// @brief 记录一次 NTP step detector 命中。
    void inc_md_clock_step(std::string_view channel);

    /// @brief 累加 correction 后仍为负的 latency 样本数。
    void inc_md_negative_latency(std::string_view channel, uint64_t count);

    /// @brief 记录一次 WS reconnect；estimator window 会重建，bootstrap offset 保留。
    void inc_md_ws_reconnect(std::string_view channel);

    /// @brief 记录一次 aggTrade aggregate-id 不连续（漏成交）事件。
    /// @param channel 传 "aggTrade"，与同组 MD latency metric 的 channel label 对齐。
    void inc_md_aggtrade_gap(std::string_view channel);

    /// @brief aggTrade gap 回补结果计数。result=replayed|failed|truncated；n 为增量。
    void inc_md_aggtrade_backfill(std::string_view result, uint64_t n);

    /// @brief 参数热更新事件。计 xt_param_updates_total{key=...}。
    void on_param_update(std::string_view strategy,
                         std::string_view actor,
                         std::string_view key);

    /// @brief Kill switch 翻转事件。计 xt_kill_switch_trips_total{state=...}。
    void on_kill_switch(std::string_view venue,
                        std::string_view actor,
                        std::string_view state,    ///< "tripped" / "resumed"
                        std::string_view reason);

    // ── gauge setter (周期性快照) ──────────────────────────────────────────
    //
    // 这些 metric 不来自单个 event，而由 runner control loop 周期性调用。

    /// @brief 设置 xt_pnl_net{venue=...,account=...} cumulative gauge。
    ///
    /// Account-level session PnL。Today-PnL 面板使用 `xt_today_pnl_net_v2`
    /// baseline-aware strategy gauge；本 gauge 只服务 account/wallet consistency
    /// 与 session 诊断，不维护 process-local day-open snapshot。
    /// @param venue        "ctp" / "binance_u" / etc
    /// @param account      "_default" / 子账户 ID (账户级 metric, 非 strategy-attributed)
    /// @param pnl_net      double, last_equity - session_baseline_equity
    void set_pnl_net(std::string_view venue,
                     std::string_view account,
                     double pnl_net);

    /// @brief 设置 xt_position_qty_fp{strategy=...,...} gauge（×1e6 fp）。
    void set_position_qty_fp(std::string_view strategy,
                             std::string_view book,
                             std::string_view venue,
                             std::string_view symbol,
                             int64_t qty_fp);

    /// @brief 设置 xt_strategy_position_qty_fp{venue,account,strategy,book,symbol} gauge（×1e6 fp）。
    void set_strategy_position_qty_fp(std::string_view venue,
                                      std::string_view account,
                                      std::string_view strategy,
                                      std::string_view book,
                                      std::string_view symbol,
                                      int64_t qty_fp);
    /// @brief 设置 xt_strategy_position_avg_price_fp{venue,account,strategy,book,symbol} gauge（×1e6 fp）。
    void set_strategy_position_avg_price_fp(std::string_view venue,
                                            std::string_view account,
                                            std::string_view strategy,
                                            std::string_view book,
                                            std::string_view symbol,
                                            int64_t v);
    /// @brief 设置 xt_strategy_position_unrealized_pnl_fp{venue,account,strategy,book,symbol} gauge（×1e6 fp）。
    void set_strategy_position_unrealized_pnl_fp(std::string_view venue,
                                                 std::string_view account,
                                                 std::string_view strategy,
                                                 std::string_view book,
                                                 std::string_view symbol,
                                                 int64_t v);

    /// @brief 设置 xt_kill_switch_active{venue=...} gauge (1 = active, 0 = inactive)。
    void set_kill_switch_active(std::string_view venue, bool active);

    /// @brief 设置 xt_td_ready{venue=...} gauge (1 = TD can accept trading, 0 = not ready).
    void set_td_ready(std::string_view venue, bool ready);

    /// @brief 设置 xt_trading_ready{venue=...} gauge (1 = MD+TD ready, 0 = gated).
    void set_trading_ready(std::string_view venue, bool ready);

    /// @brief 设置 xt_td_recovery_wedged{venue=...} gauge (1 = recovery re-arm 超阈仍未 ready, 0 = 正常).
    ///
    /// CTP gateway 直 emit；re-arm 轮数 >= alert_rounds 置 1，recovery 成功或
    /// session reset 清 0。置 1 后仍会继续 re-arm，成功后可回到 0。
    void set_td_recovery_wedged(std::string_view venue, bool wedged);

    /// @brief 按 delta 累加聚合 xt_md_ticks_total{venue,symbol="_all"}。
    void inc_md_ticks_total(std::string_view venue, uint64_t delta);

    /// @brief 设置 xt_strategy_enabled{strategy,venue} gauge (1 = enabled, 0 = paused)。
    void set_strategy_enabled(std::string_view strategy,
                              std::string_view venue,
                              bool enabled);

    /// @brief 设置 xt_strategy_custom_metric{strategy,name} gauge。
    ///
    /// 策略 report_metric 的 Prometheus 桥；name label 是 metric key。
    /// HFT 主 strategy context 通过 1Hz 控制环冷路径 emit，venue 由 external_labels 注入。
    void set_strategy_custom_metric(std::string_view strategy,
                                    std::string_view name,
                                    double value);

    /// @brief 设置 xt_today_pnl_net_v2{strategy,book,venue,symbol} gauge。
    ///
    /// Baseline-aware today PnL：调用方用 aggregator snapshot 减去
    /// XtStrategyPnlBaseline 开盘累计值；跨 day rotation 后重新归零。
    /// @param strategy / book / venue / symbol  strategy-scoped label set
    /// @param pnl_net      double, today's realized PNL net (post-baseline subtraction)
    void set_today_pnl_net_v2(std::string_view strategy,
                              std::string_view book,
                              std::string_view venue,
                              std::string_view symbol,
                              double pnl_net);

    /// @brief 设置 xt_today_fills_total_v2{strategy,venue,symbol} gauge。
    ///
    /// 与 set_today_pnl_net_v2 同口径：调用方提交 baseline-aware today fills。
    /// @param fills_count  uint64 (gauge value, 不用 counter 因 跨 day rotation 会 reset)
    void set_today_fills_total_v2(std::string_view strategy,
                                  std::string_view venue,
                                  std::string_view symbol,
                                  uint64_t fills_count);

    /// @brief 累加 xt_strategy_baseline_new_total{strategy,venue} counter。
    ///
    /// Strategy 首次出现且 baseline 无该 key 时调用，用于识别 mid-day hot-add。
    void inc_strategy_baseline_new_total(std::string_view strategy,
                                         std::string_view venue);

    /// @brief 累加 xt_baseline_rotated_total{venue} counter。
    ///
    /// 每次 venue day rotation 成功后调用，用于观测 daily reset。
    void inc_baseline_rotated_total(std::string_view venue);

    /// @brief 累加 xt_strategy_baseline_load_failed_total{venue,reason}。
    void inc_strategy_baseline_load_failed_total(std::string_view venue,
                                                 std::string_view reason);

    // ── Account truth direct emit ──────────────────────────────────────────
    struct AccountTruthSample {
        double wallet_balance              = 0.0;
        double equity                      = 0.0;
        double position_value              = 0.0;
        double unrealized_pnl              = 0.0;
        double fees_today                  = 0.0;
        double external_cashflow_today     = 0.0;
        double trading_day_id              = 0.0;  ///< Prom gauge 只支持 double，YYYYMMDD 也按 double emit。
        double available_balance           = 0.0;  ///< Binance availableBalance；CTP 使用 balance 近似。
        double maint_margin                = 0.0;  ///< Binance totalMaintMargin；其他 venue 缺省 0。
        double margin_ratio                = 0.0;  ///< maint_margin / total_margin_balance；分母为 0 时 emit 0。
    };

    /// @brief 设置 xt_account_truth_* / xt_account_*{venue,account,source} 10 个 gauge。
    ///
    /// Runner control loop 1Hz 调用；一次调用发出整组字段，降低 scrape cross-field skew。
    /// @param venue   "binance_u" / "ctp" / "xtp" (跟 xt_pnl_net venue 一致)
    /// @param account "_default" / 子账户 ID (同 set_pnl_net account label)
    /// @param sample  10 字段 atomic snapshot；一次调用全 emit，降低 cross-field skew。
    /// @param source  "runtime" (default) / "csv" / "test" — source attribution label
    void set_account_truth_snapshot(std::string_view venue,
                                    std::string_view account,
                                    const AccountTruthSample& sample,
                                    std::string_view source = "runtime");

    /// @brief 设置 xt_venue_equity_raw{venue,account} gauge。
    ///
    /// 仅在 venue 提供真实 total_margin_balance 时 emit，用于和 account_store-derived equity 对比。
    /// @param venue   "binance_u" 等
    /// @param account "_default" / 子账户 ID
    /// @param value   total_margin_balance (post xt_fp::from_fp)
    void set_venue_equity_raw(std::string_view venue,
                              std::string_view account,
                              double value);

    /// @brief 一次 emit xt_venue_cash_reanchor_divergence gauge（带符号最近样本）
    ///        + xt_venue_cash_reanchor_correction_total counter（累加 |divergence|）。
    ///
    /// engine set_cash 重锚 divergence 采样（apply 之前）；CTP HFT worker 线程
    /// 按 account_refresh_ms 周期调用（冷路径）。
    /// @param venue      "ctp" 等
    /// @param account    "_default" / 子账户 ID
    /// @param divergence broker − derived（post xt_fp::from_fp，带符号）
    void observe_cash_reanchor(std::string_view venue,
                               std::string_view account,
                               double divergence);

    // ── State-divergence telemetry ────────────────────────────────────────
    /// @brief 设置 xt_state_divergence{field,symbol} gauge。
    ///
    /// Runner control loop 1Hz emit `|account_store.field - broker_snapshot.field|`。
    /// @param field   "qty" or "unrealized_pnl"
    /// @param symbol  instrument code, or "_aggregate" for account-level fields
    /// @param value   |store − broker| in human-readable units (post xt_fp::from_fp)
    void set_state_divergence(std::string_view field,
                              std::string_view symbol,
                              double value);

    /// @brief 累加 xt_state_divergence_emit_heartbeat counter。
    ///
    /// 每个 divergence emit tick 最后调用；rate 为 0 表示安全网自身停止。
    void inc_state_divergence_emit_heartbeat();

    /// @brief 设置 xt_reconcile_snapshot_age_ms gauge。
    ///
    /// reconcile loop 每周期调用：fetch 成功写 0，失败写距上次成功快照的毫秒数。
    /// 持续增长表示 loop fail-closed 空转，期间没有 position compare 覆盖。
    void set_reconcile_snapshot_age_ms(double age_ms);

    // ── Maker/taker fills ──────────────────────────────────────────────────

    /// @brief Fill 事件 — 按 is_maker 路由到 maker_fills_total 或 taker_fills_total counter.
    ///
    /// on_trade callback / fee observer fill handler 每笔成交调用一次。
    /// @param strategy / book / venue / symbol  label set 同 fills_total
    /// @param is_maker  true → maker 计 maker_fills_total; false → taker 计 taker_fills_total
    void on_fill_maker_taker(std::string_view strategy,
                             std::string_view book,
                             std::string_view venue,
                             std::string_view symbol,
                             bool is_maker);

    // ── Risk limits per-strategy gauge ─────────────────────────────────────
    struct RiskLimitsSample {
        double max_order_qty           = 0.0;
        double max_pos_qty             = 0.0;
        double max_drawdown_pct        = 0.0;
        double max_position_value_usd  = 0.0;
        double max_orders_per_min      = 0.0;
        bool   allow_short             = true;  // 卖空许可 (gauge 1=allowed / 0=blocked)
    };

    /// @brief 设置 xt_strategy_limit_*{strategy,book,venue} 6 个 gauge。
    ///
    /// Runner 1Hz per-strategy emit 6 个 limit gauge。
    /// @param strategy / book / venue  label set 同 set_today_pnl_net_v2 (无 symbol — limit 是 strategy 级)
    /// @param limits  6 字段 snapshot (含 allow_short; atomic emit 防 cross-field skew)
    void set_strategy_limits(std::string_view strategy,
                             std::string_view book,
                             std::string_view venue,
                             const RiskLimitsSample& limits);

    // ── Per-strategy auto-liquidation observability ────────────────────────
    //
    // PerSidLiquidator::step() 1Hz control-loop emit (HFT runner only)。仅在
    // auto-liquidation 启用时产生序列；默认 OFF → step() early-return 不 emit。

    /// @brief 设置 xt_strategy_auto_liquidating{strategy} gauge (1 = latched, 0 = not)。
    void set_strategy_auto_liquidating(std::string_view strategy, bool latched);

    /// @brief 设置 xt_strategy_auto_liquidation_trigger_pnl_fp{strategy} gauge (×1e6)。
    /// @param trigger_pnl_fp  today_realized + unrealized，对照 -(pct×starting_balance) 阈值。
    void set_strategy_auto_liquidation_trigger_pnl_fp(std::string_view strategy,
                                                      int64_t trigger_pnl_fp);

    /// @brief 累加 xt_strategy_auto_liquidation_breach_total{strategy} (上升沿 arm)。
    void inc_strategy_auto_liquidation_breach_total(std::string_view strategy);

    /// @brief 累加 xt_strategy_auto_liquidation_submit_total{strategy} (ForceClosed)。
    void inc_strategy_auto_liquidation_submit_total(std::string_view strategy);

    /// @brief 累加 xt_strategy_auto_liquidation_deferred_total{strategy}
    ///        (NoTd / NoMarkPrice / SubmitFailed / CancelledOnly — latch 保留下帧重试)。
    void inc_strategy_auto_liquidation_deferred_total(std::string_view strategy);

    /// @brief 累加 xt_strategy_auto_liquidation_1to1_violation_total{strategy}
    ///        (declared_symbols != 1，warn-once per sid)。
    void inc_strategy_auto_liquidation_1to1_violation_total(std::string_view strategy);

    // ── Engine-hosted conditional stop orders ──────────────────────────────
    //
    // XtStrategyContext per-ctx stop-order book 生命周期事件；HFT+CTA 双引擎。
    // event label 为有限枚举防 cardinality 爆。

    /// @brief 累加 xt_stop_order_total{event} counter。
    /// @param event  "fired" / "refired" / "consumed" / "clamped" / "dropped_flat" /
    ///               "dropped_roll" / "dropped_exhausted" / "arm_rejected_full"（有限枚举）。
    /// @param delta  累加量（roll 清仓一次清多个 slot 时传批量数）；0 为 no-op。
    void inc_stop_order_total(std::string_view event, uint64_t delta = 1);

    /// @brief 设置 xt_stop_order_armed{strategy} gauge（当前 Armed+Fired slot 数）。
    void set_stop_order_armed(std::string_view strategy, uint32_t count);

    // ── Control plane gRPC observability ───────────────────────────────────
    //
    // gRPC server 线程在 RPC 完成后调用；prometheus-cpp 自身保证 Family/Add thread-safe。

    /// @brief 记录 control plane RPC apply latency（μs）。
    /// @param action  proto RPC method，如 "Pause" / "FlattenAll"。
    /// @param result  "ok" / "invalid_argument" / "unimplemented" / "internal" / ...
    void observe_control_rpc_us(std::string_view action,
                                 std::string_view result,
                                 uint64_t latency_us);

    /// @brief 累加 control plane RPC count；labels 为 action / actor / result。
    /// @param action  proto RPC method
    /// @param actor   cert CN 或 "unknown"
    /// @param result  "ok" / non-OK status string
    void inc_control_rpc_total(std::string_view action,
                                std::string_view actor,
                                std::string_view result);

    // ── Gateway connection observability ───────────────────────────────────
    //
    // reason 必须是 finite enum 防 cardinality 爆；详细字符串走日志/recorder。

    /// @brief 累加网关断连事件 counter。
    ///
    /// 每次 gateway socket / WS 进入 disconnected 状态触发一次，不依赖底层 reconnect 计数。
    /// @param venue  "ctp" / "binance_u" / etc (gateway 所属 venue label)
    /// @param role   "md" / "td"  (market-data / trade-data 区分)
    /// @param reason 见上方枚举表;调用方必须传 enum 字面值,不可 free-form
    void inc_gateway_disconnect_total(std::string_view venue,
                                       std::string_view role,
                                       std::string_view reason);

    // ── Risk close path 双源取严 disagreement ──────────────────────────────
    // 风控 close path 取 max(local_outstanding, broker_frozen):
    //   - local: XtOrderRuntimeBook 累加的 in-flight close 量
    //   - broker: CTP InvestorPosition.LongFrozen / ShortFrozen (binance 无,恒 0)

    /// @brief 累加双源 close 配额不一致 counter。
    /// @param venue  "ctp" / "binance_u" / etc
    void inc_close_quota_disagreement_total(std::string_view venue);

    // ── Recovery reverse-sync alive in-flight rebuild ──────────────────────
    // reverse sync = warm reconnect / cold-start 时拉 venue 活跃在途单,
    // 反解 local_id 灌回 XtOrderRuntimeBook，补齐本地缺失 slot。

    /// @brief 累加 rebuild alive in-flight 调用 counter。
    /// @param venue   "binance_u" / "ctp" / "xtp"
    /// @param outcome "success" / "failure"
    void inc_recovery_rebuild_alive_total(std::string_view venue,
                                          std::string_view outcome);

    /// @brief rebuild alive in-flight 因 foreign / 不可反解 client_order_id 跳过的笔数。
    /// @param venue   "binance_u" / "ctp" / "xtp"
    void inc_recovery_rebuild_alive_skipped_foreign_total(std::string_view venue);

    /// @brief 按 delta 累加孤儿回收撤单数 xt_recovery_orphan_reaped_total{venue}。
    ///
    /// recovered-unowned 在途单被 reaper 发出撤单的累计；
    /// HftRunnerControl 1Hz 从 engine counter delta-emit。
    void inc_recovery_orphan_reaped_total(std::string_view venue, uint64_t delta);

    /// @brief 按 delta 累加 position snapshot 丢弃数 xt_td_position_snapshot_drop_total{venue}。
    ///
    /// PositionSnapshot 走 must_not_drop 路径；本 counter 非零表示物理队列满导致首帧丢失，
    /// td_position_ready 会 fail-closed 并拒绝 submit。
    /// HftRunnerControl 1Hz 从 XtRunnerSignals.td_drop_position_cnt delta-emit。
    void inc_td_position_snapshot_drop_total(std::string_view venue, uint64_t delta);

    /// @brief 按 delta 累加 control event 丢弃数 xt_td_control_event_drop_total{venue}。
    ///
    /// ChannelReady/ChannelLost 走 must_not_drop 路径；本 counter 非零表示物理队列满丢弃。
    /// 丢 ChannelReady 会让 worker td_channel_up_ 保持 false，直到下次断连边沿 re-arm。
    /// HftRunnerControl 1Hz 从 XtRunnerSignals.td_drop_control_cnt delta-emit。
    void inc_td_control_event_drop_total(std::string_view venue, uint64_t delta);

    /// @brief 按 delta 累加 other 桶丢弃数 xt_td_other_event_drop_total{venue}。
    ///
    /// position/control 之外的残余桶（exec/account + 非边沿 control 事件）。
    /// 非 must_not_drop：软水位 shedding、物理满、loop 未运行拒绝皆计入。
    /// HftRunnerControl 1Hz 从 XtRunnerSignals.td_drop_other_cnt delta-emit。
    void inc_td_other_event_drop_total(std::string_view venue, uint64_t delta);

    /// @brief 按 delta 累加 HFT worker guard 异常数 xt_hft_worker_guard_fail_total{venue,stage}。
    /// @param stage "body"（backstop 逃逸 → fail-stop）/ "normalize"（passthrough raw fill）/
    ///              "record"（telemetry swallow）；有限枚举防 cardinality。
    void inc_hft_worker_guard_fail_total(std::string_view venue, std::string_view stage,
                                         uint64_t delta);

    /// @brief 设置本地 order book alive 在途单数 xt_open_orders{venue}。
    ///
    /// 含 Submitted 未 ack 与 recovered 孤儿；单 symbol runner 下可作为 venue
    /// per-symbol open-order cap 的本地代理。HftRunnerControl 1Hz set。
    void set_open_orders(std::string_view venue, uint32_t count);

    // ── shm MD reader attach observability (T1.141①) ───────────────────────

    /// @brief 设置 shm MD reader attach 态 xt_shm_md_attach_failing{ring}（1=attach 失败未恢复）。
    /// @note gauge：HftRunnerControl 1Hz frame-tail set，每帧 emit 保序列非惰性、免 series-birth 盲区。
    void set_shm_md_attach_failing(std::string_view ring, uint32_t failing);
    /// @brief 设置 shm MD reader 累计 attach 失败数 xt_shm_md_attach_fail_count{ring}（单调 gauge 直设）。
    void set_shm_md_attach_fail_count(std::string_view ring, uint64_t count);

    /// @brief 设置 STP 已注册品种数 xt_stp_registered_symbols{venue}（0 = STP 自成交防护未上膛=旁路）。
    /// @note gauge：runner 装配期一次性 set（pre_warm_risk 后）；gauge 序列持久，scrape 恒可见。
    void set_stp_registered_symbols(std::string_view venue, uint32_t count);

    // ── Account store health (F02 P6) ─────────────────────────────
    struct AccountStoreHealthSample {
        uint64_t position_slots_used          = 0;  ///< 活世代已消耗 slot 数（三源并集口径）。
        uint64_t position_slots_capacity      = 0;  ///< kMaxPositionSlots。
        uint64_t position_slot_overflow_total = 0;  ///< 累计 slot/intern 溢出事件（单调）。
        uint64_t publish_post_failed_total    = 0;  ///< 累计发布闭包投递失败（单调）。
        int64_t  frame_age_ms                 = -1; ///< 最新已发布帧龄；<0 = 尚无首帧，跳过 emit。
        /// 最新发布的 wall-clock unix 秒（emit 点由 steady 差值换算）；<=0 跳过 emit。
        /// freshness alert 用 time()−本值：emitter（控制循环）冻结时本值停走而 time()
        /// 继续，告警仍可触发——frame_age_ms 的值会随 emitter 一起冻结，只作诊断面。
        double   last_publish_unix_ts_seconds = -1.0;
        uint64_t session_begin_deferred_total = 0;  ///< 累计 session-begin 整块推迟帧数（单调）。
        uint64_t intern_overflow_total        = 0;  ///< 累计 instrument intern 表溢出（单调）。
        uint64_t slot_reservation_rejected_total = 0;  ///< 累计 submit 预留失败拒单（单调）。
    };

    /// @brief 设置 account store 健康面 9 个 series（labels: venue）：slots 两 gauge +
    ///        frame_age/last_publish_ts 两 gauge + 五个累计 counter。
    ///
    /// 三 runner 共享的 advance_risk_frame 帧尾 1Hz 调用。counter 类取累计值，
    /// 内部按 delta 递增导出（同 label 单线程 emit 前提）。注意：启动前（装配/
    /// recovery 期）已累计的增量在 Prometheus 首样本里就是非零，increase() 永远
    /// 看不到该跳变——expected-zero 类（overflow/intern/reservation）的 alert 必须用
    /// `> 0` latch 表达式而非 increase()；进程启动后的增量无 series-birth 盲区。
    void set_account_store_health(std::string_view venue,
                                  const AccountStoreHealthSample& sample);


    // ── XtOrderRuntimeBook drop counters ───────────────────────────────────

    /// @brief 累加 XtOrderRuntimeBook drop counter。
    /// @param reason "slot_pool_full" / "orphan_update" / "orphan_trade" / "orphan_reap"  (finite enum 防 cardinality)
    void inc_runtime_book_drop_total(std::string_view reason);

    // ── Multi-strategy unmatched drop + engine recovery ───────────────────

    /// @brief 按 delta 累加 multi-strategy unmatched 丢弃 xt_dropped_total{kind,reason}。
    /// @param kind   "fill" / "order_rtn"  (finite enum 防 cardinality)
    /// @param reason "empty_sid" / "unknown_sid"
    void inc_unmatched_drop_total(std::string_view kind, std::string_view reason,
                                  uint64_t delta);

    /// @brief 按 delta 累加 engine 自愈动作 xt_engine_recovery_total{kind}。
    /// @param kind "sid_fallback" / "lost_terminal"  (finite enum 防 cardinality)
    void inc_engine_recovery_total(std::string_view kind, uint64_t delta);

    // ── XtRuntimeMarketCache drop counters ─────────────────────────────────

    /// @brief 累加 XtRuntimeMarketCache drop counter。
    /// @param reason "cross_thread_direct_read"  (finite enum 防 cardinality)
    void inc_runtime_market_drop_total(std::string_view reason);

    // ── Latency tracker meta miss ─────────────────────────────────────────

    /// @brief 累加 xt_latency_meta_miss_total{venue}。
    ///
    /// latency tracker emit 需要的 strategy/code meta 缺失且 order-book 反查 fallback
    /// 也失败时计数，每订单至多一次。rate > 0 表示 latency histogram 样本覆盖率在丢失。
    void inc_latency_meta_miss_total(std::string_view venue);

    /// @brief 累加 xt_latency_eligible_total{stage,strategy,book,symbol}。
    ///
    /// submit_to_ack coverage 分母：gateway submit 成功处计（每单一次天然去重）。
    /// label 集与 histogram 一致 {strategy,book,symbol}，供 PromQL vector matching；
    /// stage 目前仅 "ack"（fill-eligible 本 campaign 不提供）。
    void inc_latency_eligible_total(std::string_view stage, std::string_view strategy,
                                    std::string_view book, std::string_view symbol);

    /// @brief 累加 xt_latency_tap_dropped_total{reason}。
    ///
    /// latency tracker producer→assembler 传输面物理满 / 容器驱逐计数。
    /// @param reason "ring_full"|"overflow_full"|"meta_evicted"|"orders_evicted"|
    ///               "trace_oversize"|"slot_exhausted"|"label_cache_full"（有限枚举）
    void inc_latency_tap_dropped_total(std::string_view reason);

    // ── CTP broker_frozen staleness ────────────────────────────────────────
    // CTP InvestorPosition.LongFrozen / ShortFrozen push 正常亚秒级,
    // 30s 阈值是保守停滞判断。binance/XTP 不推 broker_frozen，故 venue 限定 ctp。

    /// @brief 累加 CTP broker_frozen 推送停滞 counter。
    /// @param venue  "ctp" (binance/XTP 不推 broker_frozen,不触发)
    void inc_broker_frozen_stale_total(std::string_view venue);

    // ── PositionAggregator implausible-jump observability ─────────────────

    /// @brief 累加 PositionAggregator implausible-jump counter。
    /// @param sid strategy instance id；backtest 可为空。
    /// @param code instrument code。
    void inc_position_aggregator_jump_total(std::string_view sid,
                                            std::string_view code);

    // ── per-symbol venue truth gauge setters ──────────────────────────────
    //
    // {venue,symbol} 2-label，不带 strategy/book；这些值来自 venue/account
    // truth 或 account_store mark cache，不是 strategy attribution。

    /// @brief 设置 xt_position_avg_price_fp{venue,symbol} gauge（×1e6 fp）。
    void set_position_avg_price_fp(std::string_view venue, std::string_view symbol,
                                   int64_t v);
    /// @brief 设置 xt_position_mark_price_fp{venue,symbol} gauge（×1e6 fp）。
    void set_position_mark_price_fp(std::string_view venue, std::string_view symbol,
                                    int64_t v);
    /// @brief 设置 xt_position_unrealized_pnl_fp{venue,symbol} gauge（×1e6 fp）。
    void set_position_unrealized_pnl_fp(std::string_view venue, std::string_view symbol,
                                        int64_t v);
    /// @brief 设置 xt_position_margin_fp{venue,symbol} gauge（×1e6 fp）。
    void set_position_margin_fp(std::string_view venue, std::string_view symbol,
                                int64_t v);
    /// @brief 设置 account_store 内部 mark cache gauge（×1e6 fp）。
    ///
    /// 该值用于和 venue truth mark price 对比；若 live MD 正常而该 gauge 长期为 0，
    /// 说明 MarketTick 写入 account_store mark cache 的路径需要排查。
    void set_account_store_mark_price_fp(std::string_view venue,
                                          std::string_view symbol,
                                          int64_t v);

    /// @brief 设置 xt_broker_position_qty_fp{venue,account,symbol} gauge（×1e6 fp）。
    void set_broker_position_qty_fp(std::string_view venue,
                                    std::string_view account,
                                    std::string_view symbol,
                                    int64_t v);
    /// @brief 设置 xt_broker_position_avg_price_fp{venue,account,symbol} gauge（×1e6 fp）。
    void set_broker_position_avg_price_fp(std::string_view venue,
                                          std::string_view account,
                                          std::string_view symbol,
                                          int64_t v);
    /// @brief 设置 xt_broker_position_mark_price_fp{venue,account,symbol} gauge（×1e6 fp）。
    void set_broker_position_mark_price_fp(std::string_view venue,
                                           std::string_view account,
                                           std::string_view symbol,
                                           int64_t v);
    /// @brief 设置 xt_broker_position_unrealized_pnl_fp{venue,account,symbol} gauge（×1e6 fp）。
    void set_broker_position_unrealized_pnl_fp(std::string_view venue,
                                               std::string_view account,
                                               std::string_view symbol,
                                               int64_t v);
    /// @brief 设置 xt_broker_position_margin_fp{venue,account,symbol} gauge（×1e6 fp）。
    void set_broker_position_margin_fp(std::string_view venue,
                                       std::string_view account,
                                       std::string_view symbol,
                                       int64_t v);

    /// @brief runner-owned 1Hz KPI snapshot。
    struct RuntimeKpiSnapshot {
        double md_gap_ms_p99 = 0.0;   ///< tick 队列滞留窗口真 p99（直方图桶上界，保守）。
        bool has_md_gap_ms_p99 = true;
        double md_gap_ms_peak = 0.0;  ///< tick 队列滞留窗口 observed peak。
        bool has_md_gap_ms_peak = true;
        double md_age_ms = 0.0;
        bool has_md_age_ms = false;
        double event_drop = 0.0;
        double queue_depth_p99 = 0.0;
        double risk_block_count = 0.0;
    };

    /// @brief 从 generated zero_init.h 发布 contract meta gauges。
    ///
    /// `xt_runner_contract_version_info` 和 `xt_runner_bucket_schema_info`
    /// 在 start_exposer() 时发布，用于 panel/health 检测 manifest drift。
    void emit_contract_meta();

    /// @brief 用写入 kpi_1s.csv 的同一份 1Hz snapshot 发布 runner-owned KPI gauges。
    void set_runtime_kpi_snapshot(std::string_view strategy,
                                  std::string_view book,
                                  std::string_view venue,
                                  std::string_view symbol,
                                  const RuntimeKpiSnapshot& sample);

    // ── Histogram ptr accessor ────────────────────────────────────────────
    //
    // 仅 XtLiveLatencyTracker 在冷路径解析 label tuple 后缓存；backtest 不应触达。

    [[nodiscard]] prometheus::Histogram* get_submit_to_ack_us_histogram(
        std::string_view strategy, std::string_view book,
        std::string_view venue, std::string_view symbol);
    [[nodiscard]] prometheus::Histogram* get_submit_to_fill_us_histogram(
        std::string_view strategy, std::string_view book,
        std::string_view venue, std::string_view symbol);
    [[nodiscard]] prometheus::Histogram* get_submit_to_ack_paired_us_histogram(
        std::string_view strategy, std::string_view book,
        std::string_view venue, std::string_view symbol);
    [[nodiscard]] prometheus::Histogram* get_strategy_us_histogram(
        std::string_view strategy, std::string_view book,
        std::string_view venue, std::string_view symbol);
    [[nodiscard]] prometheus::Histogram* get_local_submit_us_histogram(
        std::string_view strategy, std::string_view book,
        std::string_view venue, std::string_view symbol);

    /// @brief 解析 xt_latency_eligible_total{stage,strategy,book,symbol} 的 Counter*。
    ///
    /// 仅 XtLiveLatencyTracker note_ack_eligible 冷路径缓存 Counter*（避免每单重构 Labels），
    /// 与 histogram getter 同模式；等价 inc_latency_eligible_total 但暴露句柄供缓存。
    [[nodiscard]] prometheus::Counter* get_latency_eligible_counter(
        std::string_view stage, std::string_view strategy,
        std::string_view book, std::string_view symbol);

private:
    XtMetricRegistry();
    ~XtMetricRegistry();

    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace ct::metrics
