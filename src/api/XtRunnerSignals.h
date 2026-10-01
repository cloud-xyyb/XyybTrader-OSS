#pragma once

#include <atomic>
#include <cstdint>

/// @brief Runner 持有的跨线程信号结构（shared signals board）。
///
/// HFT / CTA / SEL Runner 各持有一个实例，并将字段地址或引用注入 callback
/// bridge、EventLoop、OrderGateway、StrategyContext、HftRunnerControl 与 reporting 路径。
///
/// 不提供跨字段一致 snapshot；组合 gate 必须由调用方在使用点重新校验。
/// gate / latch 字段按读写点使用 release/acquire；计数器仅用于 telemetry，可 relaxed。
struct XtRunnerSignals {
    // kill_switch_source: 标记 kill_switch 当前 latch 的来源，决定冷启是否可自动释放。
    //   None             = 手动 / 缺省来源 / 已清除 —— 冷启绝不自动释放（保守，不误清运维手动停机）。
    //   AutoCloseoutBlock = 收盘强平 block 自动 trip —— 账户 flat 时冷启可自动释放。
    //   ReconcileDivergence = 持续对账分歧自动 trip —— 重启后首轮重新对账一致时由 reconcile loop 自清。
    //   UnmatchedBudget  = unmatched exec-event drop 预算超限自动 trip（归因丢失）——
    //                      账户 flat 时冷启可自动释放（同 AutoCloseoutBlock allow-set；
    //                      非 flat 保留，需人工 resume）。
    static constexpr uint8_t kKillSwitchSourceNone = 0;
    static constexpr uint8_t kKillSwitchSourceAutoCloseoutBlock = 1;
    static constexpr uint8_t kKillSwitchSourceReconcileDivergence = 2;
    static constexpr uint8_t kKillSwitchSourceUnmatchedBudget = 3;

    // 通道就绪与交易 gate。
    std::atomic<bool> md_ready{false};   ///< MD channel ready；front lost / stop 发布 false。
    std::atomic<bool> td_ready{false};   ///< TD channel ready；不包含 position baseline 语义。
    /// 共享交易就绪 gate；false 时策略 submit / 网关 submit 均应拒绝。
    /// HFT = md_ready && td_ready；CTA/SEL = channels.is_trading_ready()。
    std::atomic<bool> trading_ready{false};
    /// TD session 持仓基线可用标志；false 时开仓按 position-stale 拒单。
    /// HFT/CTA/SEL 均在快照完成 account_store hydration 后置位（诚实语义：已 applied，非"消息已到达"）。
    std::atomic<bool> td_position_ready{false};
    /// HFT position snapshot 已被 worker 灌入 account_store 的 latch。
    /// observer（worker 线程）在 hydration 成功后置位；与 td_ready 配对置位 td_position_ready，
    /// 处理 snapshot/ready 乱序。on_td_front 断连 / stop 清零。CTA/SEL 用各自 channels-instance flag。
    std::atomic<bool> position_snapshot_applied{false};
    /// HFT TD session generation（gen fence）。SDK 线程在 on_td_front 断连时 fetch_add(release)。
    /// PositionSnapshot 事件在 enqueue 时捕获当前 gen；observer 仅对 `snapshot_gen == 当前 gen`
    /// 置 readiness latch，旧 gen 的 stale snapshot 跳过 —— 即便断连的 ChannelLost 物理满被丢，
    /// 旧 session baseline 也不会被带进新 session（防 ChannelLost-drop fail-open）。
    /// 承重不变量 = gen++ happens-before 任何断连后 publish（equality-compare + 单调递增，不要求
    /// 严格同线程；CTP 正常路径走单 SPI 程序序；off-SPI stale_rebuild 存在有界并发 publish 窗口 ——
    /// on_front 先于 api_->Release()，旧 api snapshot 可携新 gen，详 XtHftChannels::on_td_front 注释；
    /// observer 侧 validate-after-set 再收窄）。binance 不发 on_front → gen 恒 0（inert 无害）。
    std::atomic<uint32_t> td_session_gen{0};
    /// active_sections gate；默认 true 表示未配置 session gate 时不限制策略提交。
    /// 由 HFT/CTA EventLoop 按 tick.action_time 更新（xt_live::update_session_gate_flags），
    /// StrategyContext::can_submit() 读取。
    std::atomic<bool> in_trading_section{true};
    /// closeout no-open gate；true 时 StrategyContext::can_submit() 拒绝开仓。
    /// 由 HFT/CTA EventLoop 按 tick.action_time 与 closeout_windows（闭区间）更新。
    std::atomic<bool> closeout_no_open_window{false};

    // 运行控制与恢复信号。
    /// 全局 emergency latch；true 时新订单提交被拒，且不随 MD/TD ready 自动恢复。
    std::atomic<bool> kill_switch{false};
    /// kill_switch 当前 latch 来源（见 kKillSwitchSource* 常量）；仅 kill_switch=true 时有效。
    /// manual SetKillSwitch trip/resume 置 None；自动 trip 由各自链路置对应 source。
    /// 冷启 flat 自动释放只认 allow-set（AutoCloseoutBlock / UnmatchedBudget），避免误清手动停机。
    std::atomic<uint8_t> kill_switch_source{kKillSwitchSourceNone};

    /// RESUME -> reconcile 的 one-shot 请求；XtHftReconcileLoop 用 exchange(false) 消费。
    /// true 表示需要清空 divergence cycle，避免残留计数再次触发 kill_switch。
    std::atomic<bool> reconcile_reset_request{false};

    // MD/TD 事件流计数；仅用于 telemetry，不作为业务同步屏障。
    std::atomic<uint64_t> md_in_cnt{0};    ///< MD tick 回调总数。
    std::atomic<uint64_t> enq_cnt{0};      ///< 成功入队的 tick 数。
    std::atomic<uint64_t> drop_cnt{0};     ///< tick 队列满时丢弃数。
    std::atomic<uint64_t> td_event_drop_cnt{0};        ///< TD/control 事件丢弃总数。
    std::atomic<uint64_t> flow_event_drop_cnt{0};      ///< L2 flow 事件丢弃数。
    std::atomic<uint64_t> critical_td_event_drop_cnt{0}; ///< trade/order 等关键 TD 事件丢弃数。

    // TD 丢弃原因拆分（3 桶）；aggregate td_event_drop_cnt / critical_td_event_drop_cnt 是总量口径。
    // position/control 是 must_not_drop 事件（仅物理满才丢，各有 delta-emit + critical alert）；
    // 其余事件归入 other（delta-emit + warning alert，含软水位 shedding），critical 维度由
    // critical_td_event_drop_cnt 聚合覆盖（trade 等事件 is_critical）。
    std::atomic<uint64_t> td_drop_position_cnt{0};  ///< 全量持仓快照 → xt_td_position_snapshot_drop_total。
    std::atomic<uint64_t> td_drop_control_cnt{0};   ///< ChannelReady/Lost + ReverseSyncBatch → xt_td_control_event_drop_total。
    std::atomic<uint64_t> td_drop_other_cnt{0};     ///< 其余 exec/account 事件 → xt_td_other_event_drop_total。
    std::atomic<uint64_t> tick_proc_cnt{0};              ///< EventLoop 已消费 tick 数。
    std::atomic<uint64_t> trade_fill_cnt{0};             ///< live observer 接收的成交回报数。

    // Multi-strategy 异常计数；HftRunnerControl 每帧从 engine/gateway 复制后统一上报。
    std::atomic<uint64_t> multi_strategy_dropped_fill_cnt{0};
    std::atomic<uint64_t> multi_strategy_dropped_order_update_cnt{0};
    std::atomic<uint64_t> multi_strategy_dropped_margin_reject_cnt{0};
    // reason 拆分（xt_dropped_total{kind,reason} emit 源）+ unmatched budget 分母 + 恢复计数
    //（xt_engine_recovery_total{kind} emit 源）；同上，1Hz 从 engine 复制。
    std::atomic<uint64_t> multi_strategy_dropped_fill_empty_sid_cnt{0};
    std::atomic<uint64_t> multi_strategy_dropped_fill_unknown_sid_cnt{0};
    std::atomic<uint64_t> multi_strategy_dropped_order_update_empty_sid_cnt{0};
    std::atomic<uint64_t> multi_strategy_dropped_order_update_unknown_sid_cnt{0};
    std::atomic<uint64_t> engine_exec_dispatch_cnt{0};        ///< fill+order update 派发尝试总数（budget 分母）。
    std::atomic<uint64_t> engine_sid_fallback_recovered_cnt{0};   ///< sid fallback 恢复归因次数。
    std::atomic<uint64_t> engine_lost_terminal_reconciled_cnt{0}; ///< venue-terminal/book-open 合成终态修复数。

    // 最新行情时间；0 表示尚未收到有效 tick 值。
    std::atomic<uint32_t> last_tick_date{0};        ///< action_date，格式 YYYYMMDD。
    std::atomic<uint32_t> last_tick_time{0};        ///< action_time，格式 HHMMSSmmm。
    std::atomic<uint32_t> last_tick_action_time{0}; ///< 控制面心跳使用的最近 action_time。
    /// 最近 tick 的本地 monotonic ms；与 HftRunnerControl frame 的 now_ms 同钟。
    /// 用于 risk_monitor stale_quote 判定；uint32 wrap 约 49.7 天。
    std::atomic<uint32_t> last_tick_steady_ms{0};

    // 账户快照缓存；写入方先写具体字段，最后发布 has_account_snapshot=true。
    std::atomic<bool>     has_account_snapshot{false}; ///< 已收到至少一笔 venue account snapshot。
    std::atomic<int64_t>  latest_close_profit_fp{0};   ///< venue 累计平仓盈亏，fixed-point x1e6。
    std::atomic<int64_t>  latest_balance_fp{0};        ///< venue balance/equity，fixed-point x1e6。
    std::atomic<int64_t>  latest_external_cashflow_today_fp{0}; ///< 当日入金-出金，fixed-point x1e6。
    std::atomic<int64_t>  latest_external_cashflow_trading_day{0}; ///< cashflow 所属 trading_day；0=未知。
    std::atomic<uint64_t> latest_account_snapshot_ms{0}; ///< account snapshot 接收时刻，steady ms。
    /// position freshness 时间戳，steady ms；position snapshot、成交回报和 reconcile agree 均可刷新。
    std::atomic<uint64_t> last_position_snapshot_ms{0};

    /// @brief HFT 专用：按 `md_ready && td_ready` 刷新 trading_ready。
    /// @warning 不包含 td_position_ready；CTA/SEL 不得使用该 helper。
    void refresh_trading_ready() {
        const bool ready = md_ready.load(std::memory_order_acquire) &&
                           td_ready.load(std::memory_order_acquire);
        trading_ready.store(ready, std::memory_order_release);
    }
};
