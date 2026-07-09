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

    static constexpr uint8_t kKillSwitchSourceNone = 0;
    static constexpr uint8_t kKillSwitchSourceAutoCloseoutBlock = 1;
    static constexpr uint8_t kKillSwitchSourceReconcileDivergence = 2;
    static constexpr uint8_t kKillSwitchSourceUnmatchedBudget = 3;

    std::atomic<bool> md_ready{false};
    std::atomic<bool> td_ready{false};

    std::atomic<bool> trading_ready{false};

    std::atomic<bool> td_position_ready{false};

    std::atomic<bool> position_snapshot_applied{false};

    std::atomic<uint32_t> td_session_gen{0};

    std::atomic<bool> in_trading_section{true};

    std::atomic<bool> closeout_no_open_window{false};

    std::atomic<bool> kill_switch{false};

    std::atomic<uint8_t> kill_switch_source{kKillSwitchSourceNone};

    std::atomic<bool> reconcile_reset_request{false};

    std::atomic<uint64_t> md_in_cnt{0};
    std::atomic<uint64_t> enq_cnt{0};
    std::atomic<uint64_t> drop_cnt{0};
    std::atomic<uint64_t> td_event_drop_cnt{0};
    std::atomic<uint64_t> flow_event_drop_cnt{0};
    std::atomic<uint64_t> critical_td_event_drop_cnt{0};

    std::atomic<uint64_t> td_drop_position_cnt{0};
    std::atomic<uint64_t> td_drop_control_cnt{0};
    std::atomic<uint64_t> td_drop_other_cnt{0};
    std::atomic<uint64_t> tick_proc_cnt{0};
    std::atomic<uint64_t> trade_fill_cnt{0};

    std::atomic<uint64_t> multi_strategy_dropped_fill_cnt{0};
    std::atomic<uint64_t> multi_strategy_dropped_order_update_cnt{0};
    std::atomic<uint64_t> multi_strategy_dropped_margin_reject_cnt{0};

    std::atomic<uint64_t> multi_strategy_dropped_fill_empty_sid_cnt{0};
    std::atomic<uint64_t> multi_strategy_dropped_fill_unknown_sid_cnt{0};
    std::atomic<uint64_t> multi_strategy_dropped_order_update_empty_sid_cnt{0};
    std::atomic<uint64_t> multi_strategy_dropped_order_update_unknown_sid_cnt{0};
    std::atomic<uint64_t> engine_exec_dispatch_cnt{0};
    std::atomic<uint64_t> engine_sid_fallback_recovered_cnt{0};
    std::atomic<uint64_t> engine_lost_terminal_reconciled_cnt{0};

    std::atomic<uint32_t> last_tick_date{0};
    std::atomic<uint32_t> last_tick_time{0};
    std::atomic<uint32_t> last_tick_action_time{0};

    std::atomic<uint32_t> last_tick_steady_ms{0};

    std::atomic<bool>     has_account_snapshot{false};
    std::atomic<int64_t>  latest_close_profit_fp{0};
    std::atomic<int64_t>  latest_balance_fp{0};
    std::atomic<int64_t>  latest_external_cashflow_today_fp{0};
    std::atomic<int64_t>  latest_external_cashflow_trading_day{0};
    std::atomic<uint64_t> latest_account_snapshot_ms{0};

    std::atomic<uint64_t> last_position_snapshot_ms{0};

    void refresh_trading_ready() {
        const bool ready = md_ready.load(std::memory_order_acquire) &&
                           td_ready.load(std::memory_order_acquire);
        trading_ready.store(ready, std::memory_order_release);
    }
};
