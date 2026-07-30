#pragma once

/// @file
/// @brief 控制环各实现文件之间传递的共享状态结构。
///
/// frame-loop（HftRunnerControl.cpp）每轮采样填好这些 struct，
/// 报表输出侧（XtHftControlReportEmit.cpp）消费。

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "xt_live/hft/control/HftRunnerControl.h"
#include "XtLiveOrderGateway.h"
#include "api/ITradeGateway.h"
#include "latency/XtLiveLatencyTracker.h"
#include "xt_live/control/AdvanceRiskFrame.h"
#include "xt_live/hft/control/HftUnmatchedBudget.h"

namespace xt_live::control::detail {

/// @brief 每轮控制环采样的完整 runtime 快照（事件循环 / 风控 / 延迟 / 一致性）。
/// 生命周期仅限单轮循环，下轮重新采样。
struct ControlSnapshot {
    uint64_t tick_lag_p50_us = 0;
    uint64_t tick_lag_p99_us = 0;
    uint64_t tick_lag_peak_us = 0;
    uint64_t td_flow_lag_p99_us = 0;
    uint64_t td_event_backlog = 0;
    uint64_t td_event_peak_backlog = 0;
    uint64_t td_flow_event_lag_us_max = 0;
    uint64_t td_flow_event_rejected = 0;
    uint64_t trade_count = 0;
    XtLiveOrderGateway::RiskStatsSnapshot risk_stats;
    ITradeGateway::QueryControlStats td_query_stats;
    ConsistencyReport consistency;
    XtLiveLatencyTracker::Snapshot latency_stats;
    double cash = 0.0;
    double equity = 0.0;
    double unrealized = 0.0;
    uint64_t md_in = 0;
    uint64_t enq = 0;
    uint64_t proc = 0;
    uint64_t tick_drop = 0;
    uint64_t td_event_drop = 0;
    uint64_t flow_event_drop = 0;
    uint64_t critical_td_event_drop = 0;
    bool ready = false;
    bool md_ready = false;
    bool td_ready = false;
    uint32_t last_tick_date = 0;
    uint32_t last_tick_time = 0;
    uint64_t param_ver = 0;
    size_t param_count = 0;
    bool log_stats_enabled = false;
    std::vector<XtOrderExecUpdate> recent_order_updates;
    std::vector<XtTradeRecord> recent_trades;
};

/// @brief 延迟 SLA 阈值配置：值为 0 表示该维度不检查。
struct LatencySlaConfig {
    uint64_t ack_sla_p99_us = 0;
    uint64_t fill_sla_p99_us = 0;
    uint64_t ack_fill_sla_p99_us = 0;
    int64_t warn_throttle_ms = 5000;
};

/// @brief 运行时可热更新的控制参数快照（ParamDomain > config > 默认值）。
struct ControlParamSnapshot {
    uint64_t param_ver = 0;
    size_t param_count = 0;
    bool log_stats_enabled = false;
    bool latency_enabled = true;
    uint64_t latency_stale_ttl_us = 60000000ULL;
    LatencySlaConfig latency_sla;
    int strategy_metrics_flush_sec = 5;
};

/// @brief 控制环跨轮次累积状态。生命周期 = run() 整个执行期间。
/// 用于去重 incident、节流日志、跟踪连续失败 streak。
struct ControlLoopState {
    int64_t last_heartbeat_ms = 0;
    uint64_t latency_sla_breach_total = 0;
    uint64_t critical_td_drop_seen = 0;
    uint64_t consistency_fail_total = 0;
    uint64_t consistency_fail_streak = 0;
    bool consistency_blocked = false;
    int64_t last_latency_sla_warn_ms = 0;
    int64_t last_strategy_metrics_flush_sec = 0;
    uint64_t closeout_fail_streak = 0;
    bool     closeout_blocked = false;
    bool     autolatch_release_checked = false;
    int64_t  closeout_last_try_ms = 0;
    int64_t  closeout_force_active_since_ms = 0;
    bool     reporter_baseline_set = false;
    int64_t  last_per_symbol_snap_sec = 0;
    int64_t  last_lock_warn_sec = 0;
    int64_t  last_closeout_miss_warn_sec = 0;
    int64_t  last_account_truth_snap_sec = 0;
    int64_t  last_account_truth_day = 0;
    int      last_baseline_day_id = 0;
    int64_t  last_strategy_id_prune_ms = 0;
    int64_t  last_orphan_reap_push_ms = 0;
    uint64_t last_orphan_reap_prom_total = 0;
    int64_t  last_metric_wire_ms = 0;
    uint64_t last_md_ticks_prom_total = 0;
    uint64_t last_risk_rejects_prom_total = 0;
    uint64_t last_td_position_snapshot_drop_prom_total = 0;
    uint64_t last_td_control_event_drop_prom_total = 0;
    uint64_t last_td_other_event_drop_prom_total = 0;
    uint64_t last_worker_guard_body_prom_total = 0;
    uint64_t last_worker_guard_normalize_prom_total = 0;
    uint64_t last_worker_guard_record_prom_total = 0;
    uint64_t last_dropped_fill_empty_prom_total = 0;
    uint64_t last_dropped_fill_unknown_prom_total = 0;
    uint64_t last_dropped_order_empty_prom_total = 0;
    uint64_t last_dropped_order_unknown_prom_total = 0;
    uint64_t last_sid_fallback_prom_total = 0;
    uint64_t last_lost_terminal_prom_total = 0;

    xt_live::control::UnmatchedBudgetWindow unmatched_budget;
    uint64_t kpi_window_tick_lag_us_peak = 0;

    uint64_t kpi_window_tick_lag_us_p99 = 0;

    std::vector<std::string> last_truth_symbols;

    xt_live::control::RiskFrameState risk_frame;
};

/// @brief 单轮控制帧；同一帧内复用同一个 now/snapshot 口径。
struct LoopFrame {
    ControlParamSnapshot params;
    ControlSnapshot snapshot;
    int64_t now_ms = 0;
};

}
