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

#include "xt_live/hft/control/HftRunnerControl.h"        // ConsistencyReport / XtHftRunnerControlContext / XtTradeRecord / XtOrderExecUpdate
#include "XtLiveOrderGateway.h"      // RiskStatsSnapshot
#include "api/ITradeGateway.h"       // QueryControlStats
#include "latency/XtLiveLatencyTracker.h"    // XtLiveLatencyTracker::Snapshot
#include "xt_live/control/AdvanceRiskFrame.h"  // RiskFrameState（每帧风控维护跨轮状态）
#include "xt_live/hft/control/HftUnmatchedBudget.h"  // UnmatchedBudgetWindow（预算窗口基线）

namespace xt_live::control::detail {

/// @brief 每轮控制环采样的完整 runtime 快照（事件循环 / 风控 / 延迟 / 一致性）。
/// 生命周期仅限单轮循环，下轮重新采样。
struct ControlSnapshot {
    uint64_t tick_lag_p50_us = 0;   ///< tick 队列滞留窗口真 p50（直方图桶上界，保守）
    uint64_t tick_lag_p99_us = 0;   ///< tick 队列滞留窗口真 p99（直方图桶上界，保守）
    uint64_t tick_lag_peak_us = 0;  ///< tick 队列滞留窗口 observed peak
    uint64_t td_flow_lag_p99_us = 0;  ///< TD+flow 队列滞留窗口真 p99（两队列单口径）
    uint64_t td_event_backlog = 0;       ///< TD 事件在途 instant（xt_queue_depth 源）
    uint64_t td_event_peak_backlog = 0;  ///< TD 事件在途 lifetime peak（日志诊断）
    uint64_t td_flow_event_lag_us_max = 0;
    uint64_t td_flow_event_rejected = 0;  ///< 来自 XtRunnerSignals(td+flow drop)
    uint64_t trade_count = 0;             ///< 来自 XtRunnerSignals.trade_fill_cnt
    XtLiveOrderGateway::RiskStatsSnapshot risk_stats;
    ITradeGateway::QueryControlStats td_query_stats;
    ConsistencyReport consistency;
    XtLiveLatencyTracker::Snapshot latency_stats;
    double cash = 0.0;
    double equity = 0.0;
    double unrealized = 0.0;
    uint64_t md_in = 0;        ///< MD gateway 收到的 tick 总数
    uint64_t enq = 0;          ///< 入队到策略线程的 tick 数
    uint64_t proc = 0;         ///< 策略线程实际处理的 tick 数
    uint64_t tick_drop = 0;    ///< SPSC queue 满导致的 tick 丢弃
    uint64_t td_event_drop = 0;
    uint64_t flow_event_drop = 0;
    uint64_t critical_td_event_drop = 0;  ///< 关键 TD 事件丢弃（可触发停机）
    bool ready = false;
    bool md_ready = false;
    bool td_ready = false;
    uint32_t last_tick_date = 0;
    uint32_t last_tick_time = 0;
    uint64_t param_ver = 0;
    size_t param_count = 0;
    bool log_stats_enabled = false;
    std::vector<XtOrderExecUpdate> recent_order_updates;  ///< 本轮 drain 出的订单更新
    std::vector<XtTradeRecord> recent_trades;             ///< 本轮 drain 出的成交记录
};

/// @brief 延迟 SLA 阈值配置：值为 0 表示该维度不检查。
struct LatencySlaConfig {
    uint64_t ack_sla_p99_us = 0;        ///< submit → ack P99
    uint64_t fill_sla_p99_us = 0;       ///< submit → fill P99
    uint64_t ack_fill_sla_p99_us = 0;   ///< ack → fill P99
    int64_t warn_throttle_ms = 5000;
};

/// @brief 运行时可热更新的控制参数快照（ParamDomain > config > 默认值）。
struct ControlParamSnapshot {
    uint64_t param_ver = 0;
    size_t param_count = 0;
    bool log_stats_enabled = false;
    bool latency_enabled = true;
    uint64_t latency_stale_ttl_us = 60000000ULL;  ///< 60s，超过此时长的 inflight 订单视为过期
    LatencySlaConfig latency_sla;
    int strategy_metrics_flush_sec = 5;
};

/// @brief 控制环跨轮次累积状态。生命周期 = run() 整个执行期间。
/// 用于去重 incident、节流日志、跟踪连续失败 streak。
struct ControlLoopState {
    int64_t last_heartbeat_ms = 0;
    uint64_t latency_sla_breach_total = 0;
    uint64_t critical_td_drop_seen = 0;            ///< 已处理的 critical drop 计数（增量检测用）
    uint64_t consistency_fail_total = 0;           ///< 总失败次数（跨 streak 累积）
    uint64_t consistency_fail_streak = 0;          ///< 连续失败次数（恢复后归零）
    bool consistency_blocked = false;              ///< 是否已进入 trading block 状态
    int64_t last_latency_sla_warn_ms = 0;
    int64_t last_strategy_metrics_flush_sec = 0;
    uint64_t closeout_fail_streak = 0;           ///< 强平连续被阻塞轮数（恢复/执行后归零）
    bool     closeout_blocked = false;           ///< 是否已因强平失败进入 trading block
    bool     autolatch_release_checked = false;  ///< 冷启自动释放判定 one-shot（hydrate 后判一次）
    int64_t  closeout_last_try_ms = 0;           ///< 上次强平尝试时间 ms（节流去重）
    int64_t  closeout_force_active_since_ms = 0; ///< 强平首次见持仓 steady ms（flat/出窗口归零）；passive→aggressive 升级计时基准
    bool     reporter_baseline_set = false;      ///< reporter 的 start_equity 是否已 set
    int64_t  last_per_symbol_snap_sec = 0;       ///< 上次 per-symbol snapshot 的 ts_sec, 节流到每秒边界
    int64_t  last_lock_warn_sec = 0;             ///< broker 同时持多空腿(锁仓/毛≠净)WARN 节流(秒)
    int64_t  last_closeout_miss_warn_sec = 0;    ///< closeout 边界陷阱 WARN 节流(秒)
    int64_t  last_account_truth_snap_sec = 0;    ///< 上次 account truth snapshot 的 ts_sec
    int64_t  last_account_truth_day = 0;         ///< 上次 account truth trading_day, 用于 rollover hook
    int      last_baseline_day_id = 0;           ///< per-day baseline rotation tracking
    int64_t  last_strategy_id_prune_ms = 0;      ///< order_strategy_ sidecar prune 节拍
    int64_t  last_orphan_reap_push_ms = 0;       ///< 孤儿回收 sweep 事件推送节拍
    uint64_t last_orphan_reap_prom_total = 0;    ///< xt_recovery_orphan_reaped_total 增量 emit 基线
    int64_t  last_metric_wire_ms = 0;            ///< metric wire 节拍
    uint64_t last_md_ticks_prom_total = 0;       ///< xt_md_ticks_total 增量 emit 基线
    uint64_t last_risk_rejects_prom_total = 0;   ///< xt_risk_rejects_total 增量 emit 基线
    uint64_t last_td_position_snapshot_drop_prom_total = 0; ///< xt_td_position_snapshot_drop_total 增量 emit 基线
    uint64_t last_td_control_event_drop_prom_total = 0; ///< xt_td_control_event_drop_total 增量 emit 基线
    uint64_t last_td_other_event_drop_prom_total = 0;   ///< xt_td_other_event_drop_total 增量 emit 基线
    uint64_t last_worker_guard_body_prom_total = 0;      ///< xt_hft_worker_guard_fail_total{stage=body} 基线
    uint64_t last_worker_guard_normalize_prom_total = 0; ///< xt_hft_worker_guard_fail_total{stage=normalize} 基线
    uint64_t last_worker_guard_record_prom_total = 0;    ///< xt_hft_worker_guard_fail_total{stage=record} 基线
    uint64_t last_dropped_fill_empty_prom_total = 0;      ///< xt_dropped_total{fill,empty_sid} 基线
    uint64_t last_dropped_fill_unknown_prom_total = 0;    ///< xt_dropped_total{fill,unknown_sid} 基线
    uint64_t last_dropped_order_empty_prom_total = 0;     ///< xt_dropped_total{order_rtn,empty_sid} 基线
    uint64_t last_dropped_order_unknown_prom_total = 0;   ///< xt_dropped_total{order_rtn,unknown_sid} 基线
    uint64_t last_sid_fallback_prom_total = 0;            ///< xt_engine_recovery_total{sid_fallback} 基线
    uint64_t last_lost_terminal_prom_total = 0;           ///< xt_engine_recovery_total{lost_terminal} 基线
    /// unmatched drop 预算 tumbling window 基线（trip/到期/回退 rebase）。
    xt_live::control::UnmatchedBudgetWindow unmatched_budget;
    uint64_t kpi_window_tick_lag_us_peak = 0;    ///< 与 kpi_1s.csv flush 窗口一致的 MD lag peak
    /// kpi 窗口内各 frame p99 的最大值（跨 frame 无法合并直方图，取 max = 保守上界；
    /// frame 与 kpi flush 同为 1s 时即恒等）。
    uint64_t kpi_window_tick_lag_us_p99 = 0;
    /// 上周期 per-symbol truth emit 过的 symbol；本周期消失（持仓归零被 truth
    /// source 过滤）的写 0 清 gauge 残留。
    std::vector<std::string> last_truth_symbols;
    /// 每帧风控维护（advance_risk_frame）的 integrity 节拍；随 loop state 生命周期，
    /// 多实例/重入安全。
    xt_live::control::RiskFrameState risk_frame;
};

/// @brief 单轮控制帧；同一帧内复用同一个 now/snapshot 口径。
struct LoopFrame {
    ControlParamSnapshot params;
    ControlSnapshot snapshot;
    int64_t now_ms = 0;
};

}  // namespace xt_live::control::detail
