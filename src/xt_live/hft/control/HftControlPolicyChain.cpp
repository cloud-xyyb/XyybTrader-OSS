// ── 控制环策略链 stage ──────────────────────────────────────────────────
//
// heartbeat / order_strategy sidecar prune / 孤儿回收 sweep 推送 /
// multi-strategy metric wire / 一致性升级（warn → block → kill）/
// critical TD drop 停机 / 延迟 SLA 告警。跨 TU 入口：log_control_stats /
// make_consistency_thresholds / apply_policy_chain。

#include "xt_live/hft/control/HftControlStages.h"

#include <algorithm>
#include <sstream>
#include <string>

#include "XtLogger.h"
#include "XtLiveOrderGateway.h"
#include "XtLiveOrderGatewayCfg.h"
#include "api/XtRunnerSignals.h"
#include "latency/XtLiveLatencyTracker.h"
#include "xt_live/hft/runtime/XtHftEventLoop.h"
#include "xt_live/hft/control/HftUnmatchedBudget.h"
#include "xt_util/time/XtSteadyClock.h"

namespace xt_live::control::detail {
namespace {

// 找出出现次数最多的拒单原因，用于日志摘要（"reason:count" 格式）
std::string top_reject_reason(const XtLiveOrderGateway::RiskStatsSnapshot& risk) {
    uint64_t max_count = 0;
    std::string top = "NONE";
    for (const auto& kv : risk.reject_by_reason) {
        if (kv.second > max_count) {
            max_count = kv.second;
            top = kv.first;
        }
    }
    std::ostringstream oss;
    oss << top << ":" << max_count;
    return oss.str();
}

// 按 reason 精确匹配查找拒单计数，线性扫描即可——reason 种类 < 10
uint64_t reject_count_by_reason(const XtLiveOrderGateway::RiskStatsSnapshot& risk, const char* reason) {
    if (!reason || reason[0] == '\0') return 0;
    for (const auto& kv : risk.reject_by_reason) {
        if (kv.first == reason) return kv.second;
    }
    return 0;
}

void maybe_log_heartbeat(const XtRunnerControlConfig& cfg,
                         const ControlSnapshot& s,
                         int64_t now_ms,
                         ControlLoopState& st) {
    if (cfg.heartbeat_ms <= 0) return;  // heartbeat 关闭
    if (st.last_heartbeat_ms != 0 && now_ms - st.last_heartbeat_ms < cfg.heartbeat_ms) return;
    st.last_heartbeat_ms = now_ms;
    if (cfg.heartbeat_tick_count) {
        XtLogger::info_m("RT",
                         "heartbeat md_in={} enq={} tick_drop={} td_event_drop={} flow_event_drop={} proc={} last_tick={}/{} md_ready={} td_ready={}",
                         s.md_in,
                         s.enq,
                         s.tick_drop,
                         s.td_event_drop,
                         s.flow_event_drop,
                         s.proc,
                         s.last_tick_date,
                         s.last_tick_time,
                         s.md_ready,
                         s.td_ready);
        return;
    }
    XtLogger::info_m("RT",
                     "heartbeat md_ready={} td_ready={}",
                     s.md_ready,
                     s.td_ready);
}

void maybe_wire_multi_strategy_metrics(XtHftRunnerControlContext& ctx,
                                       int64_t now_ms,
                                       ControlLoopState& st) {
    constexpr int64_t kWireIntervalMs = 1000;  // 1Hz
    if (st.last_metric_wire_ms != 0
        && now_ms - st.last_metric_wire_ms < kWireIntervalMs) {
        return;
    }
    st.last_metric_wire_ms = now_ms;
    auto& signals = ctx.runtime_shared_state;
    const auto es = ctx.strategy_event_loop.engine_unmatched_stats();
    signals.multi_strategy_dropped_fill_cnt.store(
        es.fill_empty_sid + es.fill_unknown_sid, std::memory_order_relaxed);
    signals.multi_strategy_dropped_order_update_cnt.store(
        es.order_update_empty_sid + es.order_update_unknown_sid,
        std::memory_order_relaxed);
    signals.multi_strategy_dropped_margin_reject_cnt.store(
        ctx.live_order_gateway.dropped_margin_reject_count(),
        std::memory_order_relaxed);
    signals.multi_strategy_dropped_fill_empty_sid_cnt.store(
        es.fill_empty_sid, std::memory_order_relaxed);
    signals.multi_strategy_dropped_fill_unknown_sid_cnt.store(
        es.fill_unknown_sid, std::memory_order_relaxed);
    signals.multi_strategy_dropped_order_update_empty_sid_cnt.store(
        es.order_update_empty_sid, std::memory_order_relaxed);
    signals.multi_strategy_dropped_order_update_unknown_sid_cnt.store(
        es.order_update_unknown_sid, std::memory_order_relaxed);
    signals.engine_exec_dispatch_cnt.store(es.exec_dispatch, std::memory_order_relaxed);
    signals.engine_sid_fallback_recovered_cnt.store(
        es.sid_fallback_recovered, std::memory_order_relaxed);
    signals.engine_lost_terminal_reconciled_cnt.store(
        es.lost_terminal_reconciled, std::memory_order_relaxed);
}

// unmatched drop 预算：窗口内 drop 增量越限 → trip 全局 kill_switch
//（source=UnmatchedBudget，冷启 flat 自动释放走 allow-set）。读的是上面
// 1Hz wire 进 signals 的累计值，判定/置位在 evaluate_unmatched_budget 纯函数。
void maybe_evaluate_unmatched_budget(const XtRunnerControlConfig& cfg,
                                     XtHftRunnerControlContext& ctx,
                                     int64_t now_ms,
                                     ControlLoopState& st) {
    const auto out = evaluate_unmatched_budget(
        ctx.runtime_shared_state, st.unmatched_budget, now_ms,
        cfg.unmatched_budget_window_sec, cfg.unmatched_budget_max_drops,
        cfg.unmatched_budget_max_ratio);
    if (!out.tripped) return;
    XtLogger::error_m("RT",
                      "unmatched drop budget exceeded -> kill_switch tripped "
                      "(window_drops={} window_dispatch={} window_sec={} max_drops={} max_ratio={})",
                      out.window_drops,
                      out.window_dispatch,
                      cfg.unmatched_budget_window_sec,
                      cfg.unmatched_budget_max_drops,
                      cfg.unmatched_budget_max_ratio);
    emit_kill_switch_transition(ctx, cfg, /*active=*/true, "system:unmatched-budget",
                                "unmatched_drop_budget_exceeded");
}

void maybe_prune_strategy_id_sidecar(XtHftRunnerControlContext& ctx,
                                     int64_t now_ms,
                                     ControlLoopState& st) {
    constexpr int64_t kPruneIntervalMs = 60'000;  // 60s frame loop tick

    // grace window 从 gateway cfg 取 (yaml runtime.gateway.order_strategy_grace_us)
    // 默认 60s,运维可在 monitoring dropped_fill metric 后调高 (e.g. 5-10 min)
    // 应对长 binance ws outage + 重连后 stale terminal rtn 反查 sid 场景。
    const int64_t grace_us = ctx.live_order_gateway.order_strategy_grace_us();

    if (st.last_strategy_id_prune_ms != 0
        && now_ms - st.last_strategy_id_prune_ms < kPruneIntervalMs) {
        return;
    }
    st.last_strategy_id_prune_ms = now_ms;
    const int64_t now_us = static_cast<int64_t>(XtSteadyClock::steady_now_us());
    const int64_t cutoff_us = now_us - grace_us;
    const std::size_t removed =
        ctx.live_order_gateway.prune_terminal_strategy_ids(cutoff_us);
    if (removed > 0) {
        XtLogger::info_m("RT",
                          "F8 prune order_strategy_ sidecar: removed={} cutoff_grace={}s",
                          removed, grace_us / 1'000'000);
    }
}

// 周期把孤儿回收 / lost-terminal 对账 sweep 事件推给 worker（book 单写者，扫描/撤单/合成终态
// 必须在 worker 线程）。orphan_reap（撤 recovered-unowned 在途单）与 lost_terminal_reconcile
// （book-open × venue-terminal 的 owned 单合成终态）两个能力各自 enable、互不牵连（HFT-D6 解耦）：
// 任一开启即按 sweep 节拍推送，worker 侧按事件携带的 flag 分别执行。入队失败（高水位/停机）
// 不重试，下一节拍再推；sweep 本身无孤儿时是 O(在途单数) 扫描。
void maybe_push_orphan_reap(XtHftRunnerControlContext& ctx,
                            int64_t now_ms,
                            ControlLoopState& st) {
    const XtLiveOrderGatewayCfg* gw_cfg = ctx.order_gateway_cfg;
    if (!gw_cfg) return;
    if (!gw_cfg->orphan_reap_enabled && !gw_cfg->lost_terminal_reconcile_enabled) return;
    if (st.last_orphan_reap_push_ms != 0
        && now_ms - st.last_orphan_reap_push_ms < gw_cfg->orphan_reap_interval_ms) {
        return;
    }
    st.last_orphan_reap_push_ms = now_ms;
    (void)ctx.strategy_event_loop.push_td_event(
        XtHftRunnerEvent::make_reap_recovered_orphans(
            now_ms, gw_cfg->orphan_reap_grace_ms,
            static_cast<uint32_t>(gw_cfg->orphan_reap_max_per_sweep),
            gw_cfg->lost_terminal_reconcile_enabled,
            gw_cfg->orphan_reap_enabled));
}

// ---------------------------------------------------------------------------
// 一致性策略状态机：正常 → warn → block → kill
// ---------------------------------------------------------------------------
// 三级升级：连续失败 streak 达到 warn 阈值时开始告警，
// 达到 block 阈值时禁止下单（trading_ready=false），
// 达到 kill 阈值时触发停机。恢复后自动降级并重新允许交易。
void process_consistency_policy(std::atomic<bool>& shutdown,
                                uint64_t warn_loops,
                                uint64_t block_loops,
                                uint64_t kill_loops,
                                const ControlSnapshot& s,
                                XtRunnerSignals& shared_state,
                                ControlLoopState& st) {
    // --- 正常分支：一致性检查通过 ---
    if (s.consistency.ok) {
        if (st.consistency_blocked) {
            // 从 block 状态恢复：重新评估 trading_ready（可能还有其他条件阻止交易）
            st.consistency_blocked = false;
            shared_state.refresh_trading_ready();
            XtLogger::warn_m("RT",
                             "runtime consistency recovered -> release trading block "
                             "(streak={} total={})",
                             st.consistency_fail_streak,
                             st.consistency_fail_total);
        }
        st.consistency_fail_streak = 0;
        return;
    }

    // --- 失败分支：streak 递增，按阈值升级 ---
    ++st.consistency_fail_total;
    ++st.consistency_fail_streak;
    // 首次失败立即告警，后续按 warn_loops 周期告警，避免刷屏
    if (st.consistency_fail_streak == 1 || (st.consistency_fail_streak % warn_loops) == 0) {
        XtLogger::error_m("RT",
                          "runtime consistency failed reason={} frozen_cash={} tracked={} "
                          "open_orders={} streak={} total={} thresholds(w/b/k)={}/{}/{}",
                          s.consistency.reason,
                          s.consistency.frozen_cash,
                          s.consistency.frozen_cash_by_orders,
                          s.consistency.open_order_count,
                          st.consistency_fail_streak,
                          st.consistency_fail_total,
                          warn_loops,
                          block_loops,
                          kill_loops);
    }

    // 升级到 block：禁止新订单提交，但不停机，等待自行恢复
    if (st.consistency_fail_streak >= block_loops) {
        shared_state.trading_ready.store(false, std::memory_order_release);
        if (!st.consistency_blocked) {
            st.consistency_blocked = true;
            XtLogger::error_m("RT",
                              "runtime consistency block threshold reached -> trading_ready=false "
                              "(streak={} total={})",
                              st.consistency_fail_streak,
                              st.consistency_fail_total);
        }
    }

    // 升级到 kill：一致性长时间不恢复，触发进程停机以防状态持续漂移
    if (st.consistency_fail_streak >= kill_loops) {
        XtLogger::error_m("RT",
                          "runtime consistency kill threshold reached -> request shutdown "
                          "(streak={} total={})",
                          st.consistency_fail_streak,
                          st.consistency_fail_total);
        shutdown.store(true, std::memory_order_release);
    }
}

// 检查三个延迟维度是否超过各自的 P99 SLA 阈值。
// samples > 0 条件防止样本不足时误报（如刚启动尚无订单）。
uint64_t count_latency_sla_breaches(const XtLiveLatencyTracker::Snapshot& latency,
                                    const LatencySlaConfig& cfg) {
    uint64_t breaches = 0;
    if (cfg.ack_sla_p99_us > 0 &&
        latency.submit_to_ack.samples > 0 &&
        latency.submit_to_ack.p99_us > cfg.ack_sla_p99_us) {
        ++breaches;
    }
    if (cfg.fill_sla_p99_us > 0 &&
        latency.submit_to_fill.samples > 0 &&
        latency.submit_to_fill.p99_us > cfg.fill_sla_p99_us) {
        ++breaches;
    }
    if (cfg.ack_fill_sla_p99_us > 0 &&
        latency.ack_to_fill.samples > 0 &&
        latency.ack_to_fill.p99_us > cfg.ack_fill_sla_p99_us) {
        ++breaches;
    }
    return breaches;
}

// SLA breach 处理：只告警不停机（延迟超标不等于功能故障）。
// 节流逻辑：短时间内多次 breach 只输出一条日志。
void process_latency_sla(const XtLiveLatencyTracker::Snapshot& latency,
                         const LatencySlaConfig& cfg,
                         int64_t now_ms,
                         ControlLoopState& st) {
    const uint64_t breaches_now = count_latency_sla_breaches(latency, cfg);
    if (breaches_now == 0) return;

    st.latency_sla_breach_total += breaches_now;
    // 节流：同一 warn_throttle_ms 窗口内只输出一次
    if (cfg.warn_throttle_ms != 0 &&
        st.last_latency_sla_warn_ms != 0 &&
        now_ms - st.last_latency_sla_warn_ms < cfg.warn_throttle_ms) {
        return;
    }
    st.last_latency_sla_warn_ms = now_ms;
    XtLogger::warn_m("RT",
                     "latency SLA breach ack_p99_us={}/{} fill_p99_us={}/{} ack_fill_p99_us={}/{} total={}",
                     latency.submit_to_ack.p99_us,
                     cfg.ack_sla_p99_us,
                     latency.submit_to_fill.p99_us,
                     cfg.fill_sla_p99_us,
                     latency.ack_to_fill.p99_us,
                     cfg.ack_fill_sla_p99_us,
                     st.latency_sla_breach_total);
}

// 关键 TD 事件丢弃 → 立即停机。
// 理由：TD 事件包含成交回报/撤单确认，丢弃意味着 runtime 状态与 broker 不一致，
// 继续交易可能导致重复下单或持仓漂移。增量检测避免重复触发。
void maybe_apply_failure_policy(std::atomic<bool>& shutdown,
                                const XtRunnerControlConfig& cfg,
                                const ControlSnapshot& s,
                                ControlLoopState& st) {
    if (!cfg.fail_on_critical_td_drop) return;
    if (s.critical_td_event_drop <= st.critical_td_drop_seen) return;  // 无新增丢弃
    const uint64_t delta = s.critical_td_event_drop - st.critical_td_drop_seen;
    st.critical_td_drop_seen = s.critical_td_event_drop;
    XtLogger::error_m("RT",
                      "critical td event queue drop delta={} total={} -> request shutdown",
                      delta,
                      s.critical_td_event_drop);
    shutdown.store(true, std::memory_order_release);
}

} // namespace

void log_control_stats(const ControlSnapshot& s, uint64_t latency_sla_breach_total) {
    if (!s.log_stats_enabled) return;
    const uint64_t blocked_not_ready = reject_count_by_reason(s.risk_stats, "TRADING_NOT_READY");
    const uint64_t blocked_recovering = reject_count_by_reason(s.risk_stats, "TD_RECOVERING");
    XtLogger::info_m("RT",
                     "tick md_in={} enq={} proc={} drop={} tick_lag_p50/p99/peak_us={}/{}/{} td_flow_lag_p99_us={} td_peak={} event_reject={} lag_max_us={} submit_ok={}/{} trades={} reject_top={} blocked_not_ready={} blocked_recovering={} query_pos(throttle/skip/merge)={}/{}/{} lat_ack_p99/p999_us={}/{} lat_fill_p99/p999_us={}/{} lat_ack_fill_p99/p999_us={}/{} lat_cb_strategy_p99/p999_us={}/{} lat_inflight={} lat_sla_breach_total={} param_ver={}",
                     s.md_in,
                     s.enq,
                     s.proc,
                     s.tick_drop,
                     s.tick_lag_p50_us,
                     s.tick_lag_p99_us,
                     s.tick_lag_peak_us,
                     s.td_flow_lag_p99_us,
                     s.td_event_peak_backlog,
                     s.td_flow_event_rejected,
                     s.td_flow_event_lag_us_max,
                     s.risk_stats.submit_success,
                     s.risk_stats.submit_attempts,
                     s.trade_count,
                     top_reject_reason(s.risk_stats),
                     blocked_not_ready,
                     blocked_recovering,
                     s.td_query_stats.query_pos_throttled,
                     s.td_query_stats.query_pos_inflight_skip,
                     s.td_query_stats.query_pos_dirty_merged,
                     s.latency_stats.submit_to_ack.p99_us,
                     s.latency_stats.submit_to_ack.p999_us,
                     s.latency_stats.submit_to_fill.p99_us,
                     s.latency_stats.submit_to_fill.p999_us,
                     s.latency_stats.ack_to_fill.p99_us,
                     s.latency_stats.ack_to_fill.p999_us,
                     s.latency_stats.cb_to_strategy.p99_us,
                     s.latency_stats.cb_to_strategy.p999_us,
                     s.latency_stats.inflight_orders,
                     latency_sla_breach_total,
                     s.param_ver);
}

ConsistencyThresholds make_consistency_thresholds(const XtRunnerControlConfig& cfg) {
    ConsistencyThresholds out;
    out.warn_loops = static_cast<uint64_t>(std::max(1, cfg.consistency_warn_loops));
    out.block_loops = static_cast<uint64_t>(std::max(std::max(1, cfg.consistency_warn_loops),
                                                     cfg.consistency_block_loops));
    out.kill_loops = static_cast<uint64_t>(std::max(
        std::max(std::max(1, cfg.consistency_warn_loops), cfg.consistency_block_loops),
        cfg.consistency_kill_loops));
    return out;
}

bool apply_policy_chain(std::atomic<bool>& shutdown,
                        const XtRunnerControlConfig& cfg,
                        XtHftRunnerControlContext& ctx,
                        const ConsistencyThresholds& thresholds,
                        const LoopFrame& frame,
                        ControlLoopState& st) {
    maybe_log_heartbeat(cfg, frame.snapshot, frame.now_ms, st);
    // 60s 节拍 prune order_strategy_ sidecar。冷路径 ~µs 开销，放在 heartbeat
    // 和 consistency 检查之间。
    maybe_prune_strategy_id_sidecar(ctx, frame.now_ms, st);
    // 30s 节拍推孤儿回收 sweep 给 worker（recovered-unowned 在途单撤单）。
    maybe_push_orphan_reap(ctx, frame.now_ms, st);
    // 1Hz 节拍 wire engine 内 atomic counter 到 shared_state,让 control panel
    // / Prometheus 可见。冷路径,若干 relaxed atomic store。
    maybe_wire_multi_strategy_metrics(ctx, frame.now_ms, st);
    // unmatched drop 预算判定（读上一行 wire 进 signals 的累计值，每帧评估）。
    maybe_evaluate_unmatched_budget(cfg, ctx, frame.now_ms, st);
    process_consistency_policy(
        shutdown,
        thresholds.warn_loops,
        thresholds.block_loops,
        thresholds.kill_loops,
        frame.snapshot,
        ctx.runtime_shared_state,
        st);
    if (shutdown.load(std::memory_order_acquire)) return false;

    maybe_apply_failure_policy(shutdown, cfg, frame.snapshot, st);
    if (!shutdown.load(std::memory_order_acquire)) {
        process_latency_sla(frame.snapshot.latency_stats, frame.params.latency_sla, frame.now_ms, st);
    }
    return !shutdown.load(std::memory_order_acquire);
}

}  // namespace xt_live::control::detail
