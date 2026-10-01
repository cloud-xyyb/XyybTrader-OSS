#include "xt_live/hft/control/HftRunnerControl.h"

// Operator transport 由 gRPC mTLS binder 持有；本 TU 只维护 execution-side frame-loop
// 的 stage 编排；各 stage 实现见同目录 HftControl*.cpp（入口声明 HftControlStages.h）。
#include "xt_util/runtime/SdNotify.h"  // systemd Type=notify (READY=1 / WATCHDOG=1 / STOPPING=1)
#include "xt_util/runtime/XtShutdownSignal.h"  // shutdown-aware sleep

#include <chrono>

#include "XtLiveOrderGateway.h"
#include "xt_report/live/XtLiveReporter.h"
#include "xt_report/core/XtReportTypes.h"
#include "api/ITradeGateway.h"
#include "api/XtRunnerSignals.h"
#include "xt_core/order/XtOrderTypes.h"  // XtOrderExecUpdate（ControlSnapshot 复制需完整类型）
#include "xt_risk/state/SafetyStateStore.h"  // safety state persistence
#include "xt_util/time/XtSteadyClock.h"

#include "xt_live/hft/control/HftControlStages.h"  // stage 跨 TU 入口（采样 / 策略链 / 强平 / safety）
#include "xt_live/hft/control/HftRunnerControlState.h"
#include "xt_live/hft/control/PanelForceExitLatch.h"  // panel force-exit 续帧 latch（T1.155）
#include "xt_live/hft/control/RecorderFailureDelta.h"          // recorder 失败向帧间 delta + 限频
#include "xt_live/hft/control/SideChannelFailureDelta.h"       // 本批硬化侧道 counter delta + 降级闩
#include "xt_live/hft/telemetry/XtHftControlReportEmit.h"

#include "XtLogger.h"                                          // recorder / 侧道失败告警
#include "xt_live/telemetry/recording/XtLiveRecorder.h"        // 失败向 counter 只读观测
#include "xt_live/hft/runtime/XtHftEventLoop.h"                 // engine counter forwarders（G2a suppressed / G4 init fail）
#include "xt_live/control/SafetyStateWire.h"                    // G1a safety.bin quarantine 计数（进程静态）
#include "xt_identity/OrderAttributionStore.h"                  // G2a SID 归属 journal 失败/降级（进程静态）
#include "xt_metrics/XtStrategyPnlAggregator.h"                 // G1b PnL 无效加载计数（补全 ctx 前向声明）
#include "xt_runtime/order/XtOrderRuntimeBook.h"                // G2b venue fillup 量 sanitize 计数（drops namespace）
#include "xt_live/gateway/common/XtExecDeliveryLedger.h"        // 交付投影终态域淘汰计数（CTP exec-ledger 进程静态）

using namespace xt_live::control::detail;

XtHftRunnerControlContext make_runner_control_context(ITradeGateway* td_gateway,
                                                   XtHftEventLoop& strategy_event_loop,
                                                   XtLiveOrderGateway& live_order_gateway,
                                                   XtRunnerSignals& runtime_shared_state,
                                                   const xt_account::AccountStateStore& account_store,
                                                   xt_runtime::order::XtOrderRuntimeBook& order_book,
                                                   const XtLiveOrderGatewayCfg& order_gateway_cfg,
                                                   XtLiveLatencyTracker* latency_tracker,
                                                   XtLiveParamDomain* param_domain,
                                                   IAccountTruthSource* account_truth_source) {
    XtHftRunnerControlContext ctx{td_gateway,
                               strategy_event_loop,
                               live_order_gateway,
                               runtime_shared_state,
                               &account_store,
                               &order_book,
                               &order_gateway_cfg,
                               latency_tracker,
                               param_domain,
                               account_truth_source};
    return ctx;
}

HftRunnerControl::HftRunnerControl(const XtRunnerControlConfig& cfg,
                             XtHftRunnerControlContext& ctx,
                             const Deps& deps)
    : cfg_(cfg),
      ctx_(ctx),
      live_reporter_(deps.live_reporter) {}

namespace {

/// @brief 更新最后一帧快照缓存（供停机落盘使用）。
/// @note 记录最近一轮快照及对应时间戳；仅用于循环退出后的 `on_stop` 最终总结，
///       不参与策略判断。
void update_last_snapshot_state(const LoopFrame& frame,
                                bool& has_last_snapshot,
                                ControlSnapshot& last_snapshot,
                                int64_t& last_snapshot_ms) {
    has_last_snapshot = true;
    last_snapshot = frame.snapshot;
    last_snapshot_ms = frame.now_ms;
}

// 构造当前一帧的 per-symbol 持仓 + 行情快照 rows；不过滤 zero positions。
void finalize_control_loop(const XtRunnerControlConfig& cfg,
                           XtHftRunnerControlContext& ctx,
                           xt_report::XtLiveReporter& live_tracker,
                           bool has_last_snapshot,
                           const ControlSnapshot& last_snapshot,
                           int64_t last_snapshot_ms) {
    if (!has_last_snapshot) return;

    // 最终 KPI flush + RunSummary
    live_tracker.on_equity(last_snapshot.equity);

    // H-2 final flush 时序：finalizer 消费的是 1Hz 帧缓存 last_snapshot（采样点可能漏最后
    // 一批 latency 样本）。停 tracker 后重取 fresh snapshot（stopped 直读路径）覆盖 latency
    // 部分，确保 final CSV 含 stop 前最后一批样本。stop() 幂等（TrackerStopGuard 后置兜底）。
    XtLiveLatencyTracker::Snapshot ls = last_snapshot.latency_stats;
    if (ctx.latency_tracker) {
        ctx.latency_tracker->stop();
        ls = ctx.latency_tracker->snapshot();
    }

    xt_report::OrderRatios ratios;
    if (last_snapshot.risk_stats.submit_attempts > 0) {
        const double d = static_cast<double>(last_snapshot.risk_stats.submit_attempts);
        ratios.fill_ratio   = static_cast<double>(last_snapshot.risk_stats.submit_success) / d;
        const uint64_t rejects = last_snapshot.risk_stats.submit_attempts > last_snapshot.risk_stats.submit_success
                                     ? last_snapshot.risk_stats.submit_attempts - last_snapshot.risk_stats.submit_success : 0;
        ratios.reject_ratio = static_cast<double>(rejects) / d;
    }
    if (last_snapshot.risk_stats.cancel_attempts > 0) {
        ratios.cancel_ratio = static_cast<double>(last_snapshot.risk_stats.cancel_success) /
                              static_cast<double>(last_snapshot.risk_stats.cancel_attempts);
    }
    if (last_snapshot.risk_stats.submit_success > 0) {
        ratios.order_cancel_ratio =
            static_cast<double>(last_snapshot.risk_stats.canceled_terminal_total) /
            static_cast<double>(last_snapshot.risk_stats.submit_success);
    }
    xt_report::LatencyStats latency;
    latency.submit_to_ack_us_avg  = ls.submit_to_ack.avg_us;
    latency.submit_to_ack_us_p50  = ls.submit_to_ack.p50_us;
    latency.submit_to_ack_us_p90  = ls.submit_to_ack.p90_us;
    latency.submit_to_ack_us_p95  = ls.submit_to_ack.p95_us;
    latency.submit_to_ack_us_p99  = ls.submit_to_ack.p99_us;
    latency.submit_to_ack_paired_us_avg = ls.submit_to_ack_paired.avg_us;
    latency.submit_to_ack_paired_us_p50 = ls.submit_to_ack_paired.p50_us;
    latency.submit_to_ack_paired_us_p90 = ls.submit_to_ack_paired.p90_us;
    latency.submit_to_ack_paired_us_p95 = ls.submit_to_ack_paired.p95_us;
    latency.submit_to_ack_paired_us_p99 = ls.submit_to_ack_paired.p99_us;
    latency.submit_to_fill_us_avg = ls.submit_to_fill.avg_us;
    latency.submit_to_fill_us_p50 = ls.submit_to_fill.p50_us;
    latency.submit_to_fill_us_p90 = ls.submit_to_fill.p90_us;
    latency.submit_to_fill_us_p95 = ls.submit_to_fill.p95_us;
    latency.submit_to_fill_us_p99 = ls.submit_to_fill.p99_us;
    latency.submit_to_trade_ack_us_avg = ls.submit_to_trade_ack.avg_us;
    latency.submit_to_trade_ack_us_p50 = ls.submit_to_trade_ack.p50_us;
    latency.submit_to_trade_ack_us_p90 = ls.submit_to_trade_ack.p90_us;
    latency.submit_to_trade_ack_us_p95 = ls.submit_to_trade_ack.p95_us;
    latency.submit_to_trade_ack_us_p99 = ls.submit_to_trade_ack.p99_us;
    latency.uds_first_savings_us_avg = ls.uds_first_savings.avg_us;
    latency.uds_first_savings_us_p50 = ls.uds_first_savings.p50_us;
    latency.uds_first_savings_us_p90 = ls.uds_first_savings.p90_us;
    latency.uds_first_savings_us_p95 = ls.uds_first_savings.p95_us;
    latency.uds_first_savings_us_p99 = ls.uds_first_savings.p99_us;
    latency.strategy_us_avg       = ls.strategy_processing.avg_us;
    latency.strategy_us_p50       = ls.strategy_processing.p50_us;
    latency.strategy_us_p90       = ls.strategy_processing.p90_us;
    latency.strategy_us_p99       = ls.strategy_processing.p99_us;
    latency.local_submit_us_avg   = ls.submit_wire.avg_us;
    latency.local_submit_us_p50   = ls.submit_wire.p50_us;
    latency.local_submit_us_p90   = ls.submit_wire.p90_us;
    latency.local_submit_us_p99   = ls.submit_wire.p99_us;
    latency.ack_path_us_avg       = ls.ack_path.avg_us;
    latency.ack_path_us_p50       = ls.ack_path.p50_us;
    latency.ack_path_us_p90       = ls.ack_path.p90_us;
    latency.ack_path_us_p99       = ls.ack_path.p99_us;
    latency.fill_after_ack_us_avg = ls.ack_to_fill.avg_us;
    latency.fill_after_ack_us_p50 = ls.ack_to_fill.p50_us;
    latency.fill_after_ack_us_p90 = ls.ack_to_fill.p90_us;
    latency.fill_after_ack_us_p99 = ls.ack_to_fill.p99_us;
    latency.event_to_ack_us_avg   = ls.event_to_ack.avg_us;
    latency.event_to_ack_us_p50   = ls.event_to_ack.p50_us;
    latency.event_to_ack_us_p90   = ls.event_to_ack.p90_us;
    latency.event_to_ack_us_p99   = ls.event_to_ack.p99_us;
    latency.event_to_fill_us_avg  = ls.event_to_fill.avg_us;
    latency.event_to_fill_us_p50  = ls.event_to_fill.p50_us;
    latency.event_to_fill_us_p90  = ls.event_to_fill.p90_us;
    latency.event_to_fill_us_p99  = ls.event_to_fill.p99_us;
    // Per-stage 样本计数（fail-closed budget）：0 → NO_DATA 而非"低延迟绿灯"。
    latency.submit_to_ack_us_samples  = ls.submit_to_ack.samples;
    latency.fill_after_ack_us_samples = ls.ack_to_fill.samples;
    latency.local_submit_us_samples   = ls.submit_wire.samples;
    latency.submit_to_ack_paired_us_samples = ls.submit_to_ack_paired.samples;
    latency.submit_to_trade_ack_us_samples  = ls.submit_to_trade_ack.samples;
    latency.uds_first_savings_us_samples    = ls.uds_first_savings.samples;
    latency.submit_to_fill_us_samples       = ls.submit_to_fill.samples;
    latency.strategy_us_samples             = ls.strategy_processing.samples;
    latency.ack_path_us_samples             = ls.ack_path.samples;
    latency.event_to_ack_us_samples         = ls.event_to_ack.samples;
    latency.event_to_fill_us_samples        = ls.event_to_fill.samples;
    // 真 pre-trade risk reject 累计（record_reject 单调计数），与 1Hz build_kpi_latency 同源。
    latency.risk_block_count      = last_snapshot.risk_stats.total_rejects();
    latency.md_gap_ms_p99         = static_cast<double>(last_snapshot.tick_lag_p99_us) / 1000.0;
    latency.md_gap_ms_peak        = static_cast<double>(last_snapshot.tick_lag_peak_us) / 1000.0;
    latency.event_drop            = last_snapshot.tick_drop + last_snapshot.td_flow_event_rejected;
    // queue depth = TD 事件在途 instant（与 1Hz 路径统一 instant 口径）。
    latency.queue_depth_p99       = static_cast<double>(last_snapshot.td_event_backlog);

    const bool kill_switch_active = ctx.runtime_shared_state.kill_switch.load(std::memory_order_acquire);
    live_tracker.force_flush_kpi(last_snapshot_ms, 0.0, ratios, latency, kill_switch_active);
    live_tracker.flush_run_summary();
}

// Panel gRPC FlattenAll / SetKillSwitch / CancelAll / ForceExit /
// SubmitManualOrder RPC 直接调用 cross-engine helper；RPC handler bind 在 XtHftRunner。

} // namespace

// 控制环主循环：采样 -> 策略 -> 上报 -> 收尾。
void HftRunnerControl::run(std::atomic<bool>& shutdown) {
    ControlLoopState st;
    // session-begin 的有界等待共用本循环的停机信号：停机时立即退出等待。
    // signal 是 caller 的对象，run() 返回后 ctx_ 不得再持有它（header 契约）。
    struct ShutdownFlagScope {
        const std::atomic<bool>*& slot;
        ~ShutdownFlagScope() { slot = nullptr; }
    } shutdown_flag_scope{ctx_.shutdown_flag};
    ctx_.shutdown_flag = &shutdown;
    const int64_t start_ms = live_reporter_.start_ms();
    bool has_last_snapshot = false;
    ControlSnapshot last_snapshot;
    int64_t last_snapshot_ms = start_ms;
    // 阈值只算一次：归一化保证 warn <= block <= kill 且均 >= 1。
    const ConsistencyThresholds thresholds = make_consistency_thresholds(cfg_);

    // ── bootstrap reload + periodic save 准备 ─────────────────────────────
    // 先 reload disk safety.bin，把 latched kill_switch / open_halted /
    // peak_equity 写回 atomic；接着采集当前 state 作为 dedup 基线。
    bootstrap_safety_state(ctx_, cfg_.safety_state_path);
    xt_risk::state::SafetyState last_safety_state = collect_safety_state(ctx_);
    int64_t last_safety_save_ms = XtSteadyClock::steady_now_ms();
    // bootstrap 时强制写一次（确保 file 被创建，loaded 后 schema/header 有效）。
    save_safety_state_if_due(cfg_.safety_state_path, ctx_,
                             last_safety_state, last_safety_save_ms,
                             last_safety_save_ms,
                             cfg_.safety_state_save_interval_ms,
                             "bootstrap");

    // systemd Type=notify: 所有 bootstrap 完成 → 告知 systemd "READY=1"。
    // NOTIFY_SOCKET 不设时 sd_notify_ready() 自动 no-op (dev/WSL safe)。
    // 配套 unit: Type=notify + WatchdogSec=10s; 主循环每帧 sd_notify_watchdog()。
    xt_util::sd_notify_ready();

    // recorder 失败向 counter 的帧间 delta tracker：1Hz frame-tail 观测，每原因
    // 独立限频。fail-open 政策不改（录制失败不停交易），只补失败可见性——补齐
    // reopen 失败后 live.jsonl size=0 令磁盘/未滚动告警不触发的观测盲区。
    xt_live::control::RecorderFailureDelta recorder_failure_delta;

    // 本批硬化侧道 counter 的帧间 delta tracker + SID 归属 journal 降级闩状态机
    // （与 recorder_failure_delta 同模式，每原因 60s 限频）：surface 各硬化点的
    // 失败/降级累计，硬化政策不改（fail-closed 由各 counter 生产侧完成），只补观测。
    xt_live::control::SideChannelFailureDelta side_channel_delta;
    xt_live::control::DegradedLatchState sid_journal_degraded_latch;

    // ---------------------------------------------------------------------------
    // 主循环：每轮 sleep_ms 节拍（典型 1000ms），按固定顺序执行各冷路径阶段。
    // ---------------------------------------------------------------------------
    for (;;) {
        // (1) shutdown 检查 + 短 polling sleep：shutdown 信号触发后 ≤50ms 退出。
        //     短轮询把 shutdown tail latency 限制到 50ms 级别。
        if (xt_util::XtShutdownSignal::wait_for(
                shutdown, std::chrono::milliseconds(cfg_.sleep_ms))) {
            break;
        }

        // (2) cold-path 推进 + 采样：先 advance risk/health 副作用，再构建只读 snapshot。
        const LoopFrame frame = collect_loop_frame(cfg_, ctx_, st);
        log_control_stats(frame.snapshot, st.latency_sla_breach_total);
        update_last_snapshot_state(frame, has_last_snapshot, last_snapshot, last_snapshot_ms);

        // (3) 策略链：heartbeat → 一致性检查 → 故障策略 → 延迟 SLA
        if (!apply_policy_chain(shutdown, cfg_, ctx_, thresholds, frame, st)) break;
        // (4) 可观测性输出：live contract report + 策略指标
        xt_live::control::emit_live_reports(cfg_, ctx_, live_reporter_, frame, st);
        xt_live::control::flush_strategy_metrics_if_needed(cfg_, ctx_, live_reporter_, frame, st);
        // (4b) per-sid 主动强平：daily-loss/kill_switch 突破 → edge-latch → force_exit。
        //      在收盘强平之前评估；cfg.enabled=false 时整段 no-op（默认关）。
        liquidator_.step(ctx_);
        // (4c) panel ForceExit CancelledOnly 续帧重试（T1.155）：cancel-first 后
        //      每帧续平直到 flat（Done）/ TTL 到期（Expired）；未装配时 no-op。
        if (ctx_.force_exit_latch) ctx_.force_exit_latch->step(ctx_, frame.now_ms);
        // (5) 收盘强平检查
        run_force_close_once(ctx_, cfg_, st);
        // (5b) 冷启 hydrate 完成后一次性自动释放 closeout-block 自动 latch（仅 flat）。
        maybe_auto_release_closeout_latch(ctx_, cfg_, st);
        // Operator requests enter via gRPC mTLS:
        //   - FlattenAll RPC → kill_switch atomic store
        //   - SetKillSwitch RPC → 同上 + KillSwitchState (因果链)
        //   - UpdateParam RPC → param_domain.set_checked
        //   - GetParam / GetTradingState RPC → 查 ParamDomain.snapshot / kill_switch_state.snapshot

        // (6) periodic safety state 落盘
        //     - save_safety_state_if_due 内部对比 last_state；变化即写，否则按
        //       interval (60s) 到期才写。
        //     - 写盘 ~ms 级（fsync ×2），1Hz 频率不影响节拍。
        //     - 失败不抛，下帧再试 (warn_mt 节流防 log spam)。
        {
            const int64_t safety_now_ms = XtSteadyClock::steady_now_ms();
            save_safety_state_if_due(cfg_.safety_state_path, ctx_,
                                     last_safety_state, last_safety_save_ms,
                                     safety_now_ms,
                                     cfg_.safety_state_save_interval_ms,
                                     "frame");
        }

        // (6b) recorder 失败向 counter 观测：drop / write_failed / intent_trace_*
        //      新增即限频 warn（每原因 60s 一条）。recorder 为 process-global，
        //      未启用时 global()==nullptr → 整段跳过。emit/written 是吞吐不报。
        if (const XtLiveRecorder* rec = XtLiveRecorder::global()) {
            const int64_t rec_now_ms = XtSteadyClock::steady_now_ms();
            const auto delta = recorder_failure_delta.observe(
                static_cast<uint64_t>(rec_now_ms),
                rec->drop_count(),
                rec->write_failed_count(),
                rec->intent_trace_drop_count(),
                rec->intent_trace_write_failed_count());
            if (delta.any) {
                using RFD = xt_live::control::RecorderFailureDelta;
                for (std::size_t r = 0; r < RFD::kCount; ++r) {
                    if (!delta.warn[r]) continue;
                    XtLogger::warn_m("RT",
                        "recorder failure counter increased: reason={} delta={} "
                        "(fail-open: recording degraded, trading continues)",
                        RFD::reason_name(r), delta.delta[r]);
                }
            }
        }

        // (6c) 硬化/淘汰侧道 counter 帧间 delta 观测：G1a safety quarantine / G1b PnL 无效
        //      加载 / G2a SID journal 失败 + orphan reap 抑制 / G2b venue fillup sanitize /
        //      G4 策略 init 失败 / CTP exec-ledger 终态域淘汰。与 (6b) 同模式，每原因 60s
        //      限频。这些 counter 均为进程内 atomic（静态/进程级，或经 ctx 已持有的 event
        //      loop / aggregator 转发），读取不引新跨模块引用链；硬化政策由各生产侧
        //      fail-closed，本段只补失败/淘汰可见性。
        {
            const int64_t sc_now_ms = XtSteadyClock::steady_now_ms();
            const uint64_t pnl_invalid_load =
                ctx_.strategy_pnl_aggr ? ctx_.strategy_pnl_aggr->invalid_load_count() : 0;
            const auto delta = side_channel_delta.observe(
                static_cast<uint64_t>(sc_now_ms),
                xt_live::control::safety_state_quarantine_count(),
                pnl_invalid_load,
                xt_identity::OrderAttributionStore::order_sid_journal_failure_count(),
                ctx_.strategy_event_loop.engine_orphan_reap_suppressed_count(),
                xt_runtime_book_drops::venue_fillup_qty_sanitized().load(std::memory_order_relaxed),
                ctx_.strategy_event_loop.engine_strategy_init_failure_count(),
                xt_live::gateway::exec_delivery_terminal_evicted_total().load(std::memory_order_relaxed));
            if (delta.any) {
                using SCD = xt_live::control::SideChannelFailureDelta;
                for (std::size_t r = 0; r < SCD::kCount; ++r) {
                    if (!delta.warn[r]) continue;
                    XtLogger::warn_m("RT",
                        "hardening side-channel counter increased: reason={} delta={}",
                        SCD::reason_name(r), delta.delta[r]);
                }
            }

            // SID 归属 journal 降级闩（进程静态）：进入 warn 一次、持续降级 60s 低频提醒、
            // 复原 info。降级期间 orphan reap 切告警-only、归属落盘不权威，需 operator 处置。
            const auto latch = sid_journal_degraded_latch.observe(
                static_cast<uint64_t>(sc_now_ms),
                xt_identity::OrderAttributionStore::order_sid_journal_degraded());
            if (latch.warn_enter || latch.warn_during) {
                XtLogger::warn_m("RT",
                    "SID attribution journal degraded: disk copy not authoritative, "
                    "orphan reap suppressed to alert-only (journal_failures={})",
                    xt_identity::OrderAttributionStore::order_sid_journal_failure_count());
            } else if (latch.info_recover) {
                XtLogger::info_m("RT",
                    "SID attribution journal recovered: compaction succeeded, "
                    "orphan reap re-enabled");
            }
        }

        // 帧尾钩子（如 TD handoff 重投缓冲 drain）。
        if (ctx_.frame_tail_hook) ctx_.frame_tail_hook();

        // systemd watchdog heartbeat: 跟随 1Hz frame 周期, 远低于 WatchdogSec=10s。
        // NOTIFY_SOCKET 不设 → no-op; systemd Type=notify + WatchdogSec= 触发的话整体生效。
        xt_util::sd_notify_watchdog();
    }

    // systemd: 告知 systemd 进入 shutdown, 暂停 watchdog 心跳计时
    // (避免 finalize_control_loop 期间因无 WATCHDOG=1 误触发 restart)。
    xt_util::sd_notify_stopping();

    // ── on-shutdown final save ────────────────────────────────────────────
    // 强制写最后一帧，确保 latched state 落盘后再走 finalize（reconcile 可能
    // 数十秒，期间崩溃不应丢掉本帧 latch）。
    {
        const int64_t shutdown_now_ms = XtSteadyClock::steady_now_ms();
        save_safety_state_if_due(cfg_.safety_state_path, ctx_,
                                 last_safety_state, last_safety_save_ms,
                                 shutdown_now_ms,
                                 cfg_.safety_state_save_interval_ms,
                                 "shutdown");
    }

    // 退出后只**缓存**最后一帧快照；final KPI/RunSummary flush 由 finalize_report()
    // 承担，Runner 在 stop_runtime_threads（event loop/gateway/channels 停机）之后调用，
    // 以便 tracker->stop() 的 final drain 纳入这些线程停机前产出的尾批 latency 样本
    // （否则在此处 stop 会因 enabled=false 丢弃尾样）。账户停机对账同样在 Runner 层 post-stop。
    finalize_ready_ = has_last_snapshot;
    if (has_last_snapshot) {
        last_snapshot_ = std::make_unique<ControlSnapshot>(last_snapshot);
        last_snapshot_ms_ = last_snapshot_ms;
    }
}

HftRunnerControl::~HftRunnerControl() = default;

void HftRunnerControl::finalize_report() {
    if (!finalize_ready_ || !last_snapshot_) return;
    finalize_control_loop(cfg_, ctx_, live_reporter_, /*has_last_snapshot=*/true,
                          *last_snapshot_, last_snapshot_ms_);
}
