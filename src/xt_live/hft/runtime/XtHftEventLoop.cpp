#include "xt_live/hft/runtime/XtHftEventLoop.h"

#include <algorithm>
#include <array>
#include <exception>
#include <stdexcept>
#include <utility>

#include "recording/XtLiveRecorder.h"
#include "latency/XtLiveLatencyTracker.h"
#include "XtLogger.h"
#include "xt_logger/XtCallbackGuard.h"
#include "xt_core/XtLiveTapContext.h"
#include "xt_util/time/XtSteadyClock.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_cost/XtFillCostModel.h"
#include "xt_core/XtFixedPoint.h"
#include "xt_config/catalog/XtFeeRuleBook.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_util/runtime/ThreadUtil.h"
#include "xt_core/market/XtTickConvert.h"
#include "xt_engine/hft/XtHftEngine.h"
#include "xt_risk/live/SelfMatchDetector.h"
#include "xt_live/common/SessionGateUpdate.h"
#include "xt_live/common/TapAnchorScope.h"
#include "xt_util/time/XtTime.h"

#if defined(__x86_64__)
#include <immintrin.h>  // _mm_pause for x86 spin-wait backoff.
#endif

namespace {

// Worker 空转 backoff helper。
// x86 用 PAUSE 指令提示 CPU 当前是 spin-wait，降低功耗并减少 SMT sibling 干扰；
// 非 x86 没有统一 pause intrinsic，退化为 yield，让共享核心上的其他线程先运行。
void backoff_pause(int spins) {
#if defined(__x86_64__)
    for (int i = 0; i < spins; ++i) {
        _mm_pause();
    }
#else
    for (int i = 0; i < spins; ++i) {
        std::this_thread::yield();
    }
#endif
}

// 原子更新观测峰值：peak_atomic = max(peak_atomic, value)。
// 仅用于 telemetry 计数；不建立跨线程同步关系。
void update_atomic_peak(std::atomic<uint64_t>& peak_atomic, uint64_t value) {
    uint64_t peak = peak_atomic.load(std::memory_order_relaxed);
    while (value > peak &&
           !peak_atomic.compare_exchange_weak(
               peak, value, std::memory_order_relaxed, std::memory_order_relaxed)) {}
}

// EventLoop 内部 warning 节流窗口；避免持续 backlog/starvation 时刷日志。
constexpr uint64_t kWarnThrottleIntervalUs = 1000000;

enum class StrategyGuardStage : uint8_t {
    OnInit,
    OnTick,
    OnSessionBegin,
    OnSessionEnd,
    OnChannelReady,
    OnChannelLost,
    OnOrderQueue,
    OnOrderDetail,
    OnTransaction,
    OnParams,
    SelfMatch,
};

const char* strategy_guard_stage_name(StrategyGuardStage stage) {
    switch (stage) {
    case StrategyGuardStage::OnInit:         return "on_init";
    case StrategyGuardStage::OnTick:         return "on_tick";
    case StrategyGuardStage::OnSessionBegin: return "on_session_begin";
    case StrategyGuardStage::OnSessionEnd:   return "on_session_end";
    case StrategyGuardStage::OnChannelReady: return "on_channel_ready";
    case StrategyGuardStage::OnChannelLost:  return "on_channel_lost";
    case StrategyGuardStage::OnOrderQueue:   return "on_order_queue";
    case StrategyGuardStage::OnOrderDetail:  return "on_order_detail";
    case StrategyGuardStage::OnTransaction:  return "on_transaction";
    case StrategyGuardStage::OnParams:       return "strategy.on_params";
    case StrategyGuardStage::SelfMatch:      return "live.self_match";
    }
    return "";
}

template <typename Fn>
bool run_strategy_guard(StrategyGuardStage stage,
                        std::atomic<bool>* stop_requested,
                        std::atomic<bool>* shutdown_flag,
                        Fn&& fn) {
    const char* stage_name = strategy_guard_stage_name(stage);
    if (xt_log::run_guarded("RT", stage_name ? stage_name : "", std::forward<Fn>(fn))) {
        return true;
    }
    // 失败策略 (显式): 策略/引擎异常不能跨 worker 线程边界，升级为停机请求。
    if (stop_requested) stop_requested->store(true, std::memory_order_release);
    if (shutdown_flag) shutdown_flag->store(true, std::memory_order_release);
    return false;
}

[[nodiscard]] QueueMode to_lockfree_queue_mode(XtHftEventLoopCfg::QueueMode m) {
    switch (m) {
        case XtHftEventLoopCfg::QueueMode::SPSC: return QueueMode::SPSC;
        case XtHftEventLoopCfg::QueueMode::MPSC: return QueueMode::MPSC;
    }
    return QueueMode::MPSC;
}

} // namespace

XtHftEngine& XtHftEventLoop::require_engine_(XtHftEngine* engine) {
    if (!engine) {
        throw std::invalid_argument("XtHftEventLoop requires non-null XtHftEngine");
    }
    return *engine;
}

XtHftEventLoop::XtHftEventLoop(const Config& cfg, const Deps& deps)
    : cfg_(cfg),
      budgets_(compute_worker_budgets_(cfg)),
      engine_(require_engine_(deps.engine)),
      shutdown_flag_(deps.shutdown_flag),
      last_tick_action_time_(deps.last_tick_action_time),
      last_tick_steady_ms_(deps.last_tick_steady_ms),
      tick_proc_cnt_(deps.tick_proc_cnt),
      self_match_detector_(deps.self_match_detector),
      blacklist_sink_(deps.blacklist_sink),
      marshal_overflow_drain_(deps.marshal_overflow_drain),
      latency_tracker_(deps.latency_tracker),
      live_cost_(deps.live_cost),
      live_fees_(deps.live_fees),
      catalog_(deps.catalog),
      initial_param_version_(deps.initial_param_version),
      in_trading_section_(deps.in_trading_section),
      session_sections_(deps.session_sections),
      closeout_no_open_(deps.closeout_no_open),
      closeout_windows_(deps.closeout_windows),
      tick_q_(to_lockfree_queue_mode(cfg.tick_queue_mode), cfg.tick_queue_capacity),
      td_event_q_(to_lockfree_queue_mode(cfg.td_event_queue_mode), cfg.td_event_queue_capacity),
      flow_event_q_(to_lockfree_queue_mode(cfg.flow_event_queue_mode), cfg.flow_event_queue_capacity) {
    warn_if_budgets_clamped_();
}

XtHftEventLoop::~XtHftEventLoop() {
    stop();
}

bool XtHftEventLoop::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return true;
    }
    stop_requested_.store(false, std::memory_order_release);
    worker_ = std::thread([this]() { run(); });
    return true;
}

void XtHftEventLoop::stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) return;
    stop_requested_.store(true, std::memory_order_release);
    if (worker_.joinable()) {
        worker_.join();
    }
    drain_pending_queues();
}

bool XtHftEventLoop::try_push_tick(const XtRawTick& tick) {
    // capture_ns 未知（直连 venue / 测试路径）→ 传 0，dispatch 起点 fallback enqueue。
    return try_push_tick(tick, /*capture_ns=*/0);
}

bool XtHftEventLoop::try_push_tick(const XtRawTick& tick, uint64_t capture_ns) {
    if (reject_enqueue_guard(cfg_.tick_high_watermark, tick_backlog_)) {
        return false;
    }
    TickEvent ev;
    ev.core_tick = xt_tick_convert::to_core(tick);
    // trigger 精确化（µs 内精确，非 1ms cache）：每 tick 直读 steady clock，
    // 让 strategy_us / event_to_* 数值变准（trigger tap 关联的时间基准）。
    ev.enqueue_ts_us = XtSteadyClock::steady_now_us();
    // SHM connector capture（跨进程 steady ns）截断为 µs；0=未 stamp。dispatch 时经
    // xt_live::select_trigger_anchor_us 决定 trigger 起点（capture 有效且不晚于 enqueue 则用 capture）。
    ev.capture_ts_us = capture_ns / 1000ULL;
    // reserve-before-publish：backlog 先占位再发布，push 失败回滚；
    // 与 safe_decrement_backlog() 配对，避免 consumer 先递减导致 underflow。
    tick_backlog_.fetch_add(1, std::memory_order_relaxed);
    if (!tick_q_.push(ev)) {
        tick_backlog_.fetch_sub(1, std::memory_order_relaxed);  // rollback
        return false;
    }
    return true;
}

bool XtHftEventLoop::push_td_event(XtHftRunnerEvent ev) {
    return enqueue_runner_event(std::move(ev),
                                td_event_q_,
                                cfg_.td_event_high_watermark,
                                td_event_backlog_,
                                td_event_peak_backlog_);
}

bool XtHftEventLoop::push_flow_event(XtHftRunnerEvent ev) {
    // Prom/CSV 兼容：flow 峰值仍合并到 td_event_peak_backlog_，不新增字段。
    return enqueue_runner_event(std::move(ev),
                                flow_event_q_,
                                cfg_.flow_event_high_watermark,
                                flow_event_backlog_,
                                td_event_peak_backlog_);
}

uint64_t XtHftEventLoop::tick_backlog() const {
    return tick_backlog_.load(std::memory_order_relaxed);
}

uint64_t XtHftEventLoop::td_event_backlog() const {
    return td_event_backlog_.load(std::memory_order_relaxed);
}

uint64_t XtHftEventLoop::drain_tick_lag_peak_us() {
    return tick_lag_us_peak_.exchange(0, std::memory_order_relaxed);
}

xt_live::XtLagHistogram::Stats XtHftEventLoop::drain_tick_lag_hist_stats() {
    return tick_lag_hist_.drain();
}

xt_live::XtLagHistogram::Stats XtHftEventLoop::drain_td_flow_lag_hist_stats() {
    return td_flow_lag_hist_.drain();
}

uint64_t XtHftEventLoop::td_event_peak_backlog_val() const {
    return td_event_peak_backlog_.load(std::memory_order_relaxed);
}

uint64_t XtHftEventLoop::td_flow_event_lag_us_max_val() const {
    return td_flow_event_lag_us_max_.load(std::memory_order_relaxed);
}

std::vector<std::pair<std::string, ct::metrics::XtStrategyMetricsSnapshot>>
XtHftEventLoop::drain_all_strategy_metrics() {
    std::vector<std::pair<std::string, ct::metrics::XtStrategyMetricsSnapshot>> out;
    out.reserve(engine_.context_count());
    // context 遍历归 engine（它是 slots_ 唯一 owner）；EventLoop 只负责容器与转发。
    engine_.drain_all_strategy_metrics(out);
    return out;
}

// HftRunnerControl 1Hz frame loop 读取后写入 signals，供 panel / Prometheus 观测。
uint64_t XtHftEventLoop::engine_dropped_fill_count() const noexcept {
    return engine_.dropped_fill_count();
}
uint64_t XtHftEventLoop::engine_dropped_order_update_count() const noexcept {
    return engine_.dropped_order_update_count();
}
uint64_t XtHftEventLoop::engine_orphan_reap_cancel_count() const noexcept {
    return engine_.orphan_reap_cancel_count();
}
uint64_t XtHftEventLoop::engine_orphan_reap_suppressed_count() const noexcept {
    return engine_.orphan_reap_suppressed_count();
}
uint64_t XtHftEventLoop::engine_strategy_init_failure_count() const noexcept {
    return engine_.strategy_init_failure_count();
}
uint32_t XtHftEventLoop::engine_open_order_count() const noexcept {
    return engine_.open_order_count();
}
XtHftEngine::UnmatchedStats XtHftEventLoop::engine_unmatched_stats() const noexcept {
    return engine_.unmatched_stats();
}
uint64_t XtHftEventLoop::worker_body_exception_count() const noexcept {
    return worker_body_ex_ct_.load(std::memory_order_relaxed);
}
uint64_t XtHftEventLoop::normalize_fail_count() const noexcept {
    return normalize_fail_ct_.load(std::memory_order_relaxed);
}
uint64_t XtHftEventLoop::record_fail_count() const noexcept {
    return record_fail_ct_.load(std::memory_order_relaxed);
}

bool XtHftEventLoop::reject_enqueue_guard(uint64_t high_watermark,
                                               const std::atomic<uint64_t>& backlog,
                                               bool bypass_watermark) const {
    if (!running_.load(std::memory_order_acquire) || stop_requested_.load(std::memory_order_acquire)) {
        return true;
    }
    // 成交/终态/断连等保护事件绕过软高水位；仍受物理队列容量约束。
    if (!bypass_watermark && high_watermark > 0 &&
        backlog.load(std::memory_order_relaxed) >= high_watermark) {
        return true;
    }
    return false;
}

bool XtHftEventLoop::enqueue_runner_event(XtHftRunnerEvent ev,
                                               XtLockFreeQueue<XtHftRunnerEvent>& queue,
                                               uint64_t high_watermark,
                                               std::atomic<uint64_t>& backlog_counter,
                                               std::atomic<uint64_t>& peak_backlog_counter) {
    const bool no_drop = ev.must_not_drop();
    // WorkerTask 物理满非终局（post_to_worker 溢出侧道接手），告警文案与真丢失区分；
    // push 可能 move-from ev，先取类型。
    const auto* pre_ctrl = ev.get_if<XtRunnerControlEvent>();
    const bool is_worker_task =
        pre_ctrl && pre_ctrl->type == XtRunnerControlEventType::WorkerTask;
    if (reject_enqueue_guard(high_watermark, backlog_counter, no_drop)) {
        return false;
    }
    if (ev.enqueue_ts_us == 0) {
        ev.enqueue_ts_us = XtSteadyClock::steady_now_us();
    }
    // reserve-before-publish：先占位再 push；失败回滚，consumer 端按同一计数器递减。
    const uint64_t reserved = backlog_counter.fetch_add(1, std::memory_order_relaxed) + 1;
    update_atomic_peak(peak_backlog_counter, reserved);
    if (!queue.push(std::move(ev))) {
        backlog_counter.fetch_sub(1, std::memory_order_relaxed);  // rollback
        if (no_drop) {
            if (is_worker_task) {
                XtLogger::error_m("RT",
                    "runner WorkerTask rejected by physical queue (backlog={} watermark={}) "
                    "— routed to marshal overflow side-channel",
                    backlog_counter.load(std::memory_order_relaxed), high_watermark);
            } else {
                // 保护事件被物理队列拒绝时必须升级为关键告警，避免静默漏成交或连接状态误判。
                XtLogger::error_m("RT",
                    "CRITICAL runner event DROPPED (physical queue full); backlog={} watermark={} "
                    "— phantom/missed-fill risk",
                    backlog_counter.load(std::memory_order_relaxed), high_watermark);
            }
        }
        return false;
    }
    return true;
}

void XtHftEventLoop::safe_decrement_backlog(std::atomic<uint64_t>& backlog,
                                            const char* counter_name) {
    const uint64_t old = backlog.fetch_sub(1, std::memory_order_relaxed);
    if (old == 0) {
        backlog.store(0, std::memory_order_relaxed);
        XtLogger::warn_m("RT",
                         "{} backlog underflow detected (consumer popped before producer reserve), clamped to 0",
                         counter_name);
    }
}

void XtHftEventLoop::request_worker_stop() noexcept {
    stop_requested_.store(true, std::memory_order_release);
    if (shutdown_flag_) shutdown_flag_->store(true, std::memory_order_release);
}

// ── Event dispatch ────────────────────────────────────────────────
bool XtHftEventLoop::dispatch_strategy_event(XtHftRunnerEvent& ev) {
    if (const auto* exec    = ev.get_if<XtRunnerExecEvent>())    return dispatch_exec_event(*exec);
    if (const auto* account = ev.get_if<XtRunnerAccountEvent>()) return dispatch_account_event(*account);
    if (const auto* control = ev.get_if<XtRunnerControlEvent>()) return dispatch_control_event(*control);
    return true;
}

bool XtHftEventLoop::dispatch_exec_event(const XtRunnerExecEvent& exec) {
    switch (exec.type) {
    case XtRunnerExecEventType::OrderQueue:
        if (const auto* p = exec.get_if<XtRunnerOrderQueueEvent>()) {
            return run_strategy_guard(StrategyGuardStage::OnOrderQueue,
                                      &stop_requested_, shutdown_flag_,
                                      [&]() { engine_.on_order_queue(p->value); });
        }
        return true;
    case XtRunnerExecEventType::OrderDetail:
        if (const auto* p = exec.get_if<XtRunnerOrderDetailEvent>()) {
            return run_strategy_guard(StrategyGuardStage::OnOrderDetail,
                                      &stop_requested_, shutdown_flag_,
                                      [&]() { engine_.on_order_detail(p->value); });
        }
        return true;
    case XtRunnerExecEventType::Transaction:
        if (const auto* p = exec.get_if<XtRunnerTransactionEvent>()) {
            return run_strategy_guard(StrategyGuardStage::OnTransaction,
                                      &stop_requested_, shutdown_flag_,
                                      [&]() { engine_.on_transaction(p->value); });
        }
        return true;
    case XtRunnerExecEventType::XtOrderExecUpdate:
        if (const auto* p = exec.get_if<XtRunnerOrderUpdateEvent>()) {
            return handle_exec_order_update_(*p);
        }
        return true;
    case XtRunnerExecEventType::Trade:
        if (const auto* p = exec.get_if<XtRunnerTradeEvent>()) {
            return handle_exec_trade_(*p);
        }
        return true;
    }
    return true;
}

bool XtHftEventLoop::dispatch_account_event(const XtRunnerAccountEvent& account) {
    switch (account.type) {
    case XtRunnerAccountEventType::DailySettlement:
        if (const auto* p = account.get_if<XtRunnerDailySettlementEvent>()) {
            engine_.on_daily_settlement(p->delta);
        }
        return true;
    case XtRunnerAccountEventType::CashTransfer:
        if (const auto* p = account.get_if<XtRunnerCashTransferEvent>()) {
            engine_.on_cash_transfer(p->delta);
        }
        return true;
    case XtRunnerAccountEventType::PositionSnapshot:
        if (const auto* p = account.get_if<XtRunnerPositionSnapshotEvent>()) {
            engine_.on_position_snapshot(p->value, p->td_state, p->is_cold_start, p->session_gen);
        }
        return true;
    case XtRunnerAccountEventType::AccountSnapshot:
        if (const auto* p = account.get_if<XtRunnerAccountSnapshotEvent>()) {
            engine_.on_account_snapshot(p->value);
        }
        return true;
    }
    return true;
}

bool XtHftEventLoop::dispatch_control_event(const XtRunnerControlEvent& control) {
    switch (control.type) {
    case XtRunnerControlEventType::ChannelReady:
        if (const auto* p = control.get_if<XtRunnerChannelEvent>()) {
            return run_strategy_guard(StrategyGuardStage::OnChannelReady,
                                      &stop_requested_, shutdown_flag_,
                                      [&]() { engine_.on_channel_ready(p->channel.c_str()); });
        }
        return true;
    case XtRunnerControlEventType::ChannelLost:
        if (const auto* p = control.get_if<XtRunnerChannelEvent>()) {
            return run_strategy_guard(StrategyGuardStage::OnChannelLost,
                                      &stop_requested_, shutdown_flag_,
                                      [&]() { engine_.on_channel_lost(p->channel.c_str(), p->reason); });
        }
        return true;
    case XtRunnerControlEventType::ParamsUpdated:
        if (const auto* p = control.get_if<XtRunnerParamsUpdatedEvent>()) {
            return run_strategy_guard(StrategyGuardStage::OnParams,
                                      &stop_requested_, shutdown_flag_,
                                      [&]() { engine_.on_params_updated(p->param_version); });
        }
        return true;
    case XtRunnerControlEventType::ReverseSyncBatch:
        // venue gateway reverse-synced alive in-flight orders → 灌入 order book
        // 直接调 engine handler；不走 strategy guard（无策略回调，纯 book 内部状态同步）
        if (const auto* p = control.get_if<ReverseSyncBatchEventPayload>()) {
            engine_.on_reverse_sync_batch(p->orders, p->is_cold_start);
        }
        return true;
    case XtRunnerControlEventType::ReapRecoveredOrphans:
        // 孤儿回收 / lost-terminal 对账 sweep：worker 线程扫 book + gateway 撤单/合成终态；
        // 无策略回调，不走 strategy guard。两能力各自 flag 门控（HFT-D6 解耦）：只开 reconcile
        // 时不跑 orphan reap（reap_orphans=false），反之亦然。
        if (const auto* p = control.get_if<XtRunnerReapOrphansEvent>()) {
            if (p->reap_orphans) {
                (void)engine_.reap_recovered_orphans(p->now_ms, p->grace_ms, p->max_cancels);
            }
            // lost-terminal reconcile：venue-terminal 但 book-open 的 owned 单合成终态。
            if (p->reconcile_lost_terminals) {
                (void)engine_.reconcile_lost_terminals(p->max_cancels);
            }
        }
        return true;
    case XtRunnerControlEventType::WorkerTask:
        // off-worker submit 的注册闭包：与 td rtn 同队列 FIFO 保证注册先于
        // 首条回报。纯 book/account 写，无策略回调，不走 strategy guard（同 ReverseSyncBatch）。
        if (const auto* p = control.get_if<XtRunnerWorkerTaskEvent>()) {
            if (p->fn) p->fn();
        }
        return true;
    }
    return true;
}

// ── Exec event handling ───────────────────────────────────────────
bool XtHftEventLoop::handle_exec_order_update_(const XtRunnerOrderUpdateEvent& evt) {
    // recording 是 telemetry：异常 swallow + counter，不停机（JSONL/recorder 抖动不该杀交易 runner）。
    xt_log::run_guarded("RT", "live.record_order", record_fail_ct_,
                        [&]() { emit_order_update_record(evt); });
    // engine_.on_order_update 抛异常由 run() worker-body backstop 接住升级停机；
    // 此处只处理其 bool-return drop-signaling（现状语义不变）。
    // latency_ineligible（gateway 边界盖章）→ elig，仅 gate observer latency feed。
    const XtLatencyEligibility elig = evt.latency_ineligible
        ? XtLatencyEligibility::kIneligible
        : XtLatencyEligibility::kEligible;
    if (!engine_.on_order_update(evt.value, elig)) {
        request_worker_stop();
        return false;
    }
    return true;
}

bool XtHftEventLoop::handle_exec_trade_(const XtRunnerTradeEvent& evt) {
    // normalize：抛异常时 passthrough 原始成交（fill 已发生，优先保 account ingest）+ counter + loud log；
    // 该笔 fee/PnL 未成本归一化，下游/ops 勿当权威。default-construct + 失败重置保强异常安全（无半赋值）。
    XtTradeRecord normalized;
    if (!xt_log::run_guarded("RT", "live.normalize", normalize_fail_ct_,
                             [&]() { normalized = normalize_trade_event(evt); })) {
        normalized = evt.value;   // passthrough raw：必须能走到下面 engine_.on_trade ingest
        // 兜底日志 best-effort：XtLogger::error_m 非 noexcept（module 前缀 std::string 分配可抛）。
        // 必须包 swallow guard，否则日志异常会逃到 worker-body backstop，
        // 在 fill 落账前停机并击穿 "passthrough 保 account ingest" 的兜底语义。
        xt_log::run_guarded("RT", "live.normalize.log", [&]() {
            XtLogger::error_m("RT",
                "normalize threw -> passthrough RAW fill (fee/PnL un-normalized) "
                "order_id={} code={} trade_id={}",
                evt.value.order_id, evt.value.code, evt.value.trade_id);
        });
    }

    int64_t realized_fp = 0;
    // 初值取 gateway 边界 stamping（Recovering 早退时 engine 不写回）；
    // engine 处理路径写回入口 sid 恢复后的归因终值。
    std::string effective_sid = normalized.strategy_id;
    // engine_.on_trade 抛异常由 run() worker-body backstop 接住升级停机（有意不在此 inner guard，
    // 避免与其 bool-return drop-signaling 语义纠缠）；此处只处理 bool-return。
    // latency_ineligible（gateway 边界盖章）→ elig，仅 gate observer latency feed。
    const XtLatencyEligibility trade_elig = evt.latency_ineligible
        ? XtLatencyEligibility::kIneligible
        : XtLatencyEligibility::kEligible;
    const bool engine_ok =
        engine_.on_trade(normalized, evt.td_state, &realized_fp, &effective_sid, trade_elig);

    // self-match 是 post-trade safety fallback：抛异常（detector emplace bad_alloc 或 blacklist_sink）
    // → fail-stop（run_strategy_guard 升级 stop/shutdown，null-safe）。命中不算异常，照常 blacklist+log。
    const bool self_match_ok =
        run_strategy_guard(StrategyGuardStage::SelfMatch, &stop_requested_, shutdown_flag_,
                           [&]() { check_post_trade_self_match(evt.value); });

    // record-then-stop：fill 已落账（on_trade 之上），无论 self_match 是否抛都先记该笔审计再停机，
    // 避免"状态已变却无 TRADE_RTN"的审计缺口。recording 抛异常 swallow + counter（telemetry 不停机）。
    xt_log::run_guarded("RT", "live.record_trade", record_fail_ct_,
                        [&]() { emit_trade_record(evt, normalized, realized_fp, effective_sid); });

    if (!engine_ok || !self_match_ok) {
        request_worker_stop();
        return false;
    }
    return true;
}

// ── Trade adaptation ──────────────────────────────────────────────
XtTradeRecord XtHftEventLoop::normalize_trade_event(const XtRunnerTradeEvent& evt) const {
    return (live_cost_ != nullptr)
        ? xt_fill_cost::normalize_trade_cost(evt.value, live_cost_, live_fees_, catalog_)
        : evt.value;
}

void XtHftEventLoop::check_post_trade_self_match(const XtTradeRecord& trade) {
    // 成交后自成交兜底读原始 TD 回报，不依赖 engine 内部持仓状态。
    if (self_match_detector_ && self_match_detector_->on_trade(trade)) {
        if (blacklist_sink_) {
            blacklist_sink_(trade.code.c_str(), "self_match_post_trade");
        }
        XtLogger::error_m("RT",
            "POST-TRADE SELF-MATCH detected code={} trade_id={} order_id={}",
            trade.code, trade.trade_id, trade.order_id);
    }
}

// ── Live recording ────────────────────────────────────────────────
void XtHftEventLoop::emit_order_update_record(const XtRunnerOrderUpdateEvent& evt) const {
    if (auto* rec = XtLiveRecorder::global()) {
        const uint64_t ts_local_ns = XtSteadyClock::steady_now_ns();
        char initiator_buf[16] = {};
        if (evt.value.status == XtOrderStatus::Canceled) {
            XtLiveRecorder::consume_cancel_initiator(evt.value.id, initiator_buf, sizeof(initiator_buf));
        }
        char err_buf[64] = {};
        if (evt.value.status == XtOrderStatus::Rejected) {
            XtLiveRecorder::consume_order_reject_err(evt.value.id, err_buf, sizeof(err_buf));
        }
        const char* sid_cstr = xt_identity::StrategySidRegistry::resolve_sid_cstr_or_null(evt.value.sid_id);
        rec->emit_order_rtn(ts_local_ns,
                            /*logical_id=*/evt.value.id,
                            /*phys_id=*/evt.value.id,
                            order_status_to_str(evt.value.status),
                            evt.value.filled_fp,
                            /*err_msg=*/err_buf,
                            initiator_buf,
                            /*strat_id=*/sid_cstr,
                            /*book_id=*/sid_cstr,    // recorder 暂以 sid 填充 book_id
                            /*account_id=*/nullptr);
    }
}

void XtHftEventLoop::emit_trade_record(const XtRunnerTradeEvent& evt,
                                       const XtTradeRecord& normalized,
                                       int64_t realized_fp,
                                       const std::string& effective_sid) const {
    if (auto* rec = XtLiveRecorder::global()) {
        const uint64_t ts_local_ns = XtSteadyClock::steady_now_ns();
        const int64_t price_fp = static_cast<int64_t>(normalized.exec_price * 1e6);
        const char* sid_cstr = effective_sid.empty()
                               ? nullptr : effective_sid.c_str();
        const char* route_status = evt.route_status.empty()
            ? (effective_sid.empty() ? "unmatched" : "routed")
            : evt.route_status.c_str();
        rec->emit_trade_rtn({
            .ts_local_ns       = ts_local_ns,
            .ts_exch_ns        = normalized.fill_ts_us * 1000ULL,
            .trading_day       = normalized.trading_day,
            .trade_action_date = normalized.trade_date,
            .trade_action_time = normalized.trade_time,
            .trade_id          = normalized.local_cb_us,
            .logical_id        = normalized.order_id,
            .code              = normalized.code.c_str(),
            .exchange_id       = normalized.exchange_id.c_str(),
            .side              = normalized.is_buy ? "buy" : "sell",
            .price_fp          = price_fp,
            .qty               = normalized.qty_fp,
            .is_maker          = normalized.is_passive,
            .strat_id          = sid_cstr,
            .book_id           = sid_cstr,
            .account_id        = nullptr,
            .fee_fp            = xt_fp::to_fp(normalized.fee),
            .realized_pnl_fp   = realized_fp,
            // Recovering 期 engine 不计算 realized；recorder 下游必须走自重算 fallback，
            // 不能把默认 0 标记为权威 realized PnL。
            .has_realized      = (evt.td_state != ITradeGateway::TdState::Recovering),
            .is_close          = normalized.is_close,
            .route_status      = route_status,
        });
    }
}

XtHftEventLoop::WorkerBudgets XtHftEventLoop::compute_worker_budgets_(const Config& cfg) {
    WorkerBudgets b;
    b.tick_batch_size = std::max<size_t>(1, std::min(cfg.tick_batch_size, kTickBatchMax));
    b.td_batch_size   = std::max<size_t>(1, std::min(cfg.td_event_batch_size, kTdEventBatchMax));
    b.flow_batch_size = std::max<size_t>(1, std::min(cfg.flow_event_batch_size, kFlowEventBatchMax));
    b.td_budget_per_loop = cfg.td_event_budget_per_loop > 0
                               ? cfg.td_event_budget_per_loop
                               : b.td_batch_size;
    b.flow_budget_per_loop = cfg.flow_event_budget_per_loop > 0
                                 ? cfg.flow_event_budget_per_loop
                                 : b.flow_batch_size;
    // 0 = 禁用 backlog 告警，与 cfg 注释及 high_watermark sentinel 语义一致；
    // 生产环境由 parser 默认值（= td_event_high_watermark）提供非零启用阈值。
    b.td_backlog_warn_threshold =
        cfg.td_backlog_warn_threshold > 0
            ? std::min(cfg.td_backlog_warn_threshold, cfg.td_event_queue_capacity)
            : 0;
    return b;
}

void XtHftEventLoop::warn_if_budgets_clamped_() const {
    if (budgets_.tick_batch_size != cfg_.tick_batch_size ||
        budgets_.td_batch_size != cfg_.td_event_batch_size ||
        budgets_.flow_batch_size != cfg_.flow_event_batch_size) {
        XtLogger::warn_m("RT",
                         "event batch size clamped tick={}/{} td={}/{} flow={}/{}",
                         cfg_.tick_batch_size,
                         budgets_.tick_batch_size,
                         cfg_.td_event_batch_size,
                         budgets_.td_batch_size,
                         cfg_.flow_event_batch_size,
                         budgets_.flow_batch_size);
    }
}

std::unique_ptr<XtHftEventLoop::WorkerRuntime> XtHftEventLoop::make_worker_runtime_() const {
    return std::make_unique<WorkerRuntime>();
}

void XtHftEventLoop::update_tick_pending_state(WorkerRuntime& st) {
    const uint64_t tick_backlog = tick_backlog_.load(std::memory_order_relaxed);
    if (tick_backlog > 0) {
        if (st.tick_pending_since_us == 0) st.tick_pending_since_us = XtSteadyClock::steady_now_us();
        return;
    }
    st.tick_pending_since_us = 0;
    st.tick_wait_loops = 0;
}

template <bool kCheckTickWait>
bool XtHftEventLoop::drain_runner_queue_([[maybe_unused]] WorkerRuntime& st,
                                         XtLockFreeQueue<XtHftRunnerEvent>& queue,
                                         size_t budget,
                                         XtHftRunnerEvent* batch_buf,
                                         size_t batch_size_cfg,
                                         std::atomic<uint64_t>& backlog_counter,
                                         const char* backlog_label,
                                         bool& progressed) {
    while (budget > 0) {
        if constexpr (kCheckTickWait) {
            // TD 让位：tick 等待超过 tick_max_wait_us 时中断本轮 TD drain，优先处理行情。
            if (cfg_.tick_max_wait_us > 0 && st.tick_pending_since_us > 0) {
                const uint64_t now = XtSteadyClock::steady_now_us();
                const uint64_t wait_us = now >= st.tick_pending_since_us ? (now - st.tick_pending_since_us) : 0;
                if (wait_us >= cfg_.tick_max_wait_us) break;
            }
        }
        const size_t batch = budget < batch_size_cfg ? budget : batch_size_cfg;
        const size_t n = queue.pop_bulk(batch_buf, batch);
        if (n == 0) break;
        progressed = true;
        budget -= n;
        for (size_t i = 0; i < n; ++i) {
            safe_decrement_backlog(backlog_counter, backlog_label);
            if (batch_buf[i].enqueue_ts_us != 0) {
                const uint64_t now = XtSteadyClock::steady_now_us();
                const uint64_t lag = now >= batch_buf[i].enqueue_ts_us
                                         ? (now - batch_buf[i].enqueue_ts_us) : 0;
                update_atomic_peak(td_flow_event_lag_us_max_, lag);
                td_flow_lag_hist_.record(lag);
            }
            if (!dispatch_strategy_event(batch_buf[i])) return false;
        }
    }
    return true;
}

bool XtHftEventLoop::process_td_events(WorkerRuntime& st, bool& progressed) {
    return drain_runner_queue_<true>(st, td_event_q_, budgets_.td_budget_per_loop,
                                     st.td_event_batch.data(), budgets_.td_batch_size,
                                     td_event_backlog_, "td_event", progressed);
}

void XtHftEventLoop::warn_td_backlog_if_needed(WorkerRuntime& st) {
    if (budgets_.td_backlog_warn_threshold == 0) return;
    const uint64_t td_backlog = td_event_backlog_.load(std::memory_order_relaxed);
    if (td_backlog < budgets_.td_backlog_warn_threshold) return;
    const uint64_t now = XtSteadyClock::steady_now_us();
    if (st.last_td_backlog_warn_us != 0 && now - st.last_td_backlog_warn_us < kWarnThrottleIntervalUs) return;
    st.last_td_backlog_warn_us = now;
    XtLogger::warn_m("RT",
                     "td backlog high inflight={} threshold={} tick_inflight={}",
                     td_backlog,
                     budgets_.td_backlog_warn_threshold,
                     tick_backlog_.load(std::memory_order_relaxed));
}

void XtHftEventLoop::warn_tick_starvation_if_needed(WorkerRuntime& st) {
    if (cfg_.tick_starvation_warn_loops == 0 || st.tick_wait_loops < cfg_.tick_starvation_warn_loops) return;
    const uint64_t now = XtSteadyClock::steady_now_us();
    if (st.last_tick_starve_warn_us != 0 && now - st.last_tick_starve_warn_us < kWarnThrottleIntervalUs) return;
    st.last_tick_starve_warn_us = now;
    const uint64_t wait_us = (st.tick_pending_since_us > 0 && now >= st.tick_pending_since_us)
                                 ? (now - st.tick_pending_since_us)
                                 : 0;
    XtLogger::warn_m("RT",
                     "tick starvation loops={} wait_us={} tick_inflight={} td_inflight={} td_budget={} tick_max_wait_us={}",
                     st.tick_wait_loops,
                     wait_us,
                     tick_backlog_.load(std::memory_order_relaxed),
                     td_event_backlog_.load(std::memory_order_relaxed),
                     budgets_.td_budget_per_loop,
                     cfg_.tick_max_wait_us);
}

uint64_t XtHftEventLoop::observe_tick_lag_us(const TickEvent& ev) {
    if (ev.enqueue_ts_us == 0) return 0;
    const uint64_t now = XtSteadyClock::steady_now_us();
    return now >= ev.enqueue_ts_us ? (now - ev.enqueue_ts_us) : 0;
}

void XtHftEventLoop::update_tick_observability(const XtTick& tick, uint32_t batch_steady_ms) const {
    if (tick.action_time && last_tick_action_time_) {
        last_tick_action_time_->store(tick.action_time, std::memory_order_relaxed);
    }
    if (last_tick_steady_ms_) {
        last_tick_steady_ms_->store(batch_steady_ms, std::memory_order_relaxed);
    }
}

void XtHftEventLoop::update_tick_session_flags(const XtTick& tick) {
    xt_live::update_session_gate_flags(tick.action_time, session_sections_,
                                       closeout_windows_, in_trading_section_,
                                       closeout_no_open_);
}

bool XtHftEventLoop::dispatch_tick_to_engine(const TickEvent& ev) {
    // ORDER_SUBMIT tap 只在 on_tick 调用窗口内读取该 TLS，非 tick 路径保持 0。
    // trigger 起点选择器：SHM 路径下用 connector capture（含 +IPC hop 的真 md_ingress 起点），
    // 直连 venue / capture 无效则 fallback 本进程 enqueue。
    // ── migration delta（SHM binance 路径生效，直连 venue capture=0 逐字不变）──────
    //   xt_strategy_us（trigger→submit_begin）与 event_to_ack/fill 的起点、以及 recorder
    //   ORDER_SUBMIT 的 trigger_tick_ts，从本地 enqueue 前移到 connector capture（多 +IPC hop）。
    //   → latency budget（strategy_us 阈值系本地 CPU-bound 口径）在 shm 路径部署后需重估（入债）。
    const uint64_t trigger_us = xt_live::select_trigger_anchor_us(ev.capture_ts_us, ev.enqueue_ts_us);
    xt_core::tap::set_current_tick_ts_ns(trigger_us * 1000ULL);
    // event clock re-anchor（BestEffort observability，不参与准入）：用行情
    // payload 的 action 对算 event 时刻，发布进程共享 offset，让 panel/
    // watchdog 等非 worker 线程的 submit 也能投影 event_ts。
    if (ev.core_tick.action_time != 0) {
        const uint32_t ad = ev.core_tick.action_date != 0 ? ev.core_tick.action_date
                                                          : ev.core_tick.trading_date;
        if (ad != 0) {
            xt_core::tap::publish_event_clock_anchor(
                xt_time::to_ns(ad, ev.core_tick.action_time),
                static_cast<int64_t>(XtSteadyClock::steady_now_ns()));
        }
    }
    const bool ok = run_strategy_guard(StrategyGuardStage::OnTick,
                                       &stop_requested_,
                                       shutdown_flag_,
                                       [&]() { engine_.on_tick(ev.core_tick); });
    xt_core::tap::set_current_tick_ts_ns(0);
    return ok;
}

bool XtHftEventLoop::process_ticks(WorkerRuntime& st, bool& progressed) {
    const size_t n = tick_q_.pop_bulk(st.tick_batch.data(), budgets_.tick_batch_size);

    // 队列为空：无积压时正常空转；有积压时计入等待轮次并按节流规则告警。
    if (n == 0) {
        if (tick_backlog_.load(std::memory_order_relaxed) == 0) return true;
        ++st.tick_wait_loops;
        warn_tick_starvation_if_needed(st);
        return true;
    }

    progressed = true;
    st.tick_pending_since_us = 0;
    st.tick_wait_loops = 0;

    // batch 内共享 stale_quote 心跳时间；该值只服务秒级 stale 检测，不是 tick 时间戳。
    const uint32_t batch_steady_ms = last_tick_steady_ms_
        ? static_cast<uint32_t>(XtSteadyClock::steady_now_us() / 1000) : 0;

    uint64_t batch_max_lag = 0;
    for (size_t i = 0; i < n; ++i) {
        const TickEvent& ev = st.tick_batch[i];
        safe_decrement_backlog(tick_backlog_, "tick");

        const uint64_t lag = observe_tick_lag_us(ev);
        if (lag > batch_max_lag) batch_max_lag = lag;
        // enqueue_ts_us==0 表示时间戳不可用，不喂直方图（lag=0µs 是有效样本，落桶 0）。
        if (ev.enqueue_ts_us != 0) tick_lag_hist_.record(lag);

        update_tick_observability(ev.core_tick, batch_steady_ms);
        update_tick_session_flags(ev.core_tick);
        if (!dispatch_tick_to_engine(ev)) return false;

        if (tick_proc_cnt_) tick_proc_cnt_->fetch_add(1, std::memory_order_relaxed);
    }

    if (batch_max_lag > 0) {
        update_atomic_peak(tick_lag_us_peak_, batch_max_lag);
    }
    return true;
}

bool XtHftEventLoop::process_flow_events(WorkerRuntime& st, bool& progressed) {
    return drain_runner_queue_<false>(st, flow_event_q_, budgets_.flow_budget_per_loop,
                                      st.flow_event_batch.data(), budgets_.flow_batch_size,
                                      flow_event_backlog_, "flow_event", progressed);
}

void XtHftEventLoop::drain_pending_queues() {
    TickEvent tick_ev;
    XtHftRunnerEvent sev;
    while (tick_q_.pop(tick_ev)) {}
    // td 队列：WorkerTask 注册闭包执行而非丢弃——兜住 producer 以 stale running_ 在
    // worker 收尾 sweep 之后才 push 成功的窄竞态窗（worker 已 join，本线程执行无并发
    // 写者；book 侧注册经 post_to_worker 撞 enqueue 拒 → 溢出侧道，由 runner
    // enter_quiescent 后 drain 落地）。其余事件维持既有 stop 丢弃语义。
    while (td_event_q_.pop(sev)) {
        if (const auto* ctrl = sev.get_if<XtRunnerControlEvent>()) {
            if (ctrl->type == XtRunnerControlEventType::WorkerTask) {
                const auto* p = ctrl->get_if<XtRunnerWorkerTaskEvent>();
                if (p && p->fn) {
                    // 回调守卫政策：闭包异常不得穿透 stop 调用线程；log 后吞掉继续。
                    try {
                        p->fn();
                    } catch (...) {
                        XtLogger::error_m("RT",
                            "drain_pending_queues WorkerTask closure threw; swallowed");
                    }
                }
            }
        }
    }
    while (flow_event_q_.pop(sev)) {}
    tick_backlog_.store(0, std::memory_order_relaxed);
    td_event_backlog_.store(0, std::memory_order_relaxed);
    flow_event_backlog_.store(0, std::memory_order_relaxed);
}

void XtHftEventLoop::run() {
    // ── worker-body backstop（对齐 CTA XtCtaLiveEventLoop.cpp:26）─────────────
    // guarded body 内任何逃逸异常都升级为 fail-stop，不让其逃出 worker 线程触发
    // std::terminate。覆盖：prologue 分配（make_worker_runtime_ bad_alloc）、affinity/
    // priority、engine 业务调用（on_trade/on_order_update）、account/control dispatch、
    // WorkerTask 闭包、tick-path helper、及将来新增的未守护调用。inner 已 guard 的
    // strategy 回调先被各自 run_strategy_guard 接住（不逃逸、不重复计 worker_body_ex_ct_）。
    // 注意：run_guarded 自身 noexcept，其 catch 内日志分配的 double-OOM terminate 面
    // 为所有 callsite 既有属性，本改动不 regress，不在本次 scope。
    const bool no_escape = xt_log::run_guarded("RT", "worker.body", worker_body_ex_ct_, [&]() {
    if (cfg_.worker_affinity >= 0) {
        ThreadUtil::set_current_thread_affinity(cfg_.worker_affinity);
        // 绑核后同步绑定 NUMA 本地内存分配，避免跨 NUMA 访问 100+ ns 额外延迟。
        ThreadUtil::bind_current_thread_numa(cfg_.worker_affinity);
    }
    if (!cfg_.worker_priority.empty()) {
        ThreadUtil::set_current_thread_priority(cfg_.worker_priority);
    }

    // 延迟采样：worker 线程体首部显式绑定一个确定性 SPSC producer slot，让本线程上的
    // on_submit_result/on_order_update/on_trade tap 走无锁 slot（未绑线程走 overflow）。
    if (latency_tracker_) latency_tracker_->bind_producer_slot();

    // engine_.start() 负责 multi-strategy fan-out；EventLoop 不直接访问单个 context。
    // 单策略 on_init 失败已在 engine 内 disable + 触发 per-sid latch 回调，report
    // 只作可观测补充（不改变 worker 继续跑的语义）。
    XtHftEngine::StartReport start_report;
    if (!run_strategy_guard(StrategyGuardStage::OnInit, &stop_requested_, shutdown_flag_,
                            [&]() { start_report = engine_.start(); })) return;
    if (!start_report.all_started) {
        XtLogger::warn_m("RT", "strategy start: {} context(s) fail-closed disabled",
                         start_report.failures.size());
    }
    if (initial_param_version_ > 0) {
        if (!run_strategy_guard(StrategyGuardStage::OnParams, &stop_requested_, shutdown_flag_,
                                [&]() { engine_.on_params_updated(initial_param_version_); })) {
            return;
        }
        XtLogger::info_m("RT", "initial params applied version={}", initial_param_version_);
    }

    // WorkerRuntime 含多个大型批次缓冲区，堆分配避免线程栈溢出。
    auto st = make_worker_runtime_();

    // 独占核心模式：配置开启且已绑核；提前固化，避免热循环内重复读 cfg_。
    const bool pure_spin = cfg_.enable_pure_spin && (cfg_.worker_affinity >= 0);

    int spin = 1;
    // stop_requested_ 通常为 false（运行态），__builtin_expect 引导分支预测器走
    // "继续运行"路径，减少循环头部的分支预测惩罚（branch misprediction penalty）。
    while (__builtin_expect(!stop_requested_.load(std::memory_order_acquire), 1)) {
        bool progressed = false;
        // off-worker 注册溢出侧道 drain：置于 process_td_events 之前 → 溢出注册整体先于
        // 本轮 td 队列回报被消费（保「注册先于回报」）。侧道空时 hook 内 O(1) 早退。
        if (marshal_overflow_drain_) marshal_overflow_drain_();
        update_tick_pending_state(*st);
        if (__builtin_expect(!process_td_events(*st, progressed), 0)) return;
        warn_td_backlog_if_needed(*st);
        if (__builtin_expect(!process_ticks(*st, progressed), 0)) return;
        if (__builtin_expect(!process_flow_events(*st, progressed), 0)) return;

        if (__builtin_expect(progressed, 1)) {
            // 有进展：重置 spin 计数器，立即进入下一轮
            spin = 1;
            continue;
        }

        // 空闲等待（idle backoff）
        if (pure_spin) {
            // 独占核心模式：单次 PAUSE 后立即重新检查队列。
            // 不调用 yield/sleep，降低空闲唤醒延迟。
            // CPU 占用率 100%（预期行为，该核心专用于此线程）。
            backoff_pause(1);
        } else {
            // 共享核心模式：指数退避，减少与其他线程的 CPU 竞争。
            // spin: 1 → 2 → 4 → ... → 64 次 PAUSE，之后 yield。
            // 每次退避最多执行 64 次 PAUSE，超出后 yield 让出 CPU 时间片。
            backoff_pause(spin);
            if (spin < 64) spin <<= 1;
            else std::this_thread::yield();
        }
    }
    });
    // guarded body 逃逸（engine/account/control/WorkerTask/prologue 分配等抛异常）→ fail-stop。
    // 干净路径（no_escape==true）：bool-false 早退已各自 request_worker_stop()/run_strategy_guard
    // 自置 stop，无需重复置位。request_worker_stop() null-safe。
    if (!no_escape) request_worker_stop();

    // stop 边沿收尾（仍在 worker 线程、join 前；guarded body 早退/异常逃逸路径同样覆盖）：
    // 把 td 队列里已入队成功但未被消费的 WorkerTask 注册闭包捞出直接执行（worker 单写者
    // 合法），其余事件维持既有 stop 丢弃语义（drain_pending_queues 统一清理）；随后清
    // 溢出侧道。stop 之后的新 post 被 enqueue 拒 → 溢出侧道，由 runner 在 join 后
    // enter_quiescent + drain_marshal_side_channel 收尾——注册全程不丢。
    {
        XtHftRunnerEvent ev;
        while (td_event_q_.pop(ev)) {
            safe_decrement_backlog(td_event_backlog_, "td_event_backlog");
            if (const auto* ctrl = ev.get_if<XtRunnerControlEvent>()) {
                if (ctrl->type == XtRunnerControlEventType::WorkerTask) {
                    const auto* p = ctrl->get_if<XtRunnerWorkerTaskEvent>();
                    if (p && p->fn) {
                        // 回调守卫政策：闭包异常不得逃逸 worker 线程主函数；log 后吞掉继续。
                        try {
                            p->fn();
                        } catch (...) {
                            XtLogger::error_m("RT",
                                "stop-edge WorkerTask closure threw; swallowed");
                        }
                    }
                }
            }
        }
    }
    if (marshal_overflow_drain_) marshal_overflow_drain_();
}
