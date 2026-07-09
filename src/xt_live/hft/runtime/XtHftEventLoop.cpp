#include "xt_live/hft/runtime/XtHftEventLoop.h"

#include <algorithm>
#include <array>
#include <exception>
#include <stdexcept>
#include <utility>

#include "recording/XtLiveRecorder.h"
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
#include "xt_util/time/XtTime.h"

#if defined(__x86_64__)
#include <immintrin.h>
#endif

namespace {

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

thread_local uint64_t tl_cached_enqueue_ts_us = 0;
thread_local uint64_t tl_cached_enqueue_ms = 0;

[[nodiscard]] uint64_t cached_enqueue_ts_us() {
    const uint64_t now = XtSteadyClock::steady_now_us();
    const uint64_t now_ms = now / 1000;
    if (now_ms != tl_cached_enqueue_ms) {
        tl_cached_enqueue_ms = now_ms;
        tl_cached_enqueue_ts_us = now;
    }
    return tl_cached_enqueue_ts_us;
}

void update_atomic_peak(std::atomic<uint64_t>& peak_atomic, uint64_t value) {
    uint64_t peak = peak_atomic.load(std::memory_order_relaxed);
    while (value > peak &&
           !peak_atomic.compare_exchange_weak(
               peak, value, std::memory_order_relaxed, std::memory_order_relaxed)) {}
}

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

}

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
    if (reject_enqueue_guard(cfg_.tick_high_watermark, tick_backlog_)) {
        return false;
    }
    TickEvent ev;
    ev.core_tick = xt_tick_convert::to_core(tick);
    ev.enqueue_ts_us = cached_enqueue_ts_us();

    tick_backlog_.fetch_add(1, std::memory_order_relaxed);
    if (!tick_q_.push(ev)) {
        tick_backlog_.fetch_sub(1, std::memory_order_relaxed);
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

uint64_t XtHftEventLoop::drain_tick_lag_p99_us() {
    return tick_lag_us_p99_.exchange(0, std::memory_order_relaxed);
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
    const std::size_t n = engine_.context_count();
    out.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        auto* c = engine_.context(i);
        if (!c) continue;
        out.emplace_back(c->strategy_id(), c->drain_strategy_metrics());
    }
    return out;
}

uint64_t XtHftEventLoop::engine_dropped_fill_count() const noexcept {
    return engine_.dropped_fill_count();
}
uint64_t XtHftEventLoop::engine_dropped_order_update_count() const noexcept {
    return engine_.dropped_order_update_count();
}
uint64_t XtHftEventLoop::engine_orphan_reap_cancel_count() const noexcept {
    return engine_.orphan_reap_cancel_count();
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
    if (reject_enqueue_guard(high_watermark, backlog_counter, no_drop)) {
        return false;
    }
    if (ev.enqueue_ts_us == 0) {
        ev.enqueue_ts_us = XtSteadyClock::steady_now_us();
    }

    const uint64_t reserved = backlog_counter.fetch_add(1, std::memory_order_relaxed) + 1;
    update_atomic_peak(peak_backlog_counter, reserved);
    if (!queue.push(std::move(ev))) {
        backlog_counter.fetch_sub(1, std::memory_order_relaxed);
        if (no_drop) {

            XtLogger::error_m("RT",
                "CRITICAL runner event DROPPED (physical queue full); backlog={} watermark={} "
                "— phantom/missed-fill risk",
                backlog_counter.load(std::memory_order_relaxed), high_watermark);
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

        if (const auto* p = control.get_if<ReverseSyncBatchEventPayload>()) {
            engine_.on_reverse_sync_batch(p->orders, p->is_cold_start);
        }
        return true;
    case XtRunnerControlEventType::ReapRecoveredOrphans:

        if (const auto* p = control.get_if<XtRunnerReapOrphansEvent>()) {
            (void)engine_.reap_recovered_orphans(p->now_ms, p->grace_ms, p->max_cancels);

            if (p->reconcile_lost_terminals) {
                (void)engine_.reconcile_lost_terminals(p->max_cancels);
            }
        }
        return true;
    case XtRunnerControlEventType::WorkerTask:

        if (const auto* p = control.get_if<XtRunnerWorkerTaskEvent>()) {
            if (p->fn) p->fn();
        }
        return true;
    }
    return true;
}

bool XtHftEventLoop::handle_exec_order_update_(const XtRunnerOrderUpdateEvent& evt) {

    xt_log::run_guarded("RT", "live.record_order", record_fail_ct_,
                        [&]() { emit_order_update_record(evt); });

    if (!engine_.on_order_update(evt.value)) {
        request_worker_stop();
        return false;
    }
    return true;
}

bool XtHftEventLoop::handle_exec_trade_(const XtRunnerTradeEvent& evt) {

    XtTradeRecord normalized;
    if (!xt_log::run_guarded("RT", "live.normalize", normalize_fail_ct_,
                             [&]() { normalized = normalize_trade_event(evt); })) {
        normalized = evt.value;

        xt_log::run_guarded("RT", "live.normalize.log", [&]() {
            XtLogger::error_m("RT",
                "normalize threw -> passthrough RAW fill (fee/PnL un-normalized) "
                "order_id={} code={} trade_id={}",
                evt.value.order_id, evt.value.code, evt.value.trade_id);
        });
    }

    int64_t realized_fp = 0;

    std::string effective_sid = normalized.strategy_id;

    const bool engine_ok =
        engine_.on_trade(normalized, evt.td_state, &realized_fp, &effective_sid);

    const bool self_match_ok =
        run_strategy_guard(StrategyGuardStage::SelfMatch, &stop_requested_, shutdown_flag_,
                           [&]() { check_post_trade_self_match(evt.value); });

    xt_log::run_guarded("RT", "live.record_trade", record_fail_ct_,
                        [&]() { emit_trade_record(evt, normalized, realized_fp, effective_sid); });

    if (!engine_ok || !self_match_ok) {
        request_worker_stop();
        return false;
    }
    return true;
}

XtTradeRecord XtHftEventLoop::normalize_trade_event(const XtRunnerTradeEvent& evt) const {
    return (live_cost_ != nullptr)
        ? xt_fill_cost::normalize_trade_cost(evt.value, live_cost_, live_fees_, catalog_)
        : evt.value;
}

void XtHftEventLoop::check_post_trade_self_match(const XtTradeRecord& trade) {

    if (self_match_detector_ && self_match_detector_->on_trade(trade)) {
        if (blacklist_sink_) {
            blacklist_sink_(trade.code.c_str(), "self_match_post_trade");
        }
        XtLogger::error_m("RT",
            "POST-TRADE SELF-MATCH detected code={} trade_id={} order_id={}",
            trade.code, trade.trade_id, trade.order_id);
    }
}

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
                             evt.value.id,
                             evt.value.id,
                            order_status_to_str(evt.value.status),
                            evt.value.filled_fp,
                             err_buf,
                            initiator_buf,
                             sid_cstr,
                             sid_cstr,
                             nullptr);
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

    xt_core::tap::set_current_tick_ts_ns(ev.enqueue_ts_us * 1000ULL);
    const bool ok = run_strategy_guard(StrategyGuardStage::OnTick,
                                       &stop_requested_,
                                       shutdown_flag_,
                                       [&]() { engine_.on_tick(ev.core_tick); });
    xt_core::tap::set_current_tick_ts_ns(0);
    return ok;
}

bool XtHftEventLoop::process_ticks(WorkerRuntime& st, bool& progressed) {
    const size_t n = tick_q_.pop_bulk(st.tick_batch.data(), budgets_.tick_batch_size);

    if (n == 0) {
        if (tick_backlog_.load(std::memory_order_relaxed) == 0) return true;
        ++st.tick_wait_loops;
        warn_tick_starvation_if_needed(st);
        return true;
    }

    progressed = true;
    st.tick_pending_since_us = 0;
    st.tick_wait_loops = 0;

    const uint32_t batch_steady_ms = last_tick_steady_ms_
        ? static_cast<uint32_t>(XtSteadyClock::steady_now_us() / 1000) : 0;

    uint64_t batch_max_lag = 0;
    for (size_t i = 0; i < n; ++i) {
        const TickEvent& ev = st.tick_batch[i];
        safe_decrement_backlog(tick_backlog_, "tick");

        const uint64_t lag = observe_tick_lag_us(ev);
        if (lag > batch_max_lag) batch_max_lag = lag;

        update_tick_observability(ev.core_tick, batch_steady_ms);
        update_tick_session_flags(ev.core_tick);
        if (!dispatch_tick_to_engine(ev)) return false;

        if (tick_proc_cnt_) tick_proc_cnt_->fetch_add(1, std::memory_order_relaxed);
    }

    if (batch_max_lag > 0) {
        update_atomic_peak(tick_lag_us_p99_, batch_max_lag);
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
    while (td_event_q_.pop(sev)) {}
    while (flow_event_q_.pop(sev)) {}
    tick_backlog_.store(0, std::memory_order_relaxed);
    td_event_backlog_.store(0, std::memory_order_relaxed);
    flow_event_backlog_.store(0, std::memory_order_relaxed);
}

void XtHftEventLoop::run() {

    const bool no_escape = xt_log::run_guarded("RT", "worker.body", worker_body_ex_ct_, [&]() {
    if (cfg_.worker_affinity >= 0) {
        ThreadUtil::set_current_thread_affinity(cfg_.worker_affinity);

        ThreadUtil::bind_current_thread_numa(cfg_.worker_affinity);
    }
    if (!cfg_.worker_priority.empty()) {
        ThreadUtil::set_current_thread_priority(cfg_.worker_priority);
    }

    if (!run_strategy_guard(StrategyGuardStage::OnInit, &stop_requested_, shutdown_flag_,
                            [&]() { engine_.on_init(); })) return;
    if (initial_param_version_ > 0) {
        if (!run_strategy_guard(StrategyGuardStage::OnParams, &stop_requested_, shutdown_flag_,
                                [&]() { engine_.on_params_updated(initial_param_version_); })) {
            return;
        }
        XtLogger::info_m("RT", "initial params applied version={}", initial_param_version_);
    }

    auto st = make_worker_runtime_();

    const bool pure_spin = cfg_.enable_pure_spin && (cfg_.worker_affinity >= 0);

    int spin = 1;

    while (__builtin_expect(!stop_requested_.load(std::memory_order_acquire), 1)) {
        bool progressed = false;
        update_tick_pending_state(*st);
        if (__builtin_expect(!process_td_events(*st, progressed), 0)) return;
        warn_td_backlog_if_needed(*st);
        if (__builtin_expect(!process_ticks(*st, progressed), 0)) return;
        if (__builtin_expect(!process_flow_events(*st, progressed), 0)) return;

        if (__builtin_expect(progressed, 1)) {

            spin = 1;
            continue;
        }

        if (pure_spin) {

            backoff_pause(1);
        } else {

            backoff_pause(spin);
            if (spin < 64) spin <<= 1;
            else std::this_thread::yield();
        }
    }
    });

    if (!no_escape) request_worker_stop();
}
