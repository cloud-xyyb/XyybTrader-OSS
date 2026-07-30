#include "xt_live/hft/runtime/XtHftChannels.h"

#include <cassert>

#include "XtLogger.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_live/telemetry/latency/XtLiveLatencyTracker.h"
#include "xt_util/time/XtSteadyClock.h"
#include "xt_account/api/IAccountTruthSink.h"
#include "XtFixedPoint.h"

XtHftChannels::XtHftChannels(IMdGateway* md_channel,
                               ITradeGateway* td_gateway,
                               XtHftEventLoop* event_loop,
                               XtRunnerSignals* shared_state,
                               class XtLiveLatencyTracker* latency_tracker,
                               std::function<const char*(uint64_t, ITradeGateway::TdState)> route_status_resolver)
    : md_(md_channel),
      td_(td_gateway),
      event_loop_(event_loop),
      shared_state_(shared_state),
      latency_tracker_(latency_tracker),
      route_status_resolver_(std::move(route_status_resolver)) {}

void XtHftChannels::wire() {
    IMdGateway::Callbacks md_cb;
    md_cb.on_tick         = [this](const XtRawTick& tick, uint64_t capture_ns) { on_md_tick(tick, capture_ns); };
    md_cb.on_order_queue  = [this](const XtOrderQueue& oq)    { on_md_order_queue(oq); };
    md_cb.on_order_detail = [this](const XtOrderDetail& od)   { on_md_order_detail(od); };
    md_cb.on_transaction  = [this](const XtTransaction& tr)   { on_md_transaction(tr); };
    md_cb.on_front        = [this](bool c, int r)                   { on_md_front(c, r); };
    md_cb.on_ready        = [this]()                                { on_md_ready(); };
    md_->bind_callbacks(std::move(md_cb));

    ITradeGateway::Callbacks td_cb;
    td_cb.on_order_update     = [this](const XtOrderExecUpdate& upd, XtLatencyEligibility elig) { on_td_order_update(upd, elig); };
    td_cb.on_account          = [this](const ITradeGateway::AccountSnapshot& snap)             { on_td_account_snapshot(snap); };
    td_cb.on_position         = [this](const std::vector<ITradeGateway::PositionSnapshot>& ps) {
        on_td_position_snapshot(ps, td_->state(),  false);
    };
    td_cb.on_recovery_position = [this](
        const std::vector<ITradeGateway::PositionSnapshot>& ps, bool is_cold_start) {
        on_td_position_snapshot(ps, ITradeGateway::TdState::Recovering, is_cold_start);
    };
    td_cb.on_broker_account_truth = [this](const BrokerVenueTruth& bt)                          { on_td_broker_account_truth(bt); };
    td_cb.on_trade            = [this](const XtTradeRecord& tr, XtLatencyEligibility elig)     { on_td_trade(tr, td_->state(), elig); };
    td_cb.on_daily_settlement = [this](const ITradeGateway::DailySettlementDelta& ev)          { on_td_daily_settlement(ev); };
    td_cb.on_cash_transfer    = [this](const ITradeGateway::CashTransferDelta& ev)             { on_td_cash_transfer(ev); };
    td_cb.on_ready            = [this]()                                                       { on_td_ready(); };
    td_cb.on_front            = [this](bool c, int r)                                          { on_td_front(c, r); };

    td_cb.on_transport_ack = [this](uint64_t local_id, uint64_t ack_us) {
        if (latency_tracker_) latency_tracker_->record_raw_trade_ack(local_id, ack_us);
    };
    td_cb.on_uds_arrival = [this](uint64_t local_id, uint64_t uds_ts_us) {
        if (latency_tracker_) latency_tracker_->record_raw_uds_arrival(local_id, uds_ts_us);
    };
    td_cb.on_transport_ack_unavailable = [this](uint64_t local_id, uint64_t mark_us) {
        if (latency_tracker_) latency_tracker_->record_raw_trade_ack_unavailable(local_id, mark_us);
    };
    td_cb.on_reverse_sync_batch = [this](const std::vector<AliveInflightOrder>& orders, bool is_cold_start) {
        on_td_reverse_sync_batch(orders, is_cold_start);
    };
    td_->bind_callbacks(std::move(td_cb));
}

void XtHftChannels::on_td_broker_account_truth(const BrokerVenueTruth& bt) {

    if (auto* sink = truth_sink_.load(std::memory_order_acquire)) {
        sink->update_broker_overlay(bt);
    }
}

bool XtHftChannels::start() {
    if (!md_ || !td_ || !event_loop_ || !shared_state_) {
        XtLogger::error_m("RT", "live channels start failed: invalid dependencies");
        return false;
    }

    if (!md_->start()) {
        XtLogger::error_m("RT", "md start failed");
        return false;
    }

    if (!td_->start()) {
        XtLogger::error_m("RT", "td start failed");
        td_->stop();
        md_->stop();
        return false;
    }
    XtLogger::info_m("RT", "td started");
    return true;
}

void XtHftChannels::stop() {
    shared_state_->md_ready.store(false, std::memory_order_release);
    shared_state_->td_ready.store(false, std::memory_order_release);
    shared_state_->td_position_ready.store(false, std::memory_order_release);
    shared_state_->position_snapshot_applied.store(false, std::memory_order_release);
    shared_state_->refresh_trading_ready();
    md_->stop();
    td_->stop();
}

namespace {
constexpr int64_t kDropWarnThrottleMs = 60'000;
int64_t steady_now_ms() {
    return static_cast<int64_t>(XtSteadyClock::steady_now_ms());
}

enum class TdDropReason : uint8_t {
    Position = 0,
    Control,
    Other,
};

TdDropReason classify_td_drop(const XtHftRunnerEvent& ev) {
    if (auto* acc = ev.get_if<XtRunnerAccountEvent>()) {
        if (std::holds_alternative<XtRunnerPositionSnapshotEvent>(acc->payload)) return TdDropReason::Position;
        return TdDropReason::Other;
    }
    if (auto* ctrl = ev.get_if<XtRunnerControlEvent>()) {

        if (ctrl->type == XtRunnerControlEventType::ChannelReady ||
            ctrl->type == XtRunnerControlEventType::ChannelLost ||
            ctrl->type == XtRunnerControlEventType::ReverseSyncBatch) {
            return TdDropReason::Control;
        }
        return TdDropReason::Other;
    }
    return TdDropReason::Other;
}

void increment_td_drop_counter(TdDropReason reason, XtRunnerSignals& state) {
    switch (reason) {
        case TdDropReason::Position: state.td_drop_position_cnt.fetch_add(1, std::memory_order_relaxed); break;
        case TdDropReason::Control:  state.td_drop_control_cnt.fetch_add(1, std::memory_order_relaxed);  break;
        case TdDropReason::Other:
        default:                     state.td_drop_other_cnt.fetch_add(1, std::memory_order_relaxed);    break;
    }
}
}

bool XtHftChannels::enqueue_td_event(XtHftRunnerEvent ev) {
    assert(event_loop_);
    const bool critical = ev.is_critical();

    const TdDropReason drop_reason = classify_td_drop(ev);

    auto record_drop = [&]() {
        shared_state_->td_event_drop_cnt.fetch_add(1, std::memory_order_relaxed);
        if (critical) {
            shared_state_->critical_td_event_drop_cnt.fetch_add(1, std::memory_order_relaxed);
        }

        increment_td_drop_counter(drop_reason, *shared_state_);

        ++td_drop_burst_total_;
        const int64_t now_ms = steady_now_ms();
        if (td_drop_last_warn_ms_ == 0 || now_ms - td_drop_last_warn_ms_ >= kDropWarnThrottleMs) {
            const char* mark = (td_drop_last_warn_ms_ == 0) ? "FIRST" : "BURST";
            XtLogger::warn_m("RT", "td_event drop {} burst_total={} critical={} (60s throttle)",
                              mark, td_drop_burst_total_, critical ? 1 : 0);
            td_drop_last_warn_ms_ = now_ms;
            td_drop_burst_total_ = 0;
        }
    };

    if (!ev.must_not_drop()) {

        if (td_handoff_.in_store_and_forward()) {
            drain_td_handoff_();
        }
        if (event_loop_->push_td_event(std::move(ev))) {
            return true;
        }
        record_drop();
        return false;
    }

    auto push = [this](XtHftRunnerEvent& e) {
        XtHftRunnerEvent probe = e;
        return event_loop_->push_td_event(std::move(probe));
    };
    using HandoffOffer = xt_live::TdHandoffRetryBuffer<XtHftRunnerEvent>::Offer;
    switch (td_handoff_.offer(std::move(ev), push)) {
        case HandoffOffer::kEnqueued:
            return true;
        case HandoffOffer::kBuffered: {

            ++td_handoff_burst_total_;
            const int64_t now_ms = steady_now_ms();
            if (td_handoff_last_warn_ms_ == 0 ||
                now_ms - td_handoff_last_warn_ms_ >= kDropWarnThrottleMs) {
                const auto st = td_handoff_.stats();
                XtLogger::warn_m("RT",
                    "td_event handoff buffered (store-and-forward) burst={} depth={} "
                    "redelivered_total={} (60s throttle)",
                    td_handoff_burst_total_, st.depth, st.redelivered_total);
                td_handoff_last_warn_ms_ = now_ms;
                td_handoff_burst_total_ = 0;
            }
            return true;
        }
        case HandoffOffer::kOverflow:
        default:

            record_drop();
            return false;
    }
}

size_t XtHftChannels::drain_td_handoff_buffer() {
    return drain_td_handoff_();
}

size_t XtHftChannels::drain_td_handoff_() {
    const size_t n = td_handoff_.try_drain([this](XtHftRunnerEvent& e) {

        XtHftRunnerEvent probe = e;
        return event_loop_->push_td_event(std::move(probe));
    });
    if (n > 0 && !td_handoff_.in_store_and_forward()) {
        XtLogger::info_m("RT", "td_event handoff drained {} event(s), store-and-forward exit", n);
    }
    return n;
}

bool XtHftChannels::enqueue_flow_event(XtHftRunnerEvent ev) {
    assert(event_loop_);
    if (event_loop_->push_flow_event(std::move(ev))) {
        return true;
    }
    shared_state_->flow_event_drop_cnt.fetch_add(1, std::memory_order_relaxed);

    ++flow_drop_burst_total_;
    const int64_t now_ms = steady_now_ms();
    if (flow_drop_last_warn_ms_ == 0 || now_ms - flow_drop_last_warn_ms_ >= kDropWarnThrottleMs) {
        const char* mark = (flow_drop_last_warn_ms_ == 0) ? "FIRST" : "BURST";
        XtLogger::warn_m("RT", "flow_event drop {} burst_total={} (60s throttle)",
                          mark, flow_drop_burst_total_);
        flow_drop_last_warn_ms_ = now_ms;
        flow_drop_burst_total_ = 0;
    }
    return false;
}

void XtHftChannels::publish_channel_event(std::atomic<bool>* ready_flag,
                                         bool ready,
                                         const char* channel,
                                         int reason) {

    const bool prev = ready_flag->exchange(ready, std::memory_order_acq_rel);
    if (prev == ready) {
        return;
    }
    shared_state_->refresh_trading_ready();
    XtHftRunnerEvent ev = ready
                           ? XtHftRunnerEvent::make_channel_ready(channel)
                           : XtHftRunnerEvent::make_channel_lost(channel, reason);
    (void)enqueue_td_event(std::move(ev));
    if (ready) {
        XtLogger::info_m("RT", "{} ready", channel);
    }
}

void XtHftChannels::on_md_tick(const XtRawTick& tick, uint64_t capture_ns) {
    assert(shared_state_ && event_loop_);
    shared_state_->md_in_cnt.fetch_add(1, std::memory_order_relaxed);

    if (tick.action_date) shared_state_->last_tick_date.store(tick.action_date, std::memory_order_release);
    if (tick.action_time) shared_state_->last_tick_time.store(tick.action_time, std::memory_order_release);
    if (!event_loop_->try_push_tick(tick, capture_ns)) {
        shared_state_->drop_cnt.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    shared_state_->enq_cnt.fetch_add(1, std::memory_order_relaxed);
}

void XtHftChannels::on_md_order_queue(const XtOrderQueue& oq) {
    (void)enqueue_flow_event(XtHftRunnerEvent::make_order_queue(oq));
}

void XtHftChannels::on_md_order_detail(const XtOrderDetail& od) {
    (void)enqueue_flow_event(XtHftRunnerEvent::make_order_detail(od));
}

void XtHftChannels::on_md_transaction(const XtTransaction& tr) {
    (void)enqueue_flow_event(XtHftRunnerEvent::make_transaction(tr));
}

void XtHftChannels::on_md_front(bool connected, int reason) {
    if (!connected) {
        publish_channel_event(&shared_state_->md_ready, false, "MD", reason);
    }
}

void XtHftChannels::on_md_ready() {
    publish_channel_event(&shared_state_->md_ready, true, "MD", 0);
}

void XtHftChannels::on_td_order_update(const XtOrderExecUpdate& upd, XtLatencyEligibility elig) {

    (void)enqueue_td_event(XtHftRunnerEvent::make_order_update(
        upd, elig == XtLatencyEligibility::kIneligible));
}

void XtHftChannels::on_td_account_snapshot(const ITradeGateway::AccountSnapshot& snap) {
    (void)enqueue_td_event(XtHftRunnerEvent::make_account_snapshot(snap));
}

void XtHftChannels::on_td_position_snapshot(const std::vector<ITradeGateway::PositionSnapshot>& positions,
                                             ITradeGateway::TdState td_state,
                                             bool is_cold_start) {

    (void)enqueue_td_event(XtHftRunnerEvent::make_position_snapshot(
        positions, td_state, is_cold_start,
        shared_state_->td_session_gen.load(std::memory_order_acquire)));
}

void XtHftChannels::on_td_trade(const XtTradeRecord& tr, ITradeGateway::TdState td_state,
                                XtLatencyEligibility elig) {

    if (auto* sink = truth_sink_.load(std::memory_order_acquire); sink && tr.fee != 0.0) {
        sink->note_fill_fee_fp(xt_fp::to_fp(tr.fee), tr.fill_ts_us * 1000ULL);
    }

    std::string route_status;
    if (route_status_resolver_) {
        if (const char* status = route_status_resolver_(tr.order_id, td_state)) {
            route_status = status;
        }
    }
    (void)enqueue_td_event(XtHftRunnerEvent::make_trade(
        tr, td_state, std::move(route_status), elig == XtLatencyEligibility::kIneligible));
}

void XtHftChannels::on_td_reverse_sync_batch(const std::vector<AliveInflightOrder>& orders,
                                              bool is_cold_start) {

    if (stp_rearm_sink_) stp_rearm_sink_(orders, is_cold_start);

    (void)enqueue_td_event(XtHftRunnerEvent::make_reverse_sync_batch(
        std::vector<AliveInflightOrder>(orders), is_cold_start));
}

void XtHftChannels::on_td_daily_settlement(const ITradeGateway::DailySettlementDelta& ev) {
    (void)enqueue_td_event(XtHftRunnerEvent::make_daily_settlement(ev));
}

void XtHftChannels::on_td_cash_transfer(const ITradeGateway::CashTransferDelta& ev) {
    (void)enqueue_td_event(XtHftRunnerEvent::make_cash_transfer(ev));
}

void XtHftChannels::on_td_ready() {

    publish_channel_event(&shared_state_->td_ready, true, "TD", 0);
}

void XtHftChannels::on_td_front(bool connected, int reason) {
    if (!connected) {
        shared_state_->td_session_gen.fetch_add(1, std::memory_order_release);
        shared_state_->td_position_ready.store(false, std::memory_order_release);
        shared_state_->position_snapshot_applied.store(false, std::memory_order_release);
        publish_channel_event(&shared_state_->td_ready, false, "TD", reason);
    }
}
