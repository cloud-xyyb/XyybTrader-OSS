#include "xt_identity/OrderAttributionStore.h"
#include "TraderBinanceUGatewayImpl.h"

#include <utility>

#include "transport/BinanceTdWireUtil.h"
#include "transport/BinanceIntentTrace.h"
#include "refdata/BinanceExchangeInfo.h"
#include "xt_logger/XtLogger.h"
#include "xt_live/telemetry/recording/XtLiveRecorder.h"
#ifdef XT_HAVE_METRICS
#include <string>
#include "xt_metrics/XtMetricRegistry.h"
#endif

namespace binance_u {

uint64_t TraderBinanceUGateway::Impl::submit_order_inq(
    const char* code, bool is_buy,
    int64_t price_fp, int64_t qty_fp,
    XtTimeInForce tif, const char*  ,
    XtOffset close_offset, ExecFlags exec_flags,
    uint64_t intent_trace_id) {
	    const StrategySid owner_sid = xt_identity::OrderAttributionStore::consume_thread_submit_strategy_sid();
	    const uint64_t local_id = local_id_gen_->next();

    if (const uint64_t pf = local_id_gen_->persist_fail_count();
        pf != local_id_persist_fail_seen_.load(std::memory_order_relaxed)) {
        XtLogger::warn_m("BinanceTD",
                         "local-id persist 失败 (count={}); crash 后有 id reissue 风险", pf);
        local_id_persist_fail_seen_.store(pf, std::memory_order_relaxed);
    }
    xt_identity::OrderAttributionStore::remember_order_strategy_sid(local_id, owner_sid);
    const uint64_t nonce = nonce_ctr.fetch_add(1, std::memory_order_relaxed);
    const uint64_t t0_us = lat_now_us();
    const std::string symbol = code ? code : "";
    std::string client_oid = make_client_order_id(local_id, nonce);

    {
        BinanceOrderLedger::OrderRecord rec;
        rec.client_order_id = client_oid;
        rec.symbol = symbol;
        rec.trace_id = intent_trace_id;
        rec.status = XtOrderStatus::Submitted;
        rec.is_buy = is_buy;
        rec.price_fp = price_fp;
        rec.qty_fp = qty_fp;
        rec.close_offset = close_offset;
        rec.submit_ts_ms = lat_now_us() / 1000;
        ledger_.register_submit(local_id, std::move(rec));
    }
    tracked_symbols_.note(symbol);

    if (lat_t0_log_seq_++ % kLatTraceLogSampleN == 0) {
        XtLogger::info_m("BinanceLat", "kind=t0 local_id={} ts_us={} sampled={}",
                         local_id, t0_us, kLatTraceLogSampleN);
    }
    TransportSubmitReq req;
    req.local_id = local_id;
    req.client_order_id = std::move(client_oid);
    req.symbol = symbol;
    req.is_buy = is_buy;
    req.price_fp = price_fp;
    req.qty_fp = qty_fp;
    req.tif = tif;
    req.close_offset = close_offset;
	    req.exec_flags = exec_flags;
	    req.t0_us = t0_us;
	    req.trace_id = intent_trace_id;

	    if (!binance_filters_loaded(symbol)) {
	        XtLogger::warn_m("BinanceTD",
	                         "submit rejected: venue filters not loaded symbol={} local_id={}",
	                         symbol, local_id);
	        emit_wire_reject_trace(req,
	                               "filters_not_loaded",
	                               "binance venue filters not loaded");
	        TransportAck ack;
        ack.kind = TransportAck::Kind::kRejected;
        ack.local_id = local_id;
        ack.err_msg = "binance venue filters not loaded: " + symbol;
        TransportLatencyTrace tr;
        tr.local_id = local_id;
        on_transport_ack(ack, tr);
        return local_id;
    }
    if (td_transport) td_transport->submit(std::move(req));
    return local_id;
}

bool TraderBinanceUGateway::Impl::cancel_order_inq(uint64_t local_id) {
    std::string client_order_id = ledger_.client_order_id_for(local_id);
    if (client_order_id.empty()) {
        return false;
    }
    std::string symbol = ledger_.symbol_for(local_id);
    TransportCancelReq req;
    req.local_id = local_id;
    req.client_order_id = std::move(client_order_id);
    req.symbol = std::move(symbol);
    req.t0_us = lat_now_us();
    if (td_transport) td_transport->cancel(std::move(req));
    return true;
}

void TraderBinanceUGateway::Impl::emit_through_dedup(
    const XtOrderExecUpdate& upd,
    EffectiveSignalDedup::Source src,
    XtLatencyEligibility elig) {
    if (!td_cfg.uds_first_effective_ack) {
        if (cb.on_order_update) cb.on_order_update(upd, elig);
        return;
    }

    bool should_emit = false;
    {
        std::lock_guard<std::mutex> lk(signal_state_mu_);
        should_emit = signal_dedup_.note_signal(
            upd.id, src, upd.status, upd.filled_fp,
            upd.local_cb_us ? upd.local_cb_us : lat_now_us());
    }
    if (should_emit && cb.on_order_update) cb.on_order_update(upd, elig);
}

void TraderBinanceUGateway::Impl::on_uds_order_update(const XtOrderExecUpdate& upd) {

    if (cb.on_uds_arrival && upd.id != 0 && upd.local_cb_us > 0) {
        cb.on_uds_arrival(upd.id, upd.local_cb_us);
    }

    const auto decision = ledger_.on_uds_terminal(upd);
    if (decision.action != BinanceOrderLedger::TerminalDecision::Action::kEmit) return;

    reconciler_.forward_order_update_gated(upd, EffectiveSignalDedup::Source::kUds,
                                           XtLatencyEligibility::kEligible);
}

void TraderBinanceUGateway::Impl::handle_rejected_ack(const TransportAck& ack,
                                                      const XtOrderExecUpdate& upd) {

    {
        const uint64_t rej_now_us = lat_now_us();
        if (ack.err_code != last_reject_err_code_logged_ ||
            rej_now_us - last_reject_log_us_ >= 5'000'000ULL) {
            XtLogger::warn_m("BinanceTD",
                "order REJECTED local_id={} err_code={} raw_status='{}' err_msg='{}' "
                "suppressed_since_last={}",
                ack.local_id, ack.err_code, ack.raw_status, ack.err_msg,
                reject_suppressed_count_);
            last_reject_err_code_logged_ = ack.err_code;
            last_reject_log_us_          = rej_now_us;
            reject_suppressed_count_     = 0;
        } else {
            ++reject_suppressed_count_;
        }
    }
#ifdef XT_HAVE_METRICS

    ct::metrics::XtMetricRegistry::instance().on_order_reject(
        "binance_u", std::to_string(ack.err_code));
#endif

    if (upd.close_offset != XtOffset::Open) {
        const uint64_t rsync_now_us = lat_now_us();

        if ((last_reject_resync_us_ == 0 ||
             rsync_now_us - last_reject_resync_us_ >= 30'000'000ULL) &&
            queue_reconcile_resync_if_absent_()) {
            last_reject_resync_us_ = rsync_now_us;
        }
    }

    if (td_cfg.close_reject_plain_fallback && ack.err_code == -2022 &&
        upd.close_offset != XtOffset::Open && upd.qty_fp > 0) {
        const std::string fb_sym = ledger_.symbol_for(ack.local_id);
        if (!fb_sym.empty()) {
            const uint64_t fb_now_us = lat_now_us();
            std::lock_guard<std::mutex> lk_q(cold_worker_mu);
            bool fb_queued = false;
            for (const auto& m : cold_worker_tasks) {
                if (m.kind == ColdWorkerTask::kPlainCloseFallback) { fb_queued = true; break; }
            }
            if (!fb_queued &&
                (last_reject_fallback_us_ == 0 ||
                 fb_now_us - last_reject_fallback_us_ >= 30'000'000ULL)) {
                ColdWorkerTask fb{ColdWorkerTask::kPlainCloseFallback};
                fb.fb_symbol   = fb_sym;
                fb.fb_is_buy   = upd.is_buy;
                fb.fb_price_fp = upd.price_fp;
                fb.fb_qty_fp   = upd.qty_fp;
                cold_worker_tasks.push_back(std::move(fb));
                cold_worker_cv.notify_one();
                last_reject_fallback_us_ = fb_now_us;
            }
        }
    }
}

void TraderBinanceUGateway::Impl::on_transport_ack(
    const TransportAck& ack,
    const TransportLatencyTrace& trace) {

    if (lat_trace_log_seq_++ % kLatTraceLogSampleN == 0) {
        XtLogger::info_m("BinanceLat",
            "kind=trace local_id={} transport={} t0={} t0a={} t0b={} t0c={} t1={} sampled={}",
            ack.local_id, trace.transport, trace.t0_us, trace.t0a_us,
            trace.t0b_us, trace.t0c_us, trace.t1_us, kLatTraceLogSampleN);
    }

    if (ack.req_kind == TransportAck::ReqKind::kSubmit && ack.local_id != 0) {
        if (ack.timing_origin == TransportAck::TimingOrigin::kVenueResponse) {
            if (cb.on_transport_ack && trace.t1_us > 0) {
                cb.on_transport_ack(ack.local_id, trace.t1_us);
            }
        } else if (cb.on_transport_ack_unavailable) {
            cb.on_transport_ack_unavailable(ack.local_id, lat_now_us());
        }
    }

    auto decision = ledger_.on_ack(ack, trace.t1_us ? trace.t1_us : lat_now_us());

    if (decision.action == BinanceOrderLedger::TerminalDecision::Action::kKeepInflight) {

        const uint64_t cancel_fail_now_us = lat_now_us();
        if (last_cancel_fail_log_us_ == 0 ||
            cancel_fail_now_us - last_cancel_fail_log_us_ >= 5'000'000ULL) {
            XtLogger::warn_m("BinanceTD",
                "cancel ack not-confirmed (order kept alive): local_id={} kind={} err_code={} "
                "err='{}' suppressed_since_last={}",
                ack.local_id, static_cast<int>(ack.kind), ack.err_code, ack.err_msg,
                cancel_fail_suppressed_count_);
            last_cancel_fail_log_us_      = cancel_fail_now_us;
            cancel_fail_suppressed_count_ = 0;
        } else {
            ++cancel_fail_suppressed_count_;
        }
        return;
    }

    XtOrderExecUpdate& upd = decision.update;
    upd.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(ack.local_id).raw();

    const XtLatencyEligibility elig =
        (ack.timing_origin == TransportAck::TimingOrigin::kVenueResponse)
            ? XtLatencyEligibility::kEligible
            : XtLatencyEligibility::kIneligible;

    switch (ack.kind) {
    case TransportAck::Kind::kRejected:
        handle_rejected_ack(ack, upd);
        break;
    case TransportAck::Kind::kTransportError_TimeoutAfterSend:
    case TransportAck::Kind::kTransportError_Disconnect:
        XtLogger::warn_m("BinanceTD",
            "transport ack kind={} local_id={} kept for reconcile err={}",
            static_cast<int>(ack.kind), ack.local_id, ack.err_msg);
        break;
    case TransportAck::Kind::kAccepted:
    case TransportAck::Kind::kCanceled:
    case TransportAck::Kind::kTransportError_Stale:
    case TransportAck::Kind::kTransportError_TimeoutBeforeSend:
    case TransportAck::Kind::kTransportError_QueueOverflow:
    case TransportAck::Kind::kTransportError_Internal:
        break;
    default:
        XtLogger::warn_m("BinanceTD",
            "transport ack unexpected: local_id={} kind={} err={}",
            ack.local_id, static_cast<int>(ack.kind), ack.err_msg);
        break;
    }

    if (upd.status == XtOrderStatus::Rejected) {
        XtLiveRecorder::note_order_reject(ack.local_id, ack.err_code, ack.err_msg.c_str());
    }

    if (td_cfg.uds_first_effective_ack &&
        ack.kind == TransportAck::Kind::kAccepted &&
        decision.terminal_seen_before) {
        return;
    }

    if (ack.kind == TransportAck::Kind::kTransportError_Disconnect) {
        if (cb.on_order_update) cb.on_order_update(upd, elig);
        return;
    }
    reconciler_.forward_order_update_gated(upd, EffectiveSignalDedup::Source::kAck, elig);
}

}
