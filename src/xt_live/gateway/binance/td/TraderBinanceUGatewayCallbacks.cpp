#include "xt_identity/OrderAttributionStore.h"
#include "TraderBinanceUGatewayImpl.h"

#include <utility>

#include "transport/BinanceTdWireUtil.h"  // make_client_order_id
#include "transport/BinanceIntentTrace.h"
#include "refdata/BinanceExchangeInfo.h"     // binance_filters_loaded（fail-closed gate）
#include "xt_logger/XtLogger.h"
#include "xt_live/telemetry/recording/XtLiveRecorder.h"  // note_order_reject 侧信道
#ifdef XT_HAVE_METRICS
#include <string>
#include "xt_metrics/XtMetricRegistry.h"  // xt_order_rejects_total 交易所拒单计数
#endif

namespace binance_u {

uint64_t TraderBinanceUGateway::Impl::submit_order_inq(
    const char* code, bool is_buy,
    int64_t price_fp, int64_t qty_fp,
    XtTimeInForce tif, const char* /*user_tag*/,
    XtOffset close_offset, ExecFlags exec_flags,
    uint64_t intent_trace_id) {
	    const StrategySid owner_sid = xt_identity::OrderAttributionStore::consume_thread_submit_strategy_sid();
	    const uint64_t local_id = local_id_gen_->next();
    // local-id high-water 持久化失败时，重启后可能在预留 block 内重发 local_id。
    // 这里只按失败计数去重 warn；submit 热路径不做额外恢复动作。
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
    // 身份 + pending 短窗 + in-flight 三 facet 单锁单 insert 登记（X4）。
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
    tracked_symbols_.note(symbol);   // REST 减负：submit 播种 tracked symbol 集

    // BinanceLat t0：策略线程提交到 transport 前的本地时间。
    // 同 kind=trace 1/100 采样：t0/t2 实测占健康满速期 journal ~85%
    // （~1900 行/分钟），延迟真值已有 prom histogram + live.jsonl 双通道。
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

    // fail-closed：venue tick/step filters 未加载该 symbol（exchangeInfo 缺失/未拉到）→
    // 不发 off-tick 单（否则 -4014/-1111）。合成终态 Rejected 经 on_transport_ack 走正常拒单
    // 生命周期（ledger 已注册 → 路由正确 + 清理，无 phantom）；strategy/closeout/panel
    // 三源统一在此唯一入口兜住。
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
        return false;  // 未知 local_id（或 UDS 终态已清撤单路由）
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
    // raw UDS 到达通知在 dedup 前触发，latency tracker 用物理到达时间计算 effective ack。
    if (cb.on_uds_arrival && upd.id != 0 && upd.local_cb_us > 0) {
        cb.on_uds_arrival(upd.id, upd.local_cb_us);
    }
    // 终态判定/销账单一权威在 ledger：终态单锁内记终态 + 清 in-flight + 清撤单
    // 路由（防迟到 ACK 复活已结束订单）；非终态原样放行。emit 在 ledger 锁外。
    const auto decision = ledger_.on_uds_terminal(upd);
    if (decision.action != BinanceOrderLedger::TerminalDecision::Action::kEmit) return;
    // UDS executionReport = venue 实时推送，到达时刻可采 → kEligible。
    reconciler_.forward_order_update_gated(upd, EffectiveSignalDedup::Source::kUds,
                                           XtLatencyEligibility::kEligible);
}

void TraderBinanceUGateway::Impl::handle_rejected_ack(const TransportAck& ack,
                                                      const XtOrderExecUpdate& upd) {
    // 保留 Binance 业务拒单明细；同一 err_code 在 5s 窗口内只打印首条并累计抑制数。
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
    // 指标按 err_code 聚合交易所拒单原因。
    ct::metrics::XtMetricRegistry::instance().on_order_reject(
        "binance_u", std::to_string(ack.err_code));
#endif
    // 平仓拒单会排队一次 reset 语义的 position/account 真值 resync。
    // 这里不从拒单反推持仓为 0，也不重启 UDS；只让 account_store 对齐交易所真值。
    if (upd.close_offset != XtOffset::Open) {
        const uint64_t rsync_now_us = lat_now_us();
        // 30s 节流 + 队列合并（queue_reconcile_resync_if_absent_）双门；只在真入队时打点。
        if ((last_reject_resync_us_ == 0 ||
             rsync_now_us - last_reject_resync_us_ >= 30'000'000ULL) &&
            queue_reconcile_resync_if_absent_()) {
            last_reject_resync_us_ = rsync_now_us;
        }
    }
    // config-gated plain-close fallback：仅在 reduceOnly -2022 平仓拒单时排队到冷 worker。
    // worker 会重新读取交易所持仓并做 flip-safe 数量封顶；本 transport 回调线程不直接重发。
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
    // BinanceLat trace 每 ack 一条属冗余诊断（延迟真值已有 prom histogram +
    // live.jsonl recorder 双通道），1/100 采样防 journal 风暴驱逐取证窗口；
    // 首条放行保低流量期可见，sampled 字段提示读者行量已缩。
    if (lat_trace_log_seq_++ % kLatTraceLogSampleN == 0) {
        XtLogger::info_m("BinanceLat",
            "kind=trace local_id={} transport={} t0={} t0a={} t0b={} t0c={} t1={} sampled={}",
            ack.local_id, trace.transport, trace.t0_us, trace.t0a_us,
            trace.t0b_us, trace.t0c_us, trace.t1_us, kLatTraceLogSampleN);
    }

    // L-1.4 raw transport ACK 旁路两闸（绕过 OrderUpdateHandler 直连 tracker）：
    //   闸 1 timing_origin：kVenueResponse（含 5xx/-1007 execution-unknown，http!=0）
    //     且 t1>0 → 采 raw ack 样本；kLocalFailure（DNS/超时/stale/queue/internal，
    //     任意 t1）→ on_transport_ack_unavailable 抑制 synthetic fallback，绝不产样本。
    //   闸 2 req_kind：仅 submit ack 进 submit latency；cancel ack 一律不采
    //     （防 submit-unavailable 后同 local_id 的 cancel ack 复活为假 submit ack）。
    if (ack.req_kind == TransportAck::ReqKind::kSubmit && ack.local_id != 0) {
        if (ack.timing_origin == TransportAck::TimingOrigin::kVenueResponse) {
            if (cb.on_transport_ack && trace.t1_us > 0) {
                cb.on_transport_ack(ack.local_id, trace.t1_us);
            }
        } else if (cb.on_transport_ack_unavailable) {
            cb.on_transport_ack_unavailable(ack.local_id, lat_now_us());
        }
    }

    // 终态判定/销账/字段补齐的唯一权威在 ledger（X1：判定锁内、emit 全在锁外）。
    // 「cancel 非 kCanceled 早返回短路」保持在 ledger on_ack 最前。
    auto decision = ledger_.on_ack(ack, trace.t1_us ? trace.t1_us : lat_now_us());

    if (decision.action == BinanceOrderLedger::TerminalDecision::Action::kKeepInflight) {
        // 订单保持在途（判定理由见 BinanceOrderLedger::on_ack 的 cancel-ack 契约）；
        // 这里只做限流告警。
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

    // order_update eligibility 统一判据 = ack.timing_origin（L-1.4 闸 1；kAccepted/
    // kCanceled/venue business reject → kVenueResponse=kEligible；filters fail-closed /
    // transport-error 合成 Rejected / 保 Submitted 的 timeout/disconnect → kLocalFailure=
    // kIneligible）。禁按 Kind 逐处裁定。
    const XtLatencyEligibility elig =
        (ack.timing_origin == TransportAck::TimingOrigin::kVenueResponse)
            ? XtLatencyEligibility::kEligible
            : XtLatencyEligibility::kIneligible;

    // 终态判定之外的 kind 相关善后（日志 / metric / resync / fallback 全留在 Impl）。
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

    // venue/transport 拒单原因进 recorder 侧信道；event loop 在 ORDER_RTN tap
    // consume 填 err_msg（err_code/err_msg 不扩 XtOrderExecUpdate plugin ABI）。
    if (upd.status == XtOrderStatus::Rejected) {
        XtLiveRecorder::note_order_reject(ack.local_id, ack.err_code, ack.err_msg.c_str());
    }

    // UDS 终态先到时，迟到 Accepted ACK 只补 sys_id，不再向策略回放旧状态。
    if (td_cfg.uds_first_effective_ack &&
        ack.kind == TransportAck::Kind::kAccepted &&
        decision.terminal_seen_before) {
        return;
    }

    // transport 断线批量失败保持直发，不写入 signal_state_，避免影响重连后的 UDS/reconcile 信号。
    if (ack.kind == TransportAck::Kind::kTransportError_Disconnect) {
        if (cb.on_order_update) cb.on_order_update(upd, elig);  // Disconnect → kLocalFailure=kIneligible
        return;
    }
    reconciler_.forward_order_update_gated(upd, EffectiveSignalDedup::Source::kAck, elig);
}

}  // namespace binance_u
