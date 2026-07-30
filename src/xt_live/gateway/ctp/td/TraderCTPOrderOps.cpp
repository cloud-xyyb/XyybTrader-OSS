// CTP TD order path：下单/撤单请求构造，以及 order/trade SPI 回报归一化。
// 本文件只处理订单操作和订单/成交回报；login、settlement、query/recovery 由其它分片负责。
// submit/cancel 发送前持 api_mu_ shared，避免 stop()/check_health() Release/rebuild 并发释放 api_。
// 回报路径通过 CTPStateTracker 去重和归一化后进入 callbacks_；SDK 回调线程只做轻量处理。

#include "xt_identity/OrderAttributionStore.h"
#include "ctp/td/TraderCTP.h"

#include "ctp/common/CTPUtil.h"
#include "XtLogger.h"
#include "XtFixedPoint.h"
#include "xt_util/time/XtSteadyClock.h"
#include "xt_util/string/XtStringUtil.h"
#include "xt_util/string/XtCStringUtil.h"
#include "xt_live/telemetry/recording/XtLiveRecorder.h"
#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif

#include <cmath>
#include <cstring>
#include <mutex>
#include <string>

namespace {

void apply_tif(CThostFtdcInputOrderField& req, XtTimeInForce tif, int qty) {
    req.TimeCondition  = THOST_FTDC_TC_GFD;
    req.VolumeCondition = THOST_FTDC_VC_AV;
    req.MinVolume = 1;
    if (tif == XtTimeInForce::IOC) {
        req.TimeCondition = THOST_FTDC_TC_IOC;
    } else if (tif == XtTimeInForce::FOK) {
        req.TimeCondition  = THOST_FTDC_TC_IOC;
        req.VolumeCondition = THOST_FTDC_VC_CV;
        req.MinVolume = qty;
    }
}

}

uint64_t TraderCTP::submit_order(const char* code,
                                 bool is_buy,
                                 int64_t price_fp,
                                 int64_t qty_fp,
                                 XtTimeInForce tif,
                                 const char* user_tag,
                                 XtOffset close_offset) {
    const uint64_t submit_begin_us = XtSteadyClock::steady_now_us();
    const StrategySid owner_sid = xt_identity::OrderAttributionStore::consume_thread_submit_strategy_sid();

    std::shared_lock<std::shared_mutex> api_lk(api_mu_);

    if (!api_) return 0;
    if (XtCStringUtil::is_empty(code)) return 0;
    if (qty_fp <= 0) return 0;
    const double price = xt_fp::from_fp(price_fp);
    if (!std::isfinite(price) || price <= 0.0) return 0;
    const int qty_int = static_cast<int>(qty_fp / xt_fp::kScale);
    if (qty_int <= 0) return 0;

    if (qty_fp % xt_fp::kScale != 0) {
        XtLogger::error_m("TD", "submit rejected: qty_fp={} 非整手 code={}", qty_fp, code);
        return 0;
    }

    const uint64_t local_id = order_ref_store_.next();
    if (local_id == 0) return 0;

    CThostFtdcInputOrderField req;
    std::memset(&req, 0, sizeof(req));
    XtCStringUtil::copy(req.BrokerID, broker_.c_str());
    XtCStringUtil::copy(req.InvestorID, user_.c_str());
    XtCStringUtil::copy(req.InstrumentID, code);
    const std::string order_ref = std::to_string(local_id);

    if (order_ref.size() >= sizeof(req.OrderRef)) {
        XtLogger::error_m("TD", "order_ref overflow local_id={}", local_id);
        return 0;
    }
    std::memcpy(req.OrderRef, order_ref.c_str(), order_ref.size());

    req.Direction           = is_buy ? THOST_FTDC_D_Buy : THOST_FTDC_D_Sell;
    req.CombOffsetFlag[0]   = ctp_util::to_ctp_offset_flag(close_offset, code);
    req.CombHedgeFlag[0]    = THOST_FTDC_HF_Speculation;
    req.OrderPriceType      = THOST_FTDC_OPT_LimitPrice;
    req.LimitPrice          = price;
    req.VolumeTotalOriginal = qty_int;
    apply_tif(req, tif, qty_int);
    req.ContingentCondition = THOST_FTDC_CC_Immediately;
    req.ForceCloseReason    = THOST_FTDC_FCC_NotForceClose;
    req.IsAutoSuspend       = 0;
    req.UserForceClose      = 0;

    xt_identity::OrderAttributionStore::remember_order_strategy_sid(local_id, owner_sid);
    const bool is_close = (close_offset != XtOffset::Open);
    state_.register_order(local_id, req.OrderRef, code, is_buy, price, qty_fp, user_tag,
                          is_close, close_offset == XtOffset::CloseToday,
                          submit_begin_us,  0);
    int ret = api_->ReqOrderInsert(&req, next_req_id());
    if (ret != 0) {
        state_.rollback_register(local_id);
        xt_identity::OrderAttributionStore::forget_order_strategy_sid(local_id);
        XtLogger::error_m("TD", "ReqOrderInsert failed: {}", ret);
        return 0;
    }
    const uint64_t submit_end_us = XtSteadyClock::steady_now_us();

    state_.note_submit_end(local_id, submit_end_us);

    XtLogger::info_m("TD",
        "event=td_order_submit local_id={} order_ref={} code={} side={} offset={} px={} qty={}",
        local_id, req.OrderRef, code, is_buy ? "buy" : "sell",
        ctp_util::offset_flag_label(req.CombOffsetFlag[0]), price, qty_int);

    XtOrderExecUpdate upd;
    upd.id          = local_id;
    upd.sid_id      = owner_sid.raw();
    upd.is_buy      = is_buy;
    upd.status      = XtOrderStatus::Submitted;
    upd.close_offset = close_offset;
    upd.price_fp    = price_fp;
    upd.qty_fp      = qty_fp;
    upd.leaves_fp   = upd.qty_fp;
    upd.local_cb_us = submit_end_us;

    if (callbacks_.on_order_update)
        callbacks_.on_order_update(upd, XtLatencyEligibility::kIneligible);
    return local_id;
}

bool TraderCTP::is_local_terminal(uint64_t local_id) const {
    return state_.is_local_terminal(local_id);
}

bool TraderCTP::lookup_terminal(uint64_t local_id, XtOrderTerminalRecord& out) const {
    xt_live::gateway::XtExecDeliveryLedger::TerminalRecord term;
    if (!state_.exec_ledger().terminal_record(local_id, term)) return false;

    if (!order_status_is_final(term.status)) return false;
    out.status = term.status;
    out.filled_fp = term.filled_fp;
    return true;
}

bool TraderCTP::cancel_order(uint64_t local_id) {

    std::shared_lock<std::shared_mutex> api_lk(api_mu_);
    if (!api_) {
        XtLogger::warn_m("TD", "event=td_cancel_req_skip local_id={} reason=api_not_ready",
                         local_id);
        return false;
    }
    CtpStateTracker::CancelTarget target;
    if (!state_.get_cancel_target(local_id, target)) {

        if (local_id == kReservedProbeCancelOrderId) {
            XtLogger::debug_m("TD",
                              "event=td_cancel_req_skip local_id={} reason=probe_unknown", local_id);
        } else if (state_.is_local_terminal(local_id) && callbacks_.on_order_update) {

            XtOrderExecUpdate term;
            term.id = local_id;
            term.status = XtOrderStatus::Canceled;
            term.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(local_id).raw();
            XtLogger::warn_m("TD",
                             "event=td_phantom_self_heal local_id={} reason=terminal_book_open", local_id);
            callbacks_.on_order_update(term, XtLatencyEligibility::kIneligible);
        } else {
            XtLogger::warn_m("TD",
                             "event=td_cancel_req_skip local_id={} reason=no_cancel_target", local_id);
        }
        return false;
    }

    CThostFtdcInputOrderActionField req;
    std::memset(&req, 0, sizeof(req));
    XtCStringUtil::copy(req.BrokerID, broker_.c_str());
    XtCStringUtil::copy(req.InvestorID, user_.c_str());
    XtCStringUtil::copy(req.UserID, user_.c_str());
    XtCStringUtil::copy(req.OrderRef, target.ref.c_str());
    if (!target.code.empty())
        XtCStringUtil::copy(req.InstrumentID, target.code.c_str());
    if (!target.exchange_id.empty())
        XtCStringUtil::copy(req.ExchangeID, target.exchange_id.c_str());
    if (!target.order_sys_id.empty())
        XtCStringUtil::copy(req.OrderSysID, target.order_sys_id.c_str());

    int front = target.front_id != 0 ? target.front_id : front_id_.load(std::memory_order_acquire);
    if (front != 0) req.FrontID = front;
    int session = target.session_id != 0 ? target.session_id
                                         : session_id_.load(std::memory_order_acquire);
    if (session != 0) req.SessionID = session;

    const int req_id = next_req_id();
    req.OrderActionRef = req_id;
    req.ActionFlag     = THOST_FTDC_AF_Delete;
    XtLogger::info_m("TD",
                     "event=td_cancel_req_send local_id={} req_id={} order_ref={} code={} "
                     "exchange_id={} order_sys_id={} front_id={} session_id={}",
                     local_id, req_id, target.ref, target.code,
                     target.exchange_id, target.order_sys_id, req.FrontID, req.SessionID);

    int ret = api_->ReqOrderAction(&req, req_id);
    if (ret != 0) {
        XtLogger::error_m("TD",
                          "event=td_cancel_req_failed local_id={} req_id={} order_ref={} ret={}",
                          local_id, req_id, target.ref, ret);
        return false;
    }
    XtLogger::debug_m("TD", "event=td_cancel_req_sent local_id={} req_id={} order_ref={}",
                     local_id, req_id, target.ref);
    return true;
}

void TraderCTP::OnRspOrderInsert(CThostFtdcInputOrderField* pInputOrder,
                                 CThostFtdcRspInfoField* pRspInfo,
                                 int,
                                 bool) {
    if (!pInputOrder) {
        XtLogger::warn_m("TD",
                         "event=td_order_insert_rsp_null error_id={} error_msg={}",
                         pRspInfo ? pRspInfo->ErrorID : 0,
                         pRspInfo ? XtStringUtil::gbk_to_utf8_or_original(pRspInfo->ErrorMsg) : "");
        return;
    }
    if (!is_error(pRspInfo)) return;
    const int error_id = pRspInfo ? pRspInfo->ErrorID : 0;
    const std::string err_utf8 =
        pRspInfo ? XtStringUtil::gbk_to_utf8_or_original(pRspInfo->ErrorMsg) : std::string();

    XtLogger::warn_m("TD",
                     "event=td_order_insert_reject code={} dir={} offset={} qty={} price={} "
                     "error_id={} error_msg={}",
                     pInputOrder->InstrumentID,
                     pInputOrder->Direction == THOST_FTDC_D_Buy ? "buy" : "sell",
                     ctp_util::offset_flag_label(pInputOrder->CombOffsetFlag[0]),
                     pInputOrder->VolumeTotalOriginal,
                     pInputOrder->LimitPrice,
                     error_id, err_utf8);
    const uint64_t cb_us = XtSteadyClock::steady_now_us();
    XtOrderExecUpdate upd;
    if (state_.on_order_insert_reject(pInputOrder, pRspInfo, cb_us, upd)) {

        XtLiveRecorder::note_order_reject(upd.id, error_id, err_utf8.c_str());
#ifdef XT_HAVE_METRICS

        ct::metrics::XtMetricRegistry::instance().on_order_reject(
            "ctp", std::to_string(error_id));
#endif

        if (callbacks_.on_order_update)
            callbacks_.on_order_update(upd, XtLatencyEligibility::kEligible);
    }
}

void TraderCTP::OnRspOrderAction(CThostFtdcInputOrderActionField* pInputOrderAction,
                                 CThostFtdcRspInfoField* pRspInfo,
                                 int nRequestID,
                                 bool bIsLast) {
    const int error_id = pRspInfo ? pRspInfo->ErrorID : 0;
    if (error_id == 0) return;

    const char* order_ref = pInputOrderAction ? pInputOrderAction->OrderRef : "";
    if (!state_.has_local_order_ref(order_ref)) {
        if (should_log_foreign_cancel_error_once(order_ref, error_id)) {
            XtLogger::warn_m("TD",
                             "event=td_cancel_rsp_error_ignored req_id={} is_last={} "
                             "order_ref={} error_id={} reason=non_local_order_ref",
                             nRequestID, bIsLast ? 1 : 0,
                             order_ref ? order_ref : "", error_id);
        }
        return;
    }
    const uint64_t cb_us = XtSteadyClock::steady_now_us();
    XtOrderExecUpdate upd;
    if (state_.on_cancel_action_error(order_ref, error_id, cb_us, upd) &&
        callbacks_.on_order_update) {

        callbacks_.on_order_update(upd, XtLatencyEligibility::kIneligible);
    }
    XtLogger::error_m("TD",
                      "event=td_cancel_rsp_error req_id={} is_last={} order_ref={} "
                      "error_id={} error_msg={}",
                      nRequestID, bIsLast ? 1 : 0,
                      order_ref ? order_ref : "", error_id,
                      pRspInfo ? XtStringUtil::gbk_to_utf8_or_original(pRspInfo->ErrorMsg) : "");
}

void TraderCTP::OnErrRtnOrderInsert(CThostFtdcInputOrderField* pInputOrder,
                                    CThostFtdcRspInfoField* pRspInfo) {

    OnRspOrderInsert(pInputOrder, pRspInfo, 0, true);
}

void TraderCTP::OnErrRtnOrderAction(CThostFtdcOrderActionField* pOrderAction,
                                    CThostFtdcRspInfoField* pRspInfo) {
    const int error_id = pRspInfo ? pRspInfo->ErrorID : 0;
    if (error_id == 0) return;
    const char* order_ref = pOrderAction ? pOrderAction->OrderRef : "";
    if (!state_.has_local_order_ref(order_ref)) {
        if (should_log_foreign_cancel_error_once(order_ref, error_id)) {
            XtLogger::warn_m("TD",
                             "event=td_cancel_err_rtn_ignored order_ref={} error_id={} "
                             "reason=non_local_order_ref",
                             order_ref ? order_ref : "", error_id);
        }
        return;
    }
    const uint64_t cb_us = XtSteadyClock::steady_now_us();
    XtOrderExecUpdate upd;
    if (state_.on_cancel_action_error(order_ref, error_id, cb_us, upd) &&
        callbacks_.on_order_update) {

        callbacks_.on_order_update(upd, XtLatencyEligibility::kIneligible);
    }
    XtLogger::error_m("TD",
                      "event=td_cancel_err_rtn order_ref={} error_id={} error_msg={}",
                      order_ref ? order_ref : "", error_id,
                      pRspInfo ? XtStringUtil::gbk_to_utf8_or_original(pRspInfo->ErrorMsg) : "");
}

void TraderCTP::OnRtnOrder(CThostFtdcOrderField* pOrder) {
    if (!pOrder) {
        XtLogger::warn_m("TD", "event=td_rtn_order_null");
        return;
    }
    const uint64_t cb_us = XtSteadyClock::steady_now_us();

    const bool recovering = recovery_.is_recovering();
    const bool was_local =
        recovering && state_.has_local_order_ref(pOrder->OrderRef);
    XtOrderExecUpdate upd;
    bool suppressed_terminal = false;
    if (state_.on_order(pOrder, cb_us, upd, &suppressed_terminal) && callbacks_.on_order_update) {

        if (recovering) {
            if (was_local && upd.id != 0 && order_status_is_final(upd.status)) {
                std::lock_guard<std::mutex> lk(recovery_term_mu_);

                recovery_pending_terminals_[upd.id] =
                    RecoveryPendingTerminal{upd, XtLatencyEligibility::kEligible};
                XtLogger::debug_m("TD",
                    "event=td_order_update local_id={} status={} leaves={} filled={} (recovery_buffered)",
                    upd.id, order_status_to_str(upd.status), upd.leaves_fp, upd.filled_fp);
            }
            return;
        }

        if (order_status_is_final(upd.status)) {
            XtLogger::info_m("TD",
                "event=td_order_update local_id={} status={} leaves={} filled={}",
                upd.id, order_status_to_str(upd.status), upd.leaves_fp, upd.filled_fp);
        } else {
            XtLogger::debug_m("TD",
                "event=td_order_update local_id={} status={} leaves={} filled={}",
                upd.id, order_status_to_str(upd.status), upd.leaves_fp, upd.filled_fp);
        }

        callbacks_.on_order_update(upd, XtLatencyEligibility::kEligible);
        return;
    }

    if (suppressed_terminal && !recovering && callbacks_.on_order_update) {
        XtLogger::info_m("TD",
            "event=td_order_update local_id={} status={} leaves={} filled={} (suppressed_terminal_resynth)",
            upd.id, order_status_to_str(upd.status), upd.leaves_fp, upd.filled_fp);

        callbacks_.on_order_update(upd, XtLatencyEligibility::kEligible);
    }
}

void TraderCTP::OnRtnTrade(CThostFtdcTradeField* pTrade) {
    on_rtn_trade_(pTrade,  false);
}

bool TraderCTP::classify_query_fill_absorbed_(const CThostFtdcTradeField* pTrade) const {

    if (!exec_ledger_query_fill_delivery_) return true;
    const auto fill_us = static_cast<int64_t>(CtpStateTracker::fill_epoch_us(pTrade));

    if (fill_us <= 0) return true;

    if (fill_us < process_start_epoch_us_ + kFillSealMarginUs) return true;

    if (is_first_recovery_) {
        const int64_t anchor =
            recovery_position_anchor_us_.load(std::memory_order_acquire);
        if (anchor > 0 && fill_us <= anchor + kFillSealMarginUs) return true;
    }
    return false;
}

void TraderCTP::on_rtn_trade_(CThostFtdcTradeField* pTrade, bool from_query) {
    if (!pTrade) {
        XtLogger::warn_m("TD", "event=td_rtn_trade_null");
        return;
    }
    const uint64_t cb_us = XtSteadyClock::steady_now_us();

    const bool absorbed = from_query && classify_query_fill_absorbed_(pTrade);
    const auto src = from_query ? xt_live::gateway::FillSource::kQueryReplay
                                : xt_live::gateway::FillSource::kLivePush;
    XtTradeRecord tr;

    if (!state_.on_trade(pTrade, cb_us, tr, src, absorbed,
                         static_cast<uint32_t>(query_session_gen()))) {
        return;
    }

    if (!absorbed && callbacks_.on_trade) {
        bool deliver_inline = true;
        {

            std::lock_guard<std::mutex> lk(recovery_window_mu_);
            if (recovery_window_gate_closed_) {
                deliver_inline = false;
                if (recovery_window_trades_.size() >= kRecoveryWindowTradeCap) {

                    ++recovery_window_overflow_;
                } else {
                    recovery_window_trades_.push_back(tr);
                }
            }
        }

        if (deliver_inline) callbacks_.on_trade(tr, XtLatencyEligibility::kEligible);
    }

    maybe_emit_cum_filled_completion_(tr.order_id, cb_us);
}

void TraderCTP::maybe_emit_cum_filled_completion_(uint64_t local_id, uint64_t local_cb_us) {
    XtOrderExecUpdate comp;
    if (!state_.make_cum_filled_completion(local_id, local_cb_us, comp)) return;

    bool defer = recovery_.is_recovering();
    if (!defer) {
        std::lock_guard<std::mutex> lk(recovery_window_mu_);
        defer = recovery_window_gate_closed_;
    }
    if (defer) {
        std::lock_guard<std::mutex> lk(recovery_term_mu_);

        recovery_pending_terminals_[comp.id] =
            RecoveryPendingTerminal{comp, XtLatencyEligibility::kIneligible};
        XtLogger::debug_m("TD",
            "event=td_order_update local_id={} status={} (cum_filled_completion_buffered)",
            comp.id, order_status_to_str(comp.status));
        return;
    }
    XtLogger::info_m("TD",
        "event=td_order_update local_id={} status={} leaves={} filled={} (cum_filled_completion)",
        comp.id, order_status_to_str(comp.status), comp.leaves_fp, comp.filled_fp);

    if (callbacks_.on_order_update)
        callbacks_.on_order_update(comp, XtLatencyEligibility::kIneligible);
}

bool TraderCTP::should_log_foreign_cancel_error_once(const char* order_ref, int error_id) {

    std::string key;
    key.reserve(48);
    key.append(std::to_string(error_id));
    key.push_back('|');
    key.append(order_ref ? order_ref : "");
    if (key.back() == '|') key.append("EMPTY");

    std::lock_guard<std::mutex> guard(foreign_cancel_warn_mu_);
    if (foreign_cancel_warned_keys_.find(key) != foreign_cancel_warned_keys_.end()) {
        return false;
    }
    foreign_cancel_warned_keys_.insert(key);
    foreign_cancel_warn_fifo_.push_back(key);

    constexpr size_t kForeignWarnCap = 2048;
    while (foreign_cancel_warn_fifo_.size() > kForeignWarnCap) {
        foreign_cancel_warned_keys_.erase(foreign_cancel_warn_fifo_.front());
        foreign_cancel_warn_fifo_.pop_front();
    }
    return true;
}
