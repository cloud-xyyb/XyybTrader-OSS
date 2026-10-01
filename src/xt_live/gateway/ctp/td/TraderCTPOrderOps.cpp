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
#include "xt_live/telemetry/recording/XtLiveRecorder.h"  // note_order_reject 侧信道
#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"  // xt_order_rejects_total 柜台拒单计数
#endif

#include <cmath>
#include <cstring>
#include <mutex>
#include <string>

namespace {

// CTP 没有独立 FOK 标志，需用 IOC + CompleteVolume + MinVolume 组合模拟。
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

}  // anon namespace

// ── CTP 交易接口（submit / cancel）──────────────────
uint64_t TraderCTP::submit_order(const char* code,
                                 bool is_buy,
                                 int64_t price_fp,
                                 int64_t qty_fp,
                                 XtTimeInForce tif,
                                 const char* user_tag,
                                 XtOffset close_offset) {
    const uint64_t submit_begin_us = XtSteadyClock::steady_now_us();
    const StrategySid owner_sid = xt_identity::OrderAttributionStore::consume_thread_submit_strategy_sid();

    // 持 api_mu_ shared 到 ReqOrderInsert 返回，与 stop()/check_health 的 Release/rebuild 互斥。
    // on_order_update 为异步入队回调（SPSC），不在锁内同步回调本网关；整函数持 shared 锁安全。
    std::shared_lock<std::shared_mutex> api_lk(api_mu_);

    // TD 边界只接受可落到 CTP double/int 的正价格和正数量。
    if (!api_) return 0;
    if (XtCStringUtil::is_empty(code)) return 0;
    if (qty_fp <= 0) return 0;
    const double price = xt_fp::from_fp(price_fp);
    if (!std::isfinite(price) || price <= 0.0) return 0;
    const int qty_int = static_cast<int>(qty_fp / xt_fp::kScale);
    if (qty_int <= 0) return 0;
    // 整手契约 fail-fast：CTP VolumeTotalOriginal 是 int 手数，非整手 qty_fp 截断
    // 送单会造成 book（fp 原值注册）与 venue（截断值）数量分叉 → phantom leaves
    // 永不终态。上游（切片/风控）保证整手，此处兜底直拒。
    if (qty_fp % xt_fp::kScale != 0) {
        XtLogger::error_m("TD", "submit rejected: qty_fp={} 非整手 code={}", qty_fp, code);
        return 0;
    }

    // OrderRef 由持久化 store 分配，跨重启单调递增并保证同日唯一。
    const uint64_t local_id = order_ref_store_.next();
    if (local_id == 0) return 0;

    // CTP 请求字段在发送前一次性填满，避免 SDK 读取未初始化字段。
    CThostFtdcInputOrderField req;
    std::memset(&req, 0, sizeof(req));
    XtCStringUtil::copy(req.BrokerID, broker_.c_str());
    XtCStringUtil::copy(req.InvestorID, user_.c_str());
    XtCStringUtil::copy(req.InstrumentID, code);
    const std::string order_ref = std::to_string(local_id);
    // CTP OrderRef 是 char[13]，12 位数字 + '\0'
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

    // ReqOrderInsert 内部有锁，允许多个策略线程并发下单。
    // 预注册：owner sid + tracker 元数据 + 交付投影记录在发送前就位，SDK 极端
    // 同步回调（reject/回报先于本函数推进）也能查到归因与元数据；发送失败立即
    // 回滚（local_id 跨重启单调不复用，回滚无残留语义）。sid 先于 tracker 注册
    // （投影记录的 sid_id 由 AttributionStore 反查）。
    xt_identity::OrderAttributionStore::remember_order_strategy_sid(local_id, owner_sid);
    const bool is_close = (close_offset != XtOffset::Open);
    state_.register_order(local_id, req.OrderRef, code, is_buy, price, qty_fp, user_tag,
                          is_close, close_offset == XtOffset::CloseToday,
                          submit_begin_us, /*submit_end_us=*/0);
    int ret = api_->ReqOrderInsert(&req, next_req_id());
    if (ret != 0) {
        state_.rollback_register(local_id);
        xt_identity::OrderAttributionStore::forget_order_strategy_sid(local_id);
        XtLogger::error_m("TD", "ReqOrderInsert failed: {}", ret);
        return 0;
    }
    const uint64_t submit_end_us = XtSteadyClock::steady_now_us();
    // submit_end 延迟锚预注册时不可知，发送成功后回填。
    state_.note_submit_end(local_id, submit_end_us);

    // 出生 trace：让 journald 按 local_id 串起订单生命周期（submit → update → 终态），
    // 不再靠从撤单日志反推。order_ref == local_id（CTP OrderRef），冷路径一次/单。
    XtLogger::info_m("TD",
        "event=td_order_submit local_id={} order_ref={} code={} side={} offset={} px={} qty={}",
        local_id, req.OrderRef, code, is_buy ? "buy" : "sell",
        ctp_util::offset_flag_label(req.CombOffsetFlag[0]), price, qty_int);

    // 本地先发布 Submitted，保证上层可从本函数返回前后的同一时间点开始超时计时。
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
    // Submitted 本地预发布，时序与 venue 脱钩 → kIneligible。
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
    // payload 只承载终态值（投影写点均为终态；防御非终态泄漏进合成链）。
    if (!order_status_is_final(term.status)) return false;
    out.status = term.status;
    out.filled_fp = term.filled_fp;
    return true;
}

bool TraderCTP::cancel_order(uint64_t local_id) {
    // 持 api_mu_ shared 到 ReqOrderAction 返回，避免并发 Release/rebuild 释放 api_。
    std::shared_lock<std::shared_mutex> api_lk(api_mu_);
    if (!api_) {
        XtLogger::warn_m("TD", "event=td_cancel_req_skip local_id={} reason=api_not_ready",
                         local_id);
        return false;
    }
    CtpStateTracker::CancelTarget target;
    if (!state_.get_cancel_target(local_id, target)) {
        // 保留哨兵 = 负路径探针（如 conformance 故意撤不存在的单）：降 DEBUG + 独立 reason，
        // 与真幽灵单（丢失 cancel target）的 WARN 分开，triage 时不被探针噪声淹没。
        if (local_id == kReservedProbeCancelOrderId) {
            XtLogger::debug_m("TD",
                              "event=td_cancel_req_skip local_id={} reason=probe_unknown", local_id);
        } else if (state_.is_local_terminal(local_id) && callbacks_.on_order_update) {
            // 幽灵自愈：策略撞 no_cancel_target 但该 local_id 已在 gateway
            // 终态集 = 真幽灵（终态曾丢失未下达 book，book/策略仍 open）。用 no_cancel_target
            // 事件本身当探测器，合成终态 on_order_update 走全管线（book erase + frozen 释放 +
            // 策略 inflight_ erase），三方幂等吸收（book tombstone / account 未知 id no-op /
            // 策略终态重入）。sid 由 OrderAttributionStore 反查路由到原策略清 inflight_。
            XtOrderExecUpdate term;
            term.id = local_id;
            term.status = XtOrderStatus::Canceled;
            term.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(local_id).raw();
            XtLogger::warn_m("TD",
                             "event=td_phantom_self_heal local_id={} reason=terminal_book_open", local_id);
            callbacks_.on_order_update(term, XtLatencyEligibility::kIneligible);  // 本地合成终态
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

    // 优先使用订单创建时记录的 front/session ID；缺失时 fallback 到活动会话值。
    // 跨重连撤单时 target 中保存的值可能与活动会话不同。
    // CTP FrontID/SessionID 合法值可为负（柜台会话计数器）；0 是「未回填」哨兵。
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

// ── CTP SPI 订单回报（OnRsp / OnErrRtn / OnRtn order/trade）────────────

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
    // 拒单冷路径记录完整 CTP 边界字段；正常下单热路径不写这类详日志。
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
        // 拒单原因进 recorder 侧信道（ORDER_RTN tap consume），DTO 不扩字段。
        XtLiveRecorder::note_order_reject(upd.id, error_id, err_utf8.c_str());
#ifdef XT_HAVE_METRICS
        // 按 err_code 聚合柜台拒单原因（与 binance 同走 xt_order_rejects_total family）。
        ct::metrics::XtMetricRegistry::instance().on_order_reject(
            "ctp", std::to_string(error_id));
#endif
        // OnRspOrderInsert Rejected = venue rsp 实时转译 → kEligible。
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
    // 过滤非本进程订单的撤单错误（如手工终端撤单）以避免干扰上层状态机
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
        // 撤单错误推断终态 = 本地推断（终态形态非 venue 直发）→ kIneligible。
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
    // CTP SDK 对同一下单失败可能同时触发 OnRsp 和 OnErrRtn；
    // StateTracker 内部用 order_ref 去重，不会重复回调上层
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
        // 撤单错误推断终态 = 本地推断（终态形态非 venue 直发）→ kIneligible。
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
    // recovery gate 的在册判定：仅在册单（本进程/reverse-sync 注册）的终态需要
    // 缓冲补发；前进程纯终态行不缓冲（book 无槽位可销）。
    const bool recovering = recovery_.is_recovering();
    const bool was_local =
        recovering && state_.has_local_order_ref(pOrder->OrderRef);
    XtOrderExecUpdate upd;
    bool suppressed_terminal = false;
    if (state_.on_order(pOrder, cb_us, upd, &suppressed_terminal) && callbacks_.on_order_update) {
        // Recovery 期间的历史订单回报仅供 StateTracker 内部恢复，不立即转发；
        // 但在册单的终态必须缓冲（state_.on_order 已 mark_terminal + 消耗 dedup，
        // 直接丢弃 → tracker 挡撤单而 book 留僵尸：撤单恒 no_cancel_target 死循环
        // + frozen 泄漏）。rebuild_alive_inflight_and_emit
        // 末尾统一 flush；前进程纯终态行（未在册）不缓冲，book 无槽位可销。
        if (recovering) {
            if (was_local && upd.id != 0 && order_status_is_final(upd.status)) {
                std::lock_guard<std::mutex> lk(recovery_term_mu_);
                // OnRtnOrder 真实 push buffered，原 cb_us 保留（真实到达时刻）→ kEligible。
                recovery_pending_terminals_[upd.id] =
                    RecoveryPendingTerminal{upd, XtLatencyEligibility::kEligible};
                XtLogger::debug_m("TD",
                    "event=td_order_update local_id={} status={} leaves={} filled={} (recovery_buffered)",
                    upd.id, order_status_to_str(upd.status), upd.leaves_fp, upd.filled_fp);
            }
            return;
        }
        // 状态转换 trace（journald 实时视角，与 live.jsonl 离线录制互补）：
        // 终态 INFO（生命周期收口，每单 1 行低频高值），中间态 DEBUG。
        if (order_status_is_final(upd.status)) {
            XtLogger::info_m("TD",
                "event=td_order_update local_id={} status={} leaves={} filled={}",
                upd.id, order_status_to_str(upd.status), upd.leaves_fp, upd.filled_fp);
        } else {
            XtLogger::debug_m("TD",
                "event=td_order_update local_id={} status={} leaves={} filled={}",
                upd.id, order_status_to_str(upd.status), upd.leaves_fp, upd.filled_fp);
        }
        // OnRtnOrder venue 实时转译 → kEligible。
        callbacks_.on_order_update(upd, XtLatencyEligibility::kEligible);
        return;
    }
    // 终态被去重器吞（非-recovery）。把 recovery 分支的 swallowed-terminal
    // 投递保证扩到正常路径：合成终态下传全管线（book erase + frozen 释放 + 策略 inflight_ erase）。
    // book/账户/策略三方幂等吸收重复终态。recovery 期间：在册单终态由 recovery_pending_terminals_
    // flush 兜底（仅 on_order 返 true 时入缓冲）；被去重器吞的终态（on_order 返 false）此处不补，
    // 由 recovery 结束后周期 reconcile_lost_terminals 对账 book-open×venue-terminal 补合成。
    if (suppressed_terminal && !recovering && callbacks_.on_order_update) {
        XtLogger::info_m("TD",
            "event=td_order_update local_id={} status={} leaves={} filled={} (suppressed_terminal_resynth)",
            upd.id, order_status_to_str(upd.status), upd.leaves_fp, upd.filled_fp);
        // 同回调栈补发，原 cb_us 保留（真实到达时刻）→ kEligible。
        callbacks_.on_order_update(upd, XtLatencyEligibility::kEligible);
    }
}

void TraderCTP::OnRtnTrade(CThostFtdcTradeField* pTrade) {
    on_rtn_trade_(pTrade, /*from_query=*/false);
}

bool TraderCTP::classify_query_fill_absorbed_(const CThostFtdcTradeField* pTrade) const {
    // 回滚开关 off：回退旧行为——query 重放行一律 absorbed（记账抑制、不下发）。
    if (!exec_ledger_query_fill_delivery_) return true;
    const auto fill_us = static_cast<int64_t>(CtpStateTracker::fill_epoch_us(pTrade));
    // venue 时刻不可得（TradeDate 缺失/解析失败）→ 保守按已吸收处理，
    // 宁失 fee/on_fill 不双计仓位。
    if (fill_us <= 0) return true;
    // pre-restart 封印（主机制）：TradeTime 早于进程启动 = 前进程成交，其净效果
    // 已由 cold 快照吸收；margin 吸收 venue 秒级墙钟 vs 本地墙钟的跨时钟域差，
    // 边带保守归 absorbed。
    if (fill_us < process_start_epoch_us_ + kFillSealMarginUs) return true;
    // cold 轮 anchor 条件化（§10 #18）：首个 recovery 只吸收 ≤ 锚点(+margin) 的
    // 历史行（已含于本轮快照）；> 锚点+margin 是冷启收尾窗新 fill，交付（post 分区），
    // 否则 blanket 吸收会反向抑制同笔 live 缓冲副本 → fill 永不交付。
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
    // 来源规则（§4.3）：query 重放行按 absorbed 判定；live push 恒交付。
    const bool absorbed = from_query && classify_query_fill_absorbed_(pTrade);
    const auto src = from_query ? xt_live::gateway::FillSource::kQueryReplay
                                : xt_live::gateway::FillSource::kLivePush;
    XtTradeRecord tr;
    // exactly-once：投影 record_fill 查+记一体——双源任一先到恰好记账一次，
    // 后到（含跨 recovery 重放）被抑制。
    if (!state_.on_trade(pTrade, cb_us, tr, src, absorbed,
                         static_cast<uint32_t>(query_session_gen()))) {
        return;
    }
    // absorbed 行：记账完成（后续同 key 重放被抑制），净效果由快照承接，不下发；
    // 记账仍推进累计口径，故照样走末尾的 completion 判定。
    if (!absorbed && callbacks_.on_trade) {
        bool deliver_inline = true;
        {
            // recovery 窗口内的待交付成交（live push + warm query 重放同门）先缓冲，
            // recovery finish 按 venue TradeTime 分区发射；gate 开放 = 直发。
            std::lock_guard<std::mutex> lk(recovery_window_mu_);
            if (recovery_window_gate_closed_) {
                deliver_inline = false;
                if (recovery_window_trades_.size() >= kRecoveryWindowTradeCap) {
                    // 丢弃 = 旧行为（recovery 期不下发）；正常窗口远小于 cap。
                    ++recovery_window_overflow_;
                } else {
                    recovery_window_trades_.push_back(tr);
                }
            }
        }
        // live push 实时成交 → kEligible（recovery replay 走缓冲 flush，kIneligible）。
        if (deliver_inline) callbacks_.on_trade(tr, XtLatencyEligibility::kEligible);
    }
    // fill 记账之后判 completion（cum >= qty 且未终态 → 派生 Filled 收尾事件）；
    // 直发路径事件紧随本 fill，缓冲/recovery 路径由 finish 收尾序补发。
    maybe_emit_cum_filled_completion_(tr.order_id, cb_us);
}

void TraderCTP::maybe_emit_cum_filled_completion_(uint64_t local_id, uint64_t local_cb_us) {
    XtOrderExecUpdate comp;
    if (!state_.make_cum_filled_completion(local_id, local_cb_us, comp)) return;
    // recovery 中或窗口 gate 未开：暂存 pending terminals，finish 收尾序在窗口
    // fill flush 之后统一补发（fill→终态次序保持；map 覆盖语义安全——同 id 的
    // 真实终态若先入缓冲，completion 会因投影已终态而不再派生）。
    bool defer = recovery_.is_recovering();
    if (!defer) {
        std::lock_guard<std::mutex> lk(recovery_window_mu_);
        defer = recovery_window_gate_closed_;
    }
    if (defer) {
        std::lock_guard<std::mutex> lk(recovery_term_mu_);
        // cum-filled completion 派生终态 → kIneligible。
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
    // cum-filled completion = 派生终态（非 venue 直发）→ kIneligible。
    if (callbacks_.on_order_update)
        callbacks_.on_order_update(comp, XtLatencyEligibility::kIneligible);
}

// ── 外部撤单错误日志去重（LRU-FIFO）──────────────────────

bool TraderCTP::should_log_foreign_cancel_error_once(const char* order_ref, int error_id) {
    // 外部撤单错误（手工终端/跨进程）可能大量重复，用 LRU-FIFO 去重避免日志风暴。
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
    // 固定上限限制内存占用；FIFO 淘汰队首 key 后允许其再次打印。
    constexpr size_t kForeignWarnCap = 2048;
    while (foreign_cancel_warn_fifo_.size() > kForeignWarnCap) {
        foreign_cancel_warned_keys_.erase(foreign_cancel_warn_fifo_.front());
        foreign_cancel_warn_fifo_.pop_front();
    }
    return true;
}
