// CTP 银期转账回报分片：归一化银行入金/出金推送并发布 cash-transfer delta。
// 本文件只处理 OnRtnFromBankToFutureByFuture / OnRtnFromFutureToBankByFuture；
// 资金快照、日结 delta 与账户可用资金 overlay 由 TraderCTP.cpp 的 account 查询路径处理。
// CTP 回报中的金额以 broker double 入场；本分片只做 finite/zero/error gate 和方向符号归一化。

#include "ctp/td/TraderCTP.h"

#include "ctp/common/CTPUtil.h"
#include "XtLogger.h"
#include "xt_util/string/XtStringUtil.h"
#include "xt_util/time/XtTimeParse.h"

#include <cmath>

// ── 银期转账事件 emit ───────────────────────────

void TraderCTP::emit_cash_transfer_event(const CThostFtdcRspTransferField* pRspTransfer,
                                         bool bank_to_future) {
    // CTP 异常情况下可能给空回报；该事件不可恢复，记录后丢弃。
    if (!pRspTransfer) {
        XtLogger::warn_m("TD",
                         "event=td_cash_transfer_rtn_null direction={}",
                         bank_to_future ? "bank_to_future" : "future_to_bank");
        return;
    }
    if (!callbacks_.on_cash_transfer) return;
    // ErrorID 非 0 表示转账未成功，不进入账户 reducer，避免把失败回报当 cash delta。
    if (pRspTransfer->ErrorID != 0) {
        XtLogger::warn_m("TD",
                         "ignore cash transfer rtn with error trade_code={} error_id={} error_msg={}",
                         pRspTransfer->TradeCode, pRspTransfer->ErrorID,
                         XtStringUtil::gbk_to_utf8_or_original(pRspTransfer->ErrorMsg));
        return;
    }
    double amount = pRspTransfer->TradeAmount;
    if (!std::isfinite(amount) || amount == 0.0) return;
    // reducer 统一消费 signed delta：入金为正，出金为负。
    if (!bank_to_future) amount = -amount;

    CashTransferDelta ev;
    ev.amount      = amount;
    ev.trading_day = XtTimeParse::parse_fixed_date_yyyymmdd(pRspTransfer->TradingDay);
    ev.serial      = static_cast<uint64_t>(pRspTransfer->FutureSerial);
    callbacks_.on_cash_transfer(ev);

    XtLogger::info_m("TD",
                     "emit cash transfer amount={} trading_day={} serial={} direction={}",
                     ev.amount, ev.trading_day, ev.serial,
                     bank_to_future ? "bank_to_future" : "future_to_bank");
}

void TraderCTP::OnRtnFromBankToFutureByFuture(CThostFtdcRspTransferField* pRspTransfer) {
    emit_cash_transfer_event(pRspTransfer, true);
}

void TraderCTP::OnRtnFromFutureToBankByFuture(CThostFtdcRspTransferField* pRspTransfer) {
    emit_cash_transfer_event(pRspTransfer, false);
}
