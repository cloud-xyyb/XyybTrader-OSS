#include "backtest/XtBacktestRiskGuardContext.h"
#include "xt_account/api/AccountSnapshot.h"
#include "order/XtOrderRuntimeBook.h"
#include "xt_config/catalog/XtCommodityCatalog.h"
#include "XtFixedPoint.h"
#include "api/ISTPChecker.h"
#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif

XtBacktestRiskGuardContext::XtBacktestRiskGuardContext(const xt_account::AccountStateStore& account,
                                                       const xt_runtime::order::XtOrderRuntimeBook* order_book,
                                                       const XtCommodityCatalog* catalog,
                                                       int64_t limit_up_fp,
                                                       int64_t limit_down_fp,
                                                       const ISTPChecker* stp,
                                                       int64_t today_pnl_fp)
    : account_(account), order_book_(order_book), catalog_(catalog),
      limit_up_fp_(limit_up_fp), limit_down_fp_(limit_down_fp), stp_(stp),
      today_pnl_fp_(today_pnl_fp) {}

XtRiskSnapshot XtBacktestRiskGuardContext::make_snapshot(const xt_core::XtOrder& req,
                                                        const XtRiskEvalContext&  ) const {
    XtRiskSnapshot snap = account_.snapshot_risk(req.code ? req.code : "");

    snap.limit_up_fp   = limit_up_fp_;
    snap.limit_down_fp = limit_down_fp_;

    snap.today_pnl_fp = today_pnl_fp_;
    snap.starting_balance_fp = account_.starting_balance_fp();
    return snap;
}

XtRiskResult XtBacktestRiskGuardContext::precheck_input(const xt_core::XtOrder& req,
                                                          const XtRiskEvalContext&  ) const {

    XtRiskResult out;
    out.stage = XtRiskStage::InputGuard;

    if (stp_ && req.price_fp > 0 && stp_->would_self_trade(req)) {
        out.passed = false;
        out.reason_code = XtRiskReasonCode::SelfTradeDetected;
        out.reason_text = risk_reason_text(XtRiskReasonCode::SelfTradeDetected);
        return out;
    }
    out.passed = true;
    out.reason_code = XtRiskReasonCode::None;
    out.reason_text = risk_reason_text(XtRiskReasonCode::None);
    return out;
}

XtRiskResult XtBacktestRiskGuardContext::precheck_capability(const xt_core::XtOrder&,
                                                               const XtRiskEvalContext&  ) const {
    XtRiskResult out;
    out.passed = true;
    out.reason_code = XtRiskReasonCode::None;
    out.stage = XtRiskStage::CapabilityGuard;
    out.reason_text = risk_reason_text(XtRiskReasonCode::None);
    return out;
}

XtRiskResult XtBacktestRiskGuardContext::guard_before_submit(const xt_core::XtOrder& req,
                                                               const XtRiskEvalContext&  ) const {
    XtRiskResult out;
    if (req.close_offset == XtOffset::Open) {
        out.passed = true;
        out.reason_code = XtRiskReasonCode::None;
        out.stage = XtRiskStage::CoreRule;
        out.reason_text = risk_reason_text(XtRiskReasonCode::None);

        if (catalog_ && account_.futures_mode()) {
            const XtCommodityInfo* info = catalog_->find(req.code);
            const int64_t per_unit_fp  = catalog_->per_unit_margin_fp(info, req.code, req.price_fp);
            const int64_t required_fp  = xt_fp::mul(per_unit_fp, req.qty_fp);
            const int64_t available_fp = account_.snapshot_account().available_cash;
            out.required_margin_fp = required_fp;
            out.per_unit_margin_fp = per_unit_fp;
            out.available_cash_fp  = available_fp;
            if (required_fp > 0 && available_fp < required_fp) {
                out.passed = false;
                out.reason_code = XtRiskReasonCode::InsufficientMargin;
                out.stage = XtRiskStage::MarginPrecheck;
                out.reason_text = risk_reason_text(XtRiskReasonCode::InsufficientMargin);
            }
        }
        return out;
    }

    const XtOffset close_offset = req.close_offset;
    const auto pos_snap = account_.snapshot_position(req.code ? req.code : "");
    const auto raw_view = xt_account::closeable_view(pos_snap, req.is_buy);

    int64_t outstanding_today_fp = 0;
    int64_t outstanding_yesterday_fp = 0;
    if (order_book_) {
        order_book_->accumulate_close_outstanding_fp(
            req.code, req.is_buy, outstanding_today_fp, outstanding_yesterday_fp);
    }

    int64_t broker_frozen_fp = req.is_buy ? pos_snap.broker_short_frozen_fp
                                          : pos_snap.broker_long_frozen_fp;
    if (broker_frozen_fp < 0) broker_frozen_fp = 0;
    const int64_t local_total_outstanding = outstanding_today_fp + outstanding_yesterday_fp;
    if (broker_frozen_fp > local_total_outstanding) {
        const int64_t delta = broker_frozen_fp - local_total_outstanding;

        if (delta > kFrozenDisagreementEpsilonFp) {
#ifdef XT_HAVE_METRICS
            ct::metrics::XtMetricRegistry::instance()
                .inc_close_quota_disagreement_total("backtest");
#endif
        }

        outstanding_yesterday_fp += delta;
    }
    const int64_t available_today_fp =
        raw_view.today_fp > outstanding_today_fp ? (raw_view.today_fp - outstanding_today_fp) : 0;
    const int64_t available_yesterday_fp =
        raw_view.yesterday_fp > outstanding_yesterday_fp
            ? (raw_view.yesterday_fp - outstanding_yesterday_fp)
            : 0;

    const int64_t total_outstanding_fp = outstanding_today_fp + outstanding_yesterday_fp;
    const int64_t available_total_fp =
        raw_view.total_fp > total_outstanding_fp ? (raw_view.total_fp - total_outstanding_fp) : 0;

    const int64_t avail_fp = close_offset == XtOffset::CloseToday     ? available_today_fp
                             : close_offset == XtOffset::CloseYesterday ? available_yesterday_fp
                                                                        : available_total_fp;

    if (req.qty_fp <= avail_fp + 1) {
        out.passed = true;
        out.reason_code = XtRiskReasonCode::None;
        out.stage = XtRiskStage::CoreRule;
        out.reason_text = risk_reason_text(XtRiskReasonCode::None);
        return out;
    }

    out.passed = false;
    out.reason_code = close_offset == XtOffset::CloseToday
                          ? XtRiskReasonCode::InsufficientCloseToday
                          : (close_offset == XtOffset::CloseYesterday
                                 ? XtRiskReasonCode::InsufficientCloseYesterday
                                 : XtRiskReasonCode::InsufficientAvailablePosition);
    out.stage = XtRiskStage::PositionPrecheck;
    if (req.is_buy) {
        out.reason_text = close_offset == XtOffset::CloseToday
                              ? "insufficient today short position"
                              : (close_offset == XtOffset::CloseYesterday
                                     ? "insufficient yesterday short position"
                                     : "insufficient short position");
    } else {
        out.reason_text = close_offset == XtOffset::CloseToday
                              ? "insufficient today long position"
                              : (close_offset == XtOffset::CloseYesterday
                                     ? "insufficient yesterday long position"
                                     : "insufficient long position");
    }
    return out;
}
