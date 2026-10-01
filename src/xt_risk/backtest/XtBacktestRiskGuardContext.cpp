#include "backtest/XtBacktestRiskGuardContext.h"
#include "xt_account/api/AccountCloseableView.h"
#include "xt_account/api/AccountSnapshot.h"
#include "order/XtOrderRuntimeBook.h"
#include "xt_risk/engine/XtRiskProjection.h"
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
                                                        const XtRiskEvalContext& /*eval_ctx*/) const {
    const auto frame = account_.snapshot_account_and_position(req.code ? req.code : "");
    XtRiskSnapshot snap = xt_risk::project_risk_snapshot(frame.account, frame.position);
    // 涨跌停 venue-truth（tick 内嵌）；0 = 无限制，engine PriceOutOfDailyLimit 检查 bypass。
    snap.limit_up_fp   = limit_up_fp_;
    snap.limit_down_fp = limit_down_fp_;
    // daily_loss：realized-delta 口径（对齐 live venue close_profit 语义）；未配
    // max_daily_loss 时 engine 短路不读。starting_balance 同源 atomic 直读（live
    // 同口径；session_begin hook 冻结，未冻结时为 0）。
    snap.today_pnl_fp = today_pnl_fp_;
    snap.starting_balance_fp = account_.starting_balance_fp();
    return snap;
}

XtRiskResult XtBacktestRiskGuardContext::precheck_input(const xt_core::XtOrder& req,
                                                          const XtRiskEvalContext& /*eval_ctx*/) const {
    // 自成交预防（submit 前价格交叉）：判定与 live XtLiveRiskGuardContext 同口径，
    // InputGuard 阶段拒 SelfTradeDetected；nullptr = 关闭（默认，既有行为）。
    XtRiskResult out;
    out.stage = XtRiskStage::InputGuard;
    // price_fp<=0（未经行情解析的市价单）不进 STP：`0 <= max_bid` 恒真会把
    // InvalidPriceOrQty 误归因成 SelfTradeDetected（live 由 guard_input_validity_
    // 先拦，BT precheck 无等价前置）；放行后由 engine input guard 拒。
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
                                                               const XtRiskEvalContext& /*eval_ctx*/) const {
    XtRiskResult out;
    out.passed = true;
    out.reason_code = XtRiskReasonCode::None;
    out.stage = XtRiskStage::CapabilityGuard;
    out.reason_text = risk_reason_text(XtRiskReasonCode::None);
    return out;
}

XtRiskResult XtBacktestRiskGuardContext::guard_before_submit(const xt_core::XtOrder& req,
                                                               const XtRiskEvalContext& /*eval_ctx*/) const {
    XtRiskResult out;
    if (req.close_offset == XtOffset::Open) {
        out.passed = true;
        out.reason_code = XtRiskReasonCode::None;
        out.stage = XtRiskStage::CoreRule;
        out.reason_text = risk_reason_text(XtRiskReasonCode::None);
        // 期货在途开仓 margin 门禁（回测保真：与 live guard 同口径检查，pass 携 margin
        // 供 accept 侧冻结）。仅 futures-mode：stock-mode（A 股/SEL）资金约束由 engine
        // stock 分支负责并走 stock_capital_reserve 冻结，此处不重复以免双重记账。
        // catalog 未注入时 bypass（轻量测试路径），margin 字段保 0 → gateway 不冻结。
        // 买卖双向都查（期货 short-open 同样占用保证金）。
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

    // 在途平仓量先从本地 OrderRuntimeBook 扣减，保证同一 tick 内连续 close 单
    // 看到前序订单已占用的 today/yesterday 可平额度；这是回测与 live 共享的不变量。
    int64_t outstanding_today_fp = 0;
    int64_t outstanding_yesterday_fp = 0;
    if (order_book_) {
        order_book_->accumulate_close_outstanding_fp(
            req.code, req.is_buy, outstanding_today_fp, outstanding_yesterday_fp);
    }
    // broker_frozen 与本地在途量取更严的一侧：本地 book 覆盖本进程已发单，
    // broker_frozen 覆盖重连 / reverse-sync 后只能从交易所侧观测到的冻结量。
    // 回测通常为 0，max() 退化为 local-only；测试或仿真注入合成值时走同一保守分支。
    int64_t broker_frozen_fp = req.is_buy ? pos_snap.broker_short_frozen_fp
                                          : pos_snap.broker_long_frozen_fp;
    if (broker_frozen_fp < 0) broker_frozen_fp = 0;
    const int64_t local_total_outstanding = outstanding_today_fp + outstanding_yesterday_fp;
    if (broker_frozen_fp > local_total_outstanding) {
        const int64_t delta = broker_frozen_fp - local_total_outstanding;
        // 阈值口径见 XtRiskCfg.h::kFrozenDisagreementEpsilonFp（live/backtest 共用 SSOT）。
        if (delta > kFrozenDisagreementEpsilonFp) {
#ifdef XT_HAVE_METRICS
            ct::metrics::XtMetricRegistry::instance()
                .inc_close_quota_disagreement_total("backtest");
#endif
        }
        // 无法拆分 today/yesterday 时全部计入 yesterday，避免 T+1 路径高估可平昨仓。
        outstanding_yesterday_fp += delta;
    }
    const int64_t available_today_fp =
        raw_view.today_fp > outstanding_today_fp ? (raw_view.today_fp - outstanding_today_fp) : 0;
    const int64_t available_yesterday_fp =
        raw_view.yesterday_fp > outstanding_yesterday_fp
            ? (raw_view.yesterday_fp - outstanding_yesterday_fp)
            : 0;
    // XtOffset::Close 必须从 total_fp 扣总在途量，不能用 today+yesterday 相加。
    // T+1 producer 可把 total_fp 收窄为 yesterday-only；若重新相加独立分量，
    // 会绕过 total_fp 已表达的 venue 约束，导致 T+1 平今漏检。
    const int64_t total_outstanding_fp = outstanding_today_fp + outstanding_yesterday_fp;
    const int64_t available_total_fp =
        raw_view.total_fp > total_outstanding_fp ? (raw_view.total_fp - total_outstanding_fp) : 0;

    const int64_t avail_fp = close_offset == XtOffset::CloseToday     ? available_today_fp
                             : close_offset == XtOffset::CloseYesterday ? available_yesterday_fp
                                                                        : available_total_fp;
    // 只容忍 1 个 fixed-point 最小单位的边界尾差；不是最小交易手数放宽。
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
