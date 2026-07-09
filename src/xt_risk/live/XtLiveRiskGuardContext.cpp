#include "live/XtLiveRiskGuardContext.h"
#include "live/XtPerSidTodayPnl.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstring>
#include <optional>
#include <string>
#include <utility>

#include "api/IMdGateway.h"
#include "api/ITradeGateway.h"
#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif
#include "xt_runtime/order/XtOrderRuntimeBook.h"
#include "xt_account/api/AccountSnapshot.h"
#include "xt_account/state/AccountStateStore.h"
#include "xt_metrics/XtStrategyPnlAggregator.h"
#include "xt_metrics/XtStrategyPnlBaseline.h"
#include "xt_config/catalog/XtCommodityCatalog.h"
#include "XtFixedPoint.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_risk/engine/XtRiskMonitorState.h"
#include "xt_util/time/XtSteadyClock.h"
#include "XtLogger.h"
#include "xt_util/time/CtHHMM.h"
#include "xt_core/session/XtSessionFilter.h"
#include "xt_util/time/XtTime.h"

namespace {

using xt_hhmm::valid_hhmm;

int64_t pick_closeable_by_offset(XtOffset close_offset, int64_t today_fp, int64_t yesterday_fp) {
    switch (close_offset) {
    case XtOffset::CloseToday:
        return today_fp;
    case XtOffset::CloseYesterday:
        return yesterday_fp;
    case XtOffset::Close:
    default:
        return today_fp + yesterday_fp;
    }
}

XtRiskResult pass_precheck(XtRiskStage stage) {
    XtRiskResult out;
    out.passed = true;
    out.reason_code = XtRiskReasonCode::None;
    out.stage = stage;
    out.reason_text = risk_reason_text(XtRiskReasonCode::None);
    return out;
}

XtRiskResult reject_precheck(XtRiskReasonCode code, XtRiskStage stage, const char* text = nullptr) {
    XtRiskResult out;
    out.passed = false;
    out.reason_code = code;
    out.stage = stage;
    out.reason_text = (text && text[0] != '\0') ? text : risk_reason_text(code);
    return out;
}

XtRiskResult reject_precheck(XtRiskReasonCode code, XtRiskStage stage, std::string text) {
    XtRiskResult out;
    out.passed = false;
    out.reason_code = code;
    out.stage = stage;
    out.reason_text = text.empty() ? risk_reason_text(code) : std::move(text);
    return out;
}

// guard_before_submit 内部聚合账户、持仓和在途平仓资源；数量和金额均为 fixed-point。
struct SubmitResourceSnapshot {
    int64_t per_unit_margin_fp          = 0;
    int64_t available_cash_fp           = 0;
    bool    position_snapshot_fresh     = true;
    int64_t closeable_today_fp          = 0;
    int64_t closeable_yesterday_fp      = 0;
    int64_t outstanding_today_fp        = 0;
    int64_t outstanding_yesterday_fp    = 0;
};

}

XtLiveRiskGuardContext::XtLiveRiskGuardContext(Config cfg,
                                           std::atomic<bool>* trading_ready,
                                           std::atomic<uint32_t>* last_tick_action_time,
                                           std::atomic<bool>* td_position_ready,
                                           const std::atomic<ITradeGateway*>* td,
                                           const std::atomic<IMdGateway*>* md,
                                           const std::atomic<int64_t>* block_until_ms,
                                           OrderRateLimitFn order_rate_limit_fn)
    : cfg_(std::move(cfg)),
      trading_ready_(trading_ready),
      last_tick_action_time_(last_tick_action_time),
      td_position_ready_(td_position_ready),
      td_(td),
      md_(md),
      block_until_ms_(block_until_ms),
      order_rate_limit_fn_(std::move(order_rate_limit_fn)) {}

void XtLiveRiskGuardContext::set_strategy_pnl_aggregator(
    const ct::metrics::XtStrategyPnlAggregator* aggr) noexcept {

    strategy_pnl_aggr_.store(aggr, std::memory_order_release);
}

void XtLiveRiskGuardContext::set_strategy_pnl_baseline(
    const ct::metrics::XtStrategyPnlBaseline* baseline) noexcept {

    strategy_pnl_baseline_.store(baseline, std::memory_order_release);
}

void XtLiveRiskGuardContext::set_per_strategy_risk_monitor_state(
    const xt_risk::XtRiskMonitorState* monitor_state) noexcept {
    per_strategy_monitor_state_.store(monitor_state, std::memory_order_release);
}

void XtLiveRiskGuardContext::wire(GuardDeps deps) {

    if (wired_) {
        XtLogger::warn_m("RT", "XtLiveRiskGuardContext::wire() called twice; ignoring re-wire");
        return;
    }
    kill_switch_               = deps.kill_switch;
    stp_checker_               = deps.stp_checker;
    today_pnl_fn_              = std::move(deps.today_pnl_fn);
    velocity_limiter_fn_       = std::move(deps.velocity_limiter_fn);
    exchange_time_fn_          = std::move(deps.exchange_time_fn);
    catalog_                   = deps.catalog;
    account_store_submit_      = deps.account_store;
    order_book_submit_         = deps.order_book;
    last_position_snapshot_ms_ = deps.last_position_snapshot_ms;
    stale_guard_enabled_       = deps.stale_guard_enabled;
    stale_ms_                  = deps.stale_ms;
    wired_                     = true;
}

std::string XtLiveRiskGuardContext::validate(const XtRiskCfg& risk_cfg) const {
    if (risk_cfg.max_daily_loss > 0.0 && !today_pnl_fn_) {
        return "max_daily_loss > 0 but today_pnl_fn is not injected: "
               "DailyLossExceeded check will never trigger (silent bypass). "
               "Wire GuardDeps.today_pnl_fn (see XtLiveOrderGateway::inject_components).";
    }

    if (!risk_cfg.per_strategy_daily_loss_pct.empty() &&
        !strategy_pnl_aggr_.load(std::memory_order_acquire)) {
        return "per_strategy_daily_loss_pct configured but XtStrategyPnlAggregator "
               "is not injected: StrategyDailyLossExceeded check will never trigger "
               "(silent bypass). Call set_strategy_pnl_aggregator() before trading starts.";
    }

    if (cfg_.session_gate_enabled && cfg_.session_gate_sections.empty()) {
        return "session_gate_enabled=true but session_gate_sections is empty: "
               "all opening orders will pass the session gate (silent allow-all). "
               "Either add session_gate_sections or disable session_gate_enabled.";
    }
    return {};
}

XtRiskSnapshot XtLiveRiskGuardContext::make_snapshot(const xt_core::XtOrder& req,
                                                    const XtRiskEvalContext& eval_ctx) const {

    XtRiskSnapshot out{};
    if (account_store_submit_) {
        out = account_store_submit_->snapshot_risk(req.code);

        out.starting_balance_fp = account_store_submit_->starting_balance_fp();
    }
    if (today_pnl_fn_) out.today_pnl_fp = today_pnl_fn_();

    const auto* aggr = strategy_pnl_aggr_.load(std::memory_order_acquire);
    if (aggr && eval_ctx.strategy_id != 0) {
        if (auto* reg = xt_identity::StrategySidRegistry::global()) {
            const std::string& sid_str = reg->name(eval_ctx.strategy_id);
            if (!sid_str.empty()) {

                const auto* baseline = strategy_pnl_baseline_.load(std::memory_order_acquire);
                out.per_strategy_today_pnl_fp =
                    xt_risk::live::per_sid_today_realized_fp(aggr, baseline, sid_str);
            }
        }
    }

    const auto* mon = per_strategy_monitor_state_.load(std::memory_order_acquire);
    if (mon && eval_ctx.strategy_id != 0) {
        out.strategy_killed = mon->per_strategy.is_strategy_killed(eval_ctx.strategy_id);
    }
    return out;
}

std::optional<XtRiskResult> XtLiveRiskGuardContext::guard_kill_switch_(
    const xt_core::XtOrder& req, const XtRiskEvalContext& eval_ctx) const {

    if (kill_switch_ && kill_switch_->load(std::memory_order_acquire)) {

        if (!eval_ctx.is_forced_close || req.close_offset == XtOffset::Open)
            return reject_precheck(XtRiskReasonCode::KillSwitchActive, XtRiskStage::InputGuard);
    }
    return std::nullopt;
}

std::optional<XtRiskResult> XtLiveRiskGuardContext::guard_session_and_throttle_(
    const xt_core::XtOrder& req, const XtRiskEvalContext& eval_ctx) const {

    if (cfg_.session_gate_enabled && !cfg_.session_gate_sections.empty()) {
        const int gate_hhmm = resolve_gate_hhmm();
        if (!xt_core::in_hhmm_sections_halfopen(gate_hhmm, cfg_.session_gate_sections))
            return reject_precheck(XtRiskReasonCode::SessionClosed, XtRiskStage::InputGuard);
    }

    const bool skip_throttle = eval_ctx.is_forced_close || eval_ctx.dry_run;
    if (!skip_throttle) {
        if (block_until_ms_ && block_until_ms_->load(std::memory_order_acquire) > eval_ctx.now_ms)
            return reject_precheck(XtRiskReasonCode::Cooldown, XtRiskStage::InputGuard);
        if (order_rate_limit_fn_ && !order_rate_limit_fn_(cfg_.max_orders_per_sec, eval_ctx.now_ms))
            return reject_precheck(XtRiskReasonCode::RateLimited, XtRiskStage::InputGuard);

        if (velocity_limiter_fn_ && !velocity_limiter_fn_(req.code, req.qty_fp, eval_ctx.now_ms))
            return reject_precheck(XtRiskReasonCode::VelocityLimitExceeded, XtRiskStage::InputGuard);
    }
    return std::nullopt;
}

std::optional<XtRiskResult> XtLiveRiskGuardContext::guard_input_validity_(
    const xt_core::XtOrder& req) const {

    auto invalid_input = []() {
        return reject_precheck(XtRiskReasonCode::InvalidPriceOrQty, XtRiskStage::InputGuard);
    };
    if (req.code[0] == '\0' || std::strlen(req.code) > 15)    return invalid_input();
    if (std::strpbrk(req.code, " \t\n\r") != nullptr)         return invalid_input();
    if (req.price_fp <= 0 || req.qty_fp <= 0)                 return invalid_input();
    return std::nullopt;
}

std::optional<XtRiskResult> XtLiveRiskGuardContext::guard_trading_ready_and_tick_(
    const xt_core::XtOrder& req) const {

    if (!catalog_)
        return reject_precheck(XtRiskReasonCode::TradingNotReady, XtRiskStage::InputGuard, "runtime missing");
    if (!trading_ready_ || !trading_ready_->load(std::memory_order_acquire))
        return reject_precheck(XtRiskReasonCode::TradingNotReady, XtRiskStage::InputGuard);
    if (account_store_submit_ && account_store_submit_->symbols_frozen() &&
        !account_store_submit_->is_registered_symbol(req.code ? req.code : "")) {
        return reject_precheck(XtRiskReasonCode::TradingNotReady, XtRiskStage::InputGuard,
                               std::string("unregistered symbol after freeze: ") +
                                   (req.code ? req.code : ""));
    }
    {

        const double price = xt_fp::from_fp(req.price_fp);
        if (!std::isfinite(price) || price <= 0.0)
            return reject_precheck(XtRiskReasonCode::PriceTickMismatch, XtRiskStage::InputGuard);
        const XtCommodityInfo* info = catalog_->find(req.code);
        const double tick = (info && std::isfinite(info->price_tick) && info->price_tick > 0.0)
                                ? info->price_tick : 0.0;
        if (tick > 0.0) {
            const double units = price / tick;
            if (std::fabs(units - std::round(units)) > 1e-6)
                return reject_precheck(XtRiskReasonCode::PriceTickMismatch, XtRiskStage::InputGuard);
        }
    }
    return std::nullopt;
}

std::optional<XtRiskResult> XtLiveRiskGuardContext::guard_td_and_closeout_(
    const xt_core::XtOrder& req) const {

    if (td_position_ready_ && !td_position_ready_->load(std::memory_order_acquire))
        return reject_precheck(XtRiskReasonCode::TdRecovering, XtRiskStage::InputGuard);

    if (cfg_.closeout_enabled && req.close_offset == XtOffset::Open) {
        const int gate_hhmm = resolve_gate_hhmm();
        if (cfg_.session_gate_enabled && !cfg_.session_gate_sections.empty()
            && !xt_core::in_hhmm_sections_halfopen(gate_hhmm, cfg_.session_gate_sections))
            return reject_precheck(XtRiskReasonCode::CloseoutNoOpen, XtRiskStage::InputGuard);
        if (xt_core::in_closeout_no_open_window_closed(gate_hhmm, cfg_.closeout_windows))
            return reject_precheck(XtRiskReasonCode::CloseoutNoOpen, XtRiskStage::InputGuard);
    }

    if (td_) {
        const ITradeGateway* td = td_->load(std::memory_order_acquire);
        if (td && td->is_recovering())
            return reject_precheck(XtRiskReasonCode::TdRecovering, XtRiskStage::InputGuard);
    }
    return std::nullopt;
}

XtRiskResult XtLiveRiskGuardContext::precheck_input(const xt_core::XtOrder& req,
                                                     const XtRiskEvalContext& eval_ctx) const {

    if (auto r = guard_kill_switch_(req, eval_ctx)) return *r;
    if (!eval_ctx.run_input_guards) return pass_precheck(XtRiskStage::InputGuard);
    if (auto r = guard_session_and_throttle_(req, eval_ctx)) return *r;
    if (auto r = guard_input_validity_(req)) return *r;
    if (auto r = guard_trading_ready_and_tick_(req)) return *r;
    if (auto r = guard_td_and_closeout_(req)) return *r;

    if (stp_checker_ && stp_checker_->would_self_trade(req))
        return reject_precheck(XtRiskReasonCode::SelfTradeDetected, XtRiskStage::InputGuard);
    return pass_precheck(XtRiskStage::InputGuard);
}

XtRiskResult XtLiveRiskGuardContext::precheck_capability(const xt_core::XtOrder& req,
                                                           const XtRiskEvalContext&  ) const {
    ITradeGateway* td = td_ ? td_->load(std::memory_order_acquire) : nullptr;
    if (!td) {
        return reject_precheck(XtRiskReasonCode::TdNotInitialized, XtRiskStage::CapabilityGuard);
    }

    const ITradeGateway::Capabilities td_caps = td->capabilities();
    if (req.close_offset == XtOffset::CloseToday && !td_caps.supports_close_today) {
        return reject_precheck(XtRiskReasonCode::TdCapCloseTodayUnsupported,
                               XtRiskStage::CapabilityGuard);
    }
    if ((req.tif == XtTimeInForce::IOC || req.tif == XtTimeInForce::FOK) && !td_caps.supports_ioc_fok) {
        return reject_precheck(XtRiskReasonCode::TdCapIocFokUnsupported,
                               XtRiskStage::CapabilityGuard);
    }
    if (cfg_.require_l2_orderflow) {

        int8_t l2 = cached_l2_support_.load(std::memory_order_acquire);
        if (l2 < 0) {
            const IMdGateway* md = md_ ? md_->load(std::memory_order_acquire) : nullptr;
            if (!md) {
                return reject_precheck(XtRiskReasonCode::TdCapL2OrderFlowUnsupported,
                                       XtRiskStage::CapabilityGuard);
            }
            l2 = md->capabilities().supports_l2_orderflow ? int8_t{1} : int8_t{0};
            cached_l2_support_.store(l2, std::memory_order_release);
        }
        if (!l2) {
            return reject_precheck(XtRiskReasonCode::TdCapL2OrderFlowUnsupported,
                                   XtRiskStage::CapabilityGuard);
        }
    }
    return pass_precheck(XtRiskStage::CapabilityGuard);
}

namespace {

std::optional<XtRiskResult> check_close_quota(const xt_core::XtOrder& req,
                                              const SubmitResourceSnapshot& snap) {
    auto avail = [](int64_t total, int64_t outstanding) noexcept -> int64_t {
        return total > outstanding ? total - outstanding : int64_t{0};
    };
    const int64_t avail_today_fp     = avail(snap.closeable_today_fp, snap.outstanding_today_fp);
    const int64_t avail_yest_fp      = avail(snap.closeable_yesterday_fp, snap.outstanding_yesterday_fp);
    const int64_t raw_closeable_fp   = pick_closeable_by_offset(req.close_offset, snap.closeable_today_fp, snap.closeable_yesterday_fp);
    const int64_t avail_closeable_fp = pick_closeable_by_offset(req.close_offset, avail_today_fp, avail_yest_fp);

    if (req.qty_fp > avail_closeable_fp + 1) {
        if (req.qty_fp <= raw_closeable_fp + 1)
            return reject_precheck(XtRiskReasonCode::CloseSyncPending, XtRiskStage::PositionPrecheck);
        if (req.close_offset == XtOffset::CloseToday)
            return reject_precheck(XtRiskReasonCode::InsufficientCloseToday, XtRiskStage::PositionPrecheck);
        if (req.close_offset == XtOffset::CloseYesterday)
            return reject_precheck(XtRiskReasonCode::InsufficientCloseYesterday, XtRiskStage::PositionPrecheck);
        return reject_precheck(XtRiskReasonCode::InsufficientAvailablePosition, XtRiskStage::PositionPrecheck);
    }
    return std::nullopt;
}
}

XtRiskResult XtLiveRiskGuardContext::guard_before_submit(const xt_core::XtOrder& req,
                                                          const XtRiskEvalContext&  ) const {
    if (!account_store_submit_ || !order_book_submit_ || !catalog_)
        return reject_precheck(XtRiskReasonCode::TradingNotReady, XtRiskStage::InputGuard, "runtime missing");
    if (account_store_submit_->symbols_frozen() &&
        !account_store_submit_->is_registered_symbol(req.code ? req.code : "")) {
        return reject_precheck(XtRiskReasonCode::TradingNotReady, XtRiskStage::InputGuard,
                               std::string("unregistered symbol after freeze: ") +
                                   (req.code ? req.code : ""));
    }

    const uint64_t now_ms = stale_guard_enabled_
                                ? static_cast<uint64_t>(XtSteadyClock::steady_now_ms())
                                : 0;

    const XtCommodityInfo* info = catalog_->find(req.code);

    SubmitResourceSnapshot snap;
    snap.per_unit_margin_fp = catalog_->per_unit_margin_fp(info, req.code, req.price_fp);

    bool snapshot_fresh = true;
    if (stale_guard_enabled_) {
        const uint64_t last_ms = last_position_snapshot_ms_
                                     ? last_position_snapshot_ms_->load(std::memory_order_acquire)
                                     : 0;
        if (last_ms == 0 || now_ms < last_ms) {
            snapshot_fresh = false;
        } else {
            snapshot_fresh = (now_ms - last_ms) <= static_cast<uint64_t>(stale_ms_);
        }
    }
    snap.position_snapshot_fresh = snapshot_fresh;

    constexpr uint64_t kBrokerFrozenStaleMs = 30'000;
    int64_t broker_frozen_fp = 0;
    const bool venue_has_broker_frozen =
        (info && info->has_broker_frozen_push);
    if (snapshot_fresh) {
        const auto pos = account_store_submit_->snapshot_position(req.code ? req.code : "");
        const auto closeable = xt_account::closeable_view(pos, req.is_buy);
        snap.closeable_today_fp     = closeable.today_fp;
        snap.closeable_yesterday_fp = closeable.yesterday_fp;

        if (!pos.closeable_known) {

            return reject_precheck(XtRiskReasonCode::PositionStateUnknown,
                                   XtRiskStage::PositionPrecheck);
        }
        if (venue_has_broker_frozen) {

            broker_frozen_fp = req.is_buy ? pos.broker_short_frozen_fp
                                           : pos.broker_long_frozen_fp;
            if (broker_frozen_fp < 0) broker_frozen_fp = 0;
            if (broker_frozen_fp > 0 && stale_guard_enabled_) {
                const auto last_update_ms = static_cast<uint64_t>(pos.broker_frozen_last_update_ms);
                if (last_update_ms == 0 || now_ms < last_update_ms ||
                    (now_ms - last_update_ms) > kBrokerFrozenStaleMs) {
#ifdef XT_HAVE_METRICS
                    ct::metrics::XtMetricRegistry::instance()
                        .inc_broker_frozen_stale_total("ctp");
#endif
                    broker_frozen_fp = 0;
                }
            }
        }
    }
    snap.available_cash_fp = account_store_submit_->snapshot_account().available_cash;

    if (snap.position_snapshot_fresh) {

        order_book_submit_->accumulate_close_outstanding_fp(
            req.code ? req.code : "", req.is_buy,
            snap.outstanding_today_fp, snap.outstanding_yesterday_fp);

        const int64_t local_total_outstanding =
            snap.outstanding_today_fp + snap.outstanding_yesterday_fp;
        if (broker_frozen_fp > local_total_outstanding) {
            const int64_t delta = broker_frozen_fp - local_total_outstanding;

            if (delta > kFrozenDisagreementEpsilonFp) {
#ifdef XT_HAVE_METRICS
                ct::metrics::XtMetricRegistry::instance()
                    .inc_close_quota_disagreement_total("ctp");
#endif
            }

            snap.outstanding_yesterday_fp += delta;
        }
    }
    const int64_t required_margin_fp =
        req.close_offset == XtOffset::Open ? xt_fp::mul(snap.per_unit_margin_fp, req.qty_fp) : 0;

    auto with_margin = [&](XtRiskResult r) noexcept -> XtRiskResult {
        r.required_margin_fp = required_margin_fp;
        r.per_unit_margin_fp = snap.per_unit_margin_fp;
        r.available_cash_fp  = snap.available_cash_fp;
        return r;
    };

    if (!snap.position_snapshot_fresh)
        return reject_precheck(XtRiskReasonCode::PositionSyncStale, XtRiskStage::PositionPrecheck);

    if (req.close_offset != XtOffset::Open) {
        if (auto r = check_close_quota(req, snap)) return *r;
    }

    if (required_margin_fp > 0 && snap.available_cash_fp < required_margin_fp)
        return with_margin(reject_precheck(XtRiskReasonCode::InsufficientMargin, XtRiskStage::MarginPrecheck));

    return with_margin(pass_precheck(XtRiskStage::MarginPrecheck));
}

int XtLiveRiskGuardContext::resolve_gate_hhmm() const {
    if (exchange_time_fn_) {
        return exchange_time_fn_();
    }
    if (last_tick_action_time_) {
        const uint32_t action_tm = last_tick_action_time_->load(std::memory_order_relaxed);

        const int tick_hhmm = action_tm == 0 ? -1 : static_cast<int>(action_tm / 100000);
        return valid_hhmm(tick_hhmm) ? tick_hhmm : xt_time::local_hhmm_now();
    }
    return xt_time::local_hhmm_now();
}
