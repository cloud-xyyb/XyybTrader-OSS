#include "xt_identity/OrderAttributionStore.h"
#include "XtLiveOrderGateway.h"

#include "xt_risk/engine/AnnualLatchArm.h"
#include "xt_risk/engine/XtRiskMonitorState.h"
#include "xt_identity/StrategySidRegistry.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <string>
#include <thread>

#include <chrono>

#include "CloseOffsetCore.h"
#include "xt_core/order/XtOffsetRouter.h"
#include "xt_config/catalog/XtCommodityCatalog.h"
#include "XtLogger.h"
#include "xt_core/session/XtSessionPhase.h"
#include "latency/XtLiveLatencyTracker.h"
#include "XtLiveParamDomain.h"
#include "recording/XtLiveRecorder.h"
#include "xt_core/XtLiveTapContext.h"
#include "xt_util/time/XtTime.h"
#include "xt_util/time/XtSteadyClock.h"
#include "xt_risk/engine/RiskActionDispatch.h"
#include "live/LiveRejectReason.h"
#include "XtFixedPoint.h"
#include "xt_account/api/AccountSnapshot.h"
#include "xt_runtime/order/XtOrderRuntimeBook.h"
#include "xt_account/state/AccountStateStore.h"
#include "xt_core/order/XtOrderLifecycle.h"
#include "xt_logger/XtCallbackGuard.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/sync/XtTransparentStringHash.h"
#include "xt_account/state/AccountEvent.h"

using xt_runtime::SubmitResult;
using xt_runtime::XtRejectCode;

namespace {

std::atomic<uint64_t> g_margin_reject_callback_exception_total{0};

static XtRejectCode to_reject_code(XtLiveRejectReason c) {
    switch (c) {
    case XtLiveRejectReason::InvalidInput:                  return XtRejectCode::InvalidInput;
    case XtLiveRejectReason::UnknownStrategySid:            return XtRejectCode::UnknownStrategySid;
    case XtLiveRejectReason::InvalidFlag:                   return XtRejectCode::InvalidFlag;
    case XtLiveRejectReason::PriceTickMismatch:             return XtRejectCode::PriceTickMismatch;
    case XtLiveRejectReason::TradingNotReady:               return XtRejectCode::TradingNotReady;
    case XtLiveRejectReason::Cooldown:                      return XtRejectCode::Cooldown;
    case XtLiveRejectReason::RateLimited:                   return XtRejectCode::RateLimited;
    case XtLiveRejectReason::TdRecovering:                  return XtRejectCode::TradingNotReady;
    case XtLiveRejectReason::TdNotInitialized:              return XtRejectCode::TradingNotReady;
    case XtLiveRejectReason::TdCapCloseTodayUnsupported:    return XtRejectCode::CapabilityUnsupported;
    case XtLiveRejectReason::TdCapL2OrderFlowUnsupported:   return XtRejectCode::CapabilityUnsupported;
    case XtLiveRejectReason::TdCapIocFokUnsupported:        return XtRejectCode::CapabilityUnsupported;
    case XtLiveRejectReason::CloseoutNoOpen:                return XtRejectCode::CloseoutNoOpen;

    case XtLiveRejectReason::SessionClosed:                 return XtRejectCode::CloseoutNoOpen;
    case XtLiveRejectReason::MaxOrderQty:                   return XtRejectCode::MaxOrderQty;
    case XtLiveRejectReason::MaxOrderNotional:              return XtRejectCode::MaxOrderNotional;
    case XtLiveRejectReason::MinOrderNotional:              return XtRejectCode::MinOrderNotional;
    case XtLiveRejectReason::PositionLimit:                 return XtRejectCode::PositionLimit;
    case XtLiveRejectReason::ShortNotAllowed:               return XtRejectCode::ShortNotAllowed;
    case XtLiveRejectReason::InsufficientMargin:            return XtRejectCode::InsufficientMargin;
    case XtLiveRejectReason::InsufficientCloseToday:        return XtRejectCode::InsufficientCloseToday;
    case XtLiveRejectReason::InsufficientCloseYesterday:    return XtRejectCode::InsufficientCloseYesterday;
    case XtLiveRejectReason::InCallAuctionLimitNotAllowed:  return XtRejectCode::InCallAuctionLimitNotAllowed;
    case XtLiveRejectReason::PositionSyncStale:             return XtRejectCode::PositionSyncStale;
    case XtLiveRejectReason::CloseSyncPending:              return XtRejectCode::CloseSyncPending;
    case XtLiveRejectReason::KillSwitchActive:              return XtRejectCode::KillSwitchActive;
    case XtLiveRejectReason::HardCircuitBreakerTripped:     return XtRejectCode::HardCircuitBreakerTripped;
    case XtLiveRejectReason::SelfTradeDetected:             return XtRejectCode::SelfTradeDetected;
    case XtLiveRejectReason::VelocityLimitExceeded:         return XtRejectCode::VelocityLimitExceeded;
    case XtLiveRejectReason::DailyLossExceeded:             return XtRejectCode::DailyLossExceeded;
    case XtLiveRejectReason::GateDisabled:                  return XtRejectCode::Gated;
    case XtLiveRejectReason::OpenHalted:                    return XtRejectCode::OpenHalted;
    case XtLiveRejectReason::OppositeOpenPending:           return XtRejectCode::OppositeOpenPending;
    case XtLiveRejectReason::AutoOffsetUnresolved:          return XtRejectCode::AutoOffsetUnresolved;
    case XtLiveRejectReason::SymbolBlacklisted:            return XtRejectCode::SymbolBlacklisted;
    case XtLiveRejectReason::OrderCountExceeded:           return XtRejectCode::OrderCountExceeded;
    case XtLiveRejectReason::OrderRateExceeded:            return XtRejectCode::OrderRateExceeded;
    case XtLiveRejectReason::SelfMatchPostTrade:           return XtRejectCode::SelfMatchPostTrade;
    case XtLiveRejectReason::AnnualLossExceeded:           return XtRejectCode::AnnualLossExceeded;
    case XtLiveRejectReason::StrategyMaxPosQtyExceeded:    return XtRejectCode::StrategyMaxPosQtyExceeded;
    case XtLiveRejectReason::StrategyDailyLossExceeded:    return XtRejectCode::StrategyDailyLossExceeded;
    case XtLiveRejectReason::StrategyKillSwitchActive:     return XtRejectCode::StrategyKillSwitchActive;
    case XtLiveRejectReason::StrategyBlockedByWatchdog:    return XtRejectCode::StrategyBlockedByWatchdog;

    default:                                              return XtRejectCode::TradingNotReady;
    }
}

const char* open_halt_reason_text(xt_risk::XtOpenHaltReason reason) {
    switch (reason) {
    case xt_risk::XtOpenHaltReason::IddDrawdown: return "idd_drawdown";
    case xt_risk::XtOpenHaltReason::StaleQuote:  return "stale_quote";
    case xt_risk::XtOpenHaltReason::None:
    default: return "unknown";
    }
}

uint64_t now_us() {
    return XtSteadyClock::steady_now_us();
}

XtRiskResult reject_decision(XtRiskReasonCode code, XtRiskStage stage, const char* text = nullptr) {
    XtRiskResult out;
    out.passed = false;
    out.reason_code = code;
    out.stage = stage;
    out.reason_text = (text && text[0] != '\0') ? text : risk_reason_text(code);
    return out;
}

void enforce_td_binding_invariant(bool trading_ready,
                                  ITradeGateway* td,
                                  const char* op) {
    if (!trading_ready || td) return;
    XtLogger::error_m("RT",
                      "fatal invariant broken: trading_ready=true but td gateway missing op={}",
                      op ? op : "unknown");
    assert(false && "trading_ready=true requires td gateway bound");
    std::abort();
}

struct TdReadySnapshot {
    ITradeGateway* td = nullptr;
    bool ready = false;
};

	TdReadySnapshot load_td_ready_snapshot(const std::atomic<ITradeGateway*>& td_slot,
	                                       const std::atomic<bool>* trading_ready,
	                                       const char* op) {
    TdReadySnapshot out;
    out.td = td_slot.load(std::memory_order_acquire);
    out.ready = trading_ready && trading_ready->load(std::memory_order_acquire);
    enforce_td_binding_invariant(out.ready, out.td, op);
	    return out;
	}

	const char* trace_venue(xt_core::XtVenueKind venue) noexcept {
	    switch (venue) {
	    case xt_core::XtVenueKind::Continuous24h: return "continuous_24h";
	    case xt_core::XtVenueKind::FuturesCN:     return "futures_cn";
	    case xt_core::XtVenueKind::AShare:        return "a_share";
	    case xt_core::XtVenueKind::Unknown:
	    default: return "unknown";
	    }
	}

	const char* trace_tif(XtTimeInForce tif) noexcept {
	    switch (tif) {
	    case XtTimeInForce::GTC: return "GTC";
	    case XtTimeInForce::IOC: return "IOC";
	    case XtTimeInForce::FOK: return "FOK";
	    }
	    return "unknown";
	}

	const char* trace_offset(XtOffset offset) noexcept {
	    switch (offset) {
	    case XtOffset::Open:           return "open";
	    case XtOffset::Close:          return "close";
	    case XtOffset::CloseToday:     return "close_today";
	    case XtOffset::CloseYesterday: return "close_yesterday";
	    case XtOffset::Auto:           return "auto";
	    }
	    return "unknown";
	}

	std::string trace_exec_flags(ExecFlags flags) {
	    if (flags == exec_none()) return "none";
	    std::string out;
	    const auto add = [&out](const char* s) {
	        if (!out.empty()) out += '|';
	        out += s;
	    };
	    if (has_flag(flags, ExecFlag::ReduceOnly)) add("reduce_only");
	    if (has_flag(flags, ExecFlag::PostOnly)) add("post_only");
	    if (has_flag(flags, ExecFlag::Hidden)) add("hidden");
	    if (has_flag(flags, ExecFlag::Market)) add("market");
	    constexpr ExecFlags known = static_cast<uint32_t>(ExecFlag::ReduceOnly)
	                              | static_cast<uint32_t>(ExecFlag::PostOnly)
	                              | static_cast<uint32_t>(ExecFlag::Hidden)
	                              | static_cast<uint32_t>(ExecFlag::Market);
	    if ((flags & ~known) != 0) add("custom");
	    return out.empty() ? "custom" : out;
	}

	uint64_t trace_hash(std::string_view s) noexcept {
	    if (s.empty()) return 0;
	    return xt_core::sync::fnv1a_bytes(s.data(), s.size());
	}

/// @brief emit_gateway_intent_trace 收参结构（栈上可平凡拷贝，无堆分配）。
///
/// 字符串字段只借用 caller 生命周期内的缓冲（const char* / string_view），
/// 不做拷贝；trace 关闭（trace_id==0）时 emit 直接 early-return，字符串
/// 构造只发生在 trace 开启后（emit 内部）。字段默认值即“未提供”语义。
struct GatewayIntentTraceCtx {
    uint64_t trace_id = 0;
    const char* stage = nullptr;
    xt_core::XtVenueKind venue = xt_core::XtVenueKind::Unknown;
    const char* code = nullptr;
    bool is_buy = false;
    int64_t price_fp = 0;
    int64_t qty_fp = 0;
    XtTimeInForce tif = XtTimeInForce::GTC;
    XtOffset offset = XtOffset::Open;
    ExecFlags exec_flags = exec_none();
    std::string_view user_tag;
    std::string_view strategy_id;
    uint64_t decision_us = 0;
    uint64_t local_id = 0;
    uint64_t logical_id = 0;
    uint64_t phys_id = 0;
    uint32_t slice_index = 0;
    uint32_t slice_count = 0;
    const char* reject_scope = nullptr;
    const char* reject_code = nullptr;
    const char* reject_reason = nullptr;
    bool partial = false;
    uint32_t failure_slice_index = 0;
    uint32_t accepted_slice_count = 0;
    uint32_t expected_slice_count = 0;
};

void emit_gateway_intent_trace(const GatewayIntentTraceCtx& t) {
    if (t.trace_id == 0) return;
    auto* rec = XtLiveRecorder::global();
    if (!rec) return;
    const std::string exec_flags_text = trace_exec_flags(t.exec_flags);
    const std::string sid(t.strategy_id);
    rec->emit_intent_trace({
        .trace_id           = t.trace_id,
        .stage              = t.stage,
        .venue              = trace_venue(t.venue),
        .strategy_id        = sid.c_str(),
        .book_id            = sid.c_str(),
        .account_id         = nullptr,
        .code_raw           = t.code,
        .code_effective     = t.code,
        .dir                = t.is_buy ? "buy" : "sell",
        .price_raw_fp       = t.price_fp,
        .price_effective_fp = t.price_fp,
        .price_wire_fp      = 0,
        .qty_raw_fp         = t.qty_fp,
        .qty_effective_fp   = t.qty_fp,
        .qty_wire_fp        = 0,
        .tif_raw            = trace_tif(t.tif),
        .tif_effective      = trace_tif(t.tif),
        .tif_wire           = nullptr,
        .offset_raw         = trace_offset(t.offset),
        .offset_effective   = trace_offset(t.offset),
        .reduce_only        = t.offset != XtOffset::Open,
        .exec_flags         = exec_flags_text.c_str(),
        .local_id           = t.local_id,
        .logical_id         = t.logical_id,
        .phys_id            = t.phys_id,
        .slice_index        = t.slice_index,
        .slice_count        = t.slice_count,
        .partial            = t.partial,
        .failure_slice_index = t.failure_slice_index,
        .accepted_slice_count = t.accepted_slice_count,
        .expected_slice_count = t.expected_slice_count,
        .trigger_tick_ts    = xt_core::tap::current_tick_ts_ns(),
        .decision_mono_ns   = t.decision_us * 1000ULL,
        .event_mono_ns      = XtSteadyClock::steady_now_ns(),
        .reject_scope       = t.reject_scope,
        .reject_code        = t.reject_code,
        .reject_reason      = t.reject_reason,
        .user_tag_hash      = trace_hash(t.user_tag),
    });
}

	XtLiveRiskGuardContext::Config build_live_risk_guard_config(const XtLiveOrderGateway::Config& cfg) {
    XtLiveRiskGuardContext::Config out;
    out.max_orders_per_sec = cfg.max_orders_per_sec;
    out.require_l2_orderflow = cfg.require_l2_orderflow;
    out.closeout_enabled = cfg.closeout_enabled;
    out.session_gate_enabled = cfg.session_gate_enabled;
    out.session_gate_sections = cfg.session_gate_sections;
    out.reject_on_position_unknown = cfg.reject_on_position_unknown;

    out.closeout_windows = cfg.closeout_windows;

    return out;
}

}

XtLiveOrderGateway::XtLiveOrderGateway(const Config& cfg,
                                       xt_strategy_engine::IStrategyExecSink* exec_sink,
                                       std::atomic<bool>* trading_ready,
                                       std::atomic<uint32_t>* last_tick_action_time,
                                       std::atomic<bool>* td_position_ready,
                                       XtLiveParamDomain* param_domain,
                                       XtLiveLatencyTracker* latency_tracker,
                                       std::atomic<bool>* kill_switch,
                                       XtRiskEngine::InitDeps risk_engine_deps)
    : cfg_(cfg),
      exec_sink_(exec_sink),
      trading_ready_(trading_ready),
      last_tick_action_time_(last_tick_action_time),
      td_position_ready_(td_position_ready),
      param_domain_(param_domain),
      latency_tracker_(latency_tracker),
      risk_engine_(cfg.risk, risk_engine_deps),
      order_rate_limit_fn_(
          [this](int limit, int64_t now_ms) { return order_limiter_.allow(limit, now_ms); }),
      velocity_limiter_([&cfg] {
          XtVelocityLimiter::Config vc;
          vc.window_ms = cfg.velocity_window_ms;
          vc.max_qty_per_window_fp = cfg.velocity_max_qty > 0.0
              ? xt_fp::to_fp(cfg.velocity_max_qty) : 0;
          return vc;
      }()),
      risk_guard_ctx_(build_live_risk_guard_config(cfg),
                      trading_ready_,
                      last_tick_action_time_,
                      td_position_ready_,
                      &td_,
                      &md_,
                      &block_until_ms_,
                      order_rate_limit_fn_) {
    risk_policy_ = XtRiskPolicy{true, true, false};
    risk_gateway_.bind(risk_engine_, risk_policy_);
    for (auto& c : reject_counts_) {
        c.store(0, std::memory_order_relaxed);
    }

    if (cfg.order_strategy_grace_us > 0) {
        order_strategy_grace_us_ = cfg.order_strategy_grace_us;
    }
    if (param_domain_) {
        gate_seen_version_.store(param_domain_->cached_version(), std::memory_order_relaxed);
        gate_submit_enabled_.store(
            param_domain_->get_bool_or("trading.submit.enabled", true),
            std::memory_order_relaxed);
        gate_cancel_enabled_.store(
            param_domain_->get_bool_or("trading.cancel.enabled", true),
            std::memory_order_relaxed);
    }
    if (cfg_.closeout_enabled) {
        std::string windows;
        for (size_t i = 0; i < cfg_.closeout_windows.size(); ++i) {
            const auto& win = cfg_.closeout_windows[i];
            if (i > 0) windows += "|";
            windows += std::to_string(win.no_open_hhmm) + "-" + std::to_string(win.force_hhmm);
        }
        XtLogger::info_m("RT",
                         "closeout enabled windows={} retry_ms={}",
                         windows,
                         cfg_.closeout_retry_ms);
    }

    kill_switch_ = kill_switch;
}

int64_t XtLiveOrderGateway::xt_steady_now_us_() {
    return static_cast<int64_t>(XtSteadyClock::steady_now_us());
}

uint64_t XtLiveOrderGateway::lookup_trace_id(uint64_t order_id) const noexcept {
    std::shared_lock<std::shared_mutex> lock(order_trace_mu_);
    const auto it = order_trace_.find(order_id);
    return it != order_trace_.end() ? it->second.trace_id : 0;
}

void XtLiveOrderGateway::note_trace_attribution_(uint64_t order_id, uint64_t trace_id) {
    if (order_id == 0 || trace_id == 0) return;
    std::unique_lock<std::shared_mutex> lock(order_trace_mu_);
    order_trace_[order_id] = OrderTraceEntry{trace_id, 0};
}

void XtLiveOrderGateway::mark_order_tracking_terminal_(uint64_t order_id, int64_t now_us) {
    if (order_id == 0) return;
    {
        std::unique_lock<std::shared_mutex> lock(order_strategy_mu_);
        auto it = order_strategy_.find(order_id);
        if (it != order_strategy_.end() && it->second.terminal_at_us == 0) {
            it->second.terminal_at_us = now_us;
        }
    }
    {
        std::unique_lock<std::shared_mutex> lock(order_trace_mu_);
        auto it = order_trace_.find(order_id);
        if (it != order_trace_.end() && it->second.terminal_at_us == 0) {
            it->second.terminal_at_us = now_us;
        }
    }
}

void XtLiveOrderGateway::erase_order_tracking(uint64_t order_id) {
    const int64_t now_us = xt_steady_now_us_();
    mark_order_tracking_terminal_(order_id, now_us);

    const uint64_t done_clord = logical_order_tracker_.lookup_done_clord(order_id);
    if (done_clord != 0 && done_clord != order_id) {
        mark_order_tracking_terminal_(done_clord, now_us);
    }
}

std::size_t XtLiveOrderGateway::prune_terminal_strategy_ids(int64_t cutoff_us) {
    std::size_t removed = 0;
    {
        std::unique_lock<std::shared_mutex> lock(order_strategy_mu_);
        for (auto it = order_strategy_.begin(); it != order_strategy_.end(); ) {
            if (it->second.terminal_at_us != 0 && it->second.terminal_at_us < cutoff_us) {
                it = order_strategy_.erase(it);
                ++removed;
            } else {
                ++it;
            }
        }
    }
    {
        std::unique_lock<std::shared_mutex> lock(order_trace_mu_);
        for (auto it = order_trace_.begin(); it != order_trace_.end(); ) {
            if (it->second.terminal_at_us != 0 && it->second.terminal_at_us < cutoff_us) {
                it = order_trace_.erase(it);
            } else {
                ++it;
            }
        }
    }
    return removed;
}

void XtLiveOrderGateway::insert_self_match_blacklist(const char* code, const char* reason) noexcept {
    risk_engine_.insert_blacklist(code, reason);
}

XtRiskCfg XtLiveOrderGateway::risk_config() const {
    return risk_engine_.config();
}

void XtLiveOrderGateway::set_risk_config(const XtRiskCfg& cfg) {
    risk_engine_.set_config(cfg);
}

XtLiveOrderGateway::AnnualLatchSnapshot XtLiveOrderGateway::annual_latch_snapshot() const {

    return risk_engine_.annual_latch_snapshot();
}

void XtLiveOrderGateway::restore_annual_latch(int year, int64_t year_start_equity_fp,
                                              bool cutoff_active, int64_t year_start_net_pnl_fp,
                                              int64_t risk_fund_balance_fp) {
    risk_engine_.restore_annual_state(year, year_start_equity_fp, cutoff_active,
                                      year_start_net_pnl_fp, risk_fund_balance_fp);
}

void XtLiveOrderGateway::arm_annual_loss_on_session_begin(
    const xt_account::AccountStateStore* account_store,
    uint32_t trading_date, int64_t equity_fp) noexcept {

    if (!account_store) return;
    xt_risk::arm_annual_loss_on_session_begin(
        risk_engine_, trading_date, equity_fp,
        [account_store](int64_t& realized_fp, int64_t& fees_fp, int64_t& unrealized_fp) {
            const auto a = account_store->snapshot_account();
            realized_fp   = a.realized_pnl;
            fees_fp       = a.fees;
            unrealized_fp = a.unrealized_pnl;
        });
}

void XtLiveOrderGateway::set_strategy_pnl_aggregator(
    const ct::metrics::XtStrategyPnlAggregator* aggr) noexcept {

    risk_guard_ctx_.set_strategy_pnl_aggregator(aggr);
}

void XtLiveOrderGateway::set_strategy_pnl_baseline(
    const ct::metrics::XtStrategyPnlBaseline* baseline) noexcept {

    risk_guard_ctx_.set_strategy_pnl_baseline(baseline);
}

void XtLiveOrderGateway::set_per_strategy_risk_monitor_state(
    const xt_risk::XtRiskMonitorState* monitor_state) noexcept {

    risk_guard_ctx_.set_per_strategy_risk_monitor_state(monitor_state);
}

void XtLiveOrderGateway::inject_components(
    xt_runtime::order::XtOrderRuntimeBook* order_book,
    xt_account::AccountStateStore* account_store,
    std::atomic<int64_t>* close_profit_fp,
    const XtCommodityCatalog* catalog,
    const std::atomic<uint64_t>* last_position_snapshot_ms,
    bool stale_guard_enabled,
    int  stale_ms) {
    order_book_direct_      = order_book;
    catalog_direct_         = catalog;
    account_store_direct_   = account_store;
    close_profit_fp_direct_ = close_profit_fp;

    XtLiveRiskGuardContext::GuardDeps deps;
    deps.kill_switch = kill_switch_;
    deps.stp_checker = cfg_.stp_enabled ? &stp_checker_ : nullptr;
    deps.today_pnl_fn = [this]() -> int64_t {
        if (close_profit_fp_direct_) return close_profit_fp_direct_->load(std::memory_order_acquire);
        return 0;
    };
    if (cfg_.velocity_enabled && cfg_.velocity_max_qty > 0.0) {
        deps.velocity_limiter_fn = [this](std::string_view code, int64_t qty_fp, int64_t now_ms) {
            return velocity_limiter_.check_and_record(code, qty_fp, now_ms);
        };
    }
    deps.exchange_time_fn = [this]() -> int {
        if (!last_tick_action_time_) return xt_time::local_hhmm_now();
        const uint32_t action_tm = last_tick_action_time_->load(std::memory_order_relaxed);
        if (action_tm == 0) return xt_time::local_hhmm_now();
        return static_cast<int>(xt_time::action_time_to_hhmm(action_tm));
    };
    if (catalog && order_book && account_store) {
        deps.catalog                   = catalog;
        deps.account_store             = account_store;
        deps.order_book                = order_book;
        deps.last_position_snapshot_ms = last_position_snapshot_ms;
        deps.stale_guard_enabled       = stale_guard_enabled;
        deps.stale_ms                  = stale_ms;
    }
    risk_guard_ctx_.wire(std::move(deps));

}

std::string XtLiveOrderGateway::validate_risk_config() {
    return risk_guard_ctx_.validate(risk_engine_.config());
}

size_t XtLiveOrderGateway::live_orders() const {
    if (!order_book_direct_) return 0;
    return order_book_direct_->open_order_count();
}

void XtLiveOrderGateway::pre_warm_risk(const std::vector<std::string>& symbols) {
    for (const auto& s : symbols) {
        stp_checker_.register_symbol(s);
        velocity_limiter_.register_symbol(s);
    }
}

void XtLiveOrderGateway::notify_order_terminal(const char* code, [[maybe_unused]] bool is_buy) {
    if (!cfg_.stp_enabled) return;

    std::vector<int64_t> bid_prices, ask_prices;
    if (order_book_direct_) {
        order_book_direct_->collect_active_order_prices(code, bid_prices, ask_prices);
    }
    stp_checker_.on_order_terminal(code,
                                   bid_prices.data(), bid_prices.size(),
                                   ask_prices.data(), ask_prices.size());
}

void XtLiveOrderGateway::refresh_param_gate_cache() {
    if (!param_domain_) return;
    const uint64_t cur = param_domain_->cached_version();
    const uint64_t seen = gate_seen_version_.load(std::memory_order_relaxed);
    if (cur == seen) return;
    gate_submit_enabled_.store(param_domain_->get_bool_or("trading.submit.enabled", true), std::memory_order_relaxed);
    gate_cancel_enabled_.store(param_domain_->get_bool_or("trading.cancel.enabled", true), std::memory_order_relaxed);

    gate_seen_version_.store(param_domain_->cached_version(), std::memory_order_release);
}

void XtLiveOrderGateway::bind_td(ITradeGateway* td) {
    td_.store(td, std::memory_order_release);
}

void XtLiveOrderGateway::bind_md(IMdGateway* md) {
    md_.store(md, std::memory_order_release);
}

int64_t XtLiveOrderGateway::now_ms() {
    return XtSteadyClock::steady_now_ms();
}

bool XtLiveOrderGateway::is_valid_code(const char* code) {
    if (!code || code[0] == '\0') return false;
    size_t len = 0;
    for (const char* p = code; *p != '\0'; ++p) {
        if (++len > 32) return false;
        if (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') return false;
    }
    return true;
}

bool XtLiveOrderGateway::is_valid_order_input(const char* code, int64_t price_fp, int64_t qty_fp) {
    if (!is_valid_code(code)) return false;
    if (price_fp <= 0 || qty_fp <= 0) return false;
    return true;
}

bool XtLiveOrderGateway::build_close_slices(const char* code,
                                          bool is_buy,
                                          int64_t qty_fp,
                                          bool prefer_today,
                                          SubmitSliceArray& out) const {
    out.count = 0;
    if (qty_fp <= 0) return false;
    xt_account::CloseableView closeable{};
    if (account_store_direct_) {
        closeable = xt_account::closeable_view(
            account_store_direct_->snapshot_position(code ? code : ""), is_buy);
    }

    close_offset_core::CloseSlice slices[2];
    const int n = close_offset_core::resolve_close_slices(
        code, qty_fp, closeable.today_fp, closeable.yesterday_fp, prefer_today, slices);
    if (n == 0) return false;
    for (int i = 0; i < n; ++i) out.push(slices[i].offset, slices[i].qty_fp);
    return true;
}

void XtLiveOrderGateway::record_reject(XtLiveRejectReason code) {
    size_t idx = live_reject_reason_index(code);
    if (idx >= reject_counts_.size()) {
        idx = live_reject_reason_index(XtLiveRejectReason::Unknown);
    }
    reject_counts_[idx].fetch_add(1, std::memory_order_relaxed);

    if (auto* rec = XtLiveRecorder::global()) {
        const uint64_t ts_local_ns = XtSteadyClock::steady_now_ns();
        rec->emit_order_rtn(ts_local_ns,
                             0,
                             0,
                            "Rejected",
                             0,
                            live_reject_reason_str(code),
                             "");
    }
}

void XtLiveOrderGateway::maybe_enter_cooldown_on_reject(int64_t now_ms) {
    if (cfg_.max_rejects_per_min <= 0 || cfg_.cooldown_ms <= 0) return;
    if (reject_limiter_.allow(cfg_.max_rejects_per_min, now_ms)) return;
    const int64_t until = now_ms + cfg_.cooldown_ms;
    int64_t cur = block_until_ms_.load(std::memory_order_relaxed);
    while (cur < until &&
           !block_until_ms_.compare_exchange_weak(
               cur, until, std::memory_order_release, std::memory_order_relaxed)) {}
}

XtLiveOrderGateway::SubmitSliceArray XtLiveOrderGateway::build_submit_slices(
    const char* code,
    bool is_buy,
    int64_t qty_fp,
    XtOffset close_offset,
    xt_core::XtOrderSource source) const {
    SubmitSliceArray out;
    SubmitSliceArray raw;

    if (close_offset == XtOffset::Open) {
        bool rewritten = false;
        if (source == xt_core::XtOrderSource::Strategy &&
            catalog_direct_ && order_book_direct_ && account_store_direct_ &&
            catalog_direct_->effective_offset_policy(code) == XtOffsetPolicy::CloseFirst) {
            xt_offset_router::OffsetRouteInput in;
            in.code = code;
            in.policy = XtOffsetPolicy::CloseFirst;
            in.req_qty_fp = qty_fp;

            const xt_account::CloseableView cv = xt_account::closeable_view(
                account_store_direct_->snapshot_position(code), is_buy);
            in.opposite_closeable_today_fp = cv.today_fp;
            in.opposite_closeable_yesterday_fp = cv.yesterday_fp;
            order_book_direct_->accumulate_close_outstanding_fp(
                code, is_buy, in.opposite_close_outstanding_today_fp,
                in.opposite_close_outstanding_yesterday_fp);

            in.opposite_open_outstanding_fp =
                order_book_direct_->open_no_trade_qty_fp(code, !is_buy);

            const auto d = xt_offset_router::route_open_intent(in);
            using Action = xt_offset_router::OffsetRouteDecision::Action;
            if (d.action == Action::kDefer) {
                out.deferred = true;
                return out;
            }
            if (d.action == Action::kRewrite) {

                for (int i = 0; i < d.close_slice_count; ++i)
                    raw.push(d.close_slices[i].offset, d.close_slices[i].qty_fp);
                if (d.open_remainder_fp > 0) raw.push(XtOffset::Open, d.open_remainder_fp);
                rewritten = true;
            }

        }
        if (!rewritten) {

            out.push(XtOffset::Open, qty_fp);
            return out;
        }
    }

    if (raw.count == 0) {
        if (close_offset == XtOffset::CloseToday || close_offset == XtOffset::CloseYesterday) {
            raw.push(close_offset, qty_fp);
        } else if (!build_close_slices(code, is_buy, qty_fp, false, raw)) {
            raw.push(close_offset, qty_fp);
        }
    }

    const double max_qty = cfg_.risk.max_order_qty;
    if (max_qty <= 0.0) return raw;
    const int64_t max_qty_fp = xt_fp::to_fp(max_qty);
    if (max_qty_fp <= 0) return raw;

    close_offset_core::CloseSlice routed[kMaxSubmitSlices];
    for (uint8_t i = 0; i < raw.count; ++i) {
        routed[i] = close_offset_core::CloseSlice{raw.slices[i].close_offset,
                                                  raw.slices[i].qty_fp};
    }
    close_offset_core::CloseSlice chunked[kMaxSubmitSlices];
    bool truncated = false;
    const int n = close_offset_core::chunk_slices_by_max_qty(
        routed, static_cast<int>(raw.count), max_qty_fp, chunked,
        static_cast<int>(kMaxSubmitSlices), truncated);
    for (int i = 0; i < n; ++i) out.push(chunked[i].offset, chunked[i].qty_fp);
    out.truncated = truncated;

    if (out.count == 0) out = raw;
    return out;
}

void XtLiveOrderGateway::emit_margin_reject(bool is_buy,
                                          int64_t price_fp,
                                          int64_t qty_fp,
                                          int64_t tnow,
                                          std::string_view strategy_id) {
    XtOrderExecUpdate rej;
    rej.id = 0;
    rej.is_buy = is_buy;
    rej.close_offset = XtOffset::Open;
    rej.status = XtOrderStatus::Rejected;
    rej.price_fp = price_fp;
    rej.qty_fp = qty_fp;
    rej.filled_fp = 0;
    rej.leaves_fp = qty_fp > 0 ? qty_fp : 0;
    rej.local_cb_us = tnow > 0 ? static_cast<uint64_t>(tnow) * 1000ULL : 0;

    xt_strategy_engine::IStrategyExecSink* target_sink =
        strategy_id.empty() ? exec_sink_ : nullptr;
    bool routed_by_sid = false;
    if (!strategy_id.empty() && sid_registry_ != nullptr && !exec_sinks_.empty()) {
        const std::string sid_key(strategy_id);
        const uint32_t sid_id = sid_registry_->lookup(sid_key);

        if (sid_id != xt_identity::StrategySidRegistry::kNoSidId
                && sid_id <= exec_sinks_.size()
                && exec_sinks_[sid_id - 1] != nullptr) {
            target_sink = exec_sinks_[sid_id - 1];
            routed_by_sid = true;
        }
    }
    if (target_sink) {
        xt_log::run_guarded(
            "XtLiveOrderGateway",
            "strategy.on_order.margin_reject",
            g_margin_reject_callback_exception_total,
            [&]() { target_sink->on_order(rej); });
    }

    if (!strategy_id.empty() && !routed_by_sid) {
        dropped_margin_reject_total_.fetch_add(1, std::memory_order_relaxed);
        XtLogger::error_m("RT",
            "margin reject route failed for strategy_id='{}'; reject not delivered to default sink",
            std::string(strategy_id));
    }
    record_reject(XtLiveRejectReason::InsufficientMargin);
    maybe_enter_cooldown_on_reject(tnow);
}

void XtLiveOrderGateway::publish_margin_reject(const xt_core::XtOrder& req,
                                               int64_t now_ms,
                                               std::string_view strategy_id) {
    emit_margin_reject(req.is_buy, req.price_fp, req.qty_fp, now_ms, strategy_id);
}

void XtLiveOrderGateway::publish_reject_reason(XtLiveRejectReason code) {
    record_reject(code);
}

namespace {

void emit_order_submit_tap(const char* code, uint64_t phys_id, bool is_buy, XtOffset close_offset,
                           int64_t price_fp, int64_t qty_fp, std::string_view strategy_id,
                           uint64_t decision_us, std::string_view user_tag) {
    auto* rec = XtLiveRecorder::global();
    if (!rec) return;
    const uint64_t ts_local_ns = XtSteadyClock::steady_now_ns();
    const char* dir = is_buy ? "buy" : "sell";
    const char* offset = (close_offset == XtOffset::Open)           ? "open"
                       : (close_offset == XtOffset::CloseYesterday) ? "close_yesterday"
                       : (close_offset == XtOffset::CloseToday)     ? "close_today"
                                                                    : "close";
    const std::string user_tag_str_tap(user_tag);
    const std::string sid_str(strategy_id);
    char reason_buf[48];
    xt_core::tap::peek_next_order_reason(reason_buf, sizeof(reason_buf));
    rec->emit_order_submit({
        .ts_local_ns      = ts_local_ns,
        .logical_id       = phys_id,
        .code             = code,
        .dir              = dir,
        .offset           = offset,
        .price_fp         = price_fp,
        .qty              = qty_fp,
        .parent_strat_id  = sid_str.c_str(),
        .decision_ts      = decision_us * 1000ULL,
        .trigger_tick_ts  = xt_core::tap::current_tick_ts_ns(),
        .reason           = reason_buf,
        .strat_id         = sid_str.c_str(),
        .book_id          = sid_str.c_str(),
        .account_id       = nullptr,
        .user_tag         = user_tag_str_tap.c_str(),
        .decision_mono_ns = decision_us * 1000ULL,
    });
}
}

void XtLiveOrderGateway::register_submitted_order_(uint64_t phys_id,
                                                   const char* code,
                                                   bool is_buy,
                                                   int64_t qty_fp,
                                                   int64_t price_fp,
                                                   XtOffset close_offset,
                                                   StrategySid owner_sid,
                                                   int64_t required_margin_fp,
                                                   int64_t per_unit_margin_fp) {

    if (order_book_direct_->should_skip_submit(phys_id)) return;
    const bool is_close = (close_offset != XtOffset::Open);
    xt_account::CloseableView closeable{};
    if (is_close) {
        closeable = xt_account::closeable_view(
            account_store_direct_->snapshot_position(code ? code : ""), is_buy);
    }
    order_book_direct_->on_order_submitted(phys_id, code, is_buy, qty_fp, price_fp,
                                           close_offset, closeable, owner_sid);
    account_store_direct_->apply(
        XtAccountEvent::order_accepted(phys_id, required_margin_fp, per_unit_margin_fp));
}

uint64_t XtLiveOrderGateway::submit_slices(const char* code,
                                         bool is_buy,
                                         int64_t price_fp,
                                         XtTimeInForce tif,
                                         std::string_view user_tag,
                                         const SubmitSliceArray& slices,
                                         XtOffset orig_close_offset,
                                         ExecFlags exec_flags,
                                         int64_t tnow,
                                         uint64_t decision_us,
                                         IRejectSink& reject_sink,
                                         bool is_forced_close,
                                         std::string_view strategy_id,
                                         uint64_t intent_trace_id) {

    std::unique_lock<std::mutex> submit_guard(submit_mu_, std::defer_lock);
    if (!cfg_.single_threaded_submit) {
        submit_guard.lock();
    }

    SliceRunState st;

    StrategySid owner_sid = StrategySid::legacy();
    bool owner_sid_valid = true;
    if (!strategy_id.empty() && sid_registry_ != nullptr) {
        owner_sid = StrategySid::from_raw(sid_registry_->lookup(std::string(strategy_id)));
        owner_sid_valid = owner_sid.has_value();
    }

    for (uint8_t i = 0; i < slices.count; ++i) {
        const SliceOutcome outcome =
            submit_one_slice_(code, is_buy, price_fp, tif, user_tag, slices, i,
                              exec_flags, tnow, decision_us, reject_sink,
                              is_forced_close, strategy_id, intent_trace_id,
                              owner_sid, owner_sid_valid, st);
        if (outcome == SliceOutcome::kRejectAll) return 0;
        if (outcome == SliceOutcome::kStopPartial) break;
    }

    return finalize_multi_slice_(st, is_buy, price_fp, orig_close_offset, intent_trace_id);
}

XtLiveOrderGateway::SliceOutcome XtLiveOrderGateway::submit_one_slice_(
    const char* code,
    bool is_buy,
    int64_t price_fp,
    XtTimeInForce tif,
    std::string_view user_tag,
    const SubmitSliceArray& slices,
    uint8_t slice_index,
    ExecFlags exec_flags,
    int64_t tnow,
    uint64_t decision_us,
    IRejectSink& reject_sink,
    bool is_forced_close,
    std::string_view strategy_id,
    uint64_t intent_trace_id,
    StrategySid owner_sid,
    bool owner_sid_valid,
    SliceRunState& st) {
    const uint8_t i = slice_index;
    const auto any_submitted = [&]() { return st.first_phys_id != 0; };
    const auto emit_slice_failure_trace =
        [&](uint8_t fail_slice_index,
            XtOffset offset,
            int64_t slice_qty_fp,
            const char* scope,
            const char* code_text,
            const char* reason_text) {
            GatewayIntentTraceCtx t;
            t.trace_id             = intent_trace_id;
            t.stage                = any_submitted() ? "GATEWAY_PARTIAL_FAILURE"
                                                     : "GATEWAY_REJECT";
            t.venue                = cfg_.venue_kind;
            t.code                 = code;
            t.is_buy               = is_buy;
            t.price_fp             = price_fp;
            t.qty_fp               = slice_qty_fp;
            t.tif                  = tif;
            t.offset               = offset;
            t.exec_flags           = exec_flags;
            t.user_tag             = user_tag;
            t.strategy_id          = strategy_id;
            t.decision_us          = decision_us;
            t.slice_index          = fail_slice_index;
            t.slice_count          = slices.count;
            t.reject_scope         = scope;
            t.reject_code          = code_text;
            t.reject_reason        = reason_text;
            t.partial              = any_submitted();
            t.failure_slice_index  = fail_slice_index;
            t.accepted_slice_count = st.phys_count;
            t.expected_slice_count = slices.count;
            emit_gateway_intent_trace(t);
        };

    const SubmitSlice& sl = slices.slices[i];
    if (sl.qty_fp <= 0) {

        if (!any_submitted()) {
            emit_slice_failure_trace(i, sl.close_offset, sl.qty_fp, "gateway_gate",
                                     "empty_slice", "empty submit slice");
            return SliceOutcome::kRejectAll;
        }
        emit_slice_failure_trace(i, sl.close_offset, sl.qty_fp, "gateway_gate",
                                 "empty_slice", "empty submit slice");
        return SliceOutcome::kStopPartial;
    }
    xt_core::XtOrder req;
    req.set_code(code);
    req.is_buy = is_buy;
    req.price_fp = price_fp;
    req.qty_fp = sl.qty_fp;
    req.tif = tif;
    req.close_offset = sl.close_offset;

    if (cfg_.venue_kind == xt_core::XtVenueKind::Unknown) {
        emit_slice_failure_trace(i, sl.close_offset, sl.qty_fp, "gateway_gate",
                                 "trading_not_ready", "runtime venue provider invalid");
        risk_action::dispatch_reject(
            reject_sink,
            req,
            reject_decision(XtRiskReasonCode::TradingNotReady,
                            XtRiskStage::InputGuard,
                            "runtime venue provider invalid"),
            tnow,
            strategy_id);
        if (!any_submitted()) return SliceOutcome::kRejectAll;
        return SliceOutcome::kStopPartial;
    }

    if (!owner_sid_valid) {
        emit_slice_failure_trace(i, sl.close_offset, sl.qty_fp, "gateway_gate",
                                 "unknown_strategy_sid", "unknown strategy sid");
        risk_action::dispatch_reject(
            reject_sink,
            req,
            reject_decision(XtRiskReasonCode::UnknownStrategySid, XtRiskStage::InputGuard),
            tnow,
            strategy_id);
        if (!any_submitted()) return SliceOutcome::kRejectAll;
        return SliceOutcome::kStopPartial;
    }

    XtRiskResult risk_dec;

    if (__builtin_expect(cfg_.bypass_risk_for_bench, 0)) {
        risk_dec.passed = true;
    } else {

        const bool run_input_guards = (i == 0);

        bool is_call_auction = false;
        if (cfg_.venue_kind == xt_core::XtVenueKind::AShare) {
            const uint32_t hhmm = static_cast<uint32_t>(xt_time::local_hhmm_now());
            const auto phase = xt_core::classify_phase(cfg_.venue_kind, hhmm);
            is_call_auction = xt_core::is_call_auction(phase);
        }
        const XtRiskEvalContext eval_ctx{
            .now_ms             = tnow,
            .run_input_guards   = run_input_guards,
            .is_forced_close    = is_forced_close,
            .is_in_call_auction = is_call_auction,
            .dry_run            = false,
            .strategy_id        = owner_sid.raw(),
        };
        risk_dec = risk_gateway_.evaluate(req, risk_guard_ctx_, eval_ctx);
        if (!risk_dec.passed) {
            const char* reason = risk_dec.reason_text.empty()
                                     ? risk_reason_text(risk_dec.reason_code)
                                     : risk_dec.reason_text.c_str();
            emit_slice_failure_trace(i, sl.close_offset, sl.qty_fp, "risk",
                                     live_reject_reason_str(
                                         live_reject_reason::from_risk(risk_dec.reason_code)),
                                     reason);
            risk_action::dispatch_reject(reject_sink, req, risk_dec, tnow, strategy_id);
            if (!any_submitted()) return SliceOutcome::kRejectAll;
            return SliceOutcome::kStopPartial;
        }
    }

    const TdReadySnapshot snap = load_td_ready_snapshot(td_, trading_ready_, "submit_slices");
    ITradeGateway* td = snap.td;
    if (!td) {
        emit_slice_failure_trace(i, sl.close_offset, sl.qty_fp, "gateway_gate",
                                 "td_not_initialized", "td gateway not initialized");
        risk_action::dispatch_reject(
            reject_sink,
            req,
            reject_decision(XtRiskReasonCode::TdNotInitialized, XtRiskStage::CapabilityGuard),
            tnow,
            strategy_id);
        if (!any_submitted()) return SliceOutcome::kRejectAll;
        return SliceOutcome::kStopPartial;
    }

    if (!trading_ready_->load(std::memory_order_acquire)) {
        emit_slice_failure_trace(i, sl.close_offset, sl.qty_fp, "gateway_gate",
                                 "trading_not_ready", "trading_ready=false before td submit");
        risk_action::dispatch_reject(
            reject_sink,
            req,
            reject_decision(XtRiskReasonCode::TradingNotReady, XtRiskStage::CapabilityGuard),
            tnow,
            strategy_id);
        if (!any_submitted()) return SliceOutcome::kRejectAll;
        return SliceOutcome::kStopPartial;
    }

    const std::string user_tag_str(user_tag);
    xt_identity::OrderAttributionStore::set_thread_submit_strategy_sid(owner_sid);
    const uint64_t phys_id =
        intent_trace_id != 0
            ? td->submit_order_with_intent_trace(code,
                                                 is_buy,
                                                 price_fp,
                                                 sl.qty_fp,
                                                 tif,
                                                 user_tag_str.c_str(),
                                                 sl.close_offset,
                                                 exec_flags,
                                                 intent_trace_id)
            : td->submit_order(code,
                               is_buy,
                               price_fp,
                               sl.qty_fp,
                               tif,
                               user_tag_str.c_str(),
                               sl.close_offset,
                               exec_flags);
    xt_identity::OrderAttributionStore::set_thread_submit_strategy_sid(StrategySid::legacy());
    if (phys_id == 0) {
        emit_slice_failure_trace(i, sl.close_offset, sl.qty_fp, "td_submit",
                                 "td_submit_failed", "td submit returned zero");
        if (!any_submitted()) return SliceOutcome::kRejectAll;
        XtLogger::warn_m("RT",
                         "partial_slice_failure code={} slice={}/{} submitted_qty_fp={}",
                         code, i, slices.count, st.total_submitted_qty_fp);
        return SliceOutcome::kStopPartial;
    }

    if (order_book_direct_ && account_store_direct_) {
        if (order_book_direct_->is_bound_worker_thread()) {
            register_submitted_order_(phys_id, code, is_buy, sl.qty_fp, price_fp,
                                      sl.close_offset, owner_sid,
                                      risk_dec.required_margin_fp,
                                      risk_dec.per_unit_margin_fp);
        } else {

            std::string code_copy(code ? code : "");
            const bool posted = order_book_direct_->post_to_worker(
                [this, phys_id, code_copy = std::move(code_copy), is_buy,
                 qty_fp = sl.qty_fp, price_fp, close_offset = sl.close_offset, owner_sid,
                 required_margin_fp = risk_dec.required_margin_fp,
                 per_unit_margin_fp = risk_dec.per_unit_margin_fp] {
                    register_submitted_order_(phys_id, code_copy.c_str(), is_buy, qty_fp,
                                              price_fp, close_offset, owner_sid,
                                              required_margin_fp, per_unit_margin_fp);
                });
            if (!posted) {

                XtLogger::warn_m("RT",
                                 "submit register enqueue failed, inline fallback id={}",
                                 phys_id);
                register_submitted_order_(phys_id, code, is_buy, sl.qty_fp, price_fp,
                                          sl.close_offset, owner_sid,
                                          risk_dec.required_margin_fp,
                                          risk_dec.per_unit_margin_fp);
            }
        }
    }

    if (cfg_.stp_enabled) {
        stp_checker_.on_order_submitted(code, is_buy, price_fp);
    }
    submit_success_.fetch_add(1, std::memory_order_relaxed);

    emit_order_submit_tap(code, phys_id, is_buy, sl.close_offset, price_fp, sl.qty_fp,
                          strategy_id, decision_us, user_tag);
    note_trace_attribution_(phys_id, intent_trace_id);
    {
        GatewayIntentTraceCtx t;
        t.trace_id    = intent_trace_id;
        t.stage       = "PHYSICAL_SLICE";
        t.venue       = cfg_.venue_kind;
        t.code        = code;
        t.is_buy      = is_buy;
        t.price_fp    = price_fp;
        t.qty_fp      = sl.qty_fp;
        t.tif         = tif;
        t.offset      = sl.close_offset;
        t.exec_flags  = exec_flags;
        t.user_tag    = user_tag;
        t.strategy_id = strategy_id;
        t.decision_us = decision_us;
        t.local_id    = phys_id;
        t.logical_id  = slices.count == 1 ? phys_id : 0;
        t.phys_id     = phys_id;
        t.slice_index = i;
        t.slice_count = slices.count;
        emit_gateway_intent_trace(t);
    }

    assert(st.phys_count < kMaxSubmitSlices);
    if (st.phys_count == 0) st.first_phys_id = phys_id;
    st.phys_ids[st.phys_count]      = phys_id;
    st.phys_qty_fps[st.phys_count]  = sl.qty_fp;
    st.total_submitted_qty_fp   += sl.qty_fp;
    ++st.phys_count;
    return SliceOutcome::kSubmitted;
}

uint64_t XtLiveOrderGateway::finalize_multi_slice_(const SliceRunState& st,
                                                   bool is_buy,
                                                   int64_t price_fp,
                                                   XtOffset orig_close_offset,
                                                   uint64_t intent_trace_id) {

    if (st.phys_count <= 1) {
        return st.first_phys_id;
    }

    const uint64_t clord_id = logical_order_tracker_.register_multi_slice(
        st.phys_ids,
        st.phys_qty_fps,
        st.phys_count,
        st.total_submitted_qty_fp,
        is_buy,
        price_fp,
        orig_close_offset,
         0);
    note_trace_attribution_(clord_id, intent_trace_id);
    return clord_id;
}

SubmitResult XtLiveOrderGateway::submit(const char* code,
                                        bool is_buy,
                                        int64_t price_fp,
                                        int64_t qty_fp,
                                        XtTimeInForce tif,
                                        std::string_view user_tag,
                                        XtOffset close_offset,
                                        ExecFlags exec_flags,
                                        std::string_view strategy_id) {
    return submit_with_metadata(code,
                                is_buy,
                                price_fp,
                                qty_fp,
                                tif,
                                user_tag,
                                close_offset,
                                exec_flags,
                                strategy_id,
                                xt_core::XtOrderSource::Strategy,
                                0,
                                0);
}

SubmitResult XtLiveOrderGateway::submit_with_intent_trace(const char* code,
                                                          bool is_buy,
                                                          int64_t price_fp,
                                                          int64_t qty_fp,
                                                          XtTimeInForce tif,
                                                          std::string_view user_tag,
                                                          XtOffset close_offset,
                                                          ExecFlags exec_flags,
                                                          std::string_view strategy_id,
                                                          uint64_t intent_trace_id) {
    return submit_with_metadata(code,
                                is_buy,
                                price_fp,
                                qty_fp,
                                tif,
                                user_tag,
                                close_offset,
                                exec_flags,
                                strategy_id,
                                xt_core::XtOrderSource::Strategy,
                                0,
                                intent_trace_id);
}

SubmitResult XtLiveOrderGateway::submit_with_metadata(const char* code,
                                                      bool is_buy,
                                                      int64_t price_fp,
                                                      int64_t qty_fp,
                                                      XtTimeInForce tif,
                                                      std::string_view user_tag,
                                                      XtOffset close_offset,
                                                      ExecFlags exec_flags,
	                                                      std::string_view strategy_id,
	                                                      xt_core::XtOrderSource source,
	                                                      uint64_t audit_trace_id,
	                                                      uint64_t intent_trace_id) {

    const uint64_t decision_us = now_us();
    SubmitResult result;

    GatewayIntentTraceCtx trace_ctx;
    trace_ctx.trace_id    = intent_trace_id;
    trace_ctx.stage       = "GATEWAY_REQUEST";
    trace_ctx.venue       = cfg_.venue_kind;
    trace_ctx.code        = code;
    trace_ctx.is_buy      = is_buy;
    trace_ctx.price_fp    = price_fp;
    trace_ctx.qty_fp      = qty_fp;
    trace_ctx.tif         = tif;
    trace_ctx.offset      = close_offset;
    trace_ctx.exec_flags  = exec_flags;
    trace_ctx.user_tag    = user_tag;
    trace_ctx.strategy_id = strategy_id;
    trace_ctx.decision_us = decision_us;
    emit_gateway_intent_trace(trace_ctx);
    const auto emit_gate_reject_trace = [&trace_ctx](const char* stage,
                                                     const char* scope,
                                                     const char* reject_code_text,
                                                     const char* reason_text,
                                                     uint32_t slice_count = 0) {
        GatewayIntentTraceCtx t = trace_ctx;
        t.stage         = stage;
        t.slice_count   = slice_count;
        t.reject_scope  = scope;
        t.reject_code   = reject_code_text;
        t.reject_reason = reason_text;
        emit_gateway_intent_trace(t);
    };

	    if (stopping_.load(std::memory_order_acquire)) {
        emit_gate_reject_trace("GATEWAY_REJECT",
                               "gateway_gate",
                               live_reject_reason_str(XtLiveRejectReason::TradingNotReady),
                               "gateway stopping");
	        result.reject_reason = XtRejectCode::TradingNotReady;
	        record_reject(XtLiveRejectReason::TradingNotReady);
	        return result;
    }

	    refresh_param_gate_cache();
	    if (!gate_submit_enabled_.load(std::memory_order_relaxed)) {
        emit_gate_reject_trace("GATEWAY_REJECT",
                               "gateway_gate",
                               live_reject_reason_str(XtLiveRejectReason::GateDisabled),
                               "submit gate disabled");
	        result.reject_reason = XtRejectCode::Gated;
	        record_reject(XtLiveRejectReason::GateDisabled);
	        return result;
    }
    submit_attempts_.fetch_add(1, std::memory_order_relaxed);

	    const bool track_latency = latency_tracker_ && latency_tracker_->enabled();
	    const uint64_t t0_us = track_latency ? decision_us : 0;
    const uint64_t trigger_tick_us = xt_core::tap::current_tick_ts_ns() / 1000ULL;
	    XtLiveRejectSink reject_sink(*this);

	    if (!is_valid_order_input(code, price_fp, qty_fp)) {
        emit_gate_reject_trace("GATEWAY_REJECT",
                               "gateway_gate",
                               live_reject_reason_str(XtLiveRejectReason::InvalidInput),
                               "invalid order input");
	        result.reject_reason = XtRejectCode::InvalidInput;
	        record_reject(XtLiveRejectReason::InvalidInput);
	        return result;
    }

    if (close_offset == XtOffset::Auto) {
        const auto resolved = xt_offset_router::resolve_submit_offset(close_offset, source);
        if (!resolved) {
            emit_gate_reject_trace("GATEWAY_REJECT",
                                   "gateway_gate",
                                   live_reject_reason_str(XtLiveRejectReason::AutoOffsetUnresolved),
                                   "auto offset only valid from strategy source");
            result.reject_reason = XtRejectCode::AutoOffsetUnresolved;
            record_reject(XtLiveRejectReason::AutoOffsetUnresolved);
            return result;
        }
        close_offset = *resolved;
    }

    const xt_risk::XtRiskMonitorState* rms =
        risk_monitor_state_.load(std::memory_order_acquire);
    if (rms &&
        close_offset == XtOffset::Open &&
        rms->open_halted_load()) {
        const uint64_t ver = rms->version.load(std::memory_order_acquire);
        uint64_t expected = last_halt_log_version_.load(std::memory_order_relaxed);
        if (ver != expected &&
            last_halt_log_version_.compare_exchange_strong(
                expected, ver, std::memory_order_relaxed)) {
            XtLogger::error_m(
                "RT",
                "OPEN_HALTED reject gate reason={} ver={} trigger_ms={} peak_fp={} dd_bp={} last_update_ms={}",
                open_halt_reason_text(rms->open_halt_reason_load()),
                ver,
                rms->open_halt_trigger_ms.load(std::memory_order_acquire),
                rms->peak_equity_fp.load(std::memory_order_acquire),
                rms->current_drawdown_bp.load(std::memory_order_acquire),
                rms->last_update_ms.load(std::memory_order_acquire));
	        }
        emit_gate_reject_trace("GATEWAY_REJECT",
                               "gateway_gate",
                               live_reject_reason_str(XtLiveRejectReason::OpenHalted),
                               open_halt_reason_text(rms->open_halt_reason_load()));
	        result.reject_reason = XtRejectCode::OpenHalted;
	        record_reject(XtLiveRejectReason::OpenHalted);
	        return result;
    }

    const SubmitSliceArray slices =
        build_submit_slices(code, is_buy, qty_fp, close_offset, source);

	    if (slices.deferred) {
        emit_gate_reject_trace("GATEWAY_DEFER",
                               "offset_router",
                               live_reject_reason_str(XtLiveRejectReason::OppositeOpenPending),
                               "opposite open pending");
	        result.reject_reason = XtRejectCode::OppositeOpenPending;
	        record_reject(XtLiveRejectReason::OppositeOpenPending);
	        return result;
    }

	    if (slices.truncated) {
        emit_gate_reject_trace("GATEWAY_REJECT",
                               "gateway_gate",
                               live_reject_reason_str(XtLiveRejectReason::MaxOrderQty),
                               "submit slice budget exhausted",
                                slices.count);
	        result.reject_reason = XtRejectCode::MaxOrderQty;
	        record_reject(XtLiveRejectReason::MaxOrderQty);
	        return result;
    }

	    result.order_id = submit_slices(code, is_buy, price_fp, tif, user_tag, slices, close_offset,
	                                    exec_flags, now_ms(), decision_us, reject_sink,
	                                     false, strategy_id,
	                                    intent_trace_id);

    if (track_latency) {
        const uint64_t t1_us = now_us();

        latency_tracker_->on_submit_result(result.order_id, std::string(user_tag),
                                           t0_us, t1_us, trigger_tick_us,
                                           strategy_id, std::string_view(code));
    }

    if (!result.accepted()) {
        const XtLiveRejectReason first = reject_sink.first_reject_reason();
        result.reject_reason = (first == XtLiveRejectReason::Unknown)
                                   ? XtRejectCode::Unknown
                                   : to_reject_code(first);
    }

    (void)audit_trace_id;
    const bool needs_sidecar = !strategy_id.empty()
                            || source != xt_core::XtOrderSource::Strategy;
	    if (result.accepted() && needs_sidecar) {
	        std::unique_lock<std::shared_mutex> lock(order_strategy_mu_);
	        OrderStrategyEntry entry;
	        entry.sid = std::string(strategy_id);
	        entry.source = source;
	        order_strategy_[result.order_id] = std::move(entry);
	    }
	    if (result.accepted()) {
	        note_trace_attribution_(result.order_id, intent_trace_id);
	    }
	    return result;
	}

SubmitResult XtLiveOrderGateway::submit(const xt_core::XtOrderRequest& req,
                                        ExecFlags exec_flags) {
    return submit_with_metadata(req.order.code,
                                req.order.is_buy,
                                req.order.price_fp,
                                req.order.qty_fp,
                                req.order.tif,
                                req.order.tag,
                                req.order.close_offset,
                                exec_flags,
                                req.strategy_id,
                                req.source,
                                req.audit_trace_id,
                                0);
}

bool XtLiveOrderGateway::is_order_terminal(uint64_t local_id) const {
    const ITradeGateway* td = td_.load(std::memory_order_acquire);
    return td != nullptr && td->is_order_terminal(local_id);
}

bool XtLiveOrderGateway::cancel(uint64_t id) {

    refresh_param_gate_cache();
    if (!gate_cancel_enabled_.load(std::memory_order_relaxed)) return false;
    const int64_t tnow = now_ms();
    cancel_attempts_.fetch_add(1, std::memory_order_relaxed);
    if (block_until_ms_.load(std::memory_order_acquire) > tnow) {
        record_reject(XtLiveRejectReason::CancelCooldown);
        return false;
    }
    if (!cancel_limiter_.allow(cfg_.max_cancels_per_sec, tnow)) {
        record_reject(XtLiveRejectReason::CancelRateLimited);
        return false;
    }
    const TdReadySnapshot snap = load_td_ready_snapshot(td_, trading_ready_, "cancel");
    if (!snap.ready) {
        record_reject(XtLiveRejectReason::CancelTradingNotReady);
        return false;
    }
    if (!snap.td) {
        record_reject(XtLiveRejectReason::CancelTdNotInitialized);
        return false;
    }

    uint64_t phys_ids[LogicalOrderTracker::kMaxSlices] = {};
    const uint8_t slice_count = logical_order_tracker_.expand_to_phys(
        id, phys_ids, LogicalOrderTracker::kMaxSlices);

    if (slice_count == 0) {

        XtLiveRecorder::note_cancel_initiated(id, "user");
        const bool ok = snap.td->cancel_order(id);
        if (ok) {
            cancel_success_.fetch_add(1, std::memory_order_relaxed);
        } else {
            char discard[16];
            XtLiveRecorder::consume_cancel_initiator(id, discard, sizeof(discard));
            record_reject(XtLiveRejectReason::CancelSendFailed);
        }
        return ok;
    }

    bool any_ok = false;
    for (uint8_t i = 0; i < slice_count; ++i) {
        if (phys_ids[i] == 0) continue;
        XtLiveRecorder::note_cancel_initiated(phys_ids[i], "user");
        const bool ok = snap.td->cancel_order(phys_ids[i]);
        if (ok) {
            cancel_success_.fetch_add(1, std::memory_order_relaxed);
            any_ok = true;
        } else {
            char discard[16];
            XtLiveRecorder::consume_cancel_initiator(phys_ids[i], discard, sizeof(discard));
        }
    }
    if (!any_ok) {
        record_reject(XtLiveRejectReason::CancelSendFailed);
    }
    return any_ok;
}

void XtLiveOrderGateway::stop(std::chrono::milliseconds timeout) noexcept {

    try {

        bool expected = false;
        const bool first_call = stopping_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel);
        if (first_call) {
            XtLogger::warn_m("RT",
                             "gateway stop() begin timeout_ms={} open_orders={}",
                             static_cast<long long>(timeout.count()),
                             order_book_direct_ ? order_book_direct_->open_order_count() : 0);
        }

        ITradeGateway* td = td_.load(std::memory_order_acquire);
        if (td && order_book_direct_) {
            std::vector<std::string> codes;
            order_book_direct_->collect_tracked_codes(codes);
            std::vector<uint64_t> ids_buf;
            size_t cancel_attempt = 0;
            size_t cancel_send_ok = 0;
            for (const auto& code : codes) {
                ids_buf.clear();
                order_book_direct_->collect_working_ids_for_code(code.c_str(), ids_buf);
                for (uint64_t id : ids_buf) {
                    if (id == 0) continue;
                    ++cancel_attempt;

                    XtLiveRecorder::note_cancel_initiated(id, "shutdown");
                    bool ok = false;
                    try {
                        ok = td->cancel_order(id);
                    } catch (...) {
                        ok = false;
                    }
                    if (ok) {
                        ++cancel_send_ok;
                    } else {
                        char discard[16];
                        XtLiveRecorder::consume_cancel_initiator(id, discard, sizeof(discard));
                    }
                }
            }
            if (first_call) {
                XtLogger::warn_m("RT",
                                 "gateway stop() cancel-all sent attempted={} send_ok={}",
                                 cancel_attempt, cancel_send_ok);
            }
        } else if (first_call) {
            XtLogger::warn_m("RT",
                             "gateway stop() skip cancel-all td={} order_book={}",
                             static_cast<const void*>(td),
                             static_cast<const void*>(order_book_direct_));
        }

        using std::chrono::steady_clock;
        const auto deadline = steady_clock::now() + timeout;
        constexpr std::chrono::milliseconds kPollInterval{100};
        while (steady_clock::now() < deadline) {
            const size_t remaining = order_book_direct_
                                         ? order_book_direct_->open_order_count()
                                         : 0;
            if (remaining == 0) break;
            std::this_thread::sleep_for(kPollInterval);
        }

        if (first_call) {
            const size_t leftover = order_book_direct_
                                        ? order_book_direct_->open_order_count()
                                        : 0;
            if (leftover > 0) {
                XtLogger::error_m("RT",
                                  "gateway stop() drain timeout leftover={} (broker may retain orders)",
                                  leftover);
            } else {
                XtLogger::warn_m("RT", "gateway stop() drain done leftover=0");
            }
        }
    } catch (...) {

    }
}

void XtLiveOrderGateway::record_order_terminal(XtOrderStatus status) {

    if (status == XtOrderStatus::Canceled) {
        canceled_terminal_total_.fetch_add(1, std::memory_order_relaxed);
    }
}

XtLiveOrderGateway::RiskStatsSnapshot XtLiveOrderGateway::risk_stats_snapshot() const {
    RiskStatsSnapshot out;
    out.submit_attempts = submit_attempts_.load(std::memory_order_relaxed);
    out.submit_success = submit_success_.load(std::memory_order_relaxed);
    out.cancel_attempts = cancel_attempts_.load(std::memory_order_relaxed);
    out.cancel_success = cancel_success_.load(std::memory_order_relaxed);
    out.canceled_terminal_total = canceled_terminal_total_.load(std::memory_order_relaxed);
    out.reject_by_reason.reserve(kLiveRejectReasonCount);
    for (size_t i = 0; i < kLiveRejectReasonCount; ++i) {
        const uint64_t cnt = reject_counts_[i].load(std::memory_order_relaxed);
        if (cnt == 0) continue;
        const auto code = static_cast<XtLiveRejectReason>(i);
        out.reject_by_reason.emplace_back(live_reject_reason_str(code), cnt);
    }
    std::sort(out.reject_by_reason.begin(),
              out.reject_by_reason.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    return out;
}

void XtLiveOrderGateway::process_position_snapshot(
        const std::vector<ITradeGateway::PositionSnapshot>& pos,
        ITradeGateway::TdState td_state) {

    if (td_state == ITradeGateway::TdState::Recovering) return;

    XtLogger::info_m("GW", "td position snapshot received: {} contracts", pos.size());
}
