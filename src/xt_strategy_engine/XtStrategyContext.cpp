#include "XtStrategyContext.h"
#include "XtDeclaredSymbolRegistry.h"
#include "MarketResolve.h"

#include "xt_core/market/XtRollEngine.h"
#include "xt_core/XtLiveTapContext.h"
#include "xt_core/order/XtFillEvent.h"
#include "xt_core/market/XtL2MarketStructs.h"
#include "xt_core/order/XtOrderLifecycle.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/sync/XtTransparentStringHash.h"
#include "api/XtRunnerSignals.h"

#include "xt_identity/StrategySidRegistry.h"
#include "xt_strategy_engine/IStrategyTelemetrySink.h"
#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif
#include "XtLogger.h"
#include "xt_util/string/XtStringUtil.h"
#include "xt_util/time/XtSteadyClock.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cmath>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>

namespace {

std::string alpha_prefix(const std::string& in) {
    std::string out;
    for (char c : in) {
        if (std::isalpha(static_cast<unsigned char>(c))) out.push_back(c);
        else break;
    }
    return out;
}

bool parse_main_alias_product(const std::string& in, std::string& product_out) {
    const std::string key = XtStringUtil::upper_without_space(in);
    const size_t at = key.find('@');
    if (at == std::string::npos) return false;
    if (key.substr(at + 1) != "MAIN") return false;
    const std::string product = alpha_prefix(
        XtStringUtil::strip_dot_prefix(key.substr(0, at)));
    if (product.empty()) return false;
    product_out = product;
    return true;
}

bool match_subscribed(const char* code, const std::unordered_set<std::string>& subs) {
    if (subs.empty()) return true;
    if (!code || code[0] == '\0') return false;

    if (subs.count(code)) return true;

    const std::string code_key = XtStringUtil::upper_without_space(code);
    if (subs.count(code_key)) return true;

    const std::string code_product = alpha_prefix(
        XtStringUtil::strip_dot_prefix(code_key));
    if (code_product.empty()) return false;

    for (const auto& sub : subs) {
        std::string main_product;
        if (parse_main_alias_product(sub, main_product)) {
            if (main_product == code_product) return true;
            continue;
        }
        const std::string sub_bare = XtStringUtil::strip_dot_prefix(
            XtStringUtil::upper_without_space(sub));
        if (sub_bare.empty()) continue;

        bool all_alpha = true;
        for (char c : sub_bare) {
            if (!std::isalpha(static_cast<unsigned char>(c))) {
                all_alpha = false;
                break;
            }
        }
        if (all_alpha && sub_bare == code_product) return true;
    }
    return false;
}

std::atomic<uint64_t> g_next_intent_trace_id{1};

uint64_t next_intent_trace_id() noexcept {
    const uint64_t id = g_next_intent_trace_id.fetch_add(1, std::memory_order_relaxed);
    if (id != 0) return id;
    return g_next_intent_trace_id.fetch_add(1, std::memory_order_relaxed);
}

const char* trace_dir(bool is_buy) noexcept {
    return is_buy ? "BUY" : "SELL";
}

const char* trace_tif(XtTimeInForce tif) noexcept {
    switch (tif) {
    case XtTimeInForce::GTC: return "GTC";
    case XtTimeInForce::IOC: return "IOC";
    case XtTimeInForce::FOK: return "FOK";
    }
    return "UNKNOWN";
}

const char* trace_offset(XtOffset offset) noexcept {
    switch (offset) {
    case XtOffset::Open:           return "OPEN";
    case XtOffset::Close:          return "CLOSE";
    case XtOffset::CloseToday:     return "CLOSE_TODAY";
    case XtOffset::CloseYesterday: return "CLOSE_YESTERDAY";
    case XtOffset::Auto:           return "AUTO";
    }
    return "UNKNOWN";
}

std::string trace_exec_flags(ExecFlags flags) {
    std::string out;
    auto append = [&out](const char* part) {
        if (!out.empty()) out += '|';
        out += part;
    };
    if (has_flag(flags, ExecFlag::Market))     append("MARKET_AS_LIMIT");
    if (has_flag(flags, ExecFlag::ReduceOnly)) append("REDUCE_ONLY");
    if (has_flag(flags, ExecFlag::PostOnly))   append("POST_ONLY");
    if (has_flag(flags, ExecFlag::Hidden))     append("HIDDEN");
    if (out.empty()) out = "LIMIT";
    return out;
}

uint64_t trace_user_tag_hash(const char* tag) noexcept {
    if (!tag || tag[0] == '\0') return 0;
    return xt_core::sync::fnv1a_bytes(tag, std::strlen(tag));
}

const char* trace_reject_code(xt_runtime::XtRejectCode rc) noexcept {
    using xt_runtime::XtRejectCode;
    switch (rc) {
    case XtRejectCode::NoError:                      return "NoError";
    case XtRejectCode::Unknown:                      return "Unknown";
    case XtRejectCode::InvalidInput:                 return "InvalidInput";
    case XtRejectCode::UnknownStrategySid:           return "UnknownStrategySid";
    case XtRejectCode::InvalidFlag:                  return "InvalidFlag";
    case XtRejectCode::PriceTickMismatch:            return "PriceTickMismatch";
    case XtRejectCode::TradingNotReady:              return "TradingNotReady";
    case XtRejectCode::Cooldown:                     return "Cooldown";
    case XtRejectCode::RateLimited:                  return "RateLimited";
    case XtRejectCode::CapabilityUnsupported:        return "CapabilityUnsupported";
    case XtRejectCode::CloseoutNoOpen:               return "CloseoutNoOpen";
    case XtRejectCode::MaxOrderQty:                  return "MaxOrderQty";
    case XtRejectCode::MaxOrderNotional:             return "MaxOrderNotional";
    case XtRejectCode::MinOrderNotional:             return "MinOrderNotional";
    case XtRejectCode::PositionLimit:                return "PositionLimit";
    case XtRejectCode::ShortNotAllowed:              return "ShortNotAllowed";
    case XtRejectCode::InsufficientMargin:           return "InsufficientMargin";
    case XtRejectCode::InsufficientCloseToday:       return "InsufficientCloseToday";
    case XtRejectCode::InsufficientCloseYesterday:   return "InsufficientCloseYesterday";
    case XtRejectCode::InCallAuctionLimitNotAllowed: return "InCallAuctionLimitNotAllowed";
    case XtRejectCode::PositionSyncStale:            return "PositionSyncStale";
    case XtRejectCode::CloseSyncPending:             return "CloseSyncPending";
    case XtRejectCode::KillSwitchActive:             return "KillSwitchActive";
    case XtRejectCode::HardCircuitBreakerTripped:    return "HardCircuitBreakerTripped";
    case XtRejectCode::SelfTradeDetected:            return "SelfTradeDetected";
    case XtRejectCode::VelocityLimitExceeded:        return "VelocityLimitExceeded";
    case XtRejectCode::DailyLossExceeded:            return "DailyLossExceeded";
    case XtRejectCode::Gated:                        return "Gated";
    case XtRejectCode::OpenHalted:                   return "OpenHalted";
    case XtRejectCode::OppositeOpenPending:          return "OppositeOpenPending";
    case XtRejectCode::SymbolBlacklisted:            return "SymbolBlacklisted";
    case XtRejectCode::OrderCountExceeded:           return "OrderCountExceeded";
    case XtRejectCode::OrderRateExceeded:            return "OrderRateExceeded";
    case XtRejectCode::SelfMatchPostTrade:           return "SelfMatchPostTrade";
    case XtRejectCode::AnnualLossExceeded:           return "AnnualLossExceeded";
    case XtRejectCode::StrategyMaxPosQtyExceeded:    return "StrategyMaxPosQtyExceeded";
    case XtRejectCode::StrategyDailyLossExceeded:    return "StrategyDailyLossExceeded";
    case XtRejectCode::StrategyKillSwitchActive:     return "StrategyKillSwitchActive";
    case XtRejectCode::StrategyBlockedByWatchdog:    return "StrategyBlockedByWatchdog";
    case XtRejectCode::PriceExceedsLimit:            return "PriceExceedsLimit";
    case XtRejectCode::MarginCallLiquidation:        return "MarginCallLiquidation";
    case XtRejectCode::AutoOffsetUnresolved:         return "AutoOffsetUnresolved";
    case XtRejectCode::BacktestEofClosing:           return "BacktestEofClosing";
    }
    return "Unknown";
}

}

XtStrategyContext::XtStrategyContext(const Config& cfg)
    : strategy_(cfg.strategy),
      exec_gateway_(cfg.exec_gateway),
      on_intent_fn_(cfg.on_intent_fn),
      on_weights_fn_(cfg.on_weights_fn),
      runtime_view_(cfg.runtime_view),
      init_config_(cfg.init_config),
      param_port_(cfg.param_port),
      roll_engine_(cfg.roll_engine),
      telemetry_sink_(cfg.telemetry_sink),
      route_by_sid_(cfg.route_by_sid),
      declared_registry_(cfg.declared_registry),
      strategy_id_(cfg.strategy_id),
      project_inflight_(cfg.project_inflight),
      conditional_orders_enabled_(cfg.conditional_orders_enabled) {
    if (!strategy_) {
        throw std::invalid_argument("XtStrategyContext requires non-null strategy");
    }
    if (conditional_orders_enabled_) {
        stop_fired_scratch_.reserve(
            xt_strategy_engine::XtStopOrderBook::kMaxStopSlotsPerCtx);
    }

    const bool has_exec    = exec_gateway_ != nullptr;
    const bool has_intent  = static_cast<bool>(on_intent_fn_);
    const bool has_weights = static_cast<bool>(on_weights_fn_);
    if (!has_exec && !has_intent && !has_weights) {
        throw std::invalid_argument(
            "XtStrategyContext requires exec_gateway / on_intent_fn / on_weights_fn");
    }
    if ((has_exec || has_intent) && !runtime_view_) {
        throw std::invalid_argument(
            "XtStrategyContext with exec_gateway/on_intent_fn requires runtime_view");
    }
    if (param_port_) {
        last_param_version_ = param_port_->version();
    }
}

void XtStrategyContext::enable_concurrent_metrics() noexcept {
    metrics_.enable_live_mode();
}

uint64_t XtStrategyContext::submit_impl_(const char* code,
                                         bool is_buy,
                                         int64_t price_fp,
                                         int64_t qty_fp,
                                         XtTimeInForce tif,
                                         const char* tag,
                                         XtOffset offset) {

	    const bool trace_direct_path = exec_gateway_ != nullptr && !on_intent_fn_;
	    xt_strategy_engine::IStrategyTelemetrySink* intent_rec =
	        trace_direct_path ? telemetry_sink_ : nullptr;
	    uint64_t trace_id = 0;
	    if (intent_rec && intent_rec->intent_trace_enabled()) {
	        const uint64_t candidate_trace_id = next_intent_trace_id();
	        if (intent_rec->should_sample_intent_trace(candidate_trace_id)) {
	            trace_id = candidate_trace_id;
	        }
	    }
	    std::optional<xt_core::tap::ScopedIntentTraceId> trace_scope;
	    if (trace_id != 0) {
	        trace_scope.emplace(trace_id);
    }
    const uint64_t decision_mono_ns = trace_id != 0 ? XtSteadyClock::steady_now_ns() : 0;
    const uint64_t trigger_tick_ts = trace_id != 0 ? xt_core::tap::current_tick_ts_ns() : 0;
    const char* raw_code = code;
    const int64_t raw_price_fp = price_fp;
    const int64_t raw_qty_fp = qty_fp;
    const XtTimeInForce raw_tif = tif;
    const XtOffset raw_offset = offset;
    const uint64_t user_tag_hash = trace_id != 0 ? trace_user_tag_hash(tag) : 0;

    const char* physical_code = code;
    std::string phys_buf;
    if (roll_engine_ && roll_engine_->is_logical_symbol(code)) {
        const uint32_t date = runtime_view_ ? runtime_view_->trading_date() : 0;
        if (roll_engine_->to_physical(date, code, phys_buf)) {
            physical_code = phys_buf.c_str();
        }
	    }
	    ExecFlags exec_flags = exec_none();
	    const bool needs_market_tick = price_fp == 0 || trace_id != 0;
	    const XtTick* mkt_tick = (needs_market_tick && runtime_view_)
	                               ? runtime_view_->last_tick(physical_code)
	                               : nullptr;
	    const int64_t best_bid_fp = (trace_id != 0 && mkt_tick) ? mkt_tick->bid_price_fp[0] : 0;
	    const int64_t best_ask_fp = (trace_id != 0 && mkt_tick) ? mkt_tick->ask_price_fp[0] : 0;
    auto fill_trace_common = [&](xt_strategy_engine::StrategyIntentTrace& p) {
        p.trace_id = trace_id;
        p.strategy_id = strategy_id_.c_str();
        p.book_id = strategy_id_.c_str();
        p.code_raw = raw_code;
        p.dir = trace_dir(is_buy);
        p.price_raw_fp = raw_price_fp;
        p.qty_raw_fp = raw_qty_fp;
        p.tif_raw = trace_tif(raw_tif);
        p.offset_raw = trace_offset(raw_offset);
        p.trigger_tick_ts = trigger_tick_ts;
        p.decision_mono_ns = decision_mono_ns;
        p.event_mono_ns = XtSteadyClock::steady_now_ns();
        p.best_bid_fp = best_bid_fp;
        p.best_ask_fp = best_ask_fp;
        p.user_tag_hash = user_tag_hash;
    };
	    auto emit_raw_intent = [&]() {
	        if (trace_id == 0) return;
	        if (!intent_rec) return;
	        xt_strategy_engine::StrategyIntentTrace p;
	        fill_trace_common(p);
	        p.stage = "RAW_INTENT";
	        intent_rec->emit_intent_trace(p);
	    };
	    auto emit_context_resolved = [&]() {
	        if (trace_id == 0) return;
	        if (!intent_rec) return;
	        const std::string flags = trace_exec_flags(exec_flags);
	        xt_strategy_engine::StrategyIntentTrace p;
        fill_trace_common(p);
        p.stage = "CONTEXT_RESOLVED";
        p.code_effective = physical_code;
        p.price_effective_fp = price_fp;
        p.qty_effective_fp = qty_fp;
        p.tif_effective = trace_tif(tif);
	        p.offset_effective = trace_offset(offset);
	        p.reduce_only = has_flag(exec_flags, ExecFlag::ReduceOnly);
	        p.exec_flags = flags.c_str();
	        intent_rec->emit_intent_trace(p);
	    };
	    auto emit_context_reject = [&](xt_runtime::XtRejectCode guard_rc) {
	        if (trace_id == 0) return;
	        if (!intent_rec) return;
	        const std::string flags = trace_exec_flags(exec_flags);
        xt_strategy_engine::StrategyIntentTrace p;
        fill_trace_common(p);
        p.stage = "CONTEXT_REJECT";
        p.code_effective = physical_code;
        p.price_effective_fp = price_fp;
        p.qty_effective_fp = qty_fp;
        p.tif_effective = trace_tif(tif);
        p.offset_effective = trace_offset(offset);
        p.reduce_only = has_flag(exec_flags, ExecFlag::ReduceOnly);
        p.exec_flags = flags.c_str();
	        p.reject_scope = "context_guard";
	        p.reject_code = trace_reject_code(guard_rc);
	        p.reject_reason = trace_reject_code(guard_rc);
	        intent_rec->emit_intent_trace(p);
	    };

    emit_raw_intent();
    if (price_fp == 0) {
        xt_strategy_engine::resolve_market_order(
            mkt_tick, is_buy, xt_strategy_engine::kMarketSlippageBps,
            price_fp, tif, exec_flags);
    }

    if (const auto guard_rc = guard_submit(physical_code, price_fp, qty_fp, is_buy);
        guard_rc != xt_runtime::XtRejectCode::NoError) {
        emit_context_reject(guard_rc);
        last_reject_code_ = guard_rc;
        next_order_reason_.clear();
        xt_core::tap::clear_next_order_reason();
        return 0;
    }
    emit_context_resolved();

    if (!next_order_reason_.empty()) {
        xt_core::tap::set_next_order_reason(next_order_reason_.c_str());
    } else {
        xt_core::tap::clear_next_order_reason();
    }
    if (on_intent_fn_) {
        int64_t& pos_fp = virtual_pos_fp_[physical_code];
        const int64_t prev_pos_fp = pos_fp;
        const int64_t requested_pos_fp = is_buy ? prev_pos_fp + qty_fp
                                                : prev_pos_fp - qty_fp;
        const auto r = on_intent_fn_(
            strategy_id_.c_str(), physical_code,
            order_exec_from_fp(prev_pos_fp),
            order_exec_from_fp(requested_pos_fp),
            price_fp,
            tif,
            tag);

        pos_fp = prev_pos_fp + r.accepted_delta_fp;
        last_reject_code_ = (r.accepted_delta_fp != 0) ? xt_runtime::XtRejectCode::NoError
                                                       : r.reject_reason;
        next_order_reason_.clear();
        xt_core::tap::clear_next_order_reason();

        return r.accepted_delta_fp != 0 ? ++next_virtual_id_ : 0;
    }

	    const xt_runtime::SubmitResult res =
	        trace_id != 0
	            ? exec_gateway_->submit_with_intent_trace(physical_code, is_buy, price_fp, qty_fp,
	                                                      tif, tag ? tag : "", offset,
	                                                      exec_flags, strategy_id_, trace_id)
	            : exec_gateway_->submit(physical_code, is_buy, price_fp, qty_fp,
	                                    tif, tag ? tag : "", offset,
	                                    exec_flags, strategy_id_);
    last_reject_code_ = res.reject_reason;
    next_order_reason_.clear();
    xt_core::tap::clear_next_order_reason();
    return res.order_id;
}

uint64_t XtStrategyContext::buy_fp(const char* code,
                                   int64_t price_fp,
                                   int64_t qty_fp,
                                   XtTimeInForce tif,
                                   const char* tag,
                                   XtOffset offset) {
    return submit_impl_(code,  true, price_fp, qty_fp, tif, tag, offset);
}

uint64_t XtStrategyContext::sell_fp(const char* code,
                                    int64_t price_fp,
                                    int64_t qty_fp,
                                    XtTimeInForce tif,
                                    const char* tag,
                                    XtOffset offset) {
    return submit_impl_(code,  false, price_fp, qty_fp, tif, tag, offset);
}

void XtStrategyContext::cancel(uint64_t order_id) {
    if (init_phase_ || order_id == 0) return;

    if (!exec_gateway_) {
        if (!cancel_unroutable_warned_) {
            cancel_unroutable_warned_ = true;
            XtLogger::warn_m("StrategyCtx",
                             "cancel(order_id={}) ignored: strategy '{}' has no routable "
                             "cancel channel (intent/weights mode); further occurrences suppressed",
                             static_cast<unsigned long long>(order_id), strategy_id_.c_str());
        }
        return;
    }
    (void)exec_gateway_->cancel(order_id);
}

uint64_t XtStrategyContext::submit_stop_fp(const char* code, bool is_buy,
                                           int64_t trigger_price_fp, int64_t qty_fp,
                                           XtOffset offset, const char* tag) {
    using xt_runtime::XtRejectCode;
    if (!conditional_orders_enabled_ || (!exec_gateway_ && !on_intent_fn_)) {
        last_reject_code_ = XtRejectCode::CapabilityUnsupported;
        return 0;
    }
    if (!code || code[0] == '\0' || qty_fp <= 0 || trigger_price_fp <= 0) {
        last_reject_code_ = XtRejectCode::InvalidInput;
        return 0;
    }

    std::string phys_code;
    const char* armed_code = code;
    if (!on_intent_fn_ && roll_engine_ && roll_engine_->is_logical_symbol(code)) {
        const uint32_t date = runtime_view_ ? runtime_view_->trading_date() : 0;
        if (!roll_engine_->to_physical(date, code, phys_code)) {
            last_reject_code_ = XtRejectCode::InvalidInput;
            XtLogger::warn_m("StrategyCtx",
                             "[{}] stop arm rejected: logical {} unresolvable (trading_date={})",
                             strategy_id_.c_str(), code, date);
            return 0;
        }
        armed_code = phys_code.c_str();
    }
    const uint64_t stop_id =
        stop_book_.arm_fixed(armed_code, is_buy, trigger_price_fp, qty_fp, offset, tag);
    if (stop_id == 0) {
        last_reject_code_ = XtRejectCode::CapabilityUnsupported;
        XtLogger::warn_m("StrategyCtx",
                         "[{}] stop book full ({}), arm rejected code={}",
                         strategy_id_.c_str(),
                         xt_strategy_engine::XtStopOrderBook::kMaxStopSlotsPerCtx, code);
        stop_emit_event_("arm_rejected_full");
        return 0;
    }
    last_reject_code_ = XtRejectCode::NoError;
    stop_emit_armed_gauge_();
    return stop_id;
}

uint64_t XtStrategyContext::submit_trailing_stop_fp(const char* code, bool is_buy,
                                                    int64_t distance_fp, int64_t qty_fp,
                                                    XtOffset offset, const char* tag,
                                                    int64_t seed_price_fp) {
    using xt_runtime::XtRejectCode;
    if (!conditional_orders_enabled_ || (!exec_gateway_ && !on_intent_fn_)) {
        last_reject_code_ = XtRejectCode::CapabilityUnsupported;
        return 0;
    }
    if (!code || code[0] == '\0' || qty_fp <= 0 || distance_fp <= 0 || seed_price_fp < 0) {
        last_reject_code_ = XtRejectCode::InvalidInput;
        return 0;
    }

    std::string phys_code;
    const char* armed_code = code;
    if (!on_intent_fn_ && roll_engine_ && roll_engine_->is_logical_symbol(code)) {
        const uint32_t date = runtime_view_ ? runtime_view_->trading_date() : 0;
        if (!roll_engine_->to_physical(date, code, phys_code)) {
            last_reject_code_ = XtRejectCode::InvalidInput;
            XtLogger::warn_m("StrategyCtx",
                             "[{}] stop arm rejected: logical {} unresolvable (trading_date={})",
                             strategy_id_.c_str(), code, date);
            return 0;
        }
        armed_code = phys_code.c_str();
    }
    const uint64_t stop_id = stop_book_.arm_trailing(armed_code, is_buy, distance_fp, qty_fp,
                                                     offset, tag, seed_price_fp);
    if (stop_id == 0) {
        last_reject_code_ = XtRejectCode::CapabilityUnsupported;
        XtLogger::warn_m("StrategyCtx",
                         "[{}] stop book full ({}), arm rejected code={}",
                         strategy_id_.c_str(),
                         xt_strategy_engine::XtStopOrderBook::kMaxStopSlotsPerCtx, code);
        stop_emit_event_("arm_rejected_full");
        return 0;
    }
    last_reject_code_ = XtRejectCode::NoError;
    stop_emit_armed_gauge_();
    return stop_id;
}

bool XtStrategyContext::cancel_stop(uint64_t stop_id) {
    uint64_t fired_order_id = 0;
    if (!stop_book_.disarm(stop_id, fired_order_id)) return false;

    if (fired_order_id != 0 && exec_gateway_) {
        (void)exec_gateway_->cancel(fired_order_id);
    }
    stop_emit_armed_gauge_();
    return true;
}

void XtStrategyContext::check_conditional_orders(const XtTick& tick) {
    if (!has_armed_stops()) return;
    stop_fired_scratch_.clear();
    stop_book_.check(tick.code, tick.price_fp, stop_fired_scratch_);
    for (const uint64_t stop_id : stop_fired_scratch_) {
        fire_stop_(stop_id);
    }
}

void XtStrategyContext::fire_stop_(uint64_t stop_id) {
    using xt_strategy_engine::StopSlot;
    StopSlot* s = stop_book_.find(stop_id);
    if (!s) return;

    int64_t fire_qty_fp = s->qty_fp;
    if (s->offset != XtOffset::Open) {
        const int64_t closeable_fp = stop_closeable_qty_(*s);
        if (closeable_fp <= 0) {

            XtLogger::info_m("StrategyCtx",
                             "[{}] stop {} dropped: nothing closeable on {}",
                             strategy_id_.c_str(),
                             static_cast<unsigned long long>(stop_id), s->code);
            stop_book_.consume(stop_id);
            stop_emit_event_("dropped_flat");
            stop_emit_armed_gauge_();
            return;
        }
        if (closeable_fp < fire_qty_fp) {
            fire_qty_fp = closeable_fp;
            stop_emit_event_("clamped");
        }
    }

    std::string phys;
    int64_t vp_before_fp = 0;
    if (on_intent_fn_) {
        phys = resolve_physical_(s->code);
        const auto it = virtual_pos_fp_.find(phys);
        vp_before_fp = it == virtual_pos_fp_.end() ? 0 : it->second;
    }

    // submit_impl_ 出口会消费/清空 next_order_reason_ 并覆写 last_reject_code_；
    // RAII stash 保住策略已设置未消费的 reason，并在退出时恢复策略可见的
    // last_reject_code_（engine 发起的 fire 不污染策略自身受理结果——本函数内
    // 的拒因日志在 stash 恢复前读取 fire 结果，不受影响；submit 抛异常被上层
    // guard 吞掉时同样恢复）。
    struct FireStash {
        std::string& reason_slot;
        std::string saved_reason;
        xt_runtime::XtRejectCode& rc_slot;
        xt_runtime::XtRejectCode saved_rc;
        explicit FireStash(std::string& r, xt_runtime::XtRejectCode& rc)
            : reason_slot(r), rc_slot(rc), saved_rc(rc) { saved_reason.swap(reason_slot); }
        ~FireStash() {
            reason_slot = std::move(saved_reason);
            rc_slot = saved_rc;
        }
    } fire_stash(next_order_reason_, last_reject_code_);
    next_order_reason_ = "stop_fired";
    const uint64_t order_id = submit_impl_(s->code, s->is_buy,  0,
                                           fire_qty_fp, XtTimeInForce::IOC,
                                           s->tag, s->offset);

    s = stop_book_.find(stop_id);
    if (!s) return;
    if (order_id == 0) {

        if (!s->warned_local_reject) {
            s->warned_local_reject = true;
            XtLogger::warn_m("StrategyCtx",
                             "[{}] stop {} fire rejected locally (rc={}), staying armed; "
                             "further occurrences suppressed",
                             strategy_id_.c_str(),
                             static_cast<unsigned long long>(stop_id),
                             static_cast<int>(last_reject_code_));
        }
        return;
    }
    if (on_intent_fn_) {

        const auto it = virtual_pos_fp_.find(phys);
        const int64_t vp_after_fp = it == virtual_pos_fp_.end() ? 0 : it->second;
        const int64_t applied_fp = vp_after_fp - vp_before_fp;
        const int64_t expected_fp = s->is_buy ? fire_qty_fp : -fire_qty_fp;
        if (applied_fp != expected_fp) {
            XtLogger::warn_m("StrategyCtx",
                             "[{}] stop {} intent partially accepted ({} of {}), consumed; "
                             "volscale may have scaled a protective close",
                             strategy_id_.c_str(),
                             static_cast<unsigned long long>(stop_id),
                             static_cast<long long>(applied_fp),
                             static_cast<long long>(expected_fp));
            stop_emit_event_("clamped");
        }
        stop_book_.consume(stop_id);
        stop_emit_event_("fired");
        stop_emit_armed_gauge_();
        return;
    }
    stop_book_.on_fired(stop_id, order_id, fire_qty_fp);
    stop_emit_event_("fired");
    stop_emit_armed_gauge_();
}

int64_t XtStrategyContext::stop_closeable_qty_(const xt_strategy_engine::StopSlot& s) {
    if (on_intent_fn_) {

        const char* phys = resolve_physical_(s.code);
        const auto it = virtual_pos_fp_.find(phys);
        const int64_t vp = it == virtual_pos_fp_.end() ? 0 : it->second;
        return s.is_buy ? (vp < 0 ? -vp : int64_t{0}) : (vp > 0 ? vp : int64_t{0});
    }

    xt_runtime::XtStraPositionDetail detail{};
    int64_t closeable_fp = 0;
    if (get_position_detail(s.code, &detail, 1) == 1) {
        const auto& side = s.is_buy ? detail.short_side : detail.long_side;
        closeable_fp = order_exec_to_fp(side.closeable_today + side.closeable_yesterday);
    }
    if (closeable_fp <= 0) {

        const int64_t net_fp = get_position_fp(s.code);
        closeable_fp = s.is_buy ? (net_fp < 0 ? -net_fp : int64_t{0})
                                : (net_fp > 0 ? net_fp : int64_t{0});
    }
    return closeable_fp;
}

void XtStrategyContext::notify_order_update_for_stops(const XtOrderExecUpdate& upd) {
    using xt_strategy_engine::StopTerminalAction;
    if (!conditional_orders_enabled_ || stop_book_.empty()) return;
    if (!order_status_is_final(upd.status)) return;
    const int64_t remaining_fp = order_lifecycle::effective_remaining_fp(upd);
    const StopTerminalAction action = stop_book_.on_order_terminal(upd.id, remaining_fp);
    switch (action) {
        case StopTerminalAction::kConsumed:
            stop_emit_event_("consumed");
            stop_emit_armed_gauge_();
            break;
        case StopTerminalAction::kRearmed:

            stop_emit_event_("refired");
            break;
        case StopTerminalAction::kExhausted:
            XtLogger::error_m("StrategyCtx",
                              "[{}] stop dropped after {} venue fires without full fill "
                              "(order_id={}); position may be unprotected",
                              strategy_id_.c_str(),
                              xt_strategy_engine::XtStopOrderBook::kMaxStopFireAttempts,
                              static_cast<unsigned long long>(upd.id));
            stop_emit_event_("dropped_exhausted");
            stop_emit_armed_gauge_();
            break;
        case StopTerminalAction::kIgnored:
            break;
    }
}

std::size_t XtStrategyContext::clear_stops_for_code(const char* code) {
    const std::size_t cleared = stop_book_.clear_for_code(code);
    if (cleared > 0) {
        XtLogger::warn_m("StrategyCtx",
                         "[{}] roll change cleared {} stop(s) on {}; re-arm in on_roll",
                         strategy_id_.c_str(), cleared, code ? code : "<null>");
        stop_emit_event_("dropped_roll", cleared);
        stop_emit_armed_gauge_();
    }
    return cleared;
}

void XtStrategyContext::stop_emit_event_(const char* event, uint64_t delta) const {
#ifdef XT_HAVE_METRICS
    ct::metrics::XtMetricRegistry::instance().inc_stop_order_total(event, delta);
#else
    (void)event;
    (void)delta;
#endif
}

void XtStrategyContext::stop_emit_armed_gauge_() const {
#ifdef XT_HAVE_METRICS
    ct::metrics::XtMetricRegistry::instance().set_stop_order_armed(
        strategy_id_, static_cast<uint32_t>(stop_book_.size()));
#endif
}

void XtStrategyContext::dispatch_bar(const char* code, const char* period, const XtBar& bar) {
    XtStrategyBase& strategy = require_strategy(strategy_, "XtStrategyContext::dispatch_bar");
    if (!is_subscribed(code)) return;
    strategy.on_bar(this, code, period, bar);
}

void XtStrategyContext::dispatch_bar_open(const char* code, const char* period,
                                          int64_t open_px_fp) {
    XtStrategyBase& strategy = require_strategy(strategy_, "XtStrategyContext::dispatch_bar_open");
    if (!is_subscribed(code)) return;
    strategy.on_bar_open(this, code, period, open_px_fp);
}

const char* XtStrategyContext::resolve_physical_(const char* code) const {
    if (!roll_engine_ || !code || code[0] == '\0') return code;
    if (!roll_engine_->is_logical_symbol(code)) return code;
    const uint32_t date = runtime_view_ ? runtime_view_->trading_date() : 0;
    if (roll_engine_->to_physical(date, code, resolve_phys_buf_)) return resolve_phys_buf_.c_str();
    return code;
}

bool XtStrategyContext::view_or_warn_() {

    if (runtime_view_) return true;
    if (!null_view_warned_) {
        null_view_warned_ = true;
        XtLogger::warn_m("StrategyCtx",
                         "[{}] read API called without runtime_view "
                         "(weights-only ctx); returning defaults",
                         strategy_id_.c_str());
    }
    return false;
}

const XtTick* XtStrategyContext::get_last_tick(const char* code) {
    if (!view_or_warn_()) return nullptr;
    if (const XtTick* t = runtime_view_->last_tick(code)) return t;
    const char* phys = resolve_physical_(code);
    return (phys != code) ? runtime_view_->last_tick(phys) : nullptr;
}

int64_t XtStrategyContext::get_last_price_fp(const char* code) {
    if (!view_or_warn_()) return 0;
    if (const int64_t px = runtime_view_->last_price_fp(code); px != 0) return px;
    const char* phys = resolve_physical_(code);
    return (phys != code) ? runtime_view_->last_price_fp(phys) : 0;
}

size_t XtStrategyContext::get_ticks(const char* code, XtTick* buf, size_t max) {
    if (!view_or_warn_()) return 0;
    if (const size_t n = runtime_view_->ticks(code, buf, max); n != 0) return n;
    const char* phys = resolve_physical_(code);
    return (phys != code) ? runtime_view_->ticks(phys, buf, max) : 0;
}

size_t XtStrategyContext::get_bars(const char* code, const char* period, XtBar* buf, size_t max) {
    if (!view_or_warn_()) return 0;
    if (const size_t n = runtime_view_->bars(code, period, buf, max); n != 0) return n;
    const char* phys = resolve_physical_(code);
    return (phys != code) ? runtime_view_->bars(phys, period, buf, max) : 0;
}

size_t XtStrategyContext::get_open_orders(const char* code, XtOrderExecUpdate* buf, size_t max) {
    if (!buf || max == 0) return 0;
    if (!view_or_warn_()) return 0;
    const bool code_filter = (code && code[0] != '\0');

    if (code_filter) code = resolve_physical_(code);

    const uint32_t sid_filter = route_by_sid_ ? resolved_sid_raw_() : 0;
    size_t count = code_filter ? runtime_view_->open_orders(code, buf, max, sid_filter)
                               : runtime_view_->open_orders(buf, max, sid_filter);

    if (exec_gateway_ != nullptr) {
        count = exec_gateway_->translate_open_orders(buf, count);
    }

    if (route_by_sid_ && exec_gateway_ != nullptr && sid_filter == 0) {
        size_t write_idx = 0;
        for (size_t i = 0; i < count; ++i) {
            const std::string sid = exec_gateway_->lookup_strategy_id(buf[i].id);
            if (sid != strategy_id_) continue;
            if (write_idx != i) buf[write_idx] = buf[i];
            ++write_idx;
        }
        count = write_idx;
    }
    return count;
}

int64_t XtStrategyContext::get_position_fp(const char* code) {

    if (!view_or_warn_()) return 0;

    const int64_t confirmed_fp = runtime_view_->pos_qty_fp(resolve_physical_(code));
    return confirmed_fp + projected_inflight_fp(code);
}

uint32_t XtStrategyContext::resolved_sid_raw_() {

    if (!sid_resolved_) {
        if (auto* reg = xt_identity::StrategySidRegistry::global())
            resolved_sid_id_ = reg->lookup(strategy_id_);
        sid_resolved_ = true;
    }
    return resolved_sid_id_;
}

int64_t XtStrategyContext::projected_inflight_fp(const char* code) {

    if (!project_inflight_ || !runtime_view_) return 0;
    return runtime_view_->signed_inflight_fp(
               StrategySid::from_raw(resolved_sid_raw_()), resolve_physical_(code));
}

int64_t XtStrategyContext::get_position_avg_price_fp(const char* code) {
    if (!view_or_warn_()) return 0;
    return runtime_view_->pos_avg_fp(resolve_physical_(code));
}

int64_t XtStrategyContext::get_position_unrealized_fp(const char* code) {
    if (!view_or_warn_()) return 0;
    return runtime_view_->pos_unrealized_fp(resolve_physical_(code));
}

size_t XtStrategyContext::get_position_detail(const char* code,
                                              xt_runtime::XtStraPositionDetail* buf,
                                              size_t max) {
    if (!buf || max == 0) return 0;
    if (!view_or_warn_()) return 0;
    xt_runtime::XtStraPositionDetail detail{};
    if (!runtime_view_->pos_detail(resolve_physical_(code), detail)) return 0;
    buf[0] = detail;
    return 1;
}

int64_t XtStrategyContext::get_cash_fp() {
    if (!view_or_warn_()) return 0;
    return runtime_view_->cash_fp();
}

int64_t XtStrategyContext::get_equity_fp() {
    if (!view_or_warn_()) return 0;
    return runtime_view_->equity_fp();
}

const xt_runtime::XtStraContractInfo* XtStrategyContext::get_contract_info(const char* code) {
    if (!code || code[0] == '\0') return nullptr;
    if (!view_or_warn_()) return nullptr;
    xt_runtime::XtStraContractInfo info{};

    if (!runtime_view_->contract_info(resolve_physical_(code), info)) return nullptr;
    auto it = contract_cache_.insert_or_assign(code, info).first;
    return &it->second;
}

uint32_t XtStrategyContext::get_date() {
    if (!view_or_warn_()) return 0;
    return runtime_view_->date();
}

uint32_t XtStrategyContext::get_trading_date() {
    if (!view_or_warn_()) return 0;
    return runtime_view_->trading_date();
}

uint32_t XtStrategyContext::get_time_ms() {
    if (!view_or_warn_()) return 0;
    return runtime_view_->time_ms();
}

std::string XtStrategyContext::get_param_str(const char* key, const char* def) {
    const std::string* value = find_param_value(key);
    if (!value) return def ? def : "";
    return *value;
}

int64_t XtStrategyContext::get_param_int(const char* key, int64_t def) {
    const std::string* value = find_param_value(key);
    if (!value) return def;
    try {
        size_t parsed = 0;
        const int64_t out = std::stoll(*value, &parsed, 10);
        if (parsed != value->size()) return def;
        return out;
    } catch (...) {
        return def;
    }
}

double XtStrategyContext::get_param_double(const char* key, double def) {
    const std::string* value = find_param_value(key);
    if (!value) return def;
    try {
        size_t parsed = 0;
        const double out = std::stod(*value, &parsed);
        if (parsed != value->size()) return def;
        return out;
    } catch (...) {
        return def;
    }
}

bool XtStrategyContext::get_param_bool(const char* key, bool def) {
    const std::string* value = find_param_value(key);
    if (!value) return def;
    return parse_bool_value(*value, def);
}

void XtStrategyContext::sub_tick(const char* code) {
    if (!code || code[0] == '\0') return;
    subscriptions_.insert(code);
    sub_cache_.clear();
    tick_subscribed_ = true;
}

void XtStrategyContext::sub_bar(const char* code, const char* period, size_t max) {
    if (!code || code[0] == '\0' || !period || period[0] == '\0') return;

    if (max > 0 && runtime_view_) runtime_view_->set_bar_capacity(code, period, max);
    subscriptions_.insert(code);
    sub_cache_.clear();
}

void XtStrategyContext::declare_position_interest(const char* code) {

    if (!code || code[0] == '\0') return;
    if (!declared_registry_) return;
    declared_registry_->declare(strategy_id_, std::string(code));
}

void XtStrategyContext::log_debug(const char* msg) {
    XtLogger::debug_m("STRA", "{}", msg ? msg : "");
}

void XtStrategyContext::log_info(const char* msg) {
    XtLogger::info_m("STRA", "{}", msg ? msg : "");
}

void XtStrategyContext::log_warn(const char* msg) {
    XtLogger::warn_m("STRA", "{}", msg ? msg : "");
}

void XtStrategyContext::report_metric(const char* name, double val) {
    if (!name || name[0] == '\0') return;
    metrics_.observe(name, val);

    if (telemetry_sink_) {

        const uint64_t ts_local_ns = XtSteadyClock::steady_now_ns();
        telemetry_sink_->on_metric(ts_local_ns,
                                   name,
                                   val,
                                   strategy_id_.c_str(),
                                   strategy_id_.c_str());
    }
}

void XtStrategyContext::set_next_order_reason(const char* reason) {

    next_order_reason_ = reason ? reason : "";
}

void XtStrategyContext::save_userdata(const char* key, const char* val) {
    if (!key || key[0] == '\0') return;
    userdata_[key] = val ? val : "";
}

const char* XtStrategyContext::load_userdata(const char* key, const char* def) {
    if (!key || key[0] == '\0') return def;
    const auto it = userdata_.find(key);
    if (it == userdata_.end()) return def;

    load_userdata_buf_ = it->second;
    return load_userdata_buf_.c_str();
}

void XtStrategyContext::on_init() {
    XtStrategyBase& strategy = require_strategy(strategy_, "XtStrategyContext::on_init");
    {
        // on_init 期间拒绝下单，避免策略在通道/账户状态未完成前提交订单。
        // RAII 复位：on_init 抛异常被上游 guard 吞掉时不让 init_phase_ 永久卡死
        //（否则之后所有下单静默拒为 TradingNotReady）。
        struct InitPhaseGuard {
            bool& flag;
            explicit InitPhaseGuard(bool& f) : flag(f) { flag = true; }
            ~InitPhaseGuard() { flag = false; }
        } guard(init_phase_);
        strategy.on_init(this);
    }

    strategy.on_channel_ready(this, "MD");
    strategy.on_channel_ready(this, "TD");
}

void XtStrategyContext::dispatch_tick(const XtTick& tick) {
    XtStrategyBase& strategy = require_strategy(strategy_, "XtStrategyContext::dispatch_tick");
    if (!is_subscribed(tick.code)) return;
    metrics_.on_tick();
    strategy.on_tick(this, tick);
}

void XtStrategyContext::on_order_queue(const XtOrderQueue& q) {
    XtStrategyBase& strategy = require_strategy(strategy_, "XtStrategyContext::on_order_queue");
    if (!is_subscribed(q.code)) return;
    strategy.on_order_queue(this, q);
}

void XtStrategyContext::on_order_detail(const XtOrderDetail& d) {
    XtStrategyBase& strategy = require_strategy(strategy_, "XtStrategyContext::on_order_detail");
    if (!is_subscribed(d.code)) return;
    strategy.on_order_detail(this, d);
}

void XtStrategyContext::on_transaction(const XtTransaction& t) {
    XtStrategyBase& strategy = require_strategy(strategy_, "XtStrategyContext::on_transaction");
    if (!is_subscribed(t.code)) return;
    strategy.on_transaction(this, t);
}

void XtStrategyContext::on_session_begin(uint32_t trading_date) {
    XtStrategyBase& strategy = require_strategy(strategy_, "XtStrategyContext::on_session_begin");
    strategy.on_session_begin(this, trading_date);
}

void XtStrategyContext::on_session_end(uint32_t trading_date) {
    XtStrategyBase& strategy = require_strategy(strategy_, "XtStrategyContext::on_session_end");
    strategy.on_session_end(this, trading_date);
}

void XtStrategyContext::on_roll(const char* old_code, const char* new_code,
                               uint32_t trading_date, int64_t old_last_price_fp) {
    XtStrategyBase& strategy = require_strategy(strategy_, "XtStrategyContext::on_roll");
    strategy.on_roll(this, old_code, new_code, trading_date, old_last_price_fp);

    if (old_code && *old_code) {
        auto it = virtual_pos_fp_.find(old_code);
        if (it != virtual_pos_fp_.end() && it->second == 0) {
            virtual_pos_fp_.erase(it);
        }
    }
}

void XtStrategyContext::on_channel_ready(const char* channel) {
    XtStrategyBase& strategy = require_strategy(strategy_, "XtStrategyContext::on_channel_ready");
    strategy.on_channel_ready(this, channel);
}

void XtStrategyContext::on_channel_lost(const char* channel, int reason) {
    XtStrategyBase& strategy = require_strategy(strategy_, "XtStrategyContext::on_channel_lost");
    strategy.on_channel_lost(this, channel, reason);
}

void XtStrategyContext::on_params_updated(uint64_t version) {
    if (!param_port_ || version <= last_param_version_) return;
    last_param_version_ = version;

    if (!strategy_id_.empty()) {
        std::string buf;
        const std::string enabled_key = "strategy." + strategy_id_ + ".enabled";
        bool enabled = true;
        const bool found_enabled_key = param_port_->get(enabled_key, buf);
        if (found_enabled_key) {

            enabled = parse_bool_value(buf,  false);
        }
        set_control_enabled(enabled);
        XtLogger::info_m("RT",
                         "strategy enabled gate strategy={} version={} found={} value={} enabled={}",
                         strategy_id_, version, found_enabled_key,
                         found_enabled_key ? buf : "", enabled);
    }

    XtStrategyBase& strategy = require_strategy(strategy_, "XtStrategyContext::on_params_updated");
    strategy.on_params(this);
}

void XtStrategyContext::on_order(const XtOrderExecUpdate& upd) {
    XtStrategyBase& strategy = require_strategy(strategy_, "XtStrategyContext::on_order");
    strategy.on_order(this, upd);
}

void XtStrategyContext::on_fill(const XtFillEvent& fill) {
    XtStrategyBase& strategy = require_strategy(strategy_, "XtStrategyContext::on_fill");
    strategy.on_fill(this, fill);
}

void XtStrategyContext::on_account(const XtAccountUpdate& upd) {
    XtStrategyBase& strategy = require_strategy(strategy_, "XtStrategyContext::on_account");
    strategy.on_account(this, upd);
}

void XtStrategyContext::set_initial_virtual_pos(const char* code, int64_t qty_fp) {

    if (!code || code[0] == '\0') return;
    auto& slot = virtual_pos_fp_[code];
    if (slot != 0 && slot != qty_fp) {
        XtLogger::warn_m("XtStrategyContext",
                          "set_initial_virtual_pos overwriting non-zero virtual_pos "
                          "(strategy={} code={} old={} new={}) — possible reconcile "
                          "racing strategy intent",
                          strategy_id_, code,
                          static_cast<long long>(slot),
                          static_cast<long long>(qty_fp));
    }
    slot = qty_fp;
}

void XtStrategyContext::restore_userdata(std::unordered_map<std::string, std::string> kv) {

    if (!userdata_.empty()) {
        XtLogger::warn_m("XtStrategyContext",
                          "restore_userdata overwriting non-empty userdata "
                          "(strategy={} existing={} incoming={}) — restore should "
                          "run before on_init on assembly thread",
                          strategy_id_, userdata_.size(), kv.size());
    }
    userdata_ = std::move(kv);
}

ct::metrics::XtStrategyMetricsSnapshot XtStrategyContext::drain_strategy_metrics() {
    return metrics_.drain_snapshot();
}

bool XtStrategyContext::can_submit() const {
    if (!signals_) {
        return control_enabled_.load(std::memory_order_acquire);
    }
    if (!signals_->in_trading_section.load(std::memory_order_acquire)) return false;
    if (!signals_->trading_ready.load(std::memory_order_acquire))      return false;
    if (signals_->kill_switch.load(std::memory_order_acquire))         return false;
    if (signals_->closeout_no_open_window.load(std::memory_order_acquire)) return false;
    if (!control_enabled_.load(std::memory_order_acquire))             return false;
    return true;
}

XtStrategyBase& XtStrategyContext::require_strategy(XtStrategyBase* strategy, const char* where) {
    if (strategy) return *strategy;
    throw std::logic_error(std::string(where ? where : "XtStrategyContext") + ": strategy instance is null");
}

bool XtStrategyContext::is_valid_fp_input(const char* code, int64_t price_fp, int64_t qty_fp) {

    return code && code[0] != '\0' && price_fp >= 0 && qty_fp > 0;
}

xt_runtime::XtRejectCode XtStrategyContext::guard_submit(const char* code,
                                                         int64_t price_fp,
                                                         int64_t qty_fp,
                                                         bool is_buy) const {

    if (!on_intent_fn_ && !exec_gateway_) {
        XtLogger::warn_m("STRA",
                         "{} rejected: context has no execution channel "
                         "(weights/selector mode) strategy={} code={}",
                         is_buy ? "buy_fp" : "sell_fp", strategy_id_,
                         code ? code : "");
        return xt_runtime::XtRejectCode::CapabilityUnsupported;
    }
    if (init_phase_) {
        XtLogger::warn_m("STRA", "ignore {} during init", is_buy ? "buy_fp" : "sell_fp");
        return xt_runtime::XtRejectCode::TradingNotReady;
    }
    if (!is_valid_fp_input(code, price_fp, qty_fp)) {
        XtLogger::error_m("STRA",
                          "{} invalid input code={} price_fp={} qty_fp={}",
                          is_buy ? "buy_fp" : "sell_fp",
                          code ? code : "",
                          price_fp,
                          qty_fp);
        return xt_runtime::XtRejectCode::InvalidInput;
    }
    return xt_runtime::XtRejectCode::NoError;
}

bool XtStrategyContext::is_subscribed(const char* code) const {
    if (subscriptions_.empty()) return true;
    if (!code || code[0] == '\0') return false;
    const auto it = sub_cache_.find(code);
    if (it != sub_cache_.end()) return it->second;
    const bool matched = match_subscribed(code, subscriptions_);
    sub_cache_.emplace(code, matched);
    return matched;
}

const std::string* XtStrategyContext::find_param_value(const char* key) const {
    if (!key || key[0] == '\0') return nullptr;

    if (param_port_) {
        if (!strategy_id_.empty()) {
            std::string ns_key;
            ns_key.reserve(strategy_id_.size() + 10 + std::strlen(key));
            ns_key.append("strategy.").append(strategy_id_).append(".").append(key);
            if (param_port_->get(ns_key, param_value_buf_)) {
                return &param_value_buf_;
            }
        }
        if (param_port_->get(key, param_value_buf_)) {
            return &param_value_buf_;
        }
    }
    if (!init_config_) return nullptr;
    const auto it = init_config_->values.find(key);
    if (it == init_config_->values.end()) return nullptr;
    return &it->second;
}

bool XtStrategyContext::parse_bool_value(const std::string& value, bool def) {
    std::string lowered = value;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    if (lowered == "1" || lowered == "true" || lowered == "yes" || lowered == "on") return true;
    if (lowered == "0" || lowered == "false" || lowered == "no" || lowered == "off") return false;
    return def;
}

void XtStrategyContext::set_target_weights(
    const std::unordered_map<std::string, double>& weights) {
    if (on_weights_fn_) {
        on_weights_fn_(strategy_id_, weights);
    }
}
