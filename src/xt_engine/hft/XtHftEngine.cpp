#include "XtHftEngine.h"

#include "xt_strategy_engine/XtStrategyContext.h"
#include "xt_strategy_engine/XtStrategyBase.h"
#include "xt_core/market/XtTickConvert.h"
#include "xt_util/time/CtHHMM.h"
#include "xt_core/session/XtSessionFilter.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_identity/OrderAttributionStore.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <exception>
#include <stdexcept>
#include <thread>
#include <unordered_set>
#include <utility>

#include "api/AliveInflightOrder.h"
#include "xt_account/state/AccountStateStore.h"
#include "xt_account/state/AccountEvent.h"
#include "xt_account/ingest/AccountEventCodec.h"
#include "xt_config/catalog/XtCommodityCatalog.h"
#include "xt_core/XtFixedPoint.h"
#include "xt_core/order/XtOrderLifecycle.h"
#include "xt_runtime/order/XtOrderRuntimeBook.h"
#include "xt_account/api/AccountCloseableView.h"
#include "xt_account/api/AccountSnapshot.h"
#include "xt_strategy_engine/IStrategyExecSink.h"
#include "xt_util/time/XtSteadyClock.h"
#include "xt_util/path/PathUtil.h"
#include "xt_logger/XtLogger.h"
#include "xt_logger/XtCallbackGuard.h"

#include <fstream>
#include <sstream>

namespace {

template<typename Fn>
bool exec_guard(const char* stage, std::atomic<uint64_t>& counter, Fn&& fn) {
    return xt_log::run_guarded("XtHftEngine", stage, counter, std::forward<Fn>(fn));
}

template<typename Fn>
bool exec_guard_ctx(const char* stage,
                    std::size_t ctx_idx,
                    const std::string& sid,
                    std::atomic<uint64_t>& counter,
                    Fn&& fn) noexcept {
    return xt_log::run_guarded("XtHftEngine", stage,
                               xt_log::guard_item(ctx_idx, sid.c_str(), &counter),
                               std::forward<Fn>(fn));
}

/// @brief 最小 RAII scope guard：析构（任何退出路径）执行 fn。
/// @note 析构 noexcept —— fn 须自吞异常（内部 try/catch）以保栈展开安全。
template<typename Fn>
struct ScopeGuard {
    Fn fn;
    ~ScopeGuard() noexcept { fn(); }
};
template<typename Fn>
ScopeGuard<Fn> make_scope_guard(Fn&& fn) { return ScopeGuard<Fn>{std::forward<Fn>(fn)}; }

void sync_runtime_clock(xt_strategy_engine::XtRuntimeView& view,
                        bool has_last_tick,
                        const XtTick& last_tick,
                        uint32_t trading_date_state) {
    const uint32_t action_date = has_last_tick ? last_tick.action_date : 0;
    const uint32_t action_time = has_last_tick ? last_tick.action_time : 0;
    const uint32_t trading_date = trading_date_state != 0
        ? trading_date_state
        : (has_last_tick
            ? (last_tick.trading_date ? last_tick.trading_date : last_tick.action_date)
            : 0);
    view.set_clock(action_date,
                   action_time / 1000,
                   action_time,
                   trading_date);
}

}

XtHftEngine::XtHftEngine(Dependencies deps,
                         Options opts,
                         std::vector<std::unique_ptr<XtStrategyContext>> contexts,
                         Hooks hooks)
    : gateway_(deps.gateway)
    , runtime_view_(deps.view) {
    if (!gateway_ || !runtime_view_) {
        throw std::invalid_argument(
            "XtHftEngine: gateway and view are required dependencies");
    }

    slots_.reserve(contexts.size());
    for (auto& ctx : contexts) {

        if (ctx) ctx->ensure_sid_resolved();
        slots_.push_back(ContextSlot{std::move(ctx), false});
    }
    validate_assembly_(opts, hooks);

    session_ = opts.session;
    roll_    = XtRollEngine(opts.roll);
    unmatched_routing_          = opts.unmatched_routing;
    conditional_orders_enabled_ = opts.conditional_orders_enabled;
    apply_replay_ext_(opts.replay);

    account_store_       = deps.account_store;
    order_book_          = deps.order_book;
    catalog_             = deps.catalog;
    declared_registry_   = deps.declared_registry;
    order_id_translator_ = deps.order_id_translator;

    observers_                = std::move(hooks.observers);
    fill_realized_cb_         = std::move(hooks.fill_realized);
    cash_reanchor_probe_      = std::move(hooks.cash_reanchor);
    mark_tick_cb_             = std::move(hooks.mark_tick);
    trade_excursion_cb_       = std::move(hooks.trade_excursion);
    on_params_extra_          = std::move(hooks.on_params_extra);
    on_strategy_init_failure_ = std::move(hooks.on_strategy_init_failure);
    lifecycle_.init(std::move(hooks.lifecycle));

    XtLogger::set_throttle_ms("unmatched_sid_recovered", 60000);
    XtLogger::set_throttle_ms("engine_pre_start_drop", 60000);
}

void XtHftEngine::validate_assembly_(const Options& opts, const Hooks& hooks) const {
    auto fail = [](const std::string& why) {
        throw std::invalid_argument("XtHftEngine assembly: " + why);
    };

    if (slots_.empty()) fail("contexts must not be empty");

    for (std::size_t i = 0; i < hooks.observers.size(); ++i) {
        const auto* obs = hooks.observers[i];
        if (!obs) fail("hooks.observers[" + std::to_string(i) + "] is null");
        for (std::size_t j = 0; j < i; ++j) {
            if (hooks.observers[j] == obs) {
                fail("hooks.observers[" + std::to_string(i) + "] duplicates index " +
                     std::to_string(j) + " (would fan out the same observer twice)");
            }
        }
    }

    std::unordered_set<std::string> seen_ids;
    for (std::size_t i = 0; i < slots_.size(); ++i) {
        const auto* ctx = slots_[i].ctx.get();
        if (!ctx) fail("contexts[" + std::to_string(i) + "] is null");
        const std::string& sid = ctx->strategy_id();
        if (sid.empty()) fail("contexts[" + std::to_string(i) + "] has empty strategy_id");

        if (sid[0] == '_') {
            fail("strategy_id '" + sid +
                 "' uses the reserved '_' namespace ({_global, _default, _manual})");
        }
        if (!seen_ids.insert(sid).second) {
            fail("duplicate strategy_id '" + sid + "' (routing demux key must be unique)");
        }
        if (ctx->conditional_orders_enabled() != opts.conditional_orders_enabled) {
            fail("conditional_orders switch mismatch on strategy '" + sid +
                 "' (engine and context must share the same yaml source)");
        }
    }

    const bool is_multi = slots_.size() > 1;
    if (!is_multi && opts.unmatched_routing != UnmatchedRoutingPolicy::WarnAndDrop) {
        fail("non-default unmatched routing policy requires multiple contexts "
             "(single-context routing never reaches the unmatched branch)");
    }
    if (is_multi) {

        for (const auto& slot : slots_) {
            if (slot.ctx->resolved_sid_id() == xt_identity::StrategySidRegistry::kNoSidId) {
                fail("unresolved sid on strategy '" + slot.ctx->strategy_id() +
                     "' (register it in the global StrategySidRegistry before assembly)");
            }
        }
    }
}

XtHftEngine::~XtHftEngine() = default;

XtHftEngine::StartReport XtHftEngine::start() {

    if (started_) {
        throw std::logic_error("XtHftEngine::start called more than once");
    }
    started_ = true;

    StartReport report;

    for (std::size_t i = 0; i < slots_.size(); ++i) {
        const bool ok = exec_guard_ctx("strategy.on_init", i, slots_[i].ctx->strategy_id(),
                                       strategy_exception_total_,
                                       [&]() { slots_[i].ctx->on_init(); });
        if (ok) continue;
        report.failures.push_back(
            StrategyStartFailure{slots_[i].ctx->resolved_sid_id(),
                                 slots_[i].ctx->strategy_id()});
        disable_context_(i);
    }
    report.all_started = report.failures.empty();
    return report;
}

void XtHftEngine::note_pre_start_drop_(const char* entry) noexcept {
    pre_start_dropped_total_.fetch_add(1, std::memory_order_relaxed);
    XtLogger::error_mt("XtHftEngine", "engine_pre_start_drop",
                       "runtime event '{}' arrived before start() — dropped (total={})",
                       entry ? entry : "",
                       static_cast<unsigned long long>(
                           pre_start_dropped_total_.load(std::memory_order_relaxed)));
}

void XtHftEngine::disable_context_(std::size_t i) {
    if (i >= slots_.size()) return;
    if (slots_[i].disabled) return;
    slots_[i].disabled = true;
    strategy_init_failure_total_.fetch_add(1, std::memory_order_relaxed);
    const std::string& name = slots_[i].ctx->strategy_id();
    const uint32_t sid_id = slots_[i].ctx->resolved_sid_id();
    XtLogger::critical(
        "[ENGINE] strategy '{}' (sid_id={}) on_init 失败 — fail-closed disabled："
        "后续所有回调 fan-out 跳过该策略；兄弟策略不受影响。若该策略有恢复持仓则无人管理，"
        "需 operator 处置。",
        name.c_str(), sid_id);
    if (on_strategy_init_failure_) on_strategy_init_failure_(sid_id, name);
}

void XtHftEngine::apply_replay_ext_(const ReplayExt& ext) {
    calendar_filter_ = ext.calendar_filter;
    if (ext.session_offset_minutes > 0) {
        td_infer_.init(ext.session_offset_minutes, ext.next_trading_day);
    }
}

const XtStrategyContext* XtHftEngine::context(std::size_t idx) const noexcept {
    return idx < slots_.size() ? slots_[idx].ctx.get() : nullptr;
}

bool XtHftEngine::set_strategy_control_enabled(const std::string& strategy_id, bool enabled) {
    if (strategy_id.empty()) return false;
    bool matched = false;

    for (auto& slot : slots_) {
        if (!slot.ctx || slot.ctx->strategy_id() != strategy_id) continue;
        slot.ctx->set_control_enabled(enabled);
        matched = true;
    }
    return matched;
}

void XtHftEngine::restore_strategy_userdata(
    const std::function<bool(const std::string&, std::unordered_map<std::string, std::string>&)>&
        loader) {
    if (!loader) return;
    for (auto& slot : slots_) {
        if (!slot.ctx) continue;
        std::unordered_map<std::string, std::string> kv;
        if (loader(slot.ctx->strategy_id(), kv)) slot.ctx->restore_userdata(std::move(kv));
    }
}

void XtHftEngine::dump_strategy_userdata(
    const std::function<void(const std::string&, std::unordered_map<std::string, std::string>)>&
        writer) const {
    if (!writer) return;
    for (const auto& slot : slots_) {
        if (!slot.ctx) continue;
        writer(slot.ctx->strategy_id(), slot.ctx->snapshot_userdata());
    }
}

void XtHftEngine::drain_all_strategy_metrics(
    std::vector<std::pair<std::string, ct::metrics::XtStrategyMetricsSnapshot>>& out) {
    for (auto& slot : slots_) {
        if (!slot.ctx) continue;
        out.emplace_back(slot.ctx->strategy_id(), slot.ctx->drain_strategy_metrics());
    }
}

void XtHftEngine::apply_strategy_tick_state_(const XtTick& tick) {
    if (account_store_) {
        account_store_->apply(XtAccountEvent::market_tick(tick.code, tick.price_fp));
    }
}

bool XtHftEngine::prepare_cycle_(XtTick& tick) {

    uint32_t trading_date = tick.trading_date;
    if (trading_date == 0 && td_infer_.enabled()) {
        trading_date = td_infer_.infer(tick.action_date, tick.action_time);
        tick.trading_date = trading_date;
    }
    if (trading_date == 0) {
        trading_date = xt_tick_convert::trading_date(tick);
    }
    if (calendar_filter_ && !calendar_filter_(trading_date)) { ++drops_.calendar; return false; }

    check_roll_change_(trading_date);

    const XtTick& prev_snapshot = has_last_any_core_tick_ ? last_any_core_tick_ : tick;

    if (!update_session_state_(trading_date, tick.action_time, prev_snapshot)) {
        ++drops_.session;
        return false;
    }

    update_market_state_(tick);

    gateway_->advance_clock(tick.action_date != 0 ? tick.action_date
                                                  : xt_tick_convert::trading_date(tick),
                            tick.action_time, xt_tick_convert::trading_date(tick));
    apply_strategy_tick_state_(tick);

    if (mark_tick_cb_) mark_tick_cb_(tick);
    return true;
}

void XtHftEngine::dispatch_tick_to_contexts_(const XtTick& tick,
                                             const char* stage,
                                             const char* error_message) {
    const std::size_t ctx_n = slots_.size();
    bool dispatch_ok = true;
    for (std::size_t i = 0; i < ctx_n; ++i) {
        if (is_ctx_disabled_(i)) continue;
        dispatch_ok =
            exec_guard_ctx(stage, i, slots_[i].ctx->strategy_id(), strategy_exception_total_,
                           [&]() { slots_[i].ctx->dispatch_tick(tick); }) &&
            dispatch_ok;
    }
    if (!dispatch_ok) {
        throw std::runtime_error(error_message);
    }
}

void XtHftEngine::check_conditional_stops_(const XtTick& tick) {
    for (std::size_t i = 0; i < slots_.size(); ++i) {
        if (is_ctx_disabled_(i)) continue;
        if (!slots_[i].ctx->has_armed_stops()) continue;

        (void)exec_guard_ctx("strategy.check_stops", i, slots_[i].ctx->strategy_id(),
                             strategy_exception_total_,
                             [&]() { slots_[i].ctx->check_conditional_orders(tick); });
    }
}

void XtHftEngine::notify_stops_for_update_(const XtOrderExecUpdate& upd,
                                           uint32_t book_sid_hint) {
    if (slots_.size() == 1) {
        if (!is_ctx_disabled_(0)) slots_[0].ctx->notify_order_update_for_stops(upd);
        return;
    }

    const uint32_t effective_sid =
        upd.sid_id != xt_identity::StrategySidRegistry::kNoSidId ? upd.sid_id
                                                                 : book_sid_hint;
    const std::size_t idx = find_ctx_idx_by_sid_id_(effective_sid);
    if (idx < slots_.size() && !is_ctx_disabled_(idx)) {
        slots_[idx].ctx->notify_order_update_for_stops(upd);
    }
}

void XtHftEngine::drain_order_updates_() {
    thread_local std::vector<XtOrderExecUpdate> updates;

    updates.clear();
    gateway_->drain_order_updates(updates);
    for (auto& upd : updates) on_order_update(upd);
}

void XtHftEngine::run_tick_cycle_(XtTick tick) {
    assert(!slots_.empty() && gateway_);

    if (!prepare_cycle_(tick)) return;

    gateway_->set_dispatch_clock(xt_tick_convert::trading_date(tick),
                                 tick.action_time);

    if (conditional_orders_enabled_) check_conditional_stops_(tick);
    dispatch_tick_to_contexts_(tick,
                               "strategy.dispatch_tick",
                               "XtHftEngine::run_tick_cycle strategy dispatch failed");
    gateway_->process_tick(tick);
    drain_order_updates_();
}

uint32_t XtHftEngine::infer_trading_date_(const XtTick& tick) const {

    if (tick.trading_date != 0) return tick.trading_date;
    if (td_infer_.enabled()) {
        return td_infer_.infer(tick.action_date, tick.action_time);
    }
    return xt_tick_convert::trading_date(tick);
}

bool XtHftEngine::roll_accepts_tick_(const XtTick& tick) const {
    if (!roll_.active()) return true;
    const uint32_t td = infer_trading_date_(tick);
    return roll_.should_dispatch_tick(td, tick.code);
}

void XtHftEngine::on_tick(const XtTick& tick) {
    if (!started_) { note_pre_start_drop_("on_tick"); return; }
    assert(!slots_.empty() && gateway_);
    if (!roll_accepts_tick_(tick)) { ++drops_.roll; return; }

    if (xt_core::is_enabled(session_)
        && !xt_core::in_session_closed(tick.action_time, session_.sections)) {
        handle_pre_session_tick_(tick);
        return;
    }

    run_tick_cycle_(tick);
}

void XtHftEngine::handle_pre_session_tick_(const XtTick& tick) {
    const uint32_t tick_min = tick.action_time / 100000;
    bool is_preopen = false;
    for (const auto& sec : session_.sections) {
        if (!sec.cross_midnight) continue;
        if (xt_hhmm::is_next_minute(tick_min, sec.start / 100000)) {
            is_preopen = true;
            break;
        }
    }
    if (!is_preopen) { ++drops_.session; return; }

    const uint32_t trading_date = infer_trading_date_(tick);
    if (calendar_filter_ && !calendar_filter_(trading_date)) { ++drops_.calendar; return; }
    check_roll_change_(trading_date);
    update_market_state_(tick);
    dispatch_tick_to_contexts_(tick,
                               "strategy.pre_session_dispatch",
                               "XtHftEngine::pre_session strategy dispatch failed");
}

bool XtHftEngine::update_session_state_(uint32_t session_date, uint32_t action_time,
                                        const XtTick& prev_tick) {

    auto fan_session_end = [this](uint32_t td) {
        for (std::size_t i = 0; i < slots_.size(); ++i) {
            if (is_ctx_disabled_(i)) continue;
            exec_guard_ctx("strategy.on_session_end", i, slots_[i].ctx->strategy_id(),
                           strategy_exception_total_,
                           [&]() { slots_[i].ctx->on_session_end(td); });
        }
    };

    if (xt_core::is_enabled(session_)) {

        const bool in = xt_core::in_session_closed(action_time, session_.sections);
        if (!in) {
            return false;
        }

        if (last_action_date_ == 0) {
            last_action_date_ = session_date;
            if (session_date != 0) {
                enter_new_session_(session_date, prev_tick);
            }
            return true;
        }
        if (session_date != 0 && session_date != last_action_date_) {
            fan_session_end(last_action_date_);
            enter_new_session_(session_date, prev_tick);
            last_action_date_ = session_date;
        }
        return true;
    }

    if (last_action_date_ == 0) {
        last_action_date_ = session_date;
        if (last_action_date_ != 0) {
            enter_new_session_(last_action_date_, prev_tick);
        }
        return true;
    }

    if (session_date != 0 && session_date != last_action_date_) {
        fan_session_end(last_action_date_);
        enter_new_session_(session_date, prev_tick);
        last_action_date_ = session_date;
    }
    return true;
}

void XtHftEngine::enter_new_session_(uint32_t trading_date, const XtTick& prev_tick) {
    pre_warm_snapshot_(prev_tick);
    trading_date_ = trading_date;
    if (account_store_) account_store_->apply(XtAccountEvent::session_begin());
    gateway_->on_session_begin(trading_date);

    for (std::size_t i = 0; i < slots_.size(); ++i) {
        if (is_ctx_disabled_(i)) continue;
        exec_guard_ctx("strategy.on_session_begin", i, slots_[i].ctx->strategy_id(),
                       strategy_exception_total_,
                       [&]() { slots_[i].ctx->on_session_begin(trading_date); });
    }
}

void XtHftEngine::update_market_state_(const XtTick& tick) {
    if (tick.trading_date) trading_date_ = tick.trading_date;
    else if (tick.action_date) trading_date_ = tick.action_date;
    last_any_core_tick_ = tick;
    has_last_any_core_tick_ = true;

    if (auto* vc = runtime_view_->market_cache()) vc->on_tick(tick);
    sync_runtime_clock(*runtime_view_, has_last_any_core_tick_, last_any_core_tick_, trading_date_);
}

void XtHftEngine::pre_warm_snapshot_(const XtTick& tick) {
    if (tick.trading_date) trading_date_ = tick.trading_date;
    else if (tick.action_date) trading_date_ = tick.action_date;
    last_any_core_tick_ = tick;
    has_last_any_core_tick_ = true;
    if (auto* vc = runtime_view_->market_cache()) vc->update_last_only(tick);
    sync_runtime_clock(*runtime_view_, has_last_any_core_tick_, last_any_core_tick_, trading_date_);
}

void XtHftEngine::check_roll_change_(uint32_t trading_date) {
    if (!roll_.active() || !roll_.has_logical_symbol() || trading_date == last_roll_check_date_) {
        return;
    }
    last_roll_check_date_ = trading_date;
    const auto result = roll_.resolve(trading_date);
    if (!result.active || result.primary_symbol == last_primary_symbol_) {
        return;
    }
    const std::string old_sym = last_primary_symbol_;
    XtLogger::info_m("XtHftEngine",
                     "roll primary change trading_date={} old={} new={}",
                     trading_date,
                     old_sym.empty() ? "<none>" : old_sym.c_str(),
                     result.primary_symbol.c_str());
    last_primary_symbol_ = result.primary_symbol;
    if (!old_sym.empty()) {
        auto* vc = runtime_view_->market_cache();
        const int64_t old_price_fp = vc ? vc->last_price_fp(old_sym.c_str()) : 0;

        if (conditional_orders_enabled_) {
            for (std::size_t i = 0; i < slots_.size(); ++i) {
                if (is_ctx_disabled_(i)) continue;
                (void)slots_[i].ctx->clear_stops_for_code(old_sym.c_str());
            }
        }

        for (std::size_t i = 0; i < slots_.size(); ++i) {
            if (is_ctx_disabled_(i)) continue;
            exec_guard_ctx("strategy.on_roll", i, slots_[i].ctx->strategy_id(),
                           strategy_exception_total_, [&]() {
                slots_[i].ctx->on_roll(old_sym.c_str(),
                                      result.primary_symbol.c_str(),
                                      trading_date, old_price_fp);
            });
        }
    }
}

void XtHftEngine::on_order_queue(const XtOrderQueue& oq) {
    if (!started_) { note_pre_start_drop_("on_order_queue"); return; }
    assert(!slots_.empty());
    if (auto* vc = runtime_view_->market_cache()) vc->on_order_queue(oq);
    const std::size_t ctx_n = slots_.size();
    for (std::size_t i = 0; i < ctx_n; ++i) {
        if (is_ctx_disabled_(i)) continue;
        exec_guard_ctx("strategy.on_order_queue", i, slots_[i].ctx->strategy_id(),
                       strategy_exception_total_,
                       [&]() { slots_[i].ctx->on_order_queue(oq); });
    }
}

void XtHftEngine::on_order_detail(const XtOrderDetail& od) {
    if (!started_) { note_pre_start_drop_("on_order_detail"); return; }
    assert(!slots_.empty());
    if (auto* vc = runtime_view_->market_cache()) vc->on_order_detail(od);
    const std::size_t ctx_n = slots_.size();
    for (std::size_t i = 0; i < ctx_n; ++i) {
        if (is_ctx_disabled_(i)) continue;
        exec_guard_ctx("strategy.on_order_detail", i, slots_[i].ctx->strategy_id(),
                       strategy_exception_total_,
                       [&]() { slots_[i].ctx->on_order_detail(od); });
    }
}

void XtHftEngine::on_transaction(const XtTransaction& tr) {
    if (!started_) { note_pre_start_drop_("on_transaction"); return; }
    assert(!slots_.empty());
    if (auto* vc = runtime_view_->market_cache()) vc->on_transaction(tr);
    const std::size_t ctx_n = slots_.size();
    for (std::size_t i = 0; i < ctx_n; ++i) {
        if (is_ctx_disabled_(i)) continue;
        exec_guard_ctx("strategy.on_transaction", i, slots_[i].ctx->strategy_id(),
                       strategy_exception_total_,
                       [&]() { slots_[i].ctx->on_transaction(tr); });
    }
}

void XtHftEngine::on_channel_ready(const char* channel) {
    if (!started_) { note_pre_start_drop_("on_channel_ready"); return; }
    for (std::size_t i = 0; i < slots_.size(); ++i) {
        if (is_ctx_disabled_(i)) continue;
        exec_guard_ctx("strategy.on_channel_ready", i, slots_[i].ctx->strategy_id(),
                       strategy_exception_total_,
                       [&]() { slots_[i].ctx->on_channel_ready(channel); });
    }

    for (auto* o : observers_) o->on_channel_ready(channel);
}

void XtHftEngine::on_channel_lost(const char* channel, int reason) {
    if (!started_) { note_pre_start_drop_("on_channel_lost"); return; }
    for (std::size_t i = 0; i < slots_.size(); ++i) {
        if (is_ctx_disabled_(i)) continue;
        exec_guard_ctx("strategy.on_channel_lost", i, slots_[i].ctx->strategy_id(),
                       strategy_exception_total_,
                       [&]() { slots_[i].ctx->on_channel_lost(channel, reason); });
    }

    for (auto* o : observers_) o->on_channel_lost(channel, reason);
}

void XtHftEngine::on_params_updated(uint64_t param_version) {
    if (!started_) { note_pre_start_drop_("on_params_updated"); return; }
    for (std::size_t i = 0; i < slots_.size(); ++i) {
        if (is_ctx_disabled_(i)) continue;
        exec_guard_ctx("strategy.on_params_updated", i, slots_[i].ctx->strategy_id(),
                       strategy_exception_total_,
                       [&]() { slots_[i].ctx->on_params_updated(param_version); });
    }

    if (on_params_extra_) on_params_extra_(param_version);
}

bool XtHftEngine::on_trade(const XtTradeRecord& trade, ITradeGateway::TdState td_state,
                           int64_t* out_realized_fp, std::string* out_effective_sid,
                           XtLatencyEligibility elig) {
    if (!started_) { note_pre_start_drop_("on_trade"); return true; }

    if (td_state == ITradeGateway::TdState::Recovering) return true;

    XtTradeRecord tr = trade;
    tr.local_strategy_us = XtSteadyClock::steady_now_us();

    recover_missing_fill_sid_(tr);

    if (out_effective_sid) *out_effective_sid = tr.strategy_id;

    if (order_book_) {
        order_book_->on_trade(tr);
        order_book_->track_code(tr.code.c_str());

        if (gateway_ && order_book_->terminal_order_seen(tr.order_id))
            gateway_->notify_order_terminal(tr.order_id);
    }

    const FillPositionBefore pos_before =
        capture_fill_position_before_(tr, out_realized_fp != nullptr);

    int64_t fill_realized_fp = 0;
    if (account_store_) {
        const int64_t realized_before_fp = account_store_->realized_pnl();
        account_store_->apply(account_event_build::trade_event(tr));
        fill_realized_fp = account_store_->realized_pnl() - realized_before_fp;
    }
    if (out_realized_fp) *out_realized_fp = fill_realized_fp;

    tr.realized_pnl_fp = fill_realized_fp;
    if (fill_realized_cb_ && fill_realized_fp != 0 && !tr.strategy_id.empty()) {
        fill_realized_cb_(tr.strategy_id, tr.code, fill_realized_fp);
    }
    emit_trade_excursion_(tr, pos_before, fill_realized_fp);

    emit_fill_telemetry_(tr, pos_before.qty_fp, pos_before.avg_price_fp);

    uint64_t logical_order_id = 0;
    if (!dispatch_fill_to_strategy_(tr, &logical_order_id)) {
        return false;
    }

    for (auto* o : observers_) o->on_trade(tr, elig, logical_order_id);
    return true;
}

void XtHftEngine::emit_fill_telemetry_(const XtTradeRecord& tr,
                                       int64_t pos_before_qty_fp,
                                       int64_t pos_before_avg_fp) {

    if (tr.sid_id == xt_identity::StrategySidRegistry::kManualSidId) return;

    if (!lifecycle_.any_enabled()) return;

    if (tr.strategy_id.empty() || !account_store_) return;

    const auto pos_after = account_store_->snapshot_position(tr.code);
    lifecycle_.on_fill(tr, tr.strategy_id, pos_before_qty_fp, pos_before_avg_fp,
                       pos_after.qty, pos_after.avg_price);
}

XtHftEngine::FillPositionBefore XtHftEngine::capture_fill_position_before_(
    const XtTradeRecord& tr,
    bool need_realized_output) const {
    FillPositionBefore before;
    if (!account_store_ || !(lifecycle_.any_enabled() ||
                             fill_realized_cb_ || need_realized_output ||
                             trade_excursion_cb_)) {
        return before;
    }

    const auto pos = account_store_->snapshot_position(tr.code);
    before.qty_fp = pos.qty;
    before.avg_price_fp = pos.avg_price;
    return before;
}

void XtHftEngine::emit_trade_excursion_(const XtTradeRecord& tr,
                                        const FillPositionBefore& pos_before,
                                        int64_t fill_realized_fp) {

    if (!trade_excursion_cb_ || !account_store_) return;

    const auto pos_after = account_store_->snapshot_position(tr.code);
    trade_excursion_cb_(tr,
                        pos_before.qty_fp,
                        pos_after.qty,
                        pos_after.avg_price,
                        fill_realized_fp);
}

bool XtHftEngine::dispatch_fill_to_strategy_(const XtTradeRecord& tr,
                                             uint64_t* out_logical_order_id) {

    uint32_t sid_id = tr.sid_id;
    if (sid_id == xt_identity::StrategySidRegistry::kNoSidId) {
        sid_id = resolve_fill_sid_id_(tr.strategy_id);
    }

    if (sid_id != xt_identity::StrategySidRegistry::kManualSidId) {
        exec_dispatch_count_.fetch_add(1, std::memory_order_relaxed);
    }

    bool skip_unmatched = false;
    auto* sink = select_routing_sink_(sid_id, skip_unmatched);
    if (skip_unmatched) return handle_unmatched_fill_(tr);
    if (!sink) return true;

    XtFillEvent fe = build_strategy_fill_event_(tr);

    if (out_logical_order_id) *out_logical_order_id = fe.order_id;
    return exec_guard("strategy.on_fill", strategy_exception_total_, [&]() {
        sink->on_fill(fe);
    });
}

uint32_t XtHftEngine::resolve_fill_sid_id_(const std::string& strategy_id) const {
    if (strategy_id.empty()) return xt_identity::StrategySidRegistry::kNoSidId;
    if (auto* reg = xt_identity::StrategySidRegistry::global()) {
        return reg->lookup(strategy_id);
    }
    return xt_identity::StrategySidRegistry::kNoSidId;
}

bool XtHftEngine::handle_unmatched_fill_(const XtTradeRecord& tr) {

    const bool fill_no_attribution =
        tr.strategy_id.empty() && tr.sid_id == xt_identity::StrategySidRegistry::kNoSidId;
    (fill_no_attribution ? dropped_fill_empty_sid_count_ : dropped_fill_unknown_sid_count_)
        .fetch_add(1, std::memory_order_relaxed);
    if (unmatched_routing_ != UnmatchedRoutingPolicy::WarnAndDrop) {

        XtLogger::error_m("XtHftEngine",
                          "on_trade: STRICT unmatched fill: sid='{}' order_id={} code={} contexts={}",
                          tr.strategy_id.c_str(),
                          static_cast<unsigned long long>(tr.order_id),
                          tr.code.c_str(),
                          slots_.size());
        if (unmatched_routing_ == UnmatchedRoutingPolicy::DropAndAlert) {
            XtLogger::warn_m("XtHftEngine",
                             "on_trade: unattributed live fill DROPPED (sid forgotten, likely cancel/fill race); total={}; snapshot/reconcile backstops position",
                             static_cast<unsigned long long>(
                                 dropped_fill_count()));
            return true;
        }
        throw std::runtime_error(
            "XtHftEngine::on_trade strict_unmatched_routing — sid='" + tr.strategy_id +
            "' order_id=" + std::to_string(tr.order_id) +
            " code=" + tr.code + " 未命中 contexts");
    }
    XtLogger::warn_m("XtHftEngine",
                     "on_trade: multi-mode strategy_id unmatched: sid='{}' order_id={} code={} — fill dropped (total={})",
                     tr.strategy_id.c_str(),
                     static_cast<unsigned long long>(tr.order_id),
                     tr.code.c_str(),
                     static_cast<unsigned long long>(
                         dropped_fill_count()));
    return true;
}

XtFillEvent XtHftEngine::build_strategy_fill_event_(const XtTradeRecord& tr) const {
    XtFillEvent fe = tr.to_fill_event();
    if (order_id_translator_) {
        const uint64_t clord = order_id_translator_->lookup_clord(fe.order_id);
        if (clord != 0) fe.order_id = clord;
    }
    return fe;
}

bool XtHftEngine::on_order_update(const XtOrderExecUpdate& upd, XtLatencyEligibility elig) {
    if (!started_) { note_pre_start_drop_("on_order_update"); return true; }
    XtOrderExecUpdate applied_upd = upd;
    const bool is_terminal = order_status_is_final(upd.status);

    uint32_t stop_sid_hint = 0;
    if (conditional_orders_enabled_ && slots_.size() > 1 && order_book_ &&
        upd.sid_id == xt_identity::StrategySidRegistry::kNoSidId) {
        stop_sid_hint = order_book_->order_sid_id(upd.id);
    }

    apply_order_update_state_(applied_upd, is_terminal);

    bool terminal_cleanup_done = false;
    auto terminal_cleanup_guard = make_scope_guard([&]() noexcept {
        if (!is_terminal || terminal_cleanup_done) return;
        try { finish_terminal_order_update_(applied_upd); } catch (...) {}
    });

    XtOrderExecUpdate strategy_upd = build_strategy_order_update_(applied_upd);

    if (conditional_orders_enabled_) notify_stops_for_update_(strategy_upd, stop_sid_hint);
    if (!dispatch_order_update_to_strategy_(strategy_upd)) return false;

    for (auto* o : observers_) o->on_order_update(strategy_upd, elig);

    if (is_terminal) {
        finish_terminal_order_update_(applied_upd);
        terminal_cleanup_done = true;
    }
    return true;
}

void XtHftEngine::apply_order_update_state_(const XtOrderExecUpdate& applied_upd,
                                            bool is_terminal) {
    if (order_book_) {
        const int64_t effective_remaining_fp = order_lifecycle::effective_remaining_fp(applied_upd);
        order_book_->on_order_update(applied_upd, is_terminal, effective_remaining_fp);
    }

    if (account_store_) {
        account_store_->apply(account_event_build::order_event(applied_upd));
    }
}

XtOrderExecUpdate XtHftEngine::build_strategy_order_update_(
    const XtOrderExecUpdate& applied_upd) const {
    XtOrderExecUpdate strategy_upd = applied_upd;
    if (order_id_translator_) order_id_translator_->translate(applied_upd, strategy_upd);

    strategy_upd.local_strategy_us = XtSteadyClock::steady_now_us();
    return strategy_upd;
}

bool XtHftEngine::dispatch_order_update_to_strategy_(const XtOrderExecUpdate& strategy_upd) {

    if (strategy_upd.sid_id != xt_identity::StrategySidRegistry::kManualSidId) {
        exec_dispatch_count_.fetch_add(1, std::memory_order_relaxed);
    }
    bool order_skip_unmatched = false;
    auto* order_sink = select_routing_sink_(strategy_upd.sid_id, order_skip_unmatched);
    if (order_skip_unmatched) return handle_unmatched_order_update_(strategy_upd);
    if (!order_sink) return true;

    return exec_guard("strategy.on_order", strategy_exception_total_, [&]() {
        order_sink->on_order(strategy_upd);
    });
}

bool XtHftEngine::handle_unmatched_order_update_(const XtOrderExecUpdate& strategy_upd) {

    const bool upd_no_attribution =
        strategy_upd.sid_id == xt_identity::StrategySidRegistry::kNoSidId;
    (upd_no_attribution ? dropped_order_update_empty_sid_count_
                        : dropped_order_update_unknown_sid_count_)
        .fetch_add(1, std::memory_order_relaxed);
    const char* upd_sid_disp =
        xt_identity::StrategySidRegistry::resolve_sid_cstr_or_null(strategy_upd.sid_id);
    if (!upd_sid_disp) upd_sid_disp = "";
    if (unmatched_routing_ != UnmatchedRoutingPolicy::WarnAndDrop) {
        XtLogger::error_m("XtHftEngine",
                          "on_order_update: STRICT unmatched order rtn: sid='{}' order_id={} contexts={}",
                          upd_sid_disp,
                          static_cast<unsigned long long>(strategy_upd.id),
                          slots_.size());
        if (unmatched_routing_ == UnmatchedRoutingPolicy::DropAndAlert) {
            XtLogger::warn_m("XtHftEngine",
                             "on_order_update: unattributed live order rtn DROPPED (sid forgotten, likely cancel/fill race); total={}; reconcile backstops",
                             static_cast<unsigned long long>(
                                 dropped_order_update_count()));
            return true;
        }
        throw std::runtime_error(
            "XtHftEngine::on_order_update strict_unmatched_routing — sid='" +
            std::string(upd_sid_disp) +
            "' order_id=" + std::to_string(strategy_upd.id) + " 未命中 contexts");
    }

    XtLogger::warn_m("XtHftEngine",
                     "on_order_update: multi-mode strategy_id unmatched: sid='{}' order_id={} — order rtn dropped (total={})",
                     upd_sid_disp,
                     static_cast<unsigned long long>(strategy_upd.id),
                     static_cast<unsigned long long>(
                         dropped_order_update_count()));
    return true;
}

void XtHftEngine::finish_terminal_order_update_(const XtOrderExecUpdate& applied_upd) {

    if (applied_upd.sid_id == xt_identity::StrategySidRegistry::kManualSidId) {
        XtLogger::info_m("XtHftEngine",
                         "panel.manual_order terminal: order_id={} status={}",
                         static_cast<unsigned long long>(applied_upd.id),
                         static_cast<int>(applied_upd.status));
    }
    if (order_id_translator_) order_id_translator_->cleanup_done();

    gateway_->notify_order_terminal(applied_upd.id);
    gateway_->record_order_terminal(applied_upd.status);
    gateway_->erase_order_tracking(applied_upd.id);
}

void XtHftEngine::on_position_snapshot(
    const std::vector<ITradeGateway::PositionSnapshot>& positions,
    ITradeGateway::TdState td_state,
    bool is_cold_start,
    uint32_t session_gen)
{
    if (!started_) { note_pre_start_drop_("on_position_snapshot"); return; }
    const bool is_recovery = (td_state == ITradeGateway::TdState::Recovering);

    for (auto* o : observers_) o->on_position_snapshot_begin(is_recovery);

    const bool position_applied = (account_store_ && catalog_);
    if (position_applied) {
        account_store_->on_position_snapshot(positions, is_recovery, *catalog_);
    }

    std::vector<std::string> position_codes;
    position_codes.reserve(positions.size());
    for (const auto& p : positions) {
        if (!p.code.empty()) position_codes.push_back(p.code);
    }

    std::vector<std::string> stale_codes;
    if (order_book_) {
        stale_codes = order_book_->sync_position_snapshot(position_codes, is_recovery, is_cold_start);
        if (account_store_) {
            for (const auto& code : stale_codes) {
                account_store_->apply(XtAccountEvent::set_position(
                    code.c_str(),
                    xt_fp::to_fp(0.0), xt_fp::to_fp(0.0),
                    xt_fp::to_fp(0.0), xt_fp::to_fp(0.0),
                    account_store_->snapshot_position(code).mark_price,
                    xt_fp::to_fp(0.0), xt_fp::to_fp(0.0)));
            }
        }
    }

    if (lifecycle_.want_resume(is_recovery)) {
        emit_resume_for_recovery_(positions);
    }
    lifecycle_.mark_snapshot_seen();

    for (auto* o : observers_) o->on_position_snapshot_end(is_recovery, position_applied, session_gen);
}

void XtHftEngine::on_reverse_sync_batch(
    const std::vector<AliveInflightOrder>& orders,
    bool is_cold_start)
{
    if (!started_) { note_pre_start_drop_("on_reverse_sync_batch"); return; }

    (void)is_cold_start;

    if (!order_book_ || !account_store_) return;

    auto view_for = [this](const std::string& code, bool is_buy_to_close) {
        return xt_account::closeable_view(
            account_store_->snapshot_position(code),
            is_buy_to_close);
    };

    std::vector<xt_runtime::order::AcceptedRearmOrder> accepted;
    order_book_->fillup_alive_inflight_orders_batch(orders, view_for, &accepted);

    if (gateway_)
        for (const auto& a : accepted)
            gateway_->rearm_active_order(a.code, a.id, a.is_buy, a.price_fp);

    last_reverse_sync_steady_ms_ =
        static_cast<int64_t>(XtSteadyClock::steady_now_ms());
}

size_t XtHftEngine::reap_recovered_orphans(int64_t now_ms,
                                           int64_t grace_ms,
                                           size_t max_cancels) {

    if (!started_) { note_pre_start_drop_("reap_recovered_orphans"); return 0; }
    if (!order_book_) return 0;

    order_book_->sweep_stale_pre_registration();
    if (!gateway_ || max_cancels == 0) return 0;

    if (last_reverse_sync_steady_ms_ == 0 ||
        now_ms - last_reverse_sync_steady_ms_ < grace_ms) {
        return 0;
    }

    if (xt_identity::OrderAttributionStore::order_sid_journal_degraded()) {
        const uint64_t suppressed =
            orphan_reap_suppressed_total_.fetch_add(1, std::memory_order_relaxed) + 1;

        constexpr uint64_t kLogEvery = 128;
        if (suppressed == 1 || (suppressed % kLogEvery) == 0) {
            XtLogger::critical(
                "[RT] orphan reap 已切告警-only：归属 journal 持久化降级"
                "（journal_failures={}），recovered-unowned 单保留不撤，需 operator 处置"
                "（suppressed_sweeps={}）",
                xt_identity::OrderAttributionStore::order_sid_journal_failure_count(),
                suppressed);
        }
        return 0;
    }

    std::array<uint64_t, 256> ids{};

    std::vector<uint64_t> reaped;
    const size_t n = order_book_->reap_recovered_unowned(
        ids.data(), std::min(max_cancels, ids.size()), &reaped);

    for (uint64_t id : reaped) gateway_->notify_order_terminal(id);
    if (n == 0) return 0;

    size_t issued = 0;
    for (size_t i = 0; i < n; ++i) {
        if (gateway_->cancel(ids[i])) ++issued;
    }
    orphan_reap_cancel_total_.fetch_add(issued, std::memory_order_relaxed);
    XtLogger::warn_m("RT",
        "orphan reap: cancel recovered-unowned orders issued={}/{} "
        "(cross-restart 遗留，防 venue max-open-order 上限)",
        issued, n);
    return issued;
}

size_t XtHftEngine::reconcile_lost_terminals(size_t max) {

    if (!started_) { note_pre_start_drop_("reconcile_lost_terminals"); return 0; }
    if (!order_book_ || !gateway_ || max == 0) return 0;

    std::array<XtOrderExecUpdate, xt_runtime::order::kOrderSnapshotMaxOrders> open{};
    const size_t n = order_book_->collect_open_orders(open.data(), open.size());
    size_t healed = 0;
    for (size_t i = 0; i < n && healed < max; ++i) {
        const uint64_t id = open[i].id;
        if (id == 0) continue;

        XtOrderTerminalRecord rec{};
        const bool has_payload = gateway_->lookup_terminal(id, rec);
        if (!has_payload && !gateway_->is_order_terminal(id)) continue;

        XtOrderExecUpdate term = open[i];
        if (has_payload) {
            term.status = rec.status;
            if (rec.status == XtOrderStatus::Filled) {
                term.filled_fp = (rec.filled_fp > 0 && rec.filled_fp <= term.qty_fp)
                                     ? rec.filled_fp
                                     : term.qty_fp;
                term.leaves_fp = 0;
            } else if (rec.filled_fp >= 0) {
                term.filled_fp = std::min<int64_t>(rec.filled_fp, term.qty_fp);
                term.leaves_fp = std::max<int64_t>(0, term.qty_fp - term.filled_fp);
            }

        } else {
            term.status = XtOrderStatus::Canceled;
        }

        on_order_update(term, XtLatencyEligibility::kIneligible);
        ++healed;
    }
    if (healed > 0) {
        lost_terminal_reconcile_total_.fetch_add(healed, std::memory_order_relaxed);
        XtLogger::warn_m("RT",
            "lost-terminal reconcile: synthesized {} terminal(s) for book-open venue-terminal "
            "orders (B4 phantom self-heal)", healed);
    }
    return healed;
}

uint32_t XtHftEngine::open_order_count() const noexcept {

    return order_book_
        ? static_cast<uint32_t>(order_book_->open_order_count())
        : 0;
}

void XtHftEngine::emit_resume_for_recovery_(
    const std::vector<ITradeGateway::PositionSnapshot>& positions)
{

    std::vector<xt_engine::hft::HftLifecycleTelemetry::ResumeEntry> entries;
    if (declared_registry_ && !slots_.empty()) {

        for (std::size_t i = 0; i < slots_.size(); ++i) {
            const std::string& sid = slots_[i].ctx->strategy_id();
            if (sid.empty()) continue;
            const auto declared = declared_registry_->declared_symbols(sid);
            for (const auto& p : positions) {
                if (p.code.empty() || !declared.count(p.code)) continue;
                const auto pm = account_store_
                    ? account_store_->snapshot_position(p.code)
                    : xt_account::PositionSnapshot{};
                if (pm.qty == 0) continue;
                entries.push_back({sid, p.code, pm.qty, pm.avg_price});
            }
        }
    } else if (!slots_.empty()) {

        const std::string& sid = slots_[0].ctx->strategy_id();
        if (sid.empty()) return;
        for (const auto& p : positions) {
            if (p.code.empty()) continue;
            const auto pm = account_store_
                ? account_store_->snapshot_position(p.code)
                : xt_account::PositionSnapshot{};
            if (pm.qty == 0) continue;
            entries.push_back({sid, p.code, pm.qty, pm.avg_price});
        }
    }

    lifecycle_.on_resume(entries);
}

void XtHftEngine::on_account_snapshot(const ITradeGateway::AccountSnapshot& snap) {
    if (!started_) { note_pre_start_drop_("on_account_snapshot"); return; }
    if (!account_store_) return;

    if (std::isfinite(snap.balance)) {
        const int64_t broker_cash_fp = xt_fp::to_fp(snap.balance);

        if (cash_anchored_) {
            if (cash_reanchor_probe_) {
                const auto pre = account_store_->snapshot_account();
                cash_reanchor_probe_(broker_cash_fp - pre.cash);
            }
        } else {
            cash_anchored_ = true;
        }
        account_store_->apply(XtAccountEvent::set_cash(broker_cash_fp));
    } else {
        XtLogger::error_m(
            "RT",
            "ignore invalid account snapshot balance={} available={} margin={} close_profit={} position_profit={} commission={}",
            snap.balance,
            snap.available,
            snap.margin,
            snap.close_profit,
            snap.position_profit,
            snap.commission);
    }

    const auto am = account_store_->snapshot_account();

    const XtAccountUpdate acct_upd{am.cash, am.equity, am.unrealized_pnl};

    for (std::size_t i = 0; i < slots_.size(); ++i) {
        auto* b = sink_at_(i);
        if (!b || is_ctx_disabled_(i)) continue;
        exec_guard("strategy.on_account",
                   strategy_exception_total_, [&]() { b->on_account(acct_upd); });
    }

    for (auto* o : observers_) o->on_account_snapshot(snap, am);
}

void XtHftEngine::on_daily_settlement(const ITradeGateway::DailySettlementDelta& delta) {
    if (!started_) { note_pre_start_drop_("on_daily_settlement"); return; }
    if (!account_store_) return;
    account_store_->apply(XtAccountEvent::daily_settlement(xt_fp::to_fp(delta.realized_delta),
                                                           xt_fp::to_fp(delta.fee)));
}

void XtHftEngine::on_cash_transfer(const ITradeGateway::CashTransferDelta& delta) {
    if (!started_) { note_pre_start_drop_("on_cash_transfer"); return; }
    if (!account_store_) return;
    account_store_->apply(XtAccountEvent::cash_transfer(xt_fp::to_fp(delta.amount)));
}

std::size_t XtHftEngine::find_ctx_idx_by_sid_id_(uint32_t sid_id) const noexcept {
    for (std::size_t i = 0; i < slots_.size(); ++i) {
        if (slots_[i].ctx && slots_[i].ctx->resolved_sid_id() == sid_id) return i;
    }
    return slots_.size();
}

xt_strategy_engine::IStrategyExecSink* XtHftEngine::sink_at_(std::size_t i) const noexcept {
    return i < slots_.size()
        ? static_cast<xt_strategy_engine::IStrategyExecSink*>(slots_[i].ctx.get())
        : nullptr;
}

xt_strategy_engine::IStrategyExecSink* XtHftEngine::select_routing_sink_(uint32_t sid_id,
                                                   bool& skip_unmatched) const noexcept {
    skip_unmatched = false;

    if (sid_id == xt_identity::StrategySidRegistry::kManualSidId) {
        return nullptr;
    }

    if (slots_.size() <= 1) {
        if (is_ctx_disabled_(0)) return nullptr;
        return sink_at_(0);
    }

    if (sid_id == xt_identity::StrategySidRegistry::kNoSidId) {
        skip_unmatched = true;
        return nullptr;
    }
    const std::size_t idx = find_ctx_idx_by_sid_id_(sid_id);
    if (idx >= slots_.size()) {
        skip_unmatched = true;
        return nullptr;
    }
    if (is_ctx_disabled_(idx)) return nullptr;
    return sink_at_(idx);
}

bool XtHftEngine::recover_missing_fill_sid_(XtTradeRecord& tr) {

    if (tr.sid_id != xt_identity::StrategySidRegistry::kNoSidId) {
        if (tr.strategy_id.empty()) {
            if (const char* s = xt_identity::StrategySidRegistry::resolve_sid_cstr_or_null(
                    tr.sid_id)) {
                tr.strategy_id = s;
            }
        }
        return false;
    }
    if (!tr.strategy_id.empty()) return false;

    std::string recovered = effective_sid_(tr.strategy_id, tr.code);
    if (recovered.empty()) return false;

    record_recovered_fill_sid_(tr, recovered);
    tr.strategy_id = std::move(recovered);

    tr.sid_id = resolve_fill_sid_id_(tr.strategy_id);
    return true;
}

void XtHftEngine::record_recovered_fill_sid_(const XtTradeRecord& tr,
                                             const std::string& recovered_sid) {

    unmatched_sid_recovered_count_.fetch_add(1, std::memory_order_relaxed);
    XtLogger::warn_mt("XtHftEngine", "unmatched_sid_recovered",
                      "① recovered sid='{}' for fill order_id={} code={} "
                      "(order→sid forgotten — terminal/fill race); total_recovered={}",
                      recovered_sid.c_str(),
                      static_cast<unsigned long long>(tr.order_id),
                      tr.code.c_str(),
                      static_cast<unsigned long long>(
                          unmatched_sid_recovered_count_.load(std::memory_order_relaxed)));
}

std::string XtHftEngine::effective_sid_(const std::string& sid, const std::string& code) {
    if (!sid.empty()) return sid;

    if (slots_.size() == 1) return slots_[0].ctx->strategy_id();

    if (!declared_registry_ || slots_.size() <= 1 || code.empty()) return sid;
    if (!code_to_sid_built_) build_code_to_sid_();
    auto it = code_to_sid_.find(code);
    if (it != code_to_sid_.end() && !it->second.empty()) return it->second;
    return sid;
}

void XtHftEngine::build_code_to_sid_() {

    code_to_sid_.clear();
    if (!declared_registry_) { code_to_sid_built_ = true; return; }
    std::unordered_set<std::string> ambiguous;
    for (const auto& slot : slots_) {
        if (!slot.ctx) continue;
        const std::string& sid = slot.ctx->strategy_id();
        const auto declared = declared_registry_->declared_symbols(sid);
        for (const auto& code : declared) {
            auto [it, inserted] = code_to_sid_.emplace(code, sid);
            if (!inserted && it->second != sid) ambiguous.insert(code);
        }
    }
    for (const auto& c : ambiguous) code_to_sid_.erase(c);
    code_to_sid_built_ = true;
}
