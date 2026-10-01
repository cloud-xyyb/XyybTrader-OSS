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

// xt_engine 不反向 include xt_live；live-only 依赖通过 api 接口注入。
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

/// @brief 策略回调异常守卫；委托 xt_log::run_guarded，失败由 caller 决定是否停机。
/// @param counter 调用方（engine 实例）自有的异常计数器，异常路径 +1。
template<typename Fn>
bool exec_guard(const char* stage, std::atomic<uint64_t>& counter, Fn&& fn) {
    return xt_log::run_guarded("XtHftEngine", stage, counter, std::forward<Fn>(fn));
}

/// @brief multi-strategy fan-out 中保护单个 context 的 strategy callback。
/// 单个 context 失败不阻断其他 context；caller 聚合返回值。sid/ctx_idx 仅在异常路径记录。
/// @param counter 调用方（engine 实例）自有的异常计数器，异常路径 +1。
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

} // anonymous namespace

// ── Construction and configuration ─────────────────────────────────────

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
        // registry 此刻已 freeze；装配单线程内解析 sid_id，后续回报路由只读 context 缓存值。
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

    // unmatched-sid 恢复日志 60s 节流；按 key 注册幂等，多 engine 实例重复注册无害。
    XtLogger::set_throttle_ms("unmatched_sid_recovered", 60000);
    XtLogger::set_throttle_ms("engine_pre_start_drop", 60000);
}

void XtHftEngine::validate_assembly_(const Options& opts, const Hooks& hooks) const {
    auto fail = [](const std::string& why) {
        throw std::invalid_argument("XtHftEngine assembly: " + why);
    };

    if (slots_.empty()) fail("contexts must not be empty");

    // observer 在 fan-out 热路径上无空/重复检查（每笔回报逐个直调）：空指针 = 解引用
    // 崩溃，重复指针 = 同一 observer 被投两次（report 重复行 / latency 双计）。二者都是
    // 装配错误，装配期 fail-fast。
    // 用 O(n²) 线性扫而非 hash set：生产 observer 数为 1-2（live combo + fee observer /
    // BT reporter fill observer），建 set 的堆分配不值当。
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
        // `_` 前缀是保留域（`_manual` 在 StrategySidRegistry 有内建映射，落到路由入口
        // 会命中 manual 早退 → 该策略全部回报静默丢弃；`_global`/`_default` 同属保留名）。
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
        // sid 未 resolved 的 ctx 收不到任何回报（全部走 unmatched 静默降级）；
        // 装配期 fail-fast 优于运行期静默漂移。
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
    // started_ 入口消费：任何策略回调之前置位，令 partial-start（失败回调逃逸）
    // 后的二次调用同样被拒，杜绝重跑 on_init 副作用。
    if (started_) {
        throw std::logic_error("XtHftEngine::start called more than once");
    }
    started_ = true;

    StartReport report;
    // per-ctx fail-closed：单策略 on_init 抛异常 → disable 该 ctx（后续 fan-out 跳过），
    // 兄弟策略不受影响。exec_guard_ctx noexcept 吞异常并返回 false。
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
    if (slots_[i].disabled) return;  // 幂等
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

// ── Strategy contexts ─────────────────────────────────────────────────

const XtStrategyContext* XtHftEngine::context(std::size_t idx) const noexcept {
    return idx < slots_.size() ? slots_[idx].ctx.get() : nullptr;
}

bool XtHftEngine::set_strategy_control_enabled(const std::string& strategy_id, bool enabled) {
    if (strategy_id.empty()) return false;
    bool matched = false;
    // 不在首个命中处早退：构造期已拒重复 strategy_id，全扫只是同口径的二道防线。
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

// ── Tick and session pipeline ─────────────────────────────────────────

// account_store_ 是 MarketTick 的唯一写入方；gateway 侧不得再写同一 store
// （重复投递同一事件 = 多一次 snapshot publish，apply 一旦不再幂等即成记账错误）。
// 本函数先于 gateway_->process_tick 调用，撮合/爆仓检查因此看得到本 tick 的 mark。
void XtHftEngine::apply_strategy_tick_state_(const XtTick& tick) {
    if (account_store_) {
        account_store_->apply(XtAccountEvent::market_tick(tick.code, tick.price_fp));
    }
}

bool XtHftEngine::prepare_cycle_(XtTick& tick) {
    // 本轮 trading_date 优先使用 exchange 字段；缺失时按夜盘规则和 legacy 字段推断。
    uint32_t trading_date = tick.trading_date;
    if (trading_date == 0 && td_infer_.enabled()) {
        trading_date = td_infer_.infer(tick.action_date, tick.action_time);
        tick.trading_date = trading_date;
    }
    if (trading_date == 0) {
        trading_date = xt_tick_convert::trading_date(tick);
    }
    if (calendar_filter_ && !calendar_filter_(trading_date)) { ++drops_.calendar; return false; }

    // 交易日通过后处理 roll/session；session gate 拒绝则不派发策略、不撮合。
    check_roll_change_(trading_date);

    const XtTick& prev_snapshot = has_last_any_core_tick_ ? last_any_core_tick_ : tick;
    // update_session_state_ 返 false 仅当 session gate（时段外）拒绝 → 计 session。
    if (!update_session_state_(trading_date, tick.action_time, prev_snapshot)) {
        ++drops_.session;
        return false;
    }

    // 当前 tick 先刷新 engine / gateway / account 的可见状态。
    update_market_state_(tick);
    // 双轴：event = (action_date ?: trading_date, action_time)，trading 仅供 report 戳。
    gateway_->advance_clock(tick.action_date != 0 ? tick.action_date
                                                  : xt_tick_convert::trading_date(tick),
                            tick.action_time, xt_tick_convert::trading_date(tick));
    apply_strategy_tick_state_(tick);

    // mark_tick_cb_ 在 mark 状态更新后采样 MAE/MFE；live 默认不注册 callback。
    if (mark_tick_cb_) mark_tick_cb_(tick);
    return true;
}

void XtHftEngine::dispatch_tick_to_contexts_(const XtTick& tick,
                                             const char* stage,
                                             const char* error_message) {
    const std::size_t ctx_n = slots_.size();
    bool dispatch_ok = true;
    for (std::size_t i = 0; i < ctx_n; ++i) {
        if (is_ctx_disabled_(i)) continue;  // fail-closed：disabled 策略不收 tick、不下单。
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
        if (is_ctx_disabled_(i)) continue;  // fail-closed：disabled 策略的 stop 不触发。
        if (!slots_[i].ctx->has_armed_stops()) continue;
        // 单策略 stop 判定/触发异常不阻断本 tick 其他策略与撮合推进。
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
    // stamp-miss（sid_id==0）时用 book slot 反查的 hint 选 ctx（T2.STOP-1：
    // 否则 Fired slot 永不收到终态，滞留到 64/32 封顶）；hint 只参与 stop 路由，
    // 不回写 upd（order rtn 归因主链不受影响）。
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
    // thread_local 只复用容量；每轮 drain 前清空 size，避免重复消费上一 tick 的 updates。
    updates.clear();
    gateway_->drain_order_updates(updates);
    for (auto& upd : updates) on_order_update(upd);
}

void XtHftEngine::run_tick_cycle_(XtTick tick) {
    assert(!slots_.empty() && gateway_);

    if (!prepare_cycle_(tick)) return;

    // fill 可见性由 backtest clock/drain_fills 控制；本 cycle 只同步订单状态更新。
    gateway_->set_dispatch_clock(xt_tick_convert::trading_date(tick),
                                 tick.action_time);
    // 条件单先于策略新意图触发；触发单与策略单同批进本 tick process_tick 撮合。
    if (conditional_orders_enabled_) check_conditional_stops_(tick);
    dispatch_tick_to_contexts_(tick,
                               "strategy.dispatch_tick",
                               "XtHftEngine::run_tick_cycle strategy dispatch failed");
    gateway_->process_tick(tick);
    drain_order_updates_();
}

uint32_t XtHftEngine::infer_trading_date_(const XtTick& tick) const {
    // 数据入口已填 trading_date 时直接使用，避免跨假期推断链不对称。
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
    if (!roll_accepts_tick_(tick)) { ++drops_.roll; return; }  // drop 分支计数（accepted 路零改）。

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
    if (!is_preopen) { ++drops_.session; return; }  // pre-session 非 preopen = session 丢弃。

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
    // 全局 session_begin 副作用只执行一次；strategy callback 对所有 context fan-out。
    // 逐 ctx 守卫：单策略异常不中断兄弟策略的 session 边界回调（对齐 CTA 引擎）。
    auto fan_session_end = [this](uint32_t td) {
        for (std::size_t i = 0; i < slots_.size(); ++i) {
            if (is_ctx_disabled_(i)) continue;
            exec_guard_ctx("strategy.on_session_end", i, slots_[i].ctx->strategy_id(),
                           strategy_exception_total_,
                           [&]() { slots_[i].ctx->on_session_end(td); });
        }
    };

    // 启用 session 模式：时段过滤 + 换日检测。
    if (xt_core::is_enabled(session_)) {
        // 使用闭区间，保留 section 末 auction tick。
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

    // 兜底模式：会话边界仅由交易日变化触发。
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
    // 逐 ctx 守卫：单策略异常不中断兄弟策略的 session begin 回调。
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
    // market_cache 统一走 runtime_view，engine 不持私有副本。
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
        // 条件单 clear 必须先于 on_roll fan-out：策略在 on_roll 里重挂的 stops
        // 不能被本次清仓 wipe。HFT book 键 = 物理合约，清旧主力。
        if (conditional_orders_enabled_) {
            for (std::size_t i = 0; i < slots_.size(); ++i) {
                if (is_ctx_disabled_(i)) continue;
                (void)slots_[i].ctx->clear_stops_for_code(old_sym.c_str());
            }
        }
        // 每个 strategy 自行决策平旧/开新仓；逐 ctx 守卫防单策略异常中断换月。
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

// ── Orderflow and control fan-out ─────────────────────────────────────

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
    // observer fan（mirror on_trade/on_position 的 bare loop）：live observer 用它把
    // readiness latch 置位收成 worker 单写状态机（td 通道 up + apply 配对）。
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
    // observer fan：live observer 在 worker 顺序清 readiness latch（排在 stale snapshot
    // 之后，根治 disconnect 后旧快照 re-latch）。
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
    // Runner 用于应用非策略热更新；engine 不直接依赖 risk/live 组件。
    if (on_params_extra_) on_params_extra_(param_version);
}

// ── Execution events and fill telemetry ───────────────────────────────

bool XtHftEngine::on_trade(const XtTradeRecord& trade, ITradeGateway::TdState td_state,
                           int64_t* out_realized_fp, std::string* out_effective_sid,
                           XtLatencyEligibility elig) {
    if (!started_) { note_pre_start_drop_("on_trade"); return true; }
    // Recovering 阶段不进入 fill 主链路，避免 replay 触发 account/strategy/observer。
    if (td_state == ITradeGateway::TdState::Recovering) return true;

    // 本地副本承载 engine 补齐字段；输入 trade 保持只读。
    XtTradeRecord tr = trade;
    tr.local_strategy_us = XtSteadyClock::steady_now_us();

    // sid fallback 只补齐可恢复归因；仍为空的 fill 后续走 unmatched 诊断路径。
    // 副作用披露：recover_missing_fill_sid_ 就地补 tr（stamp 反解补串 / 单 ctx 启发补
    // sid+串）；stale/越域 stamp 反解失败保持未归属，下游单 context 仍投唯一 sink（既有
    // 语义），aggregator 侧空串归 `_default` 桶（既有归一，非本批引入）。
    recover_missing_fill_sid_(tr);

    // 把恢复后的 strategy_id 回传给 caller；不可归因时为空。
    if (out_effective_sid) *out_effective_sid = tr.strategy_id;

    // book 先收敛挂单状态；account reducer 和策略回调都在其后发生。
    if (order_book_) {
        order_book_->on_trade(tr);
        order_book_->track_code(tr.code.c_str());
        // trade-first 全成：book 已 erase slot（venue 仅 Trade 无后续终态 update 时，
        // 这是唯一的 STP 边界释放点）→ 按 id 释放。仅真终态时调（partial 不触发）；
        // 与终态 update 路径重复调用幂等无害。
        if (gateway_ && order_book_->terminal_order_seen(tr.order_id))
            gateway_->notify_order_terminal(tr.order_id);
    }

    // account_store apply 会覆盖 post-fill 仓位；before-state 必须在 reducer 前采样。
    const FillPositionBefore pos_before =
        capture_fill_position_before_(tr, out_realized_fp != nullptr);

    // 本 trade 的 realized 取 account_store_ apply 前后累计 realized 差值，账本规则由 account_store_ 决定。
    int64_t fill_realized_fp = 0;
    if (account_store_) {
        const int64_t realized_before_fp = account_store_->realized_pnl();
        account_store_->apply(account_event_build::trade_event(tr));
        fill_realized_fp = account_store_->realized_pnl() - realized_before_fp;
    }
    if (out_realized_fp) *out_realized_fp = fill_realized_fp;

    // 把本 trade 的 realized 写回 tr，后续回调直接使用 account_store_ 的结果。
    tr.realized_pnl_fp = fill_realized_fp;
    if (fill_realized_cb_ && fill_realized_fp != 0 && !tr.strategy_id.empty()) {
        fill_realized_cb_(tr.strategy_id, tr.code, fill_realized_fp);
    }
    emit_trade_excursion_(tr, pos_before, fill_realized_fp);

    emit_fill_telemetry_(tr, pos_before.qty_fp, pos_before.avg_price_fp);

    // L-2 fill-link：dispatch 经 out-param 返回本次已算出的 logical order id（matched
    // sink 恰好一次 lookup）；非命中路径返回 0，observer 回退用 tr 原 order_id。
    uint64_t logical_order_id = 0;
    if (!dispatch_fill_to_strategy_(tr, &logical_order_id)) {
        return false;
    }

    // strategy dispatch 成功后，observer 接收已补齐 sid/realized 的 trade record；
    // elig gate latency tracker feed，logical id 供 latency observer 键域配对。
    for (auto* o : observers_) o->on_trade(tr, elig, logical_order_id);
    return true;
}

void XtHftEngine::emit_fill_telemetry_(const XtTradeRecord& tr,
                                       int64_t pos_before_qty_fp,
                                       int64_t pos_before_avg_fp) {
    // R2 D9：操作面手工单（保留 sid）不进 lifecycle —— lifecycle before/after 是账户级
    // 净仓，不主张手工单的持仓配对语义（spec D9「不进 lifecycle」）。置于 strategy_id
    // 判空之前，保留名反解补串（非空）也不误入 open/close。
    if (tr.sid_id == xt_identity::StrategySidRegistry::kManualSidId) return;
    // Telemetry 未接线时直接跳过，backtest 热路径无额外成本。
    if (!lifecycle_.any_enabled()) return;
    // strategy_id 归因已在 on_trade 入口恢复；仍无归属的 foreign fill 不发 telemetry。
    if (tr.strategy_id.empty() || !account_store_) return;
    // engine 采样仓位后传入 lifecycle_；lifecycle_ 不直接访问 account_store_。
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
    // 回测 MAE/MFE 用 before/after 净仓分类；realized 使用 account_store_ 前后差。
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
    // gateway 预 stamp 的 sid_id 优先（与 order_update 路由对称；同源 frozen registry
    // 保证命中时与 string lookup 等值）；0 时回退 string 解析，覆盖恢复链产物与
    // 未 stamp 的旁路 fill。
    uint32_t sid_id = tr.sid_id;
    if (sid_id == xt_identity::StrategySidRegistry::kNoSidId) {
        sid_id = resolve_fill_sid_id_(tr.strategy_id);
    }

    // R2：manual（保留值）动作不进 strategy dispatch（D3 豁免），不消耗 budget 分母
    // exec_dispatch_count_ —— 保持 unmatched 额度分母纯为真实策略派发计数。
    if (sid_id != xt_identity::StrategySidRegistry::kManualSidId) {
        exec_dispatch_count_.fetch_add(1, std::memory_order_relaxed);
    }

    bool skip_unmatched = false;
    auto* sink = select_routing_sink_(sid_id, skip_unmatched);
    if (skip_unmatched) return handle_unmatched_fill_(tr);
    if (!sink) return true;

    XtFillEvent fe = build_strategy_fill_event_(tr);
    // L-2：logical id = 翻译后的 fill event order_id（与 gateway submit track 键同域）。
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
    // reason 分桶：事件完全无归因（未 stamp 且 sid 串空）= empty_sid，否则 unknown_sid。
    const bool fill_no_attribution =
        tr.strategy_id.empty() && tr.sid_id == xt_identity::StrategySidRegistry::kNoSidId;
    (fill_no_attribution ? dropped_fill_empty_sid_count_ : dropped_fill_unknown_sid_count_)
        .fetch_add(1, std::memory_order_relaxed);
    if (unmatched_routing_ != UnmatchedRoutingPolicy::WarnAndDrop) {
        // DropAndAlert（上层持 kill-switch 入口）drop+alert；FailFast 抛异常。
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

    // stop 路由 sid 快照：必须在 apply（终态会 erase book slot）之前反查 ——
    // stamp-miss 下 slot 是唯一还活着的归属源（同 prefetch_terminal_order_code_ 模式）。
    uint32_t stop_sid_hint = 0;
    if (conditional_orders_enabled_ && slots_.size() > 1 && order_book_ &&
        upd.sid_id == xt_identity::StrategySidRegistry::kNoSidId) {
        stop_sid_hint = order_book_->order_sid_id(upd.id);
    }

    apply_order_update_state_(applied_upd, is_terminal);

    // [R5 M1] 终态 infra cleanup 必执行，不受不可信策略回调失败影响。apply 已 erase book
    // slot；若策略回调抛异常/返 false（dispatch_order_update_to_strategy_ 早退）或窗口内其它
    // 路径未捕获异常令下方 finish 被跳过，STP 边界贡献与 order tracking 永久残留（跨重启不愈；
    // 且 lost-terminal reconcile 忽略 false 照记 healed → 确定性触发）。RAII guard 在窗口内任何
    // 退出路径（dispatch 早退 / build_strategy_order_update_ / notify_stops_for_update_ /
    // observers 未捕获异常）兜执行 finish_terminal_order_update_；成功路径仍在原位置
    //（observers 之后）内联执行并置 done → guard 变 no-op，成功路径可观察执行次序不变
    //（两路径调同一 finish、产物一致）。
    bool terminal_cleanup_done = false;
    auto terminal_cleanup_guard = make_scope_guard([&]() noexcept {
        if (!is_terminal || terminal_cleanup_done) return;
        try { finish_terminal_order_update_(applied_upd); } catch (...) {}
    });

    XtOrderExecUpdate strategy_upd = build_strategy_order_update_(applied_upd);
    // 条件单 Fired 状态机先于策略回调收敛（策略在 on_order 里看到的 book 状态已终结）；
    // on_order_update 是 live（event loop 直调）/BT（drain 循环）共同入口。
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
    // R2：manual（保留值）动作不进 strategy dispatch（D3 豁免），不消耗 budget 分母
    // exec_dispatch_count_（与 dispatch_fill_to_strategy_ 对称）。
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
    // reason 分桶：sid_id 未 stamp = empty_sid，stamp 了但未命中活跃 context = unknown_sid。
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
    // D8-④：操作面手工单（保留 sid）终态回执 —— server-side lifecycle 终点。整数比较，
    // grep key `panel.manual_order terminal`（提交回执见 PanelHelpers::manual_order_panel_request）。
    if (applied_upd.sid_id == xt_identity::StrategySidRegistry::kManualSidId) {
        XtLogger::info_m("XtHftEngine",
                         "panel.manual_order terminal: order_id={} status={}",
                         static_cast<unsigned long long>(applied_upd.id),
                         static_cast<int>(applied_upd.status));
    }
    if (order_id_translator_) order_id_translator_->cleanup_done();
    // STP 按 id 释放边界贡献（id-keyed，无需 code 反查 → trade-first 先 erase slot 后
    // 终态 update 也能正确释放，消除旧 code-scan 漏释放）。
    gateway_->notify_order_terminal(applied_upd.id);
    gateway_->record_order_terminal(applied_upd.status);
    gateway_->erase_order_tracking(applied_upd.id);
}

// ── Recovery and position snapshots ──────────────────────────────────

void XtHftEngine::on_position_snapshot(
    const std::vector<ITradeGateway::PositionSnapshot>& positions,
    ITradeGateway::TdState td_state,
    bool is_cold_start,
    uint32_t session_gen)
{
    if (!started_) { note_pre_start_drop_("on_position_snapshot"); return; }
    const bool is_recovery = (td_state == ITradeGateway::TdState::Recovering);

    // Observer pre-hook：live 用于 recovery 期间暂停 trading_ready。
    for (auto* o : observers_) o->on_position_snapshot_begin(is_recovery);

    // account 域：recovery reset / live set_position。
    // position_applied 是 readiness 诚实契约的唯一真值源：仅当 account_store_ && catalog_
    // 均非空、快照真正灌入 account_store 时为 true。observer 据此置 td_position_ready，
    // 避免"消息已到达但基线未落库"时误放行 submit（锁仓根因）。
    const bool position_applied = (account_store_ && catalog_);
    if (position_applied) {
        account_store_->on_position_snapshot(positions, is_recovery, *catalog_);
    }

    // order 域：同步持仓涉及的合约，并找出 stale 合约。
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
                // stale 合约按 flat 落账：四个持仓字段全 0 ⇒ store 在 apply 之后把该
                // code 的成本价 fallback 一并派生成 0（flat 没有单一成本口径）。
                account_store_->apply(XtAccountEvent::set_position(
                    code.c_str(),
                    xt_fp::to_fp(0.0), xt_fp::to_fp(0.0),
                    xt_fp::to_fp(0.0), xt_fp::to_fp(0.0),
                    /*last_price=*/0,
                    xt_fp::to_fp(0.0), xt_fp::to_fp(0.0)));
            }
        }
    }

    // 策略仓位走 pull 模型：snapshot 只更新 account_store，不再推送 on_position。
    // 首次 snapshot 或 recovery snapshot 需要补发 RESUME，标记 inherited positions。
    if (lifecycle_.want_resume(is_recovery)) {
        emit_resume_for_recovery_(positions);
    }
    lifecycle_.mark_snapshot_seen();

    // Observer post-hook：live 更新 position snapshot 时间并刷新 ready。
    // position_applied 透传给 observer，作为 td_position_ready 诚实置位的门（见上）。
    for (auto* o : observers_) o->on_position_snapshot_end(is_recovery, position_applied, session_gen);
}

void XtHftEngine::on_reverse_sync_batch(
    const std::vector<AliveInflightOrder>& orders,
    bool is_cold_start)
{
    if (!started_) { note_pre_start_drop_("on_reverse_sync_batch"); return; }
    // dedup 由各 venue gateway 在 emit batch 前完成。
    (void)is_cold_start;

    if (!order_book_ || !account_store_) return;

    // 空 batch 表示 venue 当前没有 alive in-flight orders，仍是完整 recovery 结果。
    auto view_for = [this](const std::string& code, bool is_buy_to_close) {
        return xt_account::closeable_view(
            account_store_->snapshot_position(code),
            is_buy_to_close);
    };
    // R4：收集 book 实际接受/复活的每一单 → 逐单驱动 STP re-arm（跟随 book 裁决）。
    std::vector<xt_runtime::order::AcceptedRearmOrder> accepted;
    order_book_->fillup_alive_inflight_orders_batch(orders, view_for, &accepted);
    // STP clear/seed（cold）已在 reverse-sync 回调点（XtHftChannels::on_td_reverse_sync_batch，
    // SDK 线程同步执行，早于 trading_ready 放行）完成；warm 回调改 no-op。此处对 book 接受清单
    // 逐单 rearm_active_order（upsert + 清 tombstone，book 真值覆盖 STP 私有 tombstone）——warm
    // 路径 STP re-arm 唯一来源，cold 路径与回调 seed 幂等叠加。worker 单写线程；rearm 只碰 STP。
    if (gateway_)
        for (const auto& a : accepted)
            gateway_->rearm_active_order(a.code, a.id, a.is_buy, a.price_fp);
    // reap grace 窗口基准：worker 线程单写，与 reap_recovered_orphans 的 now_ms 同源 steady。
    last_reverse_sync_steady_ms_ =
        static_cast<int64_t>(XtSteadyClock::steady_now_ms());
}

size_t XtHftEngine::reap_recovered_orphans(int64_t now_ms,
                                           int64_t grace_ms,
                                           size_t max_cancels) {
    // 与其余 runtime 入口同一道 started_ 闸。本入口在 pre-start 期行为不可达变化：
    // reverse-sync 同样被 gate 挡住 → book 恒空、last_reverse_sync_steady_ms_ 恒 0，
    // 旧路径也只会走到 grace 早退返 0。gate 的价值是把「装配时序被破坏」计进
    // pre_start_dropped_count 而非静默 0。
    if (!started_) { note_pre_start_drop_("reap_recovered_orphans"); return 0; }
    if (!order_book_) return 0;
    // pre-registration 暂存表周期 sweep：挂 reap 节拍清理 TTL 过期的 stale 早到 update。
    // 现有 sweep 仅在插入/重放时懒触发；无后续 off-worker 注册/回报的静默期，超时未注册
    // 的暂存项会滞留。此处不受下方 grace / journal-degraded / max_cancels 门控（sweep 只清
    // stale 暂存、与撤单无关；worker 单写线程，counter/log 复用既有 orphan 口径）。
    order_book_->sweep_stale_pre_registration();
    if (!gateway_ || max_cancels == 0) return 0;
    // reverse-sync 后的 grace 窗口内不处理，给 single-strategy 恢复单自行收尾时间。
    if (last_reverse_sync_steady_ms_ == 0 ||
        now_ms - last_reverse_sync_steady_ms_ < grace_ms) {
        return 0;
    }

    // 归属 journal 持久化降级时切告警-only：book 的 reap 调用本身会消费/标记 recovered-
    // unowned slot，degraded 下若先消费再放弃撤单，会把合法 owned 单（sid 只是暂时反查
    // 不到）误销账。故在碰 book 之前 return，保留孤儿单交 operator 处置。
    if (xt_identity::OrderAttributionStore::order_sid_journal_degraded()) {
        const uint64_t suppressed =
            orphan_reap_suppressed_total_.fetch_add(1, std::memory_order_relaxed) + 1;
        // 首次抑制必打；之后按抑制次数取模限频，避免每个 sweep 周期刷屏。
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
    // book reap 首轮返回待撤 id；后续仍命中的 recovered-unowned 由 book 硬删（reaped）。
    std::vector<uint64_t> reaped;  // R4/L2：book 硬删的 id 无 terminal notify → 逐 id 清 STP 私有条目
    const size_t n = order_book_->reap_recovered_unowned(
        ids.data(), std::min(max_cancels, ids.size()), &reaped);
    // book 次轮硬删的孤儿单不发 terminal notify → STP 条目失去清理来源 → 逐 id notify
    //（erase + tombstone，跟随 book 删除裁决）。先于 n==0 早退，硬删即使无待撤也要清 STP。
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
    // 同 reap_recovered_orphans：pre-start 期 book 恒空（提交与 reverse-sync 两个灌入口
    // 都在 gate 之后），故行为不可达变化；gate 只为把破坏的装配时序计入 pre-start drop。
    if (!started_) { note_pre_start_drop_("reconcile_lost_terminals"); return 0; }
    if (!order_book_ || !gateway_ || max == 0) return 0;
    // 全量扫 open owned 单快照：lost-terminal reconcile 是纯本地对账（is_order_terminal
    // 查本地 tracker、on_order_update 本地收尾，无 venue 撤单、无限频），故扫描不受 max
    // 撤单预算截断。旧实现 min(max,256) 固定截断只扫前 max 个 → 截断区后的
    // book-open×venue-terminal 单永久饥饿（HFT-D6）：buffer 取 book slot pool 上限，
    // collect_open_orders 一趟拿全 open 集；healed 单终态 erase 离开 open 集后下轮全量扫
    // 自然推进，无需 cursor。collect_open_orders 未改动 → 策略 get_open_orders 语义与回测
    // byte-identical 不受影响。max 仅作单轮合成（heal）上限（burst 防护）。
    //（collect_open_orders 返拷贝，遍历拷贝期间 on_order_update erase book slot 安全。）
    std::array<XtOrderExecUpdate, xt_runtime::order::kOrderSnapshotMaxOrders> open{};
    const size_t n = order_book_->collect_open_orders(open.data(), open.size());
    size_t healed = 0;
    for (size_t i = 0; i < n && healed < max; ++i) {
        const uint64_t id = open[i].id;
        if (id == 0) continue;
        // 只修本进程 owned 单；recovered-unowned 不在 terminal tracking 内，交给 orphan reap。
        // 优先取终态 payload：命中则按 gateway 记录的真实 status/filled 派生事件
        // （Filled 带累计量、Rejected 保原状态）；payload 不可得（无记录/venue 不
        // 提供）时退回布尔探测 + Canceled 合成兜底。
        XtOrderTerminalRecord rec{};
        const bool has_payload = gateway_->lookup_terminal(id, rec);
        if (!has_payload && !gateway_->is_order_terminal(id)) continue;
        // book 仍 open 但 venue 已终态时，合成终态并复用 on_order_update 收尾链路。
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
            // rec.filled_fp < 0 且非 Filled：venue 不提供累计量，保留 book 侧视图。
        } else {
            term.status = XtOrderStatus::Canceled;
        }
        // R2-P1-3：reconcile 合成终态是本地推断（book-open×venue-terminal 对账），
        // 时序与 venue 脱钩 → 显式 kIneligible（默认参会静默判 eligible = 假 ack）。
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
    // 读取 book hazard snapshot：统计 leaves>0 的 open 单，含 recovered 孤儿，任意线程可读。
    return order_book_
        ? static_cast<uint32_t>(order_book_->open_order_count())
        : 0;
}

void XtHftEngine::emit_resume_for_recovery_(
    const std::vector<ITradeGateway::PositionSnapshot>& positions)
{
    // engine 负责 sid 归因与 qty==0 过滤；open_ts / hold_duration / 落盘委托 lifecycle_。
    std::vector<xt_engine::hft::HftLifecycleTelemetry::ResumeEntry> entries;
    if (declared_registry_ && !slots_.empty()) {
        // Multi mode：按 declared symbols 将 venue 仓位归因给对应 strategy。
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
        // 无 declared registry：ctx[0] 接收全部 venue 仓位。
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

// ── Account events ───────────────────────────────────────────────────

void XtHftEngine::on_account_snapshot(const ITradeGateway::AccountSnapshot& snap) {
    if (!started_) { note_pre_start_drop_("on_account_snapshot"); return; }
    if (!account_store_) return;

    if (std::isfinite(snap.balance)) {
        const int64_t broker_cash_fp = xt_fp::to_fp(snap.balance);
        // divergence 采样必须先于 apply(set_cash)：set_cash 把 store cash 直接覆盖为
        // broker balance，事后 |broker−derived| 恒 0，真实漂移撑不过 divergence alert
        // 的 for 窗口（假报健康）。首个有效快照只落锚不采样（derived 尚未锚定）。
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

    // realized_pnl / fees 不从资金快照重算；这里仅更新 cash/equity。
    const auto am = account_store_->snapshot_account();

    // account 是全局共享状态，所有 strategy 都要看到。
    const XtAccountUpdate acct_upd{am.cash, am.equity, am.unrealized_pnl};
    // sink_at_(i) 即 slots_[i].ctx；fail-closed 跳过 disabled 策略的账户广播。
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

// ── Strategy routing ─────────────────────────────────────────────────

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
    // R2 D3：操作面手工单（保留 sid）一等豁免 —— 无策略 sink 消费。置于入口最前（单 context
    // 早退与 kNoSidId 检查之前）→ 返 nullptr 且 skip_unmatched=false，caller 的
    // `if (!sink) return true` 干净早退：零 drop、零 unmatched 额度、零 ERROR，
    // observers/终态收尾照旧。非 router 无 stamp 时此值不产生 → 分支惰性。
    if (sid_id == xt_identity::StrategySidRegistry::kManualSidId) {
        return nullptr;
    }
    // fail-closed：disabled 策略不收 fill/order 派发（返 nullptr、skip_unmatched=false →
    // caller 静默跳过，不计 unmatched drop、不触发 kill-switch）。
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
    // Stamp 优先硬化（ADR 2026-07-02 stamp 优先契约）：gateway/venue 预 stamp 的 sid_id
    // 权威，启发永不覆盖已 stamp 的归属。健康路径（sid!=0 ⟺ 串非空，同源 stamp）零 delta；
    // 异常路径（sid stamp 但串空）先用权威反解补串——stale/越域 sid 反解失败则保持未归属
    // （宁缺不错，从「启发猜归属」改「未归属」更安全）。
    if (tr.sid_id != xt_identity::StrategySidRegistry::kNoSidId) {
        if (tr.strategy_id.empty()) {
            if (const char* s = xt_identity::StrategySidRegistry::resolve_sid_cstr_or_null(
                    tr.sid_id)) {
                tr.strategy_id = s;
            }
        }
        return false;  // 已 stamp：不进启发恢复
    }
    if (!tr.strategy_id.empty()) return false;

    std::string recovered = effective_sid_(tr.strategy_id, tr.code);
    if (recovered.empty()) return false;

    record_recovered_fill_sid_(tr, recovered);
    tr.strategy_id = std::move(recovered);
    // 恢复产物同步回填 sid_id，保持 record 两键自洽（路由侧另有 string fallback 兜底）。
    tr.sid_id = resolve_fill_sid_id_(tr.strategy_id);
    return true;
}

void XtHftEngine::record_recovered_fill_sid_(const XtTradeRecord& tr,
                                             const std::string& recovered_sid) {
    // throttle（init_ 注册，60s）只影响日志输出，不影响 counter 和恢复后的归因值。
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
    // single/legacy 只有一个 context，sid-less fill 归它。
    // 前提：runner 独占账户（无 foreign fill），否则误归因污染 per-strategy 口径。
    if (slots_.size() == 1) return slots_[0].ctx->strategy_id();
    // No-same-symbol 下 code->strategy 是 1:1；无唯一归属时保持空 sid 由调用方 drop。
    if (!declared_registry_ || slots_.size() <= 1 || code.empty()) return sid;
    if (!code_to_sid_built_) build_code_to_sid_();
    auto it = code_to_sid_.find(code);
    if (it != code_to_sid_.end() && !it->second.empty()) return it->second;
    return sid;
}

void XtHftEngine::build_code_to_sid_() {
    // 歧义 code 不参与 fallback，避免错误归因。
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
