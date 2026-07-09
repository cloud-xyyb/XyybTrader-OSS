
#include "xt_live/hft/control/HftControlStages.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <string>

#include "XtLogger.h"
#include "XtLiveOrderGateway.h"
#include "api/ITradeGateway.h"
#include "api/XtRunnerSignals.h"
#include "state/AccountStateStore.h"
#include "xt_account/api/AccountSnapshot.h"
#include "xt_live/hft/control/HftCloseoutGate.h"
#include "xt_risk/engine/XtRiskMonitor.h"
#include "xt_risk/engine/XtRiskMonitorState.h"
#include "xt_risk/state/SafetyStateStore.h"
#include "xt_runtime/order/XtOrderRuntimeBook.h"

#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#include "xt_metrics/XtVenueLabel.h"
#endif

namespace xt_live::control::detail {
namespace {

int64_t now_realtime_ns() {
    return static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
}

void apply_safety_state_on_bootstrap(XtHftRunnerControlContext& ctx,
                                     const xt_risk::state::SafetyState& s) {
    if (s.kill_switch_active) {
        if (!kill_switch_source_latches_across_restart(s.kill_switch_source)) {

            XtLogger::warn_m("RT",
                "safety_state: bootstrap reload -> SKIP kill_switch latch (source={} "
                "reconcile-divergence is an auto condition; re-evaluated by this "
                "process's reconcile loop)",
                static_cast<int>(s.kill_switch_source));
        } else {
            ctx.runtime_shared_state.kill_switch.store(true, std::memory_order_release);
            ctx.runtime_shared_state.kill_switch_source.store(s.kill_switch_source, std::memory_order_release);
            XtLogger::warn_m("RT",
                "safety_state: bootstrap reload -> kill_switch=true (latched across restart, source={})",
                static_cast<int>(s.kill_switch_source));
        }
    }

    if (ctx.risk_monitor) {

        ctx.risk_monitor->restore_latches(s.open_halted, s.halt_trigger_ms, s.peak_equity_fp);
    }

    if (s.current_year != 0 && s.year_start_equity_fp > 0) {
        ctx.live_order_gateway.restore_annual_latch(s.current_year, s.year_start_equity_fp,
                                                    s.annual_cutoff_active,
                                                    s.year_start_net_pnl_fp, s.risk_fund_balance_fp);
        XtLogger::warn_m("RT",
            "safety_state: bootstrap reload -> annual latch year={} year_start_equity_fp={} "
            "cutoff={} (restored across restart; will reset on next on_year_change with diff year)",
            s.current_year, s.year_start_equity_fp,
            s.annual_cutoff_active ? "true" : "false");
    }

    if (s.frozen_cash_fp > 0) {

        XtLogger::warn_m("RT",
            "safety_state: bootstrap reload -> frozen_cash_fp={} (diagnostics only; venue "
            "snapshot is source of truth, audit if mismatch)", s.frozen_cash_fp);
    }

    if (ctx.risk_monitor_state && !s.per_strategy_kill_switch.empty()) {
        auto& per = ctx.risk_monitor_state->per_strategy;
        const std::size_t n_disk = s.per_strategy_kill_switch.size();
        const std::size_t n_mem = per.size();
        const std::size_t n = std::min(n_disk, n_mem);
        std::size_t restored = 0;
        for (std::size_t i = 1; i < n; ++i) {
            if (s.per_strategy_kill_switch[i] != 0 && !per.is_strategy_killed(static_cast<uint32_t>(i))) {
                per.set_strategy_killed(static_cast<uint32_t>(i), true);
                ++restored;
            }
        }
        if (restored > 0) {
            XtLogger::warn_m("RT",
                "safety_state: bootstrap reload -> per_strategy_kill_switch restored {} sids "
                "(latched across restart)", restored);
        }
    }

    if (ctx.risk_monitor_state &&
        (!s.per_strategy_auto_liquidating.empty() ||
         !s.per_strategy_liquidating_source.empty())) {
        auto& per = ctx.risk_monitor_state->per_strategy;
        const std::size_t n_latch = s.per_strategy_auto_liquidating.size();
        const std::size_t n_src   = s.per_strategy_liquidating_source.size();
        const std::size_t n = std::min(std::max(n_latch, n_src), per.size());
        std::size_t restored = 0;
        for (std::size_t i = 1; i < n; ++i) {
            const uint8_t latch_byte = (i < n_latch) ? s.per_strategy_auto_liquidating[i] : 0;
            const uint8_t src_byte   = (i < n_src) ? s.per_strategy_liquidating_source[i] : 0;

            const auto src = xt_risk::reconcile_liquidation_restore(latch_byte, src_byte);
            if (src && !per.is_auto_liquidating(static_cast<uint32_t>(i))) {
                per.set_auto_liquidating(static_cast<uint32_t>(i), true, *src);
                ++restored;
            }
        }
        if (restored > 0) {
            XtLogger::warn_m("RT",
                "safety_state: bootstrap reload -> per_strategy_auto_liquidating restored {} sids "
                "(latched across restart; disarm on confirmed-flat)", restored);
        }
    }
}

bool account_is_flat(const xt_account::AccountStateStore& store,
                     const xt_runtime::order::XtOrderRuntimeBook* book) {
    if (book && book->open_order_count() != 0) return false;
    for (const auto& code : store.snapshot_tracked_codes()) {
        if (code.empty()) continue;
        const auto pos = store.snapshot_position(code);
        if (pos.qty != 0 || pos.long_qty != 0 || pos.short_qty != 0) return false;
    }
    return true;
}

}

void emit_kill_switch_transition(XtHftRunnerControlContext& ctx,
                                 const XtRunnerControlConfig& cfg,
                                 bool active, const char* actor, const std::string& reason) {
    const int64_t ts_ns = now_realtime_ns();
    if (ctx.record_kill_switch_trip) {
        ctx.record_kill_switch_trip(active, actor, reason, ts_ns);
    }
#ifdef XT_HAVE_METRICS
    if (!cfg.venue_label.empty()) {
        const auto venue = xt_util::canonical_venue_label(cfg.venue_label);
        auto& reg = ct::metrics::XtMetricRegistry::instance();
        reg.on_kill_switch(venue, actor, active ? "tripped" : "resumed", reason);
        reg.set_kill_switch_active(venue, active);
    }
#else
    (void)cfg;
#endif
}

xt_risk::state::SafetyState collect_safety_state(const XtHftRunnerControlContext& ctx) {
    xt_risk::state::SafetyState s;
    s.schema_version = xt_risk::state::SafetyStateStore::kSchemaVersion;
    s.kill_switch_active = ctx.runtime_shared_state.kill_switch.load(std::memory_order_acquire);
    s.kill_switch_source = ctx.runtime_shared_state.kill_switch_source.load(std::memory_order_acquire);

    if (ctx.risk_monitor) {

        const xt_risk::XtRiskMonitor* mon = ctx.risk_monitor;
        const auto* bs = mon->broadcast_state();
        if (bs) {
            s.open_halted     = bs->open_halted.load(std::memory_order_acquire);
            s.halt_trigger_ms = static_cast<int64_t>(
                bs->open_halt_trigger_ms.load(std::memory_order_acquire));
            s.peak_equity_fp  = bs->peak_equity_fp.load(std::memory_order_acquire);
        }
    }

    const auto al = ctx.live_order_gateway.annual_latch_snapshot();
    s.current_year          = al.current_year;
    s.year_start_equity_fp  = al.year_start_equity_fp;
    s.annual_cutoff_active  = al.annual_cutoff_active;

    s.year_start_net_pnl_fp = al.year_start_net_pnl_fp;

    if (ctx.account_store) {
        s.frozen_cash_fp = ctx.account_store->snapshot_account().frozen_cash;

        {
            const auto acct = ctx.account_store->snapshot_account();
            const int64_t nmf_offset = al.nmf_offset_fp;
            s.risk_fund_balance_fp = nmf_offset + (acct.realized_pnl - acct.fees);
        }
    }

    if (ctx.risk_monitor_state) {
        const auto& per = ctx.risk_monitor_state->per_strategy;
        const std::size_t n = per.size();
        s.per_strategy_kill_switch.resize(n, 0);
        s.per_strategy_auto_liquidating.resize(n, 0);
        s.per_strategy_liquidating_source.resize(n, 0);
        for (std::size_t i = 0; i < n; ++i) {
            const auto sid = static_cast<uint32_t>(i);
            s.per_strategy_kill_switch[i] = per.is_strategy_killed(sid) ? 1 : 0;

            const auto src = per.auto_liquidating_source(sid);
            s.per_strategy_auto_liquidating[i] =
                (src != xt_risk::XtAutoLiquidateSource::None) ? 1 : 0;
            s.per_strategy_liquidating_source[i] = static_cast<uint8_t>(src);
        }
    }
    return s;
}

void maybe_auto_release_closeout_latch(XtHftRunnerControlContext& ctx,
                                       const XtRunnerControlConfig& cfg,
                                       ControlLoopState& st) {
    if (st.autolatch_release_checked) return;
    if (!ctx.account_store) return;
    if (ctx.td_gateway && ctx.td_gateway->is_recovering()) return;
    auto& sig = ctx.runtime_shared_state;
    if (!sig.td_position_ready.load(std::memory_order_acquire)) return;
    if (!sig.has_account_snapshot.load(std::memory_order_acquire)) return;

    if (sig.last_position_snapshot_ms.load(std::memory_order_acquire) == 0) return;

    st.autolatch_release_checked = true;

    const bool ks     = sig.kill_switch.load(std::memory_order_acquire);
    const uint8_t src = sig.kill_switch_source.load(std::memory_order_acquire);
    const bool flat   = account_is_flat(*ctx.account_store, ctx.order_book);
    const bool auto_releasable_src = kill_switch_source_auto_releasable(src);
    if (!closeout_latch_auto_release_decision(ks, src,  true, flat)) {
        if (ks && auto_releasable_src && !flat) {
            XtLogger::warn_m("RT",
                "safety_state: auto-trip kill_switch RETAINED (source={}) -- account NOT flat "
                "post-recovery (manual SetKillSwitch resume required)",
                static_cast<int>(src));
        }
        return;
    }
    sig.kill_switch.store(false, std::memory_order_release);
    sig.kill_switch_source.store(XtRunnerSignals::kKillSwitchSourceNone, std::memory_order_release);
    const char* release_actor =
        src == XtRunnerSignals::kKillSwitchSourceUnmatchedBudget
            ? "system:unmatched-budget-autorelease"
            : "system:closeout-autorelease";
    emit_kill_switch_transition(ctx, cfg,  false, release_actor,
                                "flat_cold_start");
    XtLogger::warn_m("RT",
        "safety_state: auto-released auto-trip kill_switch (source={}, account flat post-recovery)",
        static_cast<int>(src));
}

void bootstrap_safety_state(XtHftRunnerControlContext& ctx, const std::string& path) {
    if (path.empty()) {
        XtLogger::info_m("RT", "safety_state: persistence DISABLED (path empty)");
        return;
    }
    auto loaded = xt_risk::state::SafetyStateStore::load(path);
    if (!loaded.has_value()) {
        XtLogger::warn_m("RT",
            "safety_state: load failed or file missing path={} -> using defaults", path);
        return;
    }
    apply_safety_state_on_bootstrap(ctx, *loaded);
    XtLogger::info_m("RT", "safety_state: bootstrap reload OK from {}", path);
}

void save_safety_state_if_due(const std::string& path,
                              const XtHftRunnerControlContext& ctx,
                              xt_risk::state::SafetyState& last_state,
                              int64_t& last_save_ms,
                              int64_t now_ms,
                              int interval_ms,
                              const char* reason) {
    if (path.empty()) return;
    const auto cur = collect_safety_state(ctx);
    const bool changed = (cur != last_state);

    const bool material_changed =
        cur.kill_switch_active != last_state.kill_switch_active ||
        cur.open_halted != last_state.open_halted ||
        cur.halt_trigger_ms != last_state.halt_trigger_ms ||
        cur.annual_cutoff_active != last_state.annual_cutoff_active ||
        cur.per_strategy_kill_switch != last_state.per_strategy_kill_switch ||
        cur.per_strategy_auto_liquidating != last_state.per_strategy_auto_liquidating ||
        cur.per_strategy_liquidating_source != last_state.per_strategy_liquidating_source;
    const bool due = interval_ms > 0 && (now_ms - last_save_ms) >= interval_ms;
    const bool forced = std::strcmp(reason, "shutdown") == 0
                     || std::strcmp(reason, "bootstrap") == 0;
    if (!changed && !due && !forced) return;

    if (xt_risk::state::SafetyStateStore::save(path, cur)) {
        last_state = cur;
        last_save_ms = now_ms;
        if (material_changed || forced) {
            XtLogger::info_m("RT",
                "safety_state: saved path={} reason={} kill_switch={} open_halted={} "
                "halt_trigger_ms={} peak_equity_fp={} frozen_cash_fp={}",
                path, reason,
                cur.kill_switch_active ? 1 : 0,
                cur.open_halted ? 1 : 0,
                cur.halt_trigger_ms,
                cur.peak_equity_fp,
                cur.frozen_cash_fp);
        } else {

            XtLogger::debug_m("RT",
                "safety_state: saved (non-material) reason={} frozen_cash_fp={}",
                reason, cur.frozen_cash_fp);
        }
    } else {
        XtLogger::warn_mt("RT", "safety_state_save_fail",
            "safety_state: save failed path={} reason={} (next attempt at next interval)",
            path, reason);
    }
}

}
