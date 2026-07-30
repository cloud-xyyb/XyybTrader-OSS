
#include "SafetyStateWire.h"

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>

#include "XtLiveOrderGateway.h"
#include "XtLogger.h"
#include "api/XtRunnerSignals.h"
#include "xt_account/api/AccountSnapshot.h"
#include "xt_account/state/AccountStateStore.h"
#include "xt_risk/engine/XtRiskMonitor.h"
#include "xt_risk/engine/XtRiskMonitorState.h"
#include "xt_risk/state/SafetyStateStore.h"

bool kill_switch_source_latches_across_restart(uint8_t source) noexcept {

    return source != XtRunnerSignals::kKillSwitchSourceReconcileDivergence;
}

namespace xt_live::control {

xt_risk::state::SafetyState collect_safety_state(const SafetyStateWireDeps& deps) {
    xt_risk::state::SafetyState s;
    s.schema_version = xt_risk::state::SafetyStateStore::kSchemaVersion;
    s.kill_switch_active = deps.signals->kill_switch.load(std::memory_order_acquire);
    s.kill_switch_source = deps.signals->kill_switch_source.load(std::memory_order_acquire);

    if (deps.risk_monitor) {

        const xt_risk::XtRiskMonitor* mon = deps.risk_monitor;
        const auto* bs = mon->broadcast_state();
        if (bs) {
            s.open_halted     = bs->open_halted.load(std::memory_order_acquire);
            s.halt_trigger_ms = static_cast<int64_t>(
                bs->open_halt_trigger_ms.load(std::memory_order_acquire));
            s.peak_equity_fp  = bs->peak_equity_fp.load(std::memory_order_acquire);
        }
    }

    const auto al = deps.order_gateway->annual_latch_snapshot();
    s.current_year          = al.current_year;
    s.year_start_equity_fp  = al.year_start_equity_fp;
    s.annual_cutoff_active  = al.annual_cutoff_active;

    s.year_start_net_pnl_fp = al.year_start_net_pnl_fp;

    if (deps.account_store) {
        s.frozen_cash_fp = deps.account_store->snapshot_account().frozen_cash;

        {
            const auto acct = deps.account_store->snapshot_account();
            const int64_t nmf_offset = al.nmf_offset_fp;
            s.risk_fund_balance_fp = nmf_offset + (acct.realized_pnl - acct.fees);
        }
    }

    if (deps.risk_monitor_state) {
        const auto& per = deps.risk_monitor_state->per_strategy;
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

void apply_safety_state_on_bootstrap(const SafetyStateWireDeps& deps,
                                     const xt_risk::state::SafetyState& s) {
    if (s.kill_switch_active) {
        if (!kill_switch_source_latches_across_restart(s.kill_switch_source)) {

            XtLogger::warn_m(deps.module_tag,
                "safety_state: bootstrap reload -> SKIP kill_switch latch (source={} "
                "reconcile-divergence is an auto condition; re-evaluated by this "
                "process's reconcile loop)",
                static_cast<int>(s.kill_switch_source));
        } else {
            deps.signals->kill_switch.store(true, std::memory_order_release);
            deps.signals->kill_switch_source.store(s.kill_switch_source, std::memory_order_release);
            XtLogger::warn_m(deps.module_tag,
                "safety_state: bootstrap reload -> kill_switch=true (latched across restart, source={})",
                static_cast<int>(s.kill_switch_source));
        }
    }

    if (deps.risk_monitor) {

        deps.risk_monitor->restore_latches(s.open_halted, s.halt_trigger_ms, s.peak_equity_fp);
    }

    if (s.current_year != 0 && s.year_start_equity_fp > 0) {
        deps.order_gateway->restore_annual_latch(s.current_year, s.year_start_equity_fp,
                                                 s.annual_cutoff_active,
                                                 s.year_start_net_pnl_fp, s.risk_fund_balance_fp);
        XtLogger::warn_m(deps.module_tag,
            "safety_state: bootstrap reload -> annual latch year={} year_start_equity_fp={} "
            "cutoff={} (restored across restart; will reset on next on_year_change with diff year)",
            s.current_year, s.year_start_equity_fp,
            s.annual_cutoff_active ? "true" : "false");
    }

    if (s.frozen_cash_fp > 0) {

        XtLogger::warn_m(deps.module_tag,
            "safety_state: bootstrap reload -> frozen_cash_fp={} (diagnostics only; venue "
            "snapshot is source of truth, audit if mismatch)", s.frozen_cash_fp);
    }

    if (deps.risk_monitor_state && !s.per_strategy_kill_switch.empty()) {
        auto& per = deps.risk_monitor_state->per_strategy;
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
            XtLogger::warn_m(deps.module_tag,
                "safety_state: bootstrap reload -> per_strategy_kill_switch restored {} sids "
                "(latched across restart)", restored);
        }
    }

    if (deps.restore_auto_liquidation && deps.risk_monitor_state &&
        (!s.per_strategy_auto_liquidating.empty() ||
         !s.per_strategy_liquidating_source.empty())) {
        auto& per = deps.risk_monitor_state->per_strategy;
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
            XtLogger::warn_m(deps.module_tag,
                "safety_state: bootstrap reload -> per_strategy_auto_liquidating restored {} sids "
                "(latched across restart; disarm on confirmed-flat)", restored);
        }
    }
}

namespace {

std::atomic<uint64_t> g_quarantine_count{0};

int64_t now_wall_ms() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

void quarantine_and_latch(const SafetyStateWireDeps& deps, const std::string& path) {
    const int64_t wall_ms = now_wall_ms();

    std::error_code ec;
    const std::filesystem::path src(path);
    std::filesystem::path dst(path + ".quarantine");
    if (std::filesystem::exists(dst, ec) || ec) {
        dst = std::filesystem::path(path + ".quarantine." + std::to_string(wall_ms));
    }
    std::filesystem::rename(src, dst, ec);
    if (ec) {

        XtLogger::error_m(deps.module_tag,
            "safety_state: INVALID file path={} quarantine rename FAILED ({}) -> latching anyway",
            path, ec.message());
    } else {
        XtLogger::error_m(deps.module_tag,
            "safety_state: INVALID file path={} quarantined to {}", path, dst.string());
    }

    g_quarantine_count.fetch_add(1, std::memory_order_relaxed);

    xt_risk::state::SafetyState latched;
    latched.kill_switch_active = true;
    latched.kill_switch_source = XtRunnerSignals::kKillSwitchSourceNone;
    latched.open_halted        = true;

    latched.halt_trigger_ms    = 0;
    apply_safety_state_on_bootstrap(deps, latched);

    XtLogger::error_m(deps.module_tag,
        "safety_state: bootstrap FAIL-CLOSED path={} -> kill_switch=true + open_halted=true "
        "(file existed but failed validation; manual source, operator must clear)", path);
}

}

uint64_t safety_state_quarantine_count() noexcept {
    return g_quarantine_count.load(std::memory_order_relaxed);
}

void bootstrap_safety_state(const SafetyStateWireDeps& deps, const std::string& path) {
    if (path.empty()) {
        XtLogger::info_m(deps.module_tag, "safety_state: persistence DISABLED (path empty)");
        return;
    }
    const auto result = xt_risk::state::SafetyStateStore::load_checked(path);
    switch (result.status) {
    case xt_risk::state::LoadStatus::Missing:

        XtLogger::info_m(deps.module_tag,
            "safety_state: file missing path={} -> cold-start defaults", path);
        return;
    case xt_risk::state::LoadStatus::Invalid:

        quarantine_and_latch(deps, path);
        return;
    case xt_risk::state::LoadStatus::Ok:
        apply_safety_state_on_bootstrap(deps, *result.state);
        XtLogger::info_m(deps.module_tag, "safety_state: bootstrap reload OK from {}", path);
        return;
    }
}

void save_safety_state_if_due(const std::string& path,
                              const SafetyStateWireDeps& deps,
                              xt_risk::state::SafetyState& last_state,
                              int64_t& last_save_ms,
                              int64_t now_ms,
                              int interval_ms,
                              const char* reason) {
    if (path.empty()) return;
    const auto cur = collect_safety_state(deps);
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
            XtLogger::info_m(deps.module_tag,
                "safety_state: saved path={} reason={} kill_switch={} open_halted={} "
                "halt_trigger_ms={} peak_equity_fp={} frozen_cash_fp={}",
                path, reason,
                cur.kill_switch_active ? 1 : 0,
                cur.open_halted ? 1 : 0,
                cur.halt_trigger_ms,
                cur.peak_equity_fp,
                cur.frozen_cash_fp);
        } else {

            XtLogger::debug_m(deps.module_tag,
                "safety_state: saved (non-material) reason={} frozen_cash_fp={}",
                reason, cur.frozen_cash_fp);
        }
    } else {
        XtLogger::warn_mt(deps.module_tag, "safety_state_save_fail",
            "safety_state: save failed path={} reason={} (next attempt at next interval)",
            path, reason);
    }
}

}
