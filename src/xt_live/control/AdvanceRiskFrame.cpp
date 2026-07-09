#include "AdvanceRiskFrame.h"

#include <atomic>
#include <chrono>
#include <cstdint>

#include "XtLiveOrderGateway.h"
#include "api/IMdGateway.h"
#include "api/ITradeGateway.h"
#include "api/XtRunnerSignals.h"
#include "xt_account/state/AccountStateStore.h"
#include "xt_core/session/XtTradingDay.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_risk/engine/XtRiskMonitor.h"
#include "xt_util/time/XtSteadyClock.h"

namespace xt_live::control {
namespace {

uint32_t compute_trading_date_for_venue(const std::string& venue,
                                        int64_t now_ns) noexcept {
    xt_core::TimezonePolicy policy = xt_core::TimezonePolicy::ChinaFuturesNightSession;
    if (venue == "binance_u" || venue == "binance") {
        policy = xt_core::TimezonePolicy::UtcNaturalDay;
    }
    return xt_core::derive_trading_day(now_ns / 1'000'000LL, policy);
}

}

void advance_risk_frame(const RiskFrameDeps& deps, RiskFrameState& state) {
    if (deps.risk_monitor && deps.account_store) {

        const int64_t equity_fp = deps.account_store->snapshot_account().equity;

        const int64_t cashflow_fp =
            deps.signals ? deps.signals->latest_external_cashflow_today_fp.load(
                               std::memory_order_acquire)
                         : 0;

        uint32_t cur_session_date =
            deps.signals ? deps.signals->last_tick_date.load(std::memory_order_acquire)
                         : 0u;
        if (cur_session_date == 0) {

            const int64_t now_ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
            cur_session_date = compute_trading_date_for_venue(deps.venue_label, now_ns);
        }
        if (cur_session_date != 0) {
            deps.risk_monitor->on_session_begin(cur_session_date, equity_fp, cashflow_fp);
            if (deps.order_gateway) {
                deps.order_gateway->arm_annual_loss_on_session_begin(
                    deps.account_store, cur_session_date, equity_fp);
            }
        }

        const uint32_t now_ms = static_cast<uint32_t>(XtSteadyClock::steady_now_ms());
        const uint32_t last_md_ms =
            deps.signals ? deps.signals->last_tick_steady_ms.load(std::memory_order_relaxed)
                         : 0u;
        deps.risk_monitor->on_frame(equity_fp, last_md_ms, now_ms, cashflow_fp);

        if (++state.integrity_tick >= 10) {
            state.integrity_tick = 0;
            if (deps.sid_registry) {
                (void)deps.risk_monitor->sanity_check_per_sid_state(
                    deps.sid_registry->sid_slot_count());
            }
        }
    }

    {
        const uint32_t health_now_ms =
            static_cast<uint32_t>(XtSteadyClock::steady_now_ms());
        if (deps.md) (void)deps.md->check_health(health_now_ms);
        if (deps.td) (void)deps.td->check_health(health_now_ms);
    }
}

}
