// ── latched safety state 持久化 + kill-switch 可观测 stage ──────────────
//
// safety.bin 的 collect/apply/bootstrap/save 实现在跨引擎共享 TU
// xt_live/control/SafetyStateWire.cpp；本文件保留 HFT-only 段：
// XtHftRunnerControlContext → deps 薄 wrapper、closeout-block 自动 latch 的
// 冷启 flat 释放、kill_switch 转换的 panel 因果链 hook 与 prometheus 输出。

#include "xt_live/hft/control/HftControlStages.h"

#include <chrono>
#include <string>

#include "XtLogger.h"
#include "XtLiveOrderGateway.h"
#include "api/ITradeGateway.h"
#include "api/XtRunnerSignals.h"
#include "state/AccountStateStore.h"
#include "xt_account/api/AccountSnapshot.h"
#include "xt_live/control/SafetyStateWire.h"    // 共享 collect/apply/bootstrap/save
#include "xt_live/hft/control/HftCloseoutGate.h"  // closeout_latch_auto_release_decision
#include "xt_risk/state/SafetyStateStore.h"     // SafetyState 完整类型（collect 返回值）
#include "xt_runtime/order/XtOrderRuntimeBook.h"

// XT_HAVE_METRICS PUBLIC compile def 由 xt_live_telemetry 透传过来。
#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#include "xt_metrics/XtVenueLabel.h"                // canonical venue label
#endif

namespace xt_live::control::detail {
namespace {

// realtime ns（CLOCK_REALTIME）用于 kill-switch 因果链时间戳。
int64_t now_realtime_ns() {
    return static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
}

// 共享 wire deps：HFT 是唯一 restore_auto_liquidation=true 的引擎
// （disarm owner PerSidLiquidator 只在 HFT 控制环存在）。
SafetyStateWireDeps wire_deps(const XtHftRunnerControlContext& ctx) {
    SafetyStateWireDeps deps;
    // runtime_shared_state/live_order_gateway 是引用成员：经 const ctx 访问仍产出
    // 非 const 目标（constness 不穿透引用），无需 const_cast。
    deps.signals            = &ctx.runtime_shared_state;
    deps.risk_monitor       = ctx.risk_monitor;
    deps.risk_monitor_state = ctx.risk_monitor_state;
    deps.order_gateway      = &ctx.live_order_gateway;
    deps.account_store      = ctx.account_store;
    deps.restore_auto_liquidation = true;
    deps.module_tag         = "RT";
    return deps;
}

// 账户是否 flat：无在途单 + 所有 tracked code 净/多/空 持仓为 0。
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

} // namespace

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

// ---------------------------------------------------------------------------
// latched safety state 持久化（委托共享 SafetyStateWire）
// ---------------------------------------------------------------------------

xt_risk::state::SafetyState collect_safety_state(const XtHftRunnerControlContext& ctx) {
    return xt_live::control::collect_safety_state(wire_deps(ctx));
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
    // td_position_ready 在 snapshot 灌入 account_store 后才置位。last_position_snapshot_ms!=0
    // 作独立冷启 freshness 兜底 —— ==0 表示尚无任一 position snapshot / 成交 / 对账
    // 刷新过该 witness，避免冷启空账户被误判 flat 而误释放 latch。三条件全满足才判一次（fail-safe）。
    if (sig.last_position_snapshot_ms.load(std::memory_order_acquire) == 0) return;

    st.autolatch_release_checked = true;  // hydrate 完成 → 判定一次（释放与否都只判一次）

    const bool ks     = sig.kill_switch.load(std::memory_order_acquire);
    const uint8_t src = sig.kill_switch_source.load(std::memory_order_acquire);
    const bool flat   = account_is_flat(*ctx.account_store, ctx.order_book);
    const bool auto_releasable_src = kill_switch_source_auto_releasable(src);
    if (!closeout_latch_auto_release_decision(ks, src, /*account_hydrated=*/true, flat)) {
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
    emit_kill_switch_transition(ctx, cfg, /*active=*/false, release_actor,
                                "flat_cold_start");
    XtLogger::warn_m("RT",
        "safety_state: auto-released auto-trip kill_switch (source={}, account flat post-recovery)",
        static_cast<int>(src));
}

void bootstrap_safety_state(XtHftRunnerControlContext& ctx, const std::string& path) {
    xt_live::control::bootstrap_safety_state(wire_deps(ctx), path);
}

void save_safety_state_if_due(const std::string& path,
                              const XtHftRunnerControlContext& ctx,
                              xt_risk::state::SafetyState& last_state,
                              int64_t& last_save_ms,
                              int64_t now_ms,
                              int interval_ms,
                              const char* reason) {
    xt_live::control::save_safety_state_if_due(path, wire_deps(ctx), last_state,
                                               last_save_ms, now_ms, interval_ms, reason);
}

}  // namespace xt_live::control::detail
