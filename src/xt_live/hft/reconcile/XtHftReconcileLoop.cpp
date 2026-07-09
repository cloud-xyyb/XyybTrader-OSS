#include "xt_live/hft/reconcile/XtHftReconcileLoop.h"

#include <cmath>
#include <cstdlib>
#include <unordered_set>
#include <utility>

#include "xt_account/state/AccountStateStore.h"
#include "xt_core/XtFixedPoint.h"
#include "xt_logger/XtLogger.h"
#include "xt_metrics/XtMetricRegistry.h"
#include "xt_util/time/XtSteadyClock.h"
#include "api/XtRunnerSignals.h"

namespace xt_live { namespace control {

namespace {

uint64_t steady_now_ms() {
    return XtSteadyClock::steady_now_ms();
}

}

XtHftReconcileLoop::XtHftReconcileLoop(IReconcilableGateway& gw,
                                 xt_account::AccountStateStore& account_store,
                                 std::atomic<bool>& kill_switch,
                                 std::atomic<uint64_t>* last_pos_snapshot_ms,
                                 Config cfg)
    : gw_(gw),
      account_store_(account_store),
      kill_switch_(kill_switch),
      last_pos_snapshot_ms_external_(last_pos_snapshot_ms),
      cfg_(cfg) {}

XtHftReconcileLoop::~XtHftReconcileLoop() { stop(); }

void XtHftReconcileLoop::start() {
    if (!cfg_.enabled) {
        XtLogger::info_m("Reconcile", "loop disabled by config, skipping start");
        return;
    }
    if (!gw_.reconcile_supported()) {
        XtLogger::info_m("Reconcile",
            "gateway does not support reconcile (reconcile_supported=false), skipping");
        return;
    }
    if (!account_store_.symbols_frozen()) {

        XtLogger::error_m("Reconcile",
            "account_store symbols NOT frozen at loop start — reverse phantom sweep "
            "would read an unfrozen registry cross-thread; check bootstrap ordering");
    }
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return;
    thread_ = std::thread([this] { run_loop(); });
    XtLogger::info_m("Reconcile",
        "loop started: cycle_ms={} reconnect_after_n={} kill_after_n={} epsilon_fp={} order_min_age_ms={}",
        cfg_.cycle_ms, static_cast<int>(cfg_.reconnect_after_n),
        static_cast<int>(cfg_.kill_after_n), cfg_.position_epsilon_fp, cfg_.order_min_age_ms);
    if (cfg_.kill_after_n == 0) {
        XtLogger::warn_m("Reconcile",
            "kill ladder DISABLED by config (kill_after_n=0) — divergence 只 resnap + 导 metrics，不拉 kill_switch");
    }
}

void XtHftReconcileLoop::stop() {
    if (!running_.exchange(false)) return;
    wake_cv_.notify_all();
    if (thread_.joinable()) thread_.join();
    XtLogger::info_m("Reconcile", "loop stopped");
}

void XtHftReconcileLoop::run_loop() {
    while (running_.load(std::memory_order_relaxed)) {

        std::unique_lock<std::mutex> lk(wake_mu_);
        wake_cv_.wait_for(lk, std::chrono::milliseconds(cfg_.cycle_ms),
                          [this] { return !running_.load(std::memory_order_relaxed); });
        lk.unlock();
        if (!running_.load(std::memory_order_relaxed)) break;
        tick_once();
    }
}

void XtHftReconcileLoop::tick_for_test() { tick_once(); }

void XtHftReconcileLoop::tick_once() {

    if (reset_request_external_ &&
        reset_request_external_->exchange(false, std::memory_order_acq_rel)) {

        if (metrics_) {
            for (const auto& entry : diverged_) {
                metrics_->set_state_divergence("reconcile_qty", entry.first, 0.0);
            }
        }
        diverged_.clear();
        XtLogger::warn_m("Reconcile",
            "diverged state cleared (control panel RESUME) — counters reset to 0");
    }

    reconcile_position();
    reconcile_orders();
}

void XtHftReconcileLoop::reconcile_position() {
    const uint64_t tick_now_ms = steady_now_ms();
    if (first_attempt_ms_ == 0) first_attempt_ms_ = tick_now_ms;

    auto exchange_pos = gw_.fetch_positions_for_reconcile();
    if (!exchange_pos.has_value()) {

        if (metrics_) {
            const uint64_t base =
                last_snapshot_ok_ms_ != 0 ? last_snapshot_ok_ms_ : first_attempt_ms_;
            metrics_->set_reconcile_snapshot_age_ms(
                static_cast<double>(tick_now_ms - base));
        }
        XtLogger::warn_m("Reconcile",
            "fetch_positions failed, skip this cycle (will retry next tick)");
        return;
    }
    last_snapshot_ok_ms_ = tick_now_ms;
    if (metrics_) metrics_->set_reconcile_snapshot_age_ms(0.0);

    bool any_diverged = false;
    auto apply_ladder = [this, &any_diverged](const std::string& code,
                                              int64_t exchange_net_fp,
                                              int64_t local_net_fp) {
        uint8_t& cnt = diverged_[code];
        if (cnt < 255) ++cnt;
        any_diverged = true;
        XtLogger::warn_m("Reconcile",
            "position diverged: code={} exchange_net_fp={} local_net_fp={} cycles={}",
            code, exchange_net_fp, local_net_fp, static_cast<int>(cnt));

        if (cfg_.reconnect_after_n > 0 && cnt >= cfg_.reconnect_after_n) {
            const uint64_t now_ms = steady_now_ms();
            uint64_t& last = last_resync_ms_[code];
            if (last == 0 || now_ms - last >= cfg_.resync_throttle_ms) {
                XtLogger::warn_m("Reconcile",
                    "code={} cycles={} → request_resnap() (throttled retry, "
                    "last {}ms ago, threshold {}ms)",
                    code, static_cast<int>(cnt),
                    last == 0 ? 0 : (now_ms - last),
                    cfg_.resync_throttle_ms);
                last = now_ms;
                gw_.request_resnap_for_reconcile();
            }
        }
        if (cfg_.kill_after_n > 0 && cnt >= cfg_.kill_after_n) {
            XtLogger::error_m("Reconcile",
                "code={} cycles={} >= kill_after_n → kill_switch_active=1 "
                "(reconcile-set, will auto-clear on agreement)",
                code, static_cast<int>(cnt));
            kill_switch_.store(true, std::memory_order_release);
            kill_set_by_reconcile_.store(true, std::memory_order_release);
            if (kill_switch_source_) {
                kill_switch_source_->store(XtRunnerSignals::kKillSwitchSourceReconcileDivergence,
                                           std::memory_order_release);
            }
        }
    };

    std::unordered_set<std::string> exchange_codes;
    exchange_codes.reserve(exchange_pos->size());
    for (const auto& p : *exchange_pos) {
        exchange_codes.insert(p.code);
        const int64_t exchange_net_fp = xt_fp::to_fp(p.long_qty - p.short_qty);
        const int64_t local_net_fp    = account_store_.snapshot_position(p.code).qty;
        const int64_t diff_fp         = std::abs(exchange_net_fp - local_net_fp);

        if (metrics_) metrics_->set_state_divergence("reconcile_qty", p.code, xt_fp::from_fp(diff_fp));
        if (diff_fp > cfg_.position_epsilon_fp) {
            apply_ladder(p.code, exchange_net_fp, local_net_fp);
        } else {
            diverged_[p.code] = 0;
        }
    }

    std::unordered_set<std::string> swept_local;
    for (const auto& code : account_store_.snapshot_tracked_codes()) {
        if (exchange_codes.count(code) > 0) continue;
        const int64_t local_net_fp = account_store_.snapshot_position(code).qty;
        if (std::abs(local_net_fp) <= cfg_.position_epsilon_fp) continue;
        swept_local.insert(code);

        if (metrics_) metrics_->set_state_divergence("reconcile_qty", code, xt_fp::from_fp(std::abs(local_net_fp)));
        apply_ladder(code,  0, local_net_fp);
    }

    for (auto& entry : diverged_) {
        if (entry.second == 0) continue;
        if (exchange_codes.count(entry.first) > 0) continue;
        if (swept_local.count(entry.first) > 0) continue;
        entry.second = 0;
        if (metrics_) metrics_->set_state_divergence("reconcile_qty", entry.first, 0.0);
    }

    if (metrics_) metrics_->inc_state_divergence_emit_heartbeat();

    if (!any_diverged) {
        const uint64_t now = steady_now_ms();
        last_verified_ms_.store(now, std::memory_order_relaxed);

        if (last_pos_snapshot_ms_external_) {
            last_pos_snapshot_ms_external_->store(now, std::memory_order_relaxed);
        }

        last_resync_ms_.clear();

        if (kill_switch_.load(std::memory_order_acquire)) {
            const bool owned_this_process = kill_set_by_reconcile_.load(std::memory_order_acquire);
            const bool reconcile_owned =
                kill_switch_source_
                    ? (kill_switch_source_->load(std::memory_order_acquire) ==
                       XtRunnerSignals::kKillSwitchSourceReconcileDivergence)
                    : owned_this_process;

            if (reconcile_owned) {
                XtLogger::warn_m("Reconcile",
                    "all positions agree → auto-clearing reconcile kill_switch (owned={})",
                    owned_this_process ? 1 : 0);
                kill_switch_.store(false, std::memory_order_release);
                kill_set_by_reconcile_.store(false, std::memory_order_release);
                if (kill_switch_source_) {
                    kill_switch_source_->store(XtRunnerSignals::kKillSwitchSourceNone,
                                               std::memory_order_release);
                }
            }
        }
    }
}

void XtHftReconcileLoop::reconcile_orders() {
    auto resolved = gw_.detect_and_resolve_ghost_orders_for_reconcile(cfg_.order_min_age_ms);
    if (!resolved.has_value()) {
        XtLogger::warn_m("Reconcile",
            "detect_and_resolve_ghost_orders failed, skip this cycle");
        return;
    }
    if (*resolved == 0) return;

    XtLogger::warn_m("Reconcile",
        "ghost orders resolved (synthesized into td queue): count={}", *resolved);
}

}}
