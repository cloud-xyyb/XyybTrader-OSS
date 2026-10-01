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
#include "api/XtRunnerSignals.h"  // kKillSwitchSource* 常量

namespace xt_live { namespace control {

namespace {

uint64_t steady_now_ms() {
    return XtSteadyClock::steady_now_ms();
}

}  // namespace

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
        // 反向 sweep 依赖 post-freeze 的只读 tracked 集；unfrozen 时读侧退回
        // mark-cache 可变视图（与并发 register_symbol 构成跨线程 race）。生产
        // bootstrap 先 freeze 再起 loop，此处只兜配置/装配次序错误，log-only 不阻断。
        XtLogger::error_m("Reconcile",
            "account_store symbols NOT frozen at loop start — reverse phantom sweep "
            "would read an unfrozen registry cross-thread; check bootstrap ordering");
    }
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return;  // 已起
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
        // sleep with cancellation: stop() 会 notify_all 立即唤醒
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
    // 控制面 RESUME 跨线程请求重置 diverged_ 状态。
    // 防止"RESUME 清 kill_switch 后 reconcile 因残留 cnt >= kill_after_n 又
    // 重新 store(true) → operator 视角 RESUME 失败"的死锁。
    if (reset_request_external_ &&
        reset_request_external_->exchange(false, std::memory_order_acq_rel)) {
        // gauge 先复零再 clear：被 clear 的 key 若两侧此后都 flat，将退出全部三条
        // emit 路径（正向不含缺席 code、反向对 local-flat continue、兜底只迭代
        // diverged_ 现存 entry），prometheus gauge 会永久暴露残值 →
        // min_over_time recording rule 把 XtAccountStateDivergence 闩死。
        // 真分歧下一轮会重新 emit 真值——与 RESUME 清 counter 的重新计时语义一致。
        if (metrics_) {
            for (const auto& entry : diverged_) {
                metrics_->set_state_divergence("reconcile_qty", entry.first, 0.0);
            }
        }
        diverged_.clear();
        XtLogger::warn_m("Reconcile",
            "diverged state cleared (control panel RESUME) — counters reset to 0");
    }
    // position 与 ghost-order reconcile 互不依赖；任一失败不阻断另一个。
    reconcile_position();
    reconcile_orders();
}

void XtHftReconcileLoop::reconcile_position() {
    const uint64_t tick_now_ms = steady_now_ms();
    if (first_attempt_ms_ == 0) first_attempt_ms_ = tick_now_ms;

    auto exchange_pos = gw_.fetch_positions_for_reconcile();
    if (!exchange_pos.has_value()) {
        // fail-closed：本轮不做任何 exchange_qty=0 推断；只导出快照年龄，
        // 让连续失败导致的静默空转（无比对覆盖）可被告警看到。
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

        // 持续 divergence 下按 resync_throttle_ms 重试 resnap；阈值 0 = 禁用该级阶梯
        // （cnt 自增后 ≥1，不加 guard 时 0 会退化成每轮必触发）。
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

    // 正向：exchange 返回的每个 code 跟本地 diff。
    std::unordered_set<std::string> exchange_codes;
    exchange_codes.reserve(exchange_pos->size());
    for (const auto& p : *exchange_pos) {
        exchange_codes.insert(p.code);
        const int64_t exchange_net_fp = xt_fp::to_fp(p.long_qty - p.short_qty);
        const int64_t local_net_fp    = account_store_.snapshot_position(p.code).qty;
        const int64_t diff_fp         = std::abs(exchange_net_fp - local_net_fp);
        // 导出 store-vs-exchange divergence；双方一致时写 0，让 fill 双计这类
        // store-vs-broker 看不出的分歧可告警。
        if (metrics_) metrics_->set_state_divergence("reconcile_qty", p.code, xt_fp::from_fp(diff_fp));
        if (diff_fp > cfg_.position_epsilon_fp) {
            apply_ladder(p.code, exchange_net_fp, local_net_fp);
        } else {
            diverged_[p.code] = 0;
        }
    }

    // 反向：本地非零仓位但本周期 exchange 快照缺该 code → 视 exchange_qty=0 比对。
    // lost-rtn 场景下 close 已撮合但 ACCOUNT_UPDATE 丢失，本地仍有 +qty；Binance
    // positionRisk 拉取侧滤掉 amt==0 行，不做 reverse check 会漏掉这类幻仓。
    // 比对集合 = exchange 快照 codes ∪ 本地 tracked codes：post-freeze 读侧对未注册
    // code 恒返回空 snapshot，本地非零仓位必落在 tracked codes 上，重启后立即完备
    // （不依赖进程内累积的历史 seen 集合，那正是 never-seen 幻仓的漏检口径）。
    std::unordered_set<std::string> swept_local;
    for (const auto& code : account_store_.snapshot_tracked_codes()) {
        if (exchange_codes.count(code) > 0) continue;     // 已在正向 loop 处理
        const int64_t local_net_fp = account_store_.snapshot_position(code).qty;
        if (std::abs(local_net_fp) <= cfg_.position_epsilon_fp) continue;  // 双方均 flat
        swept_local.insert(code);
        // reverse check: exchange 本轮缺该 code（视为 0）但本地仍有仓位 → phantom。
        if (metrics_) metrics_->set_state_divergence("reconcile_qty", code, xt_fp::from_fp(std::abs(local_net_fp)));
        apply_ladder(code, /*exchange_net_fp=*/0, local_net_fp);
    }

    // 兜底复零：此前 diverged 的 code 本轮两侧都缺席/为零（exchange 不报 + 本地 flat）
    // = agreement，counter 必须清零，否则残留 cnt 会让下次无关分歧提前撞 kill ladder。
    for (auto& entry : diverged_) {
        if (entry.second == 0) continue;
        if (exchange_codes.count(entry.first) > 0) continue;
        if (swept_local.count(entry.first) > 0) continue;
        entry.second = 0;
        if (metrics_) metrics_->set_state_divergence("reconcile_qty", entry.first, 0.0);
    }

    // heartbeat 表示本 loop 仍在 emit divergence；rate==0 时由告警检测 emit dead。
    if (metrics_) metrics_->inc_state_divergence_emit_heartbeat();

    if (!any_diverged) {
        const uint64_t now = steady_now_ms();
        last_verified_ms_.store(now, std::memory_order_relaxed);
        // 同步刷新外部 last_position_snapshot_ms，供 PositionSyncStale guard 使用；
        // 与 UDS push 共写，idle account 也能保鲜。
        if (last_pos_snapshot_ms_external_) {
            last_pos_snapshot_ms_external_->store(now, std::memory_order_relaxed);
        }
        // full agreement 后清 last_resync_ms_；下一次 divergence 应从新的 resync
        // 计时开始，不能被旧 throttle 状态跳过首轮 resnap。
        last_resync_ms_.clear();
        // 自动清本 loop 拉起的 divergence kill_switch（source=2 不跨重启回灌，
        // 只可能是本进程内拉起）；以 source 为准:
        // operator 手动 HALT 会置 source=None → 不再归 reconcile → 不被本分支误清。
        // 未注入 source 时以 kill_set_by_reconcile_ 判归属。
        if (kill_switch_.load(std::memory_order_acquire)) {
            const bool owned_this_process = kill_set_by_reconcile_.load(std::memory_order_acquire);
            const bool reconcile_owned =
                kill_switch_source_
                    ? (kill_switch_source_->load(std::memory_order_acquire) ==
                       XtRunnerSignals::kKillSwitchSourceReconcileDivergence)
                    : owned_this_process;
            // "对账一致" 比 "flat" 更强：phantom 仓位下本地可能 flat 但与交易所不符。
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
    // gateway 已自己 emit 给 ITradeGateway::Callbacks，继续走 td queue → event loop →
    // engine 单写者路径；这里仅记录日志，避免数据流 race。
    XtLogger::warn_m("Reconcile",
        "ghost orders resolved (synthesized into td queue): count={}", *resolved);
}

}}  // namespace xt_live::control
