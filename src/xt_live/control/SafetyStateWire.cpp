// ── latched safety state 持久化（跨引擎共享实现） ──────────────────────────
//
// bootstrap reload / 周期落盘 safety.bin（kill_switch / open_halted /
// annual latch / per-sid latch）。函数体自 HFT 实现抽出（deps 化取代
// XtHftRunnerControlContext 直绑）；HFT 侧经 detail:: 薄 wrapper 委托。

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
#include "xt_risk/engine/XtRiskMonitorState.h"  // per-strategy latch + reconcile_liquidation_restore
#include "xt_risk/state/SafetyStateStore.h"

bool kill_switch_source_latches_across_restart(uint8_t source) noexcept {
    // deny-set 只有 ReconcileDivergence：唯一在新进程内有持续重评器（reconcile
    // loop 周期对账）的来源；未知/未来 source 保守回灌（fail-safe 保留保护）。
    return source != XtRunnerSignals::kKillSwitchSourceReconcileDivergence;
}

namespace xt_live::control {

xt_risk::state::SafetyState collect_safety_state(const SafetyStateWireDeps& deps) {
    xt_risk::state::SafetyState s;
    s.schema_version = xt_risk::state::SafetyStateStore::kSchemaVersion;
    s.kill_switch_active = deps.signals->kill_switch.load(std::memory_order_acquire);
    s.kill_switch_source = deps.signals->kill_switch_source.load(std::memory_order_acquire);

    if (deps.risk_monitor) {
        // const 访问绑定 const 重载；decision 字段唯一写者是 XtRiskMonitor 本体。
        const xt_risk::XtRiskMonitor* mon = deps.risk_monitor;
        const auto* bs = mon->broadcast_state();
        if (bs) {
            s.open_halted     = bs->open_halted.load(std::memory_order_acquire);
            s.halt_trigger_ms = static_cast<int64_t>(
                bs->open_halt_trigger_ms.load(std::memory_order_acquire));
            s.peak_equity_fp  = bs->peak_equity_fp.load(std::memory_order_acquire);
        }
    }

    // annual latch 经 gateway 复合快照单次读取（engine 内单 shared_lock，防组合撕裂）。
    const auto al = deps.order_gateway->annual_latch_snapshot();
    s.current_year          = al.current_year;
    s.year_start_equity_fp  = al.year_start_equity_fp;
    s.annual_cutoff_active  = al.annual_cutoff_active;
    // 年切冻结 anchor 持久化，作为 annual-loss 分子基准。
    s.year_start_net_pnl_fp = al.year_start_net_pnl_fp;

    if (deps.account_store) {
        s.frozen_cash_fp = deps.account_store->snapshot_account().frozen_cash;
        // risk_fund_balance = nmf_offset（跨重启累计）+ 本进程(realized - fees)。
        // 纯平仓盈亏不含 unrealized (重启 position 重建会重新计入 process_net_pnl,避免双算)。
        //   (unrealized 重启后由 broker avg 重算, 与重启前本地累计 avg 可能微小 discontinuity,
        //    属 exchange-as-truth recovery 固有取舍。)
        // save 记 current cumulative; load 作下次 nmf_offset。offset 只 load 设 → 无双算。
        {
            const auto acct = deps.account_store->snapshot_account();
            const int64_t nmf_offset = al.nmf_offset_fp;
            s.risk_fund_balance_fp = nmf_offset + (acct.realized_pnl - acct.fees);
        }
    }

    // per-sid kill_switch + auto-liquidation latch 持久化。
    // deps.risk_monitor_state nullptr（BT / 未注入）时 vector 保空，落盘为 0-length。
    if (deps.risk_monitor_state) {
        const auto& per = deps.risk_monitor_state->per_strategy;
        const std::size_t n = per.size();
        s.per_strategy_kill_switch.resize(n, 0);
        s.per_strategy_auto_liquidating.resize(n, 0);
        s.per_strategy_liquidating_source.resize(n, 0);
        for (std::size_t i = 0; i < n; ++i) {
            const auto sid = static_cast<uint32_t>(i);
            s.per_strategy_kill_switch[i] = per.is_strategy_killed(sid) ? 1 : 0;
            // latch+source 单源读：从 source 派生 latch flag，杜绝撕裂态
            // (latch=1, source=None) 落盘（latch 与 source 现为同一 atomic）。
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
            // 自动条件不跨重启闩锁：exchange-as-truth recovery 已重建本地账本，
            // 分歧若仍在由本进程 reconcile loop 在其 kill ladder 内重新拉起。
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
        // 单写入口：restore 走 supervisor method，不再直接 poke broadcast。
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
        // diagnostics-only：reducer 没有 set_frozen_cash 路径，venue snapshot 仍是真值。
        XtLogger::warn_m(deps.module_tag,
            "safety_state: bootstrap reload -> frozen_cash_fp={} (diagnostics only; venue "
            "snapshot is source of truth, audit if mismatch)", s.frozen_cash_fp);
    }

    // per-sid kill_switch restore。
    // 无 per-sid 字段的 safety 文件加载后 vector 为空，for-loop 不跑，per_strategy
    // 保持默认 false。带 per-sid 字段的文件会 restore 每个 sid 的 kill_switch 状态；
    // 语义与 global kill_switch 一致：false 不覆盖 in-memory 已有 true。
    // deps.risk_monitor_state 为空时跳过。
    if (deps.risk_monitor_state && !s.per_strategy_kill_switch.empty()) {
        auto& per = deps.risk_monitor_state->per_strategy;
        const std::size_t n_disk = s.per_strategy_kill_switch.size();
        const std::size_t n_mem = per.size();
        const std::size_t n = std::min(n_disk, n_mem);
        std::size_t restored = 0;
        for (std::size_t i = 1; i < n; ++i) {  // sid_id=0 是 reserved
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

    // per-sid auto-liquidation latch restore（含 source）。
    // 语义同 kill_switch：false 不覆盖 in-memory true。restore 只把 latch 推到 true，
    // 真正 disarm 由 PerSidLiquidator::step() 在 account hydrate 确认 NoPosition 后做
    // （restart auto-disarm-on-flat gate）——避免崩溃在 mid-liquidation 后丢保护。
    // restore_auto_liquidation=false（CTA/SEL）时整段跳过：disarm owner 不存在，
    // 回灌会产生无人能解除的「清算中」僵死态（campaign Fable P2-1 scope-out）。
    if (deps.restore_auto_liquidation && deps.risk_monitor_state &&
        (!s.per_strategy_auto_liquidating.empty() ||
         !s.per_strategy_liquidating_source.empty())) {
        auto& per = deps.risk_monitor_state->per_strategy;
        const std::size_t n_latch = s.per_strategy_auto_liquidating.size();
        const std::size_t n_src   = s.per_strategy_liquidating_source.size();
        const std::size_t n = std::min(std::max(n_latch, n_src), per.size());
        std::size_t restored = 0;
        for (std::size_t i = 1; i < n; ++i) {  // sid_id=0 是 reserved
            const uint8_t latch_byte = (i < n_latch) ? s.per_strategy_auto_liquidating[i] : 0;
            const uint8_t src_byte   = (i < n_src) ? s.per_strategy_liquidating_source[i] : 0;
            // union + source 清洗契约见 xt_risk::reconcile_liquidation_restore：
            // 任一非零即恢复（fail-safe 不丢保护），缺失/非法 source 降级 DailyLoss。
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

// quarantine 事件累计（进程内可观测，不接 prometheus）。
std::atomic<uint64_t> g_quarantine_count{0};

int64_t now_wall_ms() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// safety.bin 存在但校验失败：quarantine 原件 + 带闩启动（fail-closed）。
// rename 必须先于任何 bootstrap 落盘，避免损坏原件被后续默认值 save 覆写。
void quarantine_and_latch(const SafetyStateWireDeps& deps, const std::string& path) {
    const int64_t wall_ms = now_wall_ms();

    // (a) rename 原件到 <path>.quarantine；同名已存在则追加 epoch ms 后缀保留既有证据。
    std::error_code ec;
    const std::filesystem::path src(path);
    std::filesystem::path dst(path + ".quarantine");
    if (std::filesystem::exists(dst, ec) || ec) {
        dst = std::filesystem::path(path + ".quarantine." + std::to_string(wall_ms));
    }
    std::filesystem::rename(src, dst, ec);
    if (ec) {
        // rename 失败不阻断带闩路径；仅记录（原件仍在原位，可能被后续 save 覆写）。
        XtLogger::error_m(deps.module_tag,
            "safety_state: INVALID file path={} quarantine rename FAILED ({}) -> latching anyway",
            path, ec.message());
    } else {
        XtLogger::error_m(deps.module_tag,
            "safety_state: INVALID file path={} quarantined to {}", path, dst.string());
    }

    g_quarantine_count.fetch_add(1, std::memory_order_relaxed);

    // (b) synthetic 带闩 state：kill_switch + open_halted；source=manual(0) 使
    // auto-release 路径不自动解除，必须 operator 人工处置后才恢复交易。
    xt_risk::state::SafetyState latched;
    latched.kill_switch_active = true;
    latched.kill_switch_source = XtRunnerSignals::kKillSwitchSourceNone;
    latched.open_halted        = true;
    // open_halt_trigger_ms 语义是 frame steady ms 低 32 位（restore_latches 会 uint32
    // 截断）；synthetic bootstrap 闩无精确 steady 触发时刻，置 0 表示「启动即带闩」，
    // 避免 wall ms(~1.75e12) 截断成垃圾值污染日志与 safety.bin 往返。
    latched.halt_trigger_ms    = 0;
    apply_safety_state_on_bootstrap(deps, latched);

    // (c) critical 日志：bootstrap 期一次性，说明带闩原因与处置。
    XtLogger::error_m(deps.module_tag,
        "safety_state: bootstrap FAIL-CLOSED path={} -> kill_switch=true + open_halted=true "
        "(file existed but failed validation; manual source, operator must clear)", path);
}

}  // namespace

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
        // 冷启动正常路径：文件不存在，用默认值继续。
        XtLogger::info_m(deps.module_tag,
            "safety_state: file missing path={} -> cold-start defaults", path);
        return;
    case xt_risk::state::LoadStatus::Invalid:
        // 文件存在但损坏：绝不当冷启动清零 latch，改 fail-closed 带闩启动。
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
    // material = halt 类 latch 转换（kill_switch / open_halted / halt_trigger /
    // annual_cutoff / per-sid kill）——运营关键且低频，不会刷屏。
    // frozen_cash/peak_equity 每帧抖动也会令 changed=true → 落盘照旧，但 INFO 只在
    // material 变化时打，避免无诊断价值的刷屏（必须在 last_state=cur 之前捕获）。
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
            // 非 material 落盘（frozen_cash/peak_equity 抖动）降 DEBUG，不刷屏 INFO。
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

}  // namespace xt_live::control
