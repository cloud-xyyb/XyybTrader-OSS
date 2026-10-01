#include "xt_risk/engine/XtRiskMonitor.h"

#include "xt_account/state/AccountStateStore.h"
#include "XtLogger.h"

namespace xt_risk {

XtRiskMonitor::XtRiskMonitor(Config cfg, XtRiskMonitorState* broadcast)
    : cfg_(cfg), broadcast_(broadcast) {}

void XtRiskMonitor::set_account_store(xt_account::AccountStateStore* store) noexcept {
    account_store_ = store;
}

void XtRiskMonitor::on_session_begin(uint32_t trading_date, int64_t current_equity_fp,
                                     int64_t current_cashflow_fp) {
    // 同一 trading_date 只执行一次 reset；trading_date=0 用 first-call sentinel。
    if (trading_date != 0) {
        if (trading_date == last_session_date_) return;
        last_session_date_ = trading_date;
    } else {
        if (session_initialized_) return;
        session_initialized_ = true;
    }

    peak_equity_fp_ = current_equity_fp;
    // 冻结 session 起点外部出入金；on_frame 只从 IDD 中扣除 session 内增量。
    session_open_cashflow_fp_ = current_cashflow_fp;
    if (broadcast_) {
        broadcast_->peak_equity_fp.store(current_equity_fp, std::memory_order_release);
        broadcast_->open_halted.store(false, std::memory_order_release);
        broadcast_->open_halt_reason.store(
            static_cast<uint8_t>(XtOpenHaltReason::None), std::memory_order_release);
        broadcast_->open_halt_trigger_ms.store(0, std::memory_order_release);
        broadcast_->volume_scale_fp.store(kVolumeScaleUnit, std::memory_order_release);
        broadcast_->current_drawdown_bp.store(0, std::memory_order_release);
        broadcast_->version.fetch_add(1, std::memory_order_release);
        // 新 session 日内亏损预算重置 → 清 DailyLoss-source auto-liquidation latch + 连带 kill，
        // 策略次日可再交易。KillSwitch-source latch 不动（operator 显式清）。dedup 已在函数顶部，
        // 每 session 切换只跑一次，与 PerSidLiquidator::step 同 control 线程。
        const std::size_t cleared = broadcast_->per_strategy.reset_daily_loss_latches();
        if (cleared > 0) {
            XtLogger::info_m("risk",
                "session_begin: cleared {} DailyLoss auto-liquidation latch(es) "
                "(date={})", cleared, trading_date);
        }
    }

    // starting_balance 与 peak reset 同步冻结；account_store_ 缺省时跳过对应规则。
    if (account_store_) {
        account_store_->on_session_begin_hook(current_equity_fp);
    }
}

void XtRiskMonitor::restore_latches(bool open_halted, int64_t halt_trigger_ms,
                                    int64_t peak_equity_fp) {
    if (!broadcast_) return;
    // 恢复只允许把 latch 推到 true；运行期清除必须走 on_session_begin()。
    if (open_halted) {
        broadcast_->open_halted.store(true, std::memory_order_release);
        broadcast_->open_halt_trigger_ms.store(static_cast<uint32_t>(halt_trigger_ms),
                                               std::memory_order_release);
        broadcast_->version.fetch_add(1, std::memory_order_release);
        XtLogger::warn_m("RISK",
            "safety_state restore -> open_halted=true trigger_ms={} (latched across restart)",
            halt_trigger_ms);
    }
    if (peak_equity_fp > 0) {
        const int64_t cur = broadcast_->peak_equity_fp.load(std::memory_order_acquire);
        const int64_t reload_peak = peak_equity_fp > cur ? peak_equity_fp : cur;
        broadcast_->peak_equity_fp.store(reload_peak, std::memory_order_release);
        XtLogger::info_m("RISK",
            "safety_state restore -> peak_equity_fp={} (max with current={})", reload_peak, cur);
    }
}

void XtRiskMonitor::on_frame(int64_t equity_fp, uint32_t last_md_tick_ms, uint32_t now_ms,
                             int64_t current_cashflow_fp) {
    if (!cfg_.enabled || !broadcast_) return;
    // cashflow-immune IDD：只度量交易盈亏；未提供 cashflow 时 risk_equity 等于 equity。
    const int64_t risk_equity_fp =
        equity_fp - (current_cashflow_fp - session_open_cashflow_fp_);
    update_drawdown(risk_equity_fp, now_ms);
    update_stale_quote(last_md_tick_ms, now_ms);
    broadcast_->last_update_ms.store(now_ms, std::memory_order_release);
}

void XtRiskMonitor::update_drawdown(int64_t equity_fp, uint32_t now_ms) {
    if (equity_fp > peak_equity_fp_) {
        peak_equity_fp_ = equity_fp;
        broadcast_->peak_equity_fp.store(equity_fp, std::memory_order_release);
    }
    if (peak_equity_fp_ <= 0) return;

    const int64_t drawdown_fp = peak_equity_fp_ - equity_fp;
    const int64_t drawdown_bp = (drawdown_fp * 10000) / peak_equity_fp_;
    broadcast_->current_drawdown_bp.store(drawdown_bp, std::memory_order_release);

    // halt 优先于 scale；达到 halt 后不再下调 volume scale。
    // 单写者（control frame loop；restore_latches 仅 bootstrap）：先写归因字段、
    // 后 release 发布 open_halted —— acquire reader 见 halted=true 必见 reason/trigger，
    // 消除 halted+None 短窗；已 latch 时跳过，保留首次触发归因。
    bool changed = false;
    if (drawdown_bp >= cfg_.idd_halt_threshold_bp) {
        if (!broadcast_->open_halted.load(std::memory_order_relaxed)) {
            broadcast_->open_halt_reason.store(
                static_cast<uint8_t>(XtOpenHaltReason::IddDrawdown),
                std::memory_order_relaxed);
            broadcast_->open_halt_trigger_ms.store(now_ms, std::memory_order_relaxed);
            broadcast_->open_halted.store(true, std::memory_order_release);
            changed = true;
            // 在 latch 建立点记录原因，避免 reader 侧反推触发源。
            XtLogger::warn_m("RISK",
                "open_halted latched: IddDrawdown drawdown_bp={} now_ms={}", drawdown_bp, now_ms);
        }
    } else if (drawdown_bp >= cfg_.idd_scale_threshold_bp) {
        const int64_t cur_scale = broadcast_->volume_scale_fp.load(std::memory_order_acquire);
        if (cur_scale > cfg_.idd_scale_factor_fp) {
            broadcast_->volume_scale_fp.store(cfg_.idd_scale_factor_fp,
                                              std::memory_order_release);
            changed = true;
        }
    }

    if (changed) {
        broadcast_->version.fetch_add(1, std::memory_order_release);
    }
}

bool XtRiskMonitor::sanity_check_per_sid_state(std::size_t expected_size) const noexcept {
    if (!broadcast_) return true;
    // sid_registry 未初始化或单策略路径没有 per-sid 容量约束。
    if (expected_size == 0) return true;
    const auto& per = broadcast_->per_strategy;
    const std::size_t actual = per.size();
    if (actual == expected_size) return true;
    // 只记录一致性错误，不在 frame loop 中 abort。
    XtLogger::error_m("RISK",
        "integrity drift: per_strategy.size={} != sid_registry.sid_slot_count={} "
        "(per-sid vector 跟 sid count 不一致; ops 排查 init 时序)",
        actual, expected_size);
    return false;
}

void XtRiskMonitor::update_stale_quote(uint32_t last_md_tick_ms, uint32_t now_ms) {
    if (cfg_.stale_quote_ms == 0) return;
    if (last_md_tick_ms == 0) return;
    // 乱序/回拨防御：无符号减回卷会得巨大 elapsed 误触发 halt（与
    // guard_before_submit 的 now < last 检查同型，此前不对称）。
    if (now_ms < last_md_tick_ms) return;

    const uint32_t elapsed = now_ms - last_md_tick_ms;
    if (elapsed <= cfg_.stale_quote_ms) return;

    // 同 update_drawdown：单写者下先写归因、后 release 发布，已 latch 跳过。
    if (!broadcast_->open_halted.load(std::memory_order_relaxed)) {
        broadcast_->open_halt_reason.store(
            static_cast<uint8_t>(XtOpenHaltReason::StaleQuote),
            std::memory_order_relaxed);
        broadcast_->open_halt_trigger_ms.store(now_ms, std::memory_order_relaxed);
        broadcast_->open_halted.store(true, std::memory_order_release);
        broadcast_->version.fetch_add(1, std::memory_order_release);
        // 在 latch 建立点记录原因，避免 reader 侧反推触发源。
        XtLogger::warn_m("RISK",
            "open_halted latched: StaleQuote elapsed_ms={} now_ms={}", elapsed, now_ms);
    }
}

}  // namespace xt_risk
