#pragma once

/// @file
/// @brief 本批硬化侧道 counter 的帧间 delta + 限频判定，及降级闩状态转移判定（纯逻辑，无外部依赖）。
///
/// 与 RecorderFailureDelta（recorder 失败向）同模式：在 1Hz frame-tail 消费进程内累计
/// counter —— safety.bin quarantine / per-sid PnL 状态无效加载 / SID 归属 journal 失败 /
/// journal 降级下 orphan reap 抑制 / venue fillup 量 sanitize / 策略 on_init 失败 /
/// 交付投影终态域 FIFO 淘汰（CTP exec-ledger）—— 输出「相对上次告警的新增量」并对每个
/// 原因独立限频。纯类、可直接单测。
///
/// 与 RecorderFailureDelta 的**刻意差异**：不做首帧基线抑制。这些 counter 均为进程内
/// atomic（重启清零，无跨重启历史），且多数在 bootstrap（frame loop 之前）就可能累计
/// （safety.bin 损坏 quarantine / PnL 状态无效弃用 / 策略 on_init 失败都发生在启动期），
/// 那些启动期硬化失败正是要 surface 的信号；基线恒从 0 起，首帧非零即告警。
///
/// DegradedLatchState 处理布尔降级闩（SID journal degraded）：进入 warn 一次、持续降级
/// 期低频提醒、复原 info；delta 语义对布尔状态不适用，故单列。

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace xt_live::control {

/// @brief 本批硬化侧道 counter 帧间 delta tracker（每原因独立限频）。
class SideChannelFailureDelta {
public:
    /// @brief 观测的侧道 counter 原因；顺序即 observe() 入参顺序与 delta 数组下标。
    enum Reason : std::size_t {
        kSafetyStateQuarantine = 0,  ///< safety.bin 损坏 quarantine（G1a）
        kPnlInvalidLoad,             ///< per-sid PnL 状态无效整文件弃用（G1b）
        kSidJournalFailure,          ///< SID 归属 journal append/compaction 失败（G2a）
        kOrphanReapSuppressed,       ///< journal 降级下 orphan reap 被抑制的 sweep（G2a）
        kVenueFillupSanitized,       ///< reverse-sync venue 回传量越界被 clamp（G2b）
        kStrategyInitFailure,        ///< 策略 on_init 失败被 fail-closed disable（G4）
        kExecLedgerTerminalEvicted,  ///< 交付投影终态域 FIFO 淘汰（CTP exec-ledger 淘汰口径）
        kCount
    };

    static constexpr std::string_view reason_name(std::size_t r) noexcept {
        switch (r) {
            case kSafetyStateQuarantine:    return "safety_state_quarantine";
            case kPnlInvalidLoad:           return "pnl_invalid_load";
            case kSidJournalFailure:        return "sid_journal_failure";
            case kOrphanReapSuppressed:     return "orphan_reap_suppressed";
            case kVenueFillupSanitized:     return "venue_fillup_qty_sanitized";
            case kStrategyInitFailure:      return "strategy_init_failure";
            case kExecLedgerTerminalEvicted:return "exec_ledger_terminal_evicted";
            default:                        return "unknown";
        }
    }

    /// @brief 单帧 delta + 告警决策。
    struct Report {
        bool                          any = false;  ///< 是否有任一原因触发告警
        std::array<uint64_t, kCount>  delta{};      ///< 各原因相对上次告警基线的新增量
        std::array<bool, kCount>      warn{};       ///< 各原因本帧是否应告警（新增且过限频窗）
    };

    explicit SideChannelFailureDelta(uint64_t throttle_ms = 60'000) noexcept
        : throttle_ms_(throttle_ms) {}

    /// @brief 喂本帧各累计 counter，返回 delta + 限频后的告警决策。
    /// @param now_ms  steady clock ms，限频窗判定用。
    /// @param exec_ledger_terminal_evicted  交付投影终态域 FIFO 淘汰累计（CTP-only 进程静态；
    ///        binance runner 不用该 ledger，恒 0；默认 0 兼容不喂该 counter 的 caller）。
    /// @note 基线恒从 0 起（不做首帧抑制，见文件头说明）；counter 单调递增，出现回退
    ///       （进程内不应发生）时静默 re-baseline。限频窗内不推进基线，让下一条告警带上
    ///       窗口内积攒的完整增量。
    Report observe(uint64_t now_ms,
                   uint64_t safety_state_quarantine,
                   uint64_t pnl_invalid_load,
                   uint64_t sid_journal_failure,
                   uint64_t orphan_reap_suppressed,
                   uint64_t venue_fillup_sanitized,
                   uint64_t strategy_init_failure,
                   uint64_t exec_ledger_terminal_evicted = 0) noexcept {
        const std::array<uint64_t, kCount> cur{
            safety_state_quarantine, pnl_invalid_load, sid_journal_failure,
            orphan_reap_suppressed, venue_fillup_sanitized, strategy_init_failure,
            exec_ledger_terminal_evicted};
        Report out;
        for (std::size_t i = 0; i < kCount; ++i) {
            if (cur[i] < baseline_[i]) {
                baseline_[i] = cur[i];  // 回退兜底：silent re-baseline，不告警。
                continue;
            }
            out.delta[i] = cur[i] - baseline_[i];
            const bool throttle_ok =
                last_warn_ms_[i] == 0 || now_ms - last_warn_ms_[i] >= throttle_ms_;
            if (out.delta[i] > 0 && throttle_ok) {
                out.warn[i] = true;
                out.any = true;
                baseline_[i] = cur[i];      // 告警后推进基线，下条 delta 重新累计。
                last_warn_ms_[i] = now_ms;
            }
        }
        return out;
    }

private:
    uint64_t throttle_ms_;
    std::array<uint64_t, kCount> baseline_{};      ///< 上次告警（或初始 0）时的 counter 快照
    std::array<uint64_t, kCount> last_warn_ms_{};  ///< 各原因上次告警 steady ms（0=从未）
};

/// @brief 布尔降级闩的状态转移判定（进入告警一次 / 持续期低频提醒 / 复原信息）。
///
/// 用于 SID 归属 journal degraded 这类「持续降级 state」而非「累计 count」的观测：
/// 降级期间 orphan reap 切告警-only、归属落盘不权威，需要 operator 处置——既要首次进入
/// 时立刻告警，又不能每帧刷屏，复原时给一条确认。纯类、可直接单测。
class DegradedLatchState {
public:
    /// @brief 单帧状态转移决策。
    struct Report {
        bool warn_enter = false;    ///< false→true 本帧（首次进入降级；含 boot 即降级）
        bool warn_during = false;   ///< 持续降级且过限频窗（低频提醒）
        bool info_recover = false;  ///< true→false 本帧（降级复原）
    };

    explicit DegradedLatchState(uint64_t throttle_ms = 60'000) noexcept
        : throttle_ms_(throttle_ms) {}

    /// @brief 喂本帧降级状态，返回状态转移决策。
    /// @param now_ms   steady clock ms，持续期限频窗判定用。
    /// @param degraded 当前是否处于降级态。
    Report observe(uint64_t now_ms, bool degraded) noexcept {
        Report out;
        if (degraded) {
            if (!latched_) {
                latched_ = true;
                out.warn_enter = true;
                last_warn_ms_ = now_ms;
            } else if (last_warn_ms_ == 0 || now_ms - last_warn_ms_ >= throttle_ms_) {
                out.warn_during = true;
                last_warn_ms_ = now_ms;
            }
        } else if (latched_) {
            latched_ = false;
            out.info_recover = true;
        }
        return out;
    }

private:
    uint64_t throttle_ms_;
    bool     latched_ = false;      ///< 当前是否处于降级闩内
    uint64_t last_warn_ms_ = 0;     ///< 上次（进入或提醒）告警 steady ms（0=从未）
};

}  // namespace xt_live::control
