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

    enum Reason : std::size_t {
        kSafetyStateQuarantine = 0,
        kPnlInvalidLoad,
        kSidJournalFailure,
        kOrphanReapSuppressed,
        kVenueFillupSanitized,
        kStrategyInitFailure,
        kExecLedgerTerminalEvicted,
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
        bool                          any = false;
        std::array<uint64_t, kCount>  delta{};
        std::array<bool, kCount>      warn{};
    };

    explicit SideChannelFailureDelta(uint64_t throttle_ms = 60'000) noexcept
        : throttle_ms_(throttle_ms) {}

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
                baseline_[i] = cur[i];
                continue;
            }
            out.delta[i] = cur[i] - baseline_[i];
            const bool throttle_ok =
                last_warn_ms_[i] == 0 || now_ms - last_warn_ms_[i] >= throttle_ms_;
            if (out.delta[i] > 0 && throttle_ok) {
                out.warn[i] = true;
                out.any = true;
                baseline_[i] = cur[i];
                last_warn_ms_[i] = now_ms;
            }
        }
        return out;
    }

private:
    uint64_t throttle_ms_;
    std::array<uint64_t, kCount> baseline_{};
    std::array<uint64_t, kCount> last_warn_ms_{};
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
        bool warn_enter = false;
        bool warn_during = false;
        bool info_recover = false;
    };

    explicit DegradedLatchState(uint64_t throttle_ms = 60'000) noexcept
        : throttle_ms_(throttle_ms) {}

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
    bool     latched_ = false;
    uint64_t last_warn_ms_ = 0;
};

}
