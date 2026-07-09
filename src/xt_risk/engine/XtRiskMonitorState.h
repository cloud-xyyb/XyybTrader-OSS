#pragma once

#include <atomic>
#include <cstdint>
#include <optional>
#include <vector>

/// @file
/// @brief 风控监视结论的共享状态板。
///
/// XtRiskMonitor 每帧把 open_halted、volume_scale、IDD 等结论写进来，
/// submit 热路径、control 和 telemetry 用 atomic 读走；
/// per-sid 的 kill/liquidation latch 用 sid_id 直接作数组下标。

namespace xt_risk {

constexpr int64_t kVolumeScaleUnit = 1000000;

enum class XtOpenHaltReason : uint8_t {
    None        = 0,
    IddDrawdown = 1,
    StaleQuote  = 2,
};

enum class XtAutoLiquidateSource : uint8_t {
    None       = 0,
    DailyLoss  = 1,
    KillSwitch = 2,
};

/// @brief Per-sid kill switch 与 auto-liquidation latch 的 runtime state。
///
/// `sid_id == 0` 是 reserved/no-sid sentinel；reader 对 sid 0/越界返回安全默认值，
/// writer no-op。vector 下标直接等于 sid_id，容量必须由 freeze 后的 sid registry
/// 槽位数初始化。状态通过 atomic acquire/release 暴露给 submit、control 与持久化采集路径；
/// SafetyStateStore 的落盘和恢复由调用方调度。
class PerStrategyRiskRuntimeState {
public:

    void init(std::size_t max_strategies) {
        kill_switch_.clear();
        kill_switch_ = std::vector<std::atomic<bool>>(max_strategies);

        liquidating_state_.clear();
        liquidating_state_ = std::vector<std::atomic<uint8_t>>(max_strategies);
    }

    bool is_strategy_killed(uint32_t sid_id) const noexcept {
        if (sid_id == 0 || sid_id >= kill_switch_.size()) return false;
        return kill_switch_[sid_id].load(std::memory_order_acquire);
    }

    void set_strategy_killed(uint32_t sid_id, bool v) noexcept {
        if (sid_id == 0 || sid_id >= kill_switch_.size()) return;
        kill_switch_[sid_id].store(v, std::memory_order_release);
    }

    bool is_auto_liquidating(uint32_t sid_id) const noexcept {
        if (sid_id == 0 || sid_id >= liquidating_state_.size()) return false;
        return liquidating_state_[sid_id].load(std::memory_order_acquire) != 0;
    }

    void set_auto_liquidating(uint32_t sid_id, bool v,
                              XtAutoLiquidateSource src) noexcept {
        if (sid_id == 0 || sid_id >= liquidating_state_.size()) return;
        const uint8_t st = v ? static_cast<uint8_t>(src)
                             : static_cast<uint8_t>(XtAutoLiquidateSource::None);
        liquidating_state_[sid_id].store(st, std::memory_order_release);
    }

    void clear_auto_liquidating(uint32_t sid_id) noexcept {
        set_auto_liquidating(sid_id, false, XtAutoLiquidateSource::None);
    }

    XtAutoLiquidateSource auto_liquidating_source(uint32_t sid_id) const noexcept {
        if (sid_id == 0 || sid_id >= liquidating_state_.size())
            return XtAutoLiquidateSource::None;
        return static_cast<XtAutoLiquidateSource>(
            liquidating_state_[sid_id].load(std::memory_order_acquire));
    }

    std::size_t reset_daily_loss_latches() noexcept {
        std::size_t cleared = 0;
        for (uint32_t sid = 1; sid < liquidating_state_.size(); ++sid) {

            uint8_t expected = static_cast<uint8_t>(XtAutoLiquidateSource::DailyLoss);
            if (liquidating_state_[sid].compare_exchange_strong(
                    expected, static_cast<uint8_t>(XtAutoLiquidateSource::None),
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {

                set_strategy_killed(sid, false);
                ++cleared;
            }
        }
        return cleared;
    }

    std::size_t size() const noexcept { return kill_switch_.size(); }

private:
    std::vector<std::atomic<bool>>    kill_switch_;

    std::vector<std::atomic<uint8_t>> liquidating_state_;
};

inline std::optional<XtAutoLiquidateSource> reconcile_liquidation_restore(
    uint8_t latch_byte, uint8_t source_byte) noexcept {
    if (latch_byte == 0 && source_byte == 0) return std::nullopt;
    return (source_byte == static_cast<uint8_t>(XtAutoLiquidateSource::KillSwitch))
               ? XtAutoLiquidateSource::KillSwitch
               : XtAutoLiquidateSource::DailyLoss;
}

/// @brief Active risk monitor 发布给 submit、control 和 telemetry 的 atomic broadcast state。
///
/// XtRiskMonitor 是 open_halted、volume_scale、IDD 字段的主要 writer；submit 热路径只做
/// acquire-load，不持有 monitor 锁。open_halted 是 no-open latch：新开仓拒绝，平仓通行。
/// 时间字段使用 frame loop 的 steady milliseconds；0 表示尚未触发或尚未更新。
class XtRiskMonitorState {
public:
    std::atomic<bool> open_halted{false};
    std::atomic<int64_t> volume_scale_fp{kVolumeScaleUnit};
    std::atomic<uint64_t> version{0};

    std::atomic<int64_t> peak_equity_fp{0};
    std::atomic<int64_t> current_drawdown_bp{0};
    std::atomic<uint32_t> last_update_ms{0};

    std::atomic<uint8_t> open_halt_reason{
        static_cast<uint8_t>(XtOpenHaltReason::None)};
    std::atomic<uint32_t> open_halt_trigger_ms{0};

    PerStrategyRiskRuntimeState per_strategy;

    int64_t volume_scale_fp_load() const noexcept {
        return volume_scale_fp.load(std::memory_order_acquire);
    }

    bool open_halted_load() const noexcept {
        return open_halted.load(std::memory_order_acquire);
    }

    XtOpenHaltReason open_halt_reason_load() const noexcept {
        return static_cast<XtOpenHaltReason>(
            open_halt_reason.load(std::memory_order_acquire));
    }
};

}
