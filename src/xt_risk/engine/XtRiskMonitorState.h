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

/// @brief volume_scale_fp 的 fixed-point 单位；1'000'000 表示 100% 原始下单量。
constexpr int64_t kVolumeScaleUnit = 1000000;

/// @brief open_halted no-open latch 的触发来源，用于日志、面板和 safety state 诊断。
enum class XtOpenHaltReason : uint8_t {
    None        = 0,
    IddDrawdown = 1, ///< IDD 回撤达到 halt 阈值。
    StaleQuote  = 2, ///< 行情长时间未更新。
};

/// @brief Per-sid auto-liquidation latch 的触发源；决定重启恢复和 session reset 语义。
enum class XtAutoLiquidateSource : uint8_t {
    None       = 0,
    DailyLoss  = 1, ///< daily-loss 突破触发；session-scoped，次 session_begin 清。
    KillSwitch = 2, ///< kill_switch 触发；持久，operator 清 kill 时连带清。
};

/// @brief Per-sid kill switch 与 auto-liquidation latch 的 runtime state。
///
/// `sid_id == 0` 是 reserved/no-sid sentinel；reader 对 sid 0/越界返回安全默认值，
/// writer no-op。vector 下标直接等于 sid_id，容量必须由 freeze 后的 sid registry
/// 槽位数初始化。状态通过 atomic acquire/release 暴露给 submit、control 与持久化采集路径；
/// SafetyStateStore 的落盘和恢复由调用方调度。
class PerStrategyRiskRuntimeState {
public:
    /// @brief 重新初始化 per-sid 状态槽；会清空已有 kill/liquidation latch。
    /// @param max_strategies sid slot count，包含 reserved sid 0。
    /// @warning 仅在启动 wiring 或 sid registry freeze 后调用；运行期扩容会丢弃已有 latch。
    void init(std::size_t max_strategies) {
        kill_switch_.clear();
        kill_switch_ = std::vector<std::atomic<bool>>(max_strategies);
        // vector<atomic>(n) value-initializes 为 false/0；0 也是 XtAutoLiquidateSource::None。
        liquidating_state_.clear();
        liquidating_state_ = std::vector<std::atomic<uint8_t>>(max_strategies);
    }

    /// @brief 查询 per-sid kill switch；越界返回 false。
    /// @note acquire-load 与 set_strategy_killed() 的 release-store 配对。
    bool is_strategy_killed(uint32_t sid_id) const noexcept {
        if (sid_id == 0 || sid_id >= kill_switch_.size()) return false;
        return kill_switch_[sid_id].load(std::memory_order_acquire);
    }
    /// @brief 设置 per-sid kill switch；越界 no-op。
    /// @note caller 负责决定该 latch 是否需要同步落盘；本类只维护 runtime atomic 状态。
    void set_strategy_killed(uint32_t sid_id, bool v) noexcept {
        if (sid_id == 0 || sid_id >= kill_switch_.size()) return;
        kill_switch_[sid_id].store(v, std::memory_order_release);
    }

    // per-sid auto-liquidation latch/source
    // latch + source 合成单个 atomic<uint8_t>（0=None=未 latch，else=XtAutoLiquidateSource）。
    // 单字段保证 latch 与 source 的读写原子一致——control 线程（step/session_begin/collect）
    // 与 gRPC 线程（SetKillSwitch）并发访问时，不会出现 (latched, None) 之类的撕裂态。

    /// @brief 查询 per-sid auto-liquidation latch；越界返回 false。
    /// @note 跨线程：control + gRPC 写（release），engine/control 读（acquire）。
    bool is_auto_liquidating(uint32_t sid_id) const noexcept {
        if (sid_id == 0 || sid_id >= liquidating_state_.size()) return false;
        return liquidating_state_[sid_id].load(std::memory_order_acquire) != 0;
    }
    /// @brief arm/disarm per-sid auto-liquidation latch + 来源；越界 no-op。
    /// @warning `v=true` 且 `src==None` 会写成未 latch；arm 时必须传 DailyLoss/KillSwitch。
    /// @note 单 atomic release-store；acquire 读到非 0 即同时见到 source。
    void set_auto_liquidating(uint32_t sid_id, bool v,
                              XtAutoLiquidateSource src) noexcept {
        if (sid_id == 0 || sid_id >= liquidating_state_.size()) return;
        const uint8_t st = v ? static_cast<uint8_t>(src)
                             : static_cast<uint8_t>(XtAutoLiquidateSource::None);
        liquidating_state_[sid_id].store(st, std::memory_order_release);
    }
    /// @brief disarm（== set_auto_liquidating(sid,false,None)）；越界 no-op。
    void clear_auto_liquidating(uint32_t sid_id) noexcept {
        set_auto_liquidating(sid_id, false, XtAutoLiquidateSource::None);
    }
    /// @brief 查询 latch 触发来源；越界/未 arm 返回 None。与 is_auto_liquidating 同源单读。
    XtAutoLiquidateSource auto_liquidating_source(uint32_t sid_id) const noexcept {
        if (sid_id == 0 || sid_id >= liquidating_state_.size())
            return XtAutoLiquidateSource::None;
        return static_cast<XtAutoLiquidateSource>(
            liquidating_state_[sid_id].load(std::memory_order_acquire));
    }

    /// @brief session_begin 清理 DailyLoss-source latch，并同步释放对应 kill switch。
    /// @return 已清理的 sid 数，供日志/metrics 使用。
    /// @note 只清 DailyLoss；KillSwitch-source latch 由 operator resume/clear 流程显式释放。
    ///       XtRiskMonitor::on_session_begin() 在 control 线程按 trading_date 去重后调用。
    std::size_t reset_daily_loss_latches() noexcept {
        std::size_t cleared = 0;
        for (uint32_t sid = 1; sid < liquidating_state_.size(); ++sid) {  // sid 0 reserved/no-sid
            // CAS DailyLoss → None：若并发 gRPC SetKillSwitch 已把 source 升级为 KillSwitch，
            // CAS 失败并保留该 sid 的 persistent operator kill。
            uint8_t expected = static_cast<uint8_t>(XtAutoLiquidateSource::DailyLoss);
            if (liquidating_state_[sid].compare_exchange_strong(
                    expected, static_cast<uint8_t>(XtAutoLiquidateSource::None),
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                // CAS 成功后必须释放 DailyLoss 连带 kill。若 operator kill 正好落在这几条
                // 指令窗口内，调用方需要以面板状态为准重试；此 session reset 路径不加锁。
                set_strategy_killed(sid, false);
                ++cleared;
            }
        }
        return cleared;
    }

    /// @brief sid slot 容量，包含 reserved sid 0；未 init 时为 0。
    std::size_t size() const noexcept { return kill_switch_.size(); }

private:
    std::vector<std::atomic<bool>>    kill_switch_; ///< sid-indexed kill switch；true 表示禁止新开仓。

    /// @brief sid-indexed auto-liquidation latch/source（SafetyState v5 持久化字段）。
    /// @note 取值 0=None；非 0 必须是 XtAutoLiquidateSource。latch 与 source 合一，
    ///       使落盘采集不会读到 (latched=true, source=None) 的撕裂态。
    std::vector<std::atomic<uint8_t>> liquidating_state_;
};


/// @brief SafetyState per-sid auto-liquidation 恢复判定（union 语义，fail-safe 不丢保护）。
///
/// latch 与 source 两个持久化字节由同一 runtime atomic 派生，自写文件恒一致；
/// 仅损坏/手改文件会分叉 —— 任一非零即恢复 latch（宁多保护不丢保护），
/// source 清洗：仅 KillSwitch 原样保留，缺失/非法/None 一律降级 DailyLoss（session 可清）。
/// @return nullopt = 不恢复；有值 = 以该 source arm。
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
    std::atomic<bool> open_halted{false}; ///< no-open latch；true 时平仓仍可放行。
    std::atomic<int64_t> volume_scale_fp{kVolumeScaleUnit}; ///< 下单量缩放系数，单位见 kVolumeScaleUnit。
    std::atomic<uint64_t> version{0}; ///< risk 控制信号变化版本；writer 以 release fetch_add 发布。

    std::atomic<int64_t> peak_equity_fp{0};      ///< session 内 cashflow-immune risk_equity 高水位。
    std::atomic<int64_t> current_drawdown_bp{0}; ///< 当前 IDD 回撤，basis points。
    std::atomic<uint32_t> last_update_ms{0};     ///< 最近 monitor frame 完成时的 steady ms。

    /// @brief open_halted 触发源；None 表示未触发或 session reset 后清空。
    std::atomic<uint8_t> open_halt_reason{
        static_cast<uint8_t>(XtOpenHaltReason::None)};
    std::atomic<uint32_t> open_halt_trigger_ms{0}; ///< open_halted 首次触发的 steady ms。

    /// @brief Per-sid runtime state；sid_registry.freeze() 后 init，SafetyState restore 后可置位。
    PerStrategyRiskRuntimeState per_strategy;

    /// @brief submit path 读取下单量缩放；返回 kVolumeScaleUnit 表示不缩放。
    /// @note acquire-load 与 monitor 的 release-store 配对。
    int64_t volume_scale_fp_load() const noexcept {
        return volume_scale_fp.load(std::memory_order_acquire);
    }
    /// @brief submit path 读取 no-open latch；true 只应拒绝新开仓。
    /// @note acquire-load 与 monitor/restore 的 release-store 配对。
    bool open_halted_load() const noexcept {
        return open_halted.load(std::memory_order_acquire);
    }
    /// @brief 读取 no-open latch 触发源；未触发时返回 XtOpenHaltReason::None。
    /// @note acquire-load 与 open_halted 建立点的 release-store 配对，用于日志/面板归因。
    XtOpenHaltReason open_halt_reason_load() const noexcept {
        return static_cast<XtOpenHaltReason>(
            open_halt_reason.load(std::memory_order_acquire));
    }
};

}  // namespace xt_risk
