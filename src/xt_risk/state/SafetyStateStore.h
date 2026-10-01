#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <vector>

/// @file
/// @brief 安全闩锁（kill switch / no-open / 年度止损）的磁盘快照读写。
///
/// 这些闩锁跨重启存活——kill switch 拉起后进程重启不能自动解除。
/// - runner 周期把 SafetyState 落盘，启动时读回恢复；
/// - writer 只写最新 schema，load() 兼容过时格式布局并用长度/schema/CRC 校验拒绝损坏文件；
/// - `load_checked()` 区分「文件缺失」（冷启动正常）与「文件存在但校验失败」（损坏），
///   供调用方对损坏文件 fail-closed（带闩启动）；`load()` 是不区分二者的薄便利封装。

namespace xt_risk::state {

/// @brief 可持久化的 risk safety latch 快照。
///
/// 字段只使用 bool/int32/int64/vector<uint8_t>，避免持久化 double/string ABI。
/// `schema_version` 记录磁盘布局版本；读取失败时调用方应按 `SafetyState{}`
/// cold-start fallback 处理。
struct SafetyState {
    bool    kill_switch_active     = false; ///< 全局 kill switch latch
    bool    open_halted            = false; ///< active risk monitor no-open latch
    int64_t halt_trigger_ms        = 0;     ///< open_halted 触发时间 ms
    int64_t peak_equity_fp         = 0;     ///< IDD drawdown 峰值权益
    int64_t year_start_equity_fp   = 0;     ///< 年度止损权益分母 anchor
    bool    annual_cutoff_active   = false; ///< 年度止损 latch
    int64_t frozen_cash_fp         = 0;     ///< 诊断字段；不会直接恢复进账户 reducer
    int64_t risk_fund_balance_fp   = 0;     ///< realized-fees 累计 offset，不含 unrealized
    int64_t year_start_net_pnl_fp  = 0;     ///< 年切冻结 net PnL，年度止损分子 anchor
    int32_t current_year           = 0;     ///< 年度 latch 年份；0 表示 cold-start no-op
    int32_t schema_version         = 5;     ///< v1 fixed; v2 +per-sid; v3 +net-PnL; v4 +kill_switch_source; v5 +per-sid auto-liquidation
    uint8_t kill_switch_source     = 0;     ///< kill_switch latch 来源 0=manual/compat-default 1=auto-closeout-block（v4+）

    /// @brief per-sid kill switch flags；index 对应 sid_id，uint8 取值 0/1。
    std::vector<uint8_t> per_strategy_kill_switch;

    /// @brief per-sid auto-liquidation latch flags；index 对应 sid_id，uint8 取值 0/1（v5+）。
    std::vector<uint8_t> per_strategy_auto_liquidating;

    /// @brief per-sid auto-liquidation 触发来源；index 对应 sid_id，值见 XtAutoLiquidateSource（v5+）。
    std::vector<uint8_t> per_strategy_liquidating_source;
};

/// @brief 字段相等比较；用于跨周期 dedup（避免无变化时重复落盘）。
inline bool operator==(const SafetyState& a, const SafetyState& b) noexcept {
    return a.kill_switch_active == b.kill_switch_active
        && a.open_halted == b.open_halted
        && a.halt_trigger_ms == b.halt_trigger_ms
        && a.peak_equity_fp == b.peak_equity_fp
        && a.year_start_equity_fp == b.year_start_equity_fp
        && a.annual_cutoff_active == b.annual_cutoff_active
        && a.frozen_cash_fp == b.frozen_cash_fp
        && a.risk_fund_balance_fp == b.risk_fund_balance_fp
        && a.year_start_net_pnl_fp == b.year_start_net_pnl_fp
        && a.current_year == b.current_year
        && a.schema_version == b.schema_version
        && a.kill_switch_source == b.kill_switch_source
        && a.per_strategy_kill_switch == b.per_strategy_kill_switch
        && a.per_strategy_auto_liquidating == b.per_strategy_auto_liquidating
        && a.per_strategy_liquidating_source == b.per_strategy_liquidating_source;
}
inline bool operator!=(const SafetyState& a, const SafetyState& b) noexcept {
    return !(a == b);
}

/// @brief load 结果三态：区分文件缺失与文件损坏。
enum class LoadStatus {
    Ok,       ///< 文件存在且通过全部校验，state 有值
    Missing,  ///< 文件不存在（冷启动正常路径）
    Invalid,  ///< 文件存在但任一校验失败（打不开/读不满/长度异常/format 未知/schema/CRC 失败）
};

/// @brief 带状态的 load 结果。
///
/// `state` 仅在 `status == LoadStatus::Ok` 时有值；Missing/Invalid 时为 nullopt。
struct LoadResult {
    LoadStatus                 status = LoadStatus::Missing;
    std::optional<SafetyState> state;
};

/// @brief SafetyState 磁盘读写工具。
///
/// `save()` 使用临时文件、CRC32、fsync 和原子 rename；`load()` 只返回通过完整
/// 校验的状态。本类无内部状态，调用方负责调度和串行化。
class SafetyStateStore {
public:
    /// @brief 写入 schema version；v1 固定 60B，v2 +per-sid vector，v3 +净值 anchor，v4 +kill_switch_source，v5 +per-sid auto-liquidation。
    static constexpr int32_t kSchemaVersion = 5;

    /// @brief 写入 safety state。
    ///
    /// 使用 `<target>.tmp.<pid>`、CRC32、fsync、rename 写入；rename 后父目录 fsync 为 best-effort。
    /// 失败时返回 false，并尽量清理 tmp 文件。
    static bool save(const std::filesystem::path& target, const SafetyState& state);

    /// @brief 读取 safety state（不区分缺失与损坏的薄封装）。
    ///
    /// 文件不存在、长度异常、magic/schema 不匹配、CRC 失败或 I/O error 均返回 std::nullopt。
    /// 需要区分「缺失」与「损坏」以 fail-closed 的调用方改用 `load_checked()`。
    static std::optional<SafetyState> load(const std::filesystem::path& source);

    /// @brief 读取 safety state 并区分缺失 vs 损坏。
    ///
    /// - 文件不存在 → `{Missing, nullopt}`（冷启动正常）；
    /// - 文件存在但打不开、长度异常、magic/schema/format/CRC 校验任一失败或 I/O error
    ///   → `{Invalid, nullopt}`；
    /// - 全部校验通过 → `{Ok, state}`。
    ///
    /// 文件一旦确认存在，后续任何失败都归 Invalid，绝不降级为 Missing——调用方据此对
    /// 损坏文件 fail-closed（带闩启动），对缺失走冷启动默认。
    static LoadResult load_checked(const std::filesystem::path& source);
};

}  // namespace xt_risk::state
