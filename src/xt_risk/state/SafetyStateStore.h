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
    bool    kill_switch_active     = false;
    bool    open_halted            = false;
    int64_t halt_trigger_ms        = 0;
    int64_t peak_equity_fp         = 0;
    int64_t year_start_equity_fp   = 0;
    bool    annual_cutoff_active   = false;
    int64_t frozen_cash_fp         = 0;
    int64_t risk_fund_balance_fp   = 0;
    int64_t year_start_net_pnl_fp  = 0;
    int32_t current_year           = 0;
    int32_t schema_version         = 5;
    uint8_t kill_switch_source     = 0;

    std::vector<uint8_t> per_strategy_kill_switch;

    std::vector<uint8_t> per_strategy_auto_liquidating;

    std::vector<uint8_t> per_strategy_liquidating_source;
};

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

enum class LoadStatus {
    Ok,
    Missing,
    Invalid,
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

    static constexpr int32_t kSchemaVersion = 5;

    static bool save(const std::filesystem::path& target, const SafetyState& state);

    static std::optional<SafetyState> load(const std::filesystem::path& source);

    static LoadResult load_checked(const std::filesystem::path& source);
};

}
