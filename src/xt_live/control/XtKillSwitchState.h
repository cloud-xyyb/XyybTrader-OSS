#pragma once

/// @file
/// @brief live kill switch 的 control-plane 状态文件
///
/// - 记录 kill switch 最近一次是谁设置的、为什么设置、什么时候设置
/// - 保存最近 N 次 trip/resume 事件，给 GetTradingState 展示
/// - 把 kill switch 变更追加写到按天滚动的 JSONL audit log

#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <vector>

namespace xt_live::control {

/// @brief 单次 kill-switch 状态变化。
struct KillSwitchTripEvent {
    bool        active = false;   ///< true = TRIP (active=true);false = RESUME (active=false)
    std::string actor;             ///< cert CN (e.g. "panel-client.xyybtrader.local") or sentinel
    std::string reason;            ///< 用户填写 / system trigger 描述
    int64_t     ts_unix_ns = 0;    ///< CLOCK_REALTIME at apply
};

/// @brief `snapshot()` 返回的调用方持有副本。
struct KillSwitchSnapshot {
    bool        active = false;
    std::string last_actor;
    std::string last_reason;
    int64_t     since_unix_ns = 0;
    std::string scope = "_global";
    std::vector<KillSwitchTripEvent> recent_trips;  ///< oldest → newest, cap N
};

/// @brief kill-switch 当前状态与最近 trip 事件链的线程安全记录器。
class XtKillSwitchState {
public:
    /// Ring buffer 容量；更老事件走 audit log 历史查询。
    static constexpr size_t kMaxTrips = 10;

    XtKillSwitchState() = default;

    /// @brief 应用一次 kill switch 状态变化。
    /// active 与当前状态相同也会记录，用于 audit 重复设置路径。
    void update(bool active, std::string actor, std::string reason, int64_t ts_unix_ns);

    /// @brief 返回当前 snapshot (拷贝) — gRPC handler 直接序列化。
    KillSwitchSnapshot snapshot() const;

    /// @brief 当前是否 active (atomic-equivalent fast path, 给 hot path 用)。
    bool is_active() const;

private:
    mutable std::mutex                 mu_;
    bool                               active_ = false;
    std::string                        last_actor_;
    std::string                        last_reason_;
    int64_t                            since_unix_ns_ = 0;
    std::string                        scope_ = "_global";
    std::deque<KillSwitchTripEvent>    recent_trips_;
};

// ----------------------------------------------------------------------------
// Audit log writer (append-only JSONL per-day rotation)
// ----------------------------------------------------------------------------

/// @brief 按日轮转的 append-only kill-switch audit log writer。
class XtKillSwitchAuditLog {
public:
    /// @param dir  目录,默认 `/opt/xyybtrader/var/audit`。空 → 写入 noop (gtest 友好)。
    explicit XtKillSwitchAuditLog(std::string dir = "/opt/xyybtrader/var/audit");

    /// @brief Append 一行 JSON 到当日文件;失败 silently noop (不阻塞 control path)。
    void append(const KillSwitchTripEvent& ev);

    /// @brief 当前 dir (informational)。
    const std::string& dir() const noexcept { return dir_; }

private:
    std::string  dir_;
    std::mutex   mu_;
};

}  // namespace xt_live::control
