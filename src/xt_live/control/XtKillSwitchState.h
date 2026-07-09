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
    bool        active = false;
    std::string actor;
    std::string reason;
    int64_t     ts_unix_ns = 0;
};

/// @brief `snapshot()` 返回的调用方持有副本。
struct KillSwitchSnapshot {
    bool        active = false;
    std::string last_actor;
    std::string last_reason;
    int64_t     since_unix_ns = 0;
    std::string scope = "_global";
    std::vector<KillSwitchTripEvent> recent_trips;
};

/// @brief kill-switch 当前状态与最近 trip 事件链的线程安全记录器。
class XtKillSwitchState {
public:

    static constexpr size_t kMaxTrips = 10;

    XtKillSwitchState() = default;

    void update(bool active, std::string actor, std::string reason, int64_t ts_unix_ns);

    KillSwitchSnapshot snapshot() const;

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

/// @brief 按日轮转的 append-only kill-switch audit log writer。
class XtKillSwitchAuditLog {
public:

    explicit XtKillSwitchAuditLog(std::string dir = "/opt/xyybtrader/var/audit");

    void append(const KillSwitchTripEvent& ev);

    const std::string& dir() const noexcept { return dir_; }

private:
    std::string  dir_;
    std::mutex   mu_;
};

}
