#pragma once

/// @file
/// @brief recorder 失败向计数的帧间 delta + 限频判定（纯逻辑，无外部依赖）。
///
/// recorder 失败 fail-open 是既定政策（录制坏了不停交易），但失败计数此前无
/// prod reader：rotation reopen 失败会让双轨录制暂停且仅 stderr，磁盘/未滚动
/// 告警又因 live.jsonl size=0 而不触发。本类在 1Hz frame-tail 消费
/// `XtLiveRecorder` 的 4 个失败向累计 counter，输出「相对上次告警的新增量」，
/// 并对每个原因独立限频（同一原因至多每 throttle_ms 一条），避免 reopen 持续
/// 失败时刷屏 journald。纯类、可直接单测。

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace xt_live::control {

/// @brief recorder 失败计数帧间 delta tracker（每原因独立限频）。
class RecorderFailureDelta {
public:
    /// @brief 观测的失败向原因；顺序即 observe() 入参顺序与 delta 数组下标。
    enum Reason : std::size_t {
        kDrop = 0,                  ///< 队满丢弃（drop_count）
        kWriteFailed,               ///< 写盘失败（write_failed_count）
        kIntentTraceDrop,           ///< intent-trace 队满丢弃
        kIntentTraceWriteFailed,    ///< intent-trace 写盘失败
        kCount
    };

    static constexpr std::string_view reason_name(std::size_t r) noexcept {
        switch (r) {
            case kDrop:                   return "drop";
            case kWriteFailed:            return "write_failed";
            case kIntentTraceDrop:        return "intent_trace_drop";
            case kIntentTraceWriteFailed: return "intent_trace_write_failed";
            default:                      return "unknown";
        }
    }

    /// @brief 单帧 delta + 告警决策。
    struct Report {
        bool                          any = false;  ///< 是否有任一原因触发告警
        std::array<uint64_t, kCount>  delta{};      ///< 各原因相对上次告警基线的新增量
        std::array<bool, kCount>      warn{};       ///< 各原因本帧是否应告警（新增且过限频窗）
    };

    explicit RecorderFailureDelta(uint64_t throttle_ms = 60'000) noexcept
        : throttle_ms_(throttle_ms) {}

    /// @brief 喂本帧 4 个累计 counter，返回 delta + 限频后的告警决策。
    /// @param now_ms  steady clock ms，限频窗判定用。
    /// @note 首帧只吸收基线不告警：把重启前累计的历史失败排除在「本次新增」外。
    ///       counter 单调递增；出现回退（进程内不应发生）时静默 re-baseline。
    ///       delta 报告的是「相对上次告警的累计增量」——限频窗内不推进基线，
    ///       让下一条告警带上窗口内积攒的完整增量。
    Report observe(uint64_t now_ms,
                   uint64_t drop,
                   uint64_t write_failed,
                   uint64_t intent_trace_drop,
                   uint64_t intent_trace_write_failed) noexcept {
        const std::array<uint64_t, kCount> cur{
            drop, write_failed, intent_trace_drop, intent_trace_write_failed};
        Report out;
        for (std::size_t i = 0; i < kCount; ++i) {
            if (!primed_) {
                baseline_[i] = cur[i];  // 首帧基线；不算 delta、不告警。
                continue;
            }
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
        primed_ = true;
        return out;
    }

private:
    uint64_t throttle_ms_;
    bool     primed_ = false;
    std::array<uint64_t, kCount> baseline_{};      ///< 上次告警（或首帧）时的 counter 快照
    std::array<uint64_t, kCount> last_warn_ms_{};  ///< 各原因上次告警 steady ms（0=从未）
};

}  // namespace xt_live::control
