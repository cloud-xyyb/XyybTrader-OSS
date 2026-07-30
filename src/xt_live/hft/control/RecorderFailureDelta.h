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

    enum Reason : std::size_t {
        kDrop = 0,
        kWriteFailed,
        kIntentTraceDrop,
        kIntentTraceWriteFailed,
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
        bool                          any = false;
        std::array<uint64_t, kCount>  delta{};
        std::array<bool, kCount>      warn{};
    };

    explicit RecorderFailureDelta(uint64_t throttle_ms = 60'000) noexcept
        : throttle_ms_(throttle_ms) {}

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
                baseline_[i] = cur[i];
                continue;
            }
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
        primed_ = true;
        return out;
    }

private:
    uint64_t throttle_ms_;
    bool     primed_ = false;
    std::array<uint64_t, kCount> baseline_{};
    std::array<uint64_t, kCount> last_warn_ms_{};
};

}
