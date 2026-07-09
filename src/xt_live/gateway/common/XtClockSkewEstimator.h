#pragma once

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <string_view>

namespace xt_live::gateway {

enum class ClockSkewState : uint8_t {
    Init    = 0,
    Warming = 1,
    Locked  = 2,
    Stepped = 3,
};

inline constexpr std::string_view to_string(ClockSkewState s) {
    switch (s) {
        case ClockSkewState::Init:    return "INIT";
        case ClockSkewState::Warming: return "WARMING";
        case ClockSkewState::Locked:  return "LOCKED";
        case ClockSkewState::Stepped: return "STEPPED";
    }
    return "?";
}

struct ClockSkewConfig {

    std::chrono::seconds window_duration{std::chrono::hours{1}};

    double trim_ratio{0.005};

    std::size_t locked_sample_threshold{1000};

    std::chrono::milliseconds ntp_step_threshold{10};
};

/// @brief 行情侧的交易所时间与本机时间偏移估计器
///
/// Binance 行情 WS 消息里带交易所事件时间 E，本机收到消息时记录的是本机 monotonic 时间，
/// 两者不在同一个基准上，直接相减会把“机器时间差”也算进延迟里。
/// XtClockSkewEstimator 用 WS 消息样本估算这个偏移量来校准时间
class XtClockSkewEstimator {
public:
    explicit XtClockSkewEstimator(std::string venue,
                                std::string region,
                                ClockSkewConfig cfg = {});

    static XtClockSkewEstimator& shared(std::string_view venue,
                                       std::string_view region);

    void record_sample(uint64_t recv_mono_us, uint64_t server_event_us);

    void on_ws_reconnect(std::string_view channel);

    void on_clock_step_detected(std::string_view channel);

    double offset_us() const noexcept;

    ClockSkewState state() const noexcept;

    std::size_t window_samples() const noexcept;

    double window_min_age_seconds() const noexcept;

    void publish_state(std::string_view channel);

private:
    struct Sample {
        uint64_t mono_us;
        int64_t offset_sample_us;
    };

    bool try_bootstrap(bool preserve_nan_on_failure);
    void update_offset_from_window();
    void maybe_promote_to_locked();
    void reset_window(bool preserve_bootstrap);

    std::string venue_;
    std::string region_;
    ClockSkewConfig cfg_;

    std::atomic<double> offset_us_{0.0};
    std::atomic<ClockSkewState> state_{ClockSkewState::Init};
    std::atomic<std::size_t> sample_count_{0};

    mutable std::mutex bootstrap_mu_;
    double bootstrap_offset_us_{0.0};
    bool bootstrap_done_{false};

    mutable std::mutex window_mu_;
    std::deque<Sample> window_;
};

}
