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
    Init    = 0, ///< 初始态，尚未形成可靠 offset。
    Warming = 1, ///< rolling window 正在积累样本，confidence=false。
    Locked  = 2, ///< 样本数达到阈值，offset 可作为可信估计发布。
    Stepped = 3, ///< 检测到时钟 step，offset 发布 NaN 直到重新恢复。
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
    /// @brief trimmed-max offset 估计使用的 rolling-window 时长。
    std::chrono::seconds window_duration{std::chrono::hours{1}};

    /// @brief 取最大值前裁掉的最高样本比例，用于降低极端样本影响。
    double trim_ratio{0.005};

    /// @brief 进入 LOCKED 所需最小样本数。
    std::size_t locked_sample_threshold{1000};

    /// @brief 外部 NTP-step 检测阈值；wall 与 monotonic 漂移超过该值视为 step。
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

    /// @brief 按 `(venue, region)` 返回进程级共享 estimator。
    static XtClockSkewEstimator& shared(std::string_view venue,
                                       std::string_view region);

    /// @brief 记录一笔 WS 时间样本。
    /// @param recv_mono_us 本地收到 WS 消息时的 monotonic µs。
    /// @param server_event_us 交易所 event_time(E) 转换后的 server µs。
    void record_sample(uint64_t recv_mono_us, uint64_t server_event_us);

    /// @brief WS reconnect 后清空 rolling window，并把状态转回 WARMING。
    void on_ws_reconnect(std::string_view channel);

    /// @brief 外部检测到 wall-vs-monotonic step 时调用，强制重新估计 offset。
    void on_clock_step_detected(std::string_view channel);

    /// @brief 返回当前最佳 offset 估计，单位 µs；NaN 表示处于 STEPPED 恢复期。
    double offset_us() const noexcept;

    /// @brief 返回当前状态，供 health/log/metrics 使用。
    ClockSkewState state() const noexcept;

    /// @brief 返回当前 rolling window 内样本数。
    std::size_t window_samples() const noexcept;

    /// @brief 返回当前窗口最老样本年龄，单位秒。
    double window_min_age_seconds() const noexcept;

    /// @brief 发布当前 offset/state/window 指标；幂等。
    void publish_state(std::string_view channel);

private:
    struct Sample {
        uint64_t mono_us;          ///< 本地收到消息的 monotonic µs。
        int64_t offset_sample_us;  ///< 单样本 offset：server_event_us - recv_mono_us。
    };

    bool try_bootstrap(bool preserve_nan_on_failure); ///< INIT/STEPPED bootstrap 入口。
    void update_offset_from_window();                 ///< 根据窗口重算 trimmed-max offset。
    void maybe_promote_to_locked();                   ///< 样本数达到阈值后切到 LOCKED。
    void reset_window(bool preserve_bootstrap);       ///< STEPPED/reconnect 清窗。

    std::string venue_;
    std::string region_;
    ClockSkewConfig cfg_;

    // ── 原子发布状态 ───────────────────────────────────────────────
    std::atomic<double> offset_us_{0.0};
    std::atomic<ClockSkewState> state_{ClockSkewState::Init};
    std::atomic<std::size_t> sample_count_{0};

    // ── bootstrap 状态（bootstrap_mu_ 保护）────────────────────────
    mutable std::mutex bootstrap_mu_;
    double bootstrap_offset_us_{0.0};
    bool bootstrap_done_{false};

    // ── rolling window（window_mu_ 保护）───────────────────────────
    mutable std::mutex window_mu_;
    std::deque<Sample> window_;
};

}  // namespace xt_live::gateway
