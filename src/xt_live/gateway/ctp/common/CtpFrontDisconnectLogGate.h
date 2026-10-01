#pragma once
#include <cstdint>

/// @brief CTP 断线回调（OnFrontDisconnected）日志的限频器
///
/// CTP front 不可达时(如收盘休市 / 网络抖动)会每 ~2-3s 自动重连一次,并反复触发 OnFrontDisconnected，
/// 刷出海量日志淹没真错误并刷爆日志告警。
/// CtpFrontDisconnectLogGate 只让首条断开打 error,之后整段断开期内至多每 kIntervalMs 打一条 warn(带状态)
/// 重连时复位并返回累计次数。
/// 只限频日志本身 —— metric 计数与 recorder 记录不受影响,仍计全量。
///
/// 调用线程:CTP SDK 单一回调线程(OnFrontConnected/Disconnected 序列化),无需 atomic。
class CtpFrontDisconnectLogGate {
public:
    enum class Action { LogError, LogWarn, Suppress };

    /// @brief 收到一次 OnFrontDisconnected;返回本次该如何打日志。
    /// @param now_ms steady-clock 毫秒
    Action on_disconnect(uint32_t now_ms) {
        if (++streak_ == 1) { last_log_ms_ = now_ms; return Action::LogError; }
        if (now_ms - last_log_ms_ >= kIntervalMs) { last_log_ms_ = now_ms; return Action::LogWarn; }
        return Action::Suppress;
    }

    /// @brief 收到 OnFrontConnected;返回本段断开期间累计的断线回调次数,并复位。
    uint32_t on_reconnect() {
        uint32_t n = streak_;
        streak_ = 0;
        return n;
    }

private:
    static constexpr uint32_t kIntervalMs = 60000;  ///< 断开期内日志最小间隔(≥1 条/分钟)
    uint32_t streak_ = 0;        ///< 当前连续断开期内的回调次数(reconnect 复位)
    uint32_t last_log_ms_ = 0;   ///< 上次打日志的 steady ms 锚点
};
