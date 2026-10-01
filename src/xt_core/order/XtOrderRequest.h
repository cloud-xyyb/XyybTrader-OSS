#pragma once

#include <cstdint>
#include <string>

#include "xt_core/order/XtOrder.h"

namespace xt_core {

/// @brief 下单触发来源，用于 risk gate、audit log 与行为分支。
enum class XtOrderSource : uint8_t {
    Strategy = 0,      ///< 策略 on_tick / on_bar 触发
    Manual,            ///< HftRunnerControl manual_order_panel 人工下单
    Closeout,          ///< 强制平仓通道 (closeout force-flat / kill_switch flush)
    Recovery,          ///< 重连恢复期间的补单
};

/// @brief 返回稳定的小写 source 标签；未知枚举值返回 "unknown"。
inline const char* to_cstr(XtOrderSource s) noexcept {
    switch (s) {
        case XtOrderSource::Strategy: return "strategy";
        case XtOrderSource::Manual:   return "manual";
        case XtOrderSource::Closeout: return "closeout";
        case XtOrderSource::Recovery: return "recovery";
    }
    return "unknown";
}

/**
 * @brief 进程内订单请求
 *
 * 包含 `XtOrder` 订单意图以及 submit-time metadata。该结构只在进程内传递；
 * risk/gateway 层读取 source、sid、trace、dry-run、forced-close 和时间字段后，
 * 构造各自模块的执行上下文。
 *
 * @note 该 wrapper 含 `std::string`，不是 hot-path POD wire type。
 */
struct XtOrderRequest {
    /// 订单意图 POD。
    XtOrder order{};

    /// 策略字符串 ID；多策略场景对应 strategies[].id，单策略场景可为空。
    std::string strategy_id;

    // ── submit-time metadata ──

    /// 来源标识；Closeout 路径通常与 is_forced_close 同时设置。
    XtOrderSource source = XtOrderSource::Strategy;

    /// Distributed tracing trace ID；0 表示本请求未携带 trace。
    uint64_t audit_trace_id = 0;

    /// Registry 解析后的 uint32 sid；0 表示未绑定策略。
    uint32_t sid_id = 0;

    /// Dry-run / pre-flight check；true 表示评估但不消耗风控状态。
    bool dry_run = false;

    /// Forced-close 标志；true 表示按强平语义评估。
    bool is_forced_close = false;

    /// Submit-time steady clock 毫秒值；0 表示由 gateway 填入当前时间。
    int64_t now_ms = 0;
};

} // namespace xt_core
