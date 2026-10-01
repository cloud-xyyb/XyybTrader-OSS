#pragma once

#include <cstddef>
#include <cstdint>

struct XtTick;
struct XtOrderQueue;
struct XtOrderDetail;
struct XtTransaction;

/**
 * @brief 回放运行时接口（replay runtime contract）。
 *
 * @details
 * replayer 按事件时间顺序单线程调用 `on_tick`、L2 事件与 `on_clock`。
 * 实现只需处理串行事件流，不应假设 callback 会并发进入。
 */
class IReplayRuntime {
public:
    virtual ~IReplayRuntime() = default;
    virtual void on_tick(const XtTick& tick) = 0;

    /// @warning 以下三个接口仅经过模拟数据测试，尚未用真实行情驱动过（not validated with real market data）
    virtual void on_order_queue(const XtOrderQueue& oq) = 0;
    virtual void on_order_detail(const XtOrderDetail& od) = 0;
    virtual void on_transaction(const XtTransaction& tr) = 0;

    /// @brief 时钟推进事件（无市场数据，纯时间流逝 / pure time-flow event）
    ///
    /// 由 replayer 在事件间隙按 clock_resolution_ns 网格主动注入，
    /// 网格对齐在 event-axis（自然日 epoch ns）上。
    /// 默认空实现：不感知时间推进的运行时（测试 mock 等）无需重写。
    /// @note `on_clock` 只推进 TTL、ACK 超时、延迟队列释放等时间相关状态，
    /// 不携带市场数据，也不表示新的 tick/bar 到达。
    ///
    /// @param action_date    YYYYMMDD，自然日（非交易日）——与 action_time 合成
    ///                       event-axis 真实时刻（`xt_time::to_ns` 契约域）
    /// @param action_time_ms HHMMSSmmm = HHMMSS × 1000 + ms
    /// @param trading_date   YYYYMMDD，交易日——仅供 session/report 戳，
    ///                       禁止参与任何时长/到期运算（夜盘与 action_date 差一天）
    ///
    /// Safety invariant:
    /// - 调用时 (action_date, action_time_ms) 必须严格早于“下一个真实事件”的 event 时刻
    /// - event-axis 单调递增：连续两次调用满足 ns₁ < ns₂
    /// - 不会与任何 on_tick / on_xxx 调用时刻重合
    /// - 实现方应推进 TTL / ACK 超时 / 延迟队列释放，
    ///   不应假设此时存在新市场数据
    /// - 实现方不应在 on_clock 内 submit 订单
    ///   （语义可疑：使用上次 tick 的盘口快照）
    virtual void on_clock(uint32_t /*action_date*/, uint32_t /*action_time_ms*/,
                          uint32_t /*trading_date*/) {}

    [[nodiscard]] virtual size_t active_orders() const = 0;
};
