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

    virtual void on_order_queue(const XtOrderQueue& oq) = 0;
    virtual void on_order_detail(const XtOrderDetail& od) = 0;
    virtual void on_transaction(const XtTransaction& tr) = 0;

    virtual void on_clock(uint32_t  , uint32_t  ) {}

    [[nodiscard]] virtual size_t active_orders() const = 0;
};
