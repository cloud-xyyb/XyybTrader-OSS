#pragma once

#include <cstddef>
#include <cstdint>

struct XtRawTick;

/// @brief Tick replay 报告接口。
///
/// `XtReplayer` 在同一回放线程中按 tick 顺序调用 pre/post，进行统计和输出
class IReplayReporter {
public:
    virtual ~IReplayReporter() = default;

    virtual void on_tick_pre(int64_t ts, const XtRawTick& t) = 0;

    virtual void on_tick_post(int64_t ts, size_t active_orders) = 0;
};
