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

    /// @brief 当前 tick 进入 runtime 前触发。
    /// @param ts packed timestamp，格式为 YYYYMMDD*1e9 + HHMMSSmmm。
    /// @param t 当前回放 tick。
    virtual void on_tick_pre(int64_t ts, const XtRawTick& t) = 0;

    /// @brief 当前 tick 处理完成后触发。
    /// @param ts 与 on_tick_pre 使用同一 packed timestamp 语义。
    /// @param active_orders tick 处理完成后仍在途的订单数量。
    virtual void on_tick_post(int64_t ts, size_t active_orders) = 0;
};
