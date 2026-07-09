#pragma once
#include <cstdint>

/// @brief 提供 HFT 回测订单延迟估算。
///
/// `entry_latency_ns()` 返回 submit 到可撮合的延迟；
/// `response_latency_ns()` 返回成交到策略可见的延迟。
/// 单位为 ns，返回值应非负；0 表示对应阶段立即进入下一步。
///
/// 预期在回测主线程串行调用；时间相关模型可使用 `ts_local_ns` 查询样本。
/// @note 当前 HFT sim 调用路径传入 `ts_local_ns = 0`；时间相关模型在该路径看不到真实 wall-clock ts。
class ILatencyModel {
public:
    virtual ~ILatencyModel() = default;

    virtual int64_t entry_latency_ns(int64_t ts_local_ns) const = 0;

    virtual int64_t response_latency_ns(int64_t ts_local_ns) const = 0;
};
