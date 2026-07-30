#pragma once
#include <cstdint>

/// @brief 提供 HFT 回测订单延迟估算。
///
/// `entry_latency_ns()` 返回 submit 到可撮合的延迟；
/// `response_latency_ns()` 返回成交到策略可见的延迟。
/// 单位为 ns，返回值应非负且不超过 `xt_latency::kMaxLatencyNs`
///（消费点统一二次校验，越界抛错不静默 clamp）。
///
/// 预期在回测主线程串行调用。`event_ts_ns` 为 event-axis 时刻
///（自然日 epoch ns，`xt_time::to_ns(action_date, action_time)` 域，
/// timezone-naive、单 venue 内单调）：时变模型用它查询延迟样本，
/// 与 live 录制侧 `event_ts_local_ns`（CSV v2 索引列）同轴。
class ILatencyModel {
public:
    virtual ~ILatencyModel() = default;

    virtual int64_t entry_latency_ns(int64_t event_ts_ns) const = 0;

    virtual int64_t response_latency_ns(int64_t event_ts_ns) const = 0;

    virtual bool requires_event_timestamp() const { return false; }
};
