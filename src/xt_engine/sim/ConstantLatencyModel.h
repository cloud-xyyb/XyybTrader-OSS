#pragma once

#include <cstdint>

#include "ILatencyModel.h"

/// @brief 提供固定 entry/response 延迟。
///
/// 构造后 `entry_latency_ns()` 与 `response_latency_ns()` 始终返回固定 ns 值。
/// `ConstantLatencyModel(0, 0)` 表示 submit 后立即可撮合，成交后立即可见。
/// 构造参数不做 clamp，调用方必须传入非负值以满足 ILatencyModel 契约。
class ConstantLatencyModel final : public ILatencyModel {
public:
    /// @param entry_ns submit 到可撮合的延迟 ns，必须 >= 0。
    /// @param response_ns 成交到可见的延迟 ns，必须 >= 0。
    ConstantLatencyModel(int64_t entry_ns, int64_t response_ns) noexcept
        : entry_ns_(entry_ns), response_ns_(response_ns) {}

    int64_t entry_latency_ns(int64_t /*ts_local_ns*/) const noexcept override {
        return entry_ns_;
    }

    int64_t response_latency_ns(int64_t /*ts_local_ns*/) const noexcept override {
        return response_ns_;
    }

private:
    int64_t entry_ns_;
    int64_t response_ns_;
};
