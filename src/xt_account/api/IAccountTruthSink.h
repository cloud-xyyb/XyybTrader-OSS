#pragma once

/// @file
/// @brief 账户真值（account truth）写侧接口。
///
/// gateway 在 SDK 回调线程上把 venue 回报的 broker overlay 与成交手续费喂进 source
/// 自维护的状态；runner 层 bridge 负责转发。与读侧 IAccountTruthSource 分离，
/// 读者（report / metrics / control）拿到的指针不带 mutation 能力。

#include <cstdint>

#include "api/BrokerVenueTruth.h"

/// @brief account truth 的写侧 sink 接口。
///
/// 两个入口都默认 no-op：venue 不提供 broker overlay、或以 provider 账本为 fees
/// 真值时（CTP）无需 override。实现自行保证线程安全（gateway 回调线程调用）。
class IAccountTruthSink {
public:
    virtual ~IAccountTruthSink() = default;
    /// @brief 写入 broker venue-truth overlay（available/maint/total/per_symbol）。
    /// @note runner 层 bridge 从 gateway on_broker_account_truth callback 转发；
    ///       不提供 overlay 的 sink 默认 no-op。覆盖写语义（幂等）。
    virtual void update_broker_overlay(const BrokerVenueTruth&) {}
    /// @brief 累加单笔成交手续费到 day-scoped fees（venue commission 真值）。
    /// @note runner 层 bridge 从 on_trade 转发；以 provider fees 为真值的 sink
    ///       （CTP）默认 no-op。**增量累加（非幂等）**，caller 须保证每 fill 仅一次。
    virtual void note_fill_fee_fp(int64_t /*fee_fp*/, uint64_t /*ts_ns*/) {}
};
