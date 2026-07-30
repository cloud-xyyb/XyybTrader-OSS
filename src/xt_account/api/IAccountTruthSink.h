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

    virtual void update_broker_overlay(const BrokerVenueTruth&) {}

    virtual void note_fill_fee_fp(int64_t  , uint64_t  ) {}
};
