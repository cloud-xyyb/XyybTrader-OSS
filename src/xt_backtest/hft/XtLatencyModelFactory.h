#pragma once

/// @file
/// @brief HFT backtest latency/queue model 的配置工厂。
///
/// Runner 以本文件为唯一入口把 YAML 解析后的 `LatencyModelCfg` / `QueueModelCfg`
/// 转为 match engine 使用的模型对象；具体模型实现仍在 `xt_engine/sim`。

#include <memory>

#include "XtHftBacktestEngineCfg.h"

class ILatencyModel;
class IQueueModel;

namespace xt_bt::hft {

/// @brief 按回测配置创建订单往返延迟模型。
///
/// Runner 在订单网关初始化后、行情回放开始前调用本函数；返回的模型会注入撮合引擎，
/// 决定下单到可撮合、成交到策略可见这两段等待时间。
/// 支持的配置形态由 `LatencyModelCfg` 负责说明。
///
/// @throws std::runtime_error 配置缺字段、模型类型与文件后缀不匹配，或模型文件加载失败。
std::unique_ptr<ILatencyModel> make_latency_model(const LatencyModelCfg& cfg);

/// @brief 根据 `QueueModelCfg` 创建 HFT 回测 queue model。
///
/// 支持 `ratio`、`prob_power1`、`prob_power2`。
///
/// @throws std::runtime_error type 不在支持集合内（schema enum 已前置校验，
///         此为兜底；caller 在 try/catch 边界内调用）。
std::unique_ptr<IQueueModel> make_queue_model(const QueueModelCfg& cfg);

} // namespace xt_bt::hft
