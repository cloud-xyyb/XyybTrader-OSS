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

std::unique_ptr<ILatencyModel> make_latency_model(const LatencyModelCfg& cfg);

std::unique_ptr<IQueueModel> make_queue_model(const QueueModelCfg& cfg);

}
