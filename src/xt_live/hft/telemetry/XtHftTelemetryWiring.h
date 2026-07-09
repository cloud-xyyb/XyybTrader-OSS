#pragma once

/// @file
/// @brief HFT PnL 统计组件的装配与停机保存。
///
/// wire_hft_telemetry() 在启动期把 PnL aggregator、日初 baseline 和 fee observer
/// 挂到 XtHftEngine；save_hft_telemetry() 在停机时把累计盈亏写盘，与启动时的
/// load 用同一路径，保证跨重启 PnL 连续。组件所有权归调用方传入的 slot。

#include <memory>

#include "api/IHftEngineObserver.h"
#include "xt_metrics/XtStrategyPnlAggregator.h"
#include "xt_metrics/XtStrategyPnlBaseline.h"

class XtHftEngine;
struct XtLiveConfig;

namespace xt_live::hft {

void wire_hft_telemetry(
    std::unique_ptr<ct::metrics::XtStrategyPnlAggregator>& aggr_slot,
    std::unique_ptr<ct::metrics::XtStrategyPnlBaseline>& baseline_slot,
    std::unique_ptr<xt_runtime::IHftEngineObserver>& fee_observer_slot,
    XtHftEngine& engine,
    const XtLiveConfig& cfg);

void save_hft_telemetry(const ct::metrics::XtStrategyPnlAggregator* aggr,
                        const XtLiveConfig& cfg);

}
