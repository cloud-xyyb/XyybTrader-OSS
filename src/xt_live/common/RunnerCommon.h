#pragma once

/// @file
/// @brief HFT/CTA live runner 共享 metrics 装配入口。
///
/// 本文件只放 runner bootstrap/shutdown 需要共享的 live 进程级 hook。
/// 函数名保留尾随下划线以标识 runner-internal API，不面向策略或 gateway 调用。
#include <cstdint>
#include <string>

namespace xt_live::common {

/// @brief 停止 Prometheus metrics exposer。
///
/// runner shutdown 路径应在 `XtLogger::shutdown()` 之前调用；`XT_HAVE_METRICS`
/// 未定义时为 no-op。
void stop_metrics_exposer_();

/// @brief 将 `PositionAggregator` 的仓位跳变 hook 接到 metrics registry。
///
/// 该 hook 是进程级回调，HFT/CTA runner 启动期各自调用一次即可；`XT_HAVE_METRICS`
/// 未定义时为 no-op。
void register_position_aggregator_jump_reporter_();

}  // namespace xt_live::common
