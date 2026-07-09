#pragma once

/// @file
/// @brief HFT/CTA live runner 共享 metrics 装配入口。
///
/// 本文件只放 runner bootstrap/shutdown 需要共享的 live 进程级 hook。
/// 函数名保留尾随下划线以标识 runner-internal API，不面向策略或 gateway 调用。
#include <cstdint>
#include <string>

namespace xt_live::common {

void stop_metrics_exposer_();

void register_position_aggregator_jump_reporter_();

}
