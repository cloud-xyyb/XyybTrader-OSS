#pragma once

/// @file
/// @brief tick 与 Level-2 orderflow 的统一 replay event 构建入口。
///
/// 本文件只负责把已加载的 tick/order_queue/order_detail/transaction 行归并为
/// `XtReplayEvent` 序列；数据读取、字段解析和 runtime 派发由调用方处理。

#include <string>
#include <vector>

struct XtRawTick;
struct XtReplayEvent;

bool build_replay_event_stream(const std::vector<XtRawTick>& ticks,
                               const std::vector<std::string>& order_queue_files,
                               const std::vector<std::string>& order_detail_files,
                               const std::vector<std::string>& transaction_files,
                               std::vector<XtReplayEvent>& out);
