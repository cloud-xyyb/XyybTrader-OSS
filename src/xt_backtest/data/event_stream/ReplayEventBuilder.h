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

/// @brief 构建 tick + Level-2 orderflow 的统一回放事件序列。
///
/// 调用会先清空 `out`。成功时，tick 与 order_queue/order_detail/transaction 已按统一
/// sort key、事件优先级和原始读取序号稳定排序。显式 event_stream 至少需要读到一条
/// orderflow event；orderflow 缺失或加载失败时返回 false 且 `out` 保持为空。
bool build_replay_event_stream(const std::vector<XtRawTick>& ticks,
                               const std::vector<std::string>& order_queue_files,
                               const std::vector<std::string>& order_detail_files,
                               const std::vector<std::string>& transaction_files,
                               std::vector<XtReplayEvent>& out);
