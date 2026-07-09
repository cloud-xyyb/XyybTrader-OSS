#pragma once

/// @file
/// @brief tick memory-mode 批量收集 helper。
///
/// 本文件把多个 CSV/BIN tick 文件读取为带 sort key 的内存序列；stream reader、
/// datasource 路径推导和 replay 派发由调用方处理。

#include <string>
#include <unordered_set>
#include <vector>

#include "TickFileUtil.h"

bool collect_ticks_from_paths(const std::vector<std::string>& paths,
                              std::vector<XtTickWithKey>& out,
                              const std::unordered_set<std::string>& symbol_filter,
                              double qty_base,
                              XtTickVolumeMode vol_mode);
