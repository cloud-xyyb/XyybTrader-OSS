#pragma once

/// @file
/// @brief tick memory-mode 批量收集 helper。
///
/// 本文件把多个 CSV/BIN tick 文件读取为带 sort key 的内存序列；stream reader、
/// datasource 路径推导和 replay 派发由调用方处理。

#include <cstdint>
#include <functional>
#include <string>
#include <unordered_set>
#include <vector>

#include "TickFileUtil.h"
#include "xt_core/market/XtRawTick.h"

/// @brief collect_ticks_from_paths 的载入→保留分类计数（consumed manifest 用）。
///
/// `lines_read` = reader 送入 parse 的数据行总数；`symbol`/`bad_row` = reader 层
/// 丢弃分类；`window` = keep 谓词丢弃（如交易日窗过滤）。满足
/// `lines_read == out.size() + symbol + bad_row + window`。
struct CollectStats {
    int64_t lines_read = 0;
    int64_t symbol     = 0;
    int64_t bad_row    = 0;
    int64_t window     = 0;
};

bool collect_ticks_from_paths(const std::vector<std::string>& paths,
                              std::vector<XtTickWithKey>& out,
                              const std::unordered_set<std::string>& symbol_filter,
                              double qty_base,
                              XtTickVolumeMode vol_mode,
                              const std::function<bool(const XtRawTick&)>& keep = nullptr,
                              CollectStats* out_stats = nullptr);
