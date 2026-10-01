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

#include "TickFileUtil.h"  // XtTickWithKey, XtTickVolumeMode
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

/// @brief 从多个 CSV/BIN tick 文件读取全部 tick，并按回放时间键稳定排序后输出。
///
/// 调用会先清空 `out`；读取成功后 `out` 内每个元素都携带与流式 reader 一致的
/// CTP exchange-TD sort key。空输入、reader 初始化失败或没有读到任何 tick 均返回 false。
///
/// @param paths tick 文件路径列表，格式由 reader 按后缀识别。
/// @param symbol_filter 合约白名单；空集表示不过滤。
/// @param qty_base simple CSV 缺少数量列时使用的默认数量。
/// @param vol_mode CSV cumulative / incremental 成交量解释方式。
/// @param keep 可选逐 tick 准入谓词；返回 false 的 tick 在排序前即丢弃，让调用方
///        （如 memory 模式的交易日窗过滤）把驻留内存收敛到窗内规模。谓词是逐 tick
///        独立判定，在排序前统一施加不改变留存 tick 的相对顺序，故与「整批排序后再
///        过滤」逐字节等价。`nullptr`（默认）保留全部 tick。
/// @param out_stats 非空时写入 reader 载入行数与丢弃分类（consumed manifest 用）；
///        默认 nullptr（既有调用方不受影响）。
/// @return true 表示 `out` 至少包含一条已排序 tick；false 表示调用方应视为数据源不可用。
bool collect_ticks_from_paths(const std::vector<std::string>& paths,
                              std::vector<XtTickWithKey>& out,
                              const std::unordered_set<std::string>& symbol_filter,
                              double qty_base,
                              XtTickVolumeMode vol_mode,
                              const std::function<bool(const XtRawTick&)>& keep = nullptr,
                              CollectStats* out_stats = nullptr);
