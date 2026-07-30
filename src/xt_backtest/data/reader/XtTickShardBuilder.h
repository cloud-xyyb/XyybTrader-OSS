#pragma once

/// @file
/// @brief CSV → RawShardV3 分片库建库（单 pass、驻留有界）。
///
/// 建库把一批**有序** CSV 源物化成「每交易日一个分片 + 一份闭包索引」的 generation。
/// 读取顺序是产出内容的一部分：tick 归并用稳定排序，同 sort key 记录的先后由源顺序
/// 决定，故本模块自始至终不对源列表重排（family 指纹同样按有序集算，见
/// `XtTickCacheStoreV3.h`）。
///
/// 与 DualV2 单文件 cache 的两点口径差异：
///   - 分片**不按交易日窗过滤**：内容只由「贡献该日的源 + 解析参数」决定，窗口由读端
///     行级准入施加。同一批分片因此可服务多个窗口。
///   - `trading_date == 0` 的行没有可归属的分片（其 sort key 排在所有真实交易日之前）。
///     遇到即整批放弃建库（返回 `DirtyTradingDay`），由调用方回落到不分片的 cache
///     路径——不得把这类行硬塞进某一天。
///
/// 增量建库靠 `day_filter`：调用方只把「缺的那几天」连同它们的读取集交进来，本模块
/// 只产出这些天的分片，读到的其它天整条丢弃（那些天的源没读全，产出会缺一截）。
/// `day_filter` 里一条数据都没有的天照样落一个空分片——「这天没数据」必须留下实体，
/// 否则下次装载又把它当缺片，把同一批源再读一遍。
///
/// 建库同时交出**每个源文件实际含有的交易日集合**（`XtTickShardBuildReport::source_days`）：
/// 逐行解析 trading_date 是建库本来就要做的事，顺路记下即可，调用方据此把闭包由
/// 「按文件名猜」换成「按内容定」（见 `XtTickClosureIndex`）。集合覆盖读到的**全部**行，
/// 不受 `day_filter` 影响——被过滤掉的天同样是这个文件的内容。
///
/// 驻留控制：单 pass 读源、按交易日分桶，桶总量超预算时把最大的桶落到 spill 文件；
/// 收尾再逐日「读回一天 → 排序 → 写分片」。峰值 ≈ max(预算, 单个交易日的 tick 量)，
/// 不随源集总量线性增长。

#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "TickCsvUtil.h"

/// @brief 建库输入：源集与解析参数（决定产出内容的全部输入）。
struct XtTickShardBuildOptions {
    std::vector<std::string> ordered_sources;
    std::unordered_set<std::string> symbol_filter;
    double qty_base = 100.0;
    XtTickVolumeMode volume_mode = XtTickVolumeMode::Cumulative;

    std::vector<uint32_t> day_filter;

    std::vector<uint32_t> known_days;

    uint64_t buffer_budget_bytes = 0;
};

enum class XtTickShardBuildStatus : uint8_t {
    Ok = 0,
    NoSources,
    ReaderInitFailed,
    DirtyTradingDay,
    NoTicks,
    UnexpectedTradingDay,
    WriteFailed,
};

const char* to_string(XtTickShardBuildStatus s);

/// @brief 建库统计（成功与失败路径都尽量填满，便于诊断与 bench）。
struct XtTickShardBuildReport {
    XtTickShardBuildStatus status = XtTickShardBuildStatus::NoSources;
    uint64_t ticks_read = 0;
    uint64_t ticks_written = 0;
    uint64_t ticks_skipped = 0;
    uint64_t peak_day_records = 0;
    uint64_t spilled_bytes = 0;
    std::vector<uint32_t> days;

    std::vector<uint64_t> day_records;

    std::vector<std::vector<uint32_t>> source_days;
};

XtTickShardBuildReport build_tick_shards(const XtTickShardBuildOptions& opts,
                                         const std::string& out_dir);

std::string shard_path_for_day(const std::string& out_dir, uint32_t trading_day);

bool read_shard_day_list(const std::string& out_dir, std::vector<uint32_t>& out_days);
