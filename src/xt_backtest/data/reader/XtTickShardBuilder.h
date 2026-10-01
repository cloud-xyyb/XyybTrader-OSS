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

#include "TickCsvUtil.h"  // XtTickVolumeMode

/// @brief 建库输入：源集与解析参数（决定产出内容的全部输入）。
struct XtTickShardBuildOptions {
    std::vector<std::string> ordered_sources;       ///< 有序 CSV 源；顺序即读取顺序
    std::unordered_set<std::string> symbol_filter;  ///< 合约白名单；空集不过滤
    double qty_base = 100.0;                        ///< simple CSV 缺量列时的默认数量
    XtTickVolumeMode volume_mode = XtTickVolumeMode::Cumulative;
    /// @brief 只产出这些交易日（每天必得一个分片，无数据则为空片）；空 = 产出读到的全部天。
    std::vector<uint32_t> day_filter;
    /// @brief 允许出现的交易日全集；空 = 不校验。
    ///
    /// 增量建库只读缺失日的读取集，产出的完整性因此依赖「窗内该有哪些交易日」这份预判：
    /// 预判之外的天，它的源并没有被完整读进来，写出去就是缺一截的分片。读到这样一天即
    /// 整批放弃（返回 `UnexpectedTradingDay`）让调用方回落到不分片的 cache 路径。
    ///
    /// 内容派生的闭包（见 `XtTickClosureIndex`）已把已入册文件的交易日算准，剩下能触发
    /// 本闸的只有一种情形：某个交易日的行**只**存在于尚未入册、且按文件名圈不进候选的
    /// 文件里（例如该 session 自己的文件缺失，行全靠更早的文件承载）。fail-closed 优于
    /// 静默少一段行情。
    std::vector<uint32_t> known_days;
    /// @brief 分桶驻留预算；超出即 spill 最大桶。0 表示用默认值（256 MiB）。
    uint64_t buffer_budget_bytes = 0;
};

/// @brief 建库结果；除 `Ok` 外产出目录内容不可用，调用方应回落不分片的 cache 路径。
enum class XtTickShardBuildStatus : uint8_t {
    Ok = 0,
    NoSources,            ///< 源集为空。
    ReaderInitFailed,     ///< CSV reader 打不开任何源。
    DirtyTradingDay,      ///< 存在 `trading_date == 0` 的行，无法归片（D4 兜底）。
    NoTicks,              ///< 源可读但一条 tick 都没解析出来。
    UnexpectedTradingDay, ///< 读到 `known_days` 之外的交易日（那天的源没被完整读进来）。
    WriteFailed,          ///< spill / 分片 / 清单写盘失败，或源读取中途 I/O 错误。
};

/// @brief 供日志与断言使用的稳定短名。
const char* to_string(XtTickShardBuildStatus s);

/// @brief 建库统计（成功与失败路径都尽量填满，便于诊断与 bench）。
struct XtTickShardBuildReport {
    XtTickShardBuildStatus status = XtTickShardBuildStatus::NoSources;
    uint64_t ticks_read = 0;        ///< reader 交付的 tick 数（symbol 过滤之后）。
    uint64_t ticks_written = 0;     ///< 写进分片的 tick 数。
    uint64_t ticks_skipped = 0;     ///< 交易日不在 `day_filter` 内被丢弃的 tick 数。
    uint64_t peak_day_records = 0;  ///< 单日最大 tick 数（收尾阶段驻留上界的实测值）。
    uint64_t spilled_bytes = 0;     ///< 落到 spill 文件的字节数；0 表示全程未溢出预算。
    std::vector<uint32_t> days;     ///< 产出分片的交易日，升序。
    /// @brief 各交易日的 tick 条数，与 `days` 一一对应（清单 forensic 用）。
    std::vector<uint64_t> day_records;
    /// @brief 每个源文件实际含有的交易日（升序去重），与 `opts.ordered_sources` 一一对应。
    ///
    /// 只在 `status == Ok` 时完整——中途放弃时源还没读到 EOF，集合是残缺的，不得入册。
    /// 空集合是合法结果：该文件一条可用 tick 都没有（空文件 / 全被 symbol 过滤），它就
    /// 不该进任何一天的闭包。
    std::vector<std::vector<uint32_t>> source_days;
};

/// @brief 把源集物化为 `out_dir` 内的一批按交易日切分的分片。
///
/// 只写 `out_dir` 内的文件，不碰分片池、不碰 CURRENT——搬进池与发布由调用方在校验
/// 通过后完成，读者因此不会看到半建好的产物。
/// @param out_dir 已创建且为空的输出目录（通常是本次 generation 目录）。
/// @return 状态与统计；`status != Ok` 时目录内容作废（可整目录删除）。
XtTickShardBuildReport build_tick_shards(const XtTickShardBuildOptions& opts,
                                         const std::string& out_dir);

/// @brief 单个交易日的分片路径：`<out_dir>/shard_<YYYYMMDD>.bin`。
std::string shard_path_for_day(const std::string& out_dir, uint32_t trading_day);

/// @brief 读回本次建库声明的交易日清单（`shards.list`）。
///
/// 清单是「这一批建了哪些天」的权威记录：只按目录里现有的 `shard_*.bin` 推断的话，
/// 被外部删掉一天会静默变成一段更短的行情。
/// @return false 表示清单缺失、格式不合（非 8 位数字 / 未严格升序）或为空；
///         此时 `out_days` 不被触碰，调用方应视本次产出不可用。
bool read_shard_day_list(const std::string& out_dir, std::vector<uint32_t>& out_days);
