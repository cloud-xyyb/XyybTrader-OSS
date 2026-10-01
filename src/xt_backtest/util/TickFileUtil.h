#pragma once

/// @file
/// @brief tick 文件发现、cache path 和 manifest helper。
///
/// 本文件提供 CSV tick 文件集合推导、bin cache 路径生成和 manifest signature 校验；

#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "xt_core/market/XtRawTick.h"
#include "TickCsvUtil.h"

/// @brief tick 归并队列元素。
///
/// `key` 是 make_tick_sort_key() 生成的时序排序键，`seq` 用于同 key 时保持
/// 多文件输入的稳定顺序。
struct XtTickWithKey {
    XtRawTick tick;
    uint64_t key = 0;
    uint64_t seq = 0;
};

/// @brief 将合约列表片段规范化为 cache 文件名片段。
/// @return 小写字母数字串；输入没有可用字符时返回 `"all"`。
std::string sanitize_filename_part(const std::string& s);

/// @brief 生成 tick 二进制 cache 路径。
///
/// 文件名附带解析参数指纹（qty_base / volume_mode / source 路径集的短
/// hash）：不同参数组并行建库各写各的 bin+manifest 原子对，不互踩。
/// 刻意不混 mtime/size：源数据变更不换名，内容陈旧仍由 manifest 在位重建。
/// @return `cache_dir` 为空时返回空串，表示调用方不启用 cache。
std::string build_tick_cache_path(const std::string& cache_dir,
                                  uint32_t start_date,
                                  uint32_t end_date,
                                  const std::vector<std::string>& tick_symbols,
                                  double qty_base,
                                  XtTickVolumeMode vol_mode,
                                  const std::vector<std::string>& sources);

/// @brief 从文件名解析交易日（优先 8 位连续数字 `20220527`，其次 `YYYY-MM-DD`）。
/// @return true 表示 `out_date` 已写入；false 表示文件名无可识别日期，
///         调用方应保守保留该文件（交由行级过滤兜底）。
bool extract_date_from_filename(const std::string& name, uint32_t& out_date);

/// @brief 夜盘跨度 guard 的下界（自然日）。
///
/// 交易日 D 的 21:00 夜盘段落在按**上一个 session 的自然日**命名的文件里，两者相隔多远
/// 由休市长度决定：周末 3 天，国内期货最长的春节休市 11 天（2024 年 02-08 → 02-19）。
/// 按文件名取源的地方一律按 guard 向前放宽，宁可多读几个文件也不漏夜盘段；多读进来的
/// 窗外行由读端行级过滤（`in_day_range`）精确裁掉，故放宽不改变交付内容。
///
/// 实际 guard 由语料自身推导（见 `derive_night_carry_guard_days`），本常量是推不出时的
/// 保底值，也是推导结果的下界——语料稀疏（如按周落盘）不该反过来把 guard 缩窄。
constexpr uint32_t kTickNightCarryGuardDays = 15;

/// @brief 由语料的文件名日集合推导夜盘跨度 guard。
///
/// = max(`kTickNightCarryGuardDays`, 相邻文件名日的最大自然日间隔 + 1)。休市多长，
/// 语料里就会留下多长的一段空档；固定 15 天在超长休市（或按周/按段落盘的语料）下会把
/// 承载夜盘段的那个文件挡在窗外，静默丢掉一段行情。日集合取自调用方**本来就要做的**
/// 目录枚举或显式文件列表，不额外 stat/open 任何文件。
/// @param filename_days 每个语料文件一个条目（解析不出日期记 0），可乱序含重复；
///                      条数即语料规模，用来区分「语料为空」与「命名里全无日期」。
/// @return 清洗后不足两个日期（无从测间隔）时返回 `kTickNightCarryGuardDays`；
///         入参非空却一个合法日期都没有时另发一次进程级 warn（语料完全无从枚举）。
uint32_t derive_night_carry_guard_days(std::vector<uint32_t> filename_days);

/// @brief `derive_night_carry_guard_days` 的路径版：日集合现从文件名解析。
uint32_t derive_night_carry_guard_days_for_paths(const std::vector<std::string>& paths);

/// @brief 从 tick.root 递归收集 CSV 文件。
///
/// start/end 均为 0 时扫描 root 下全部 CSV；否则按 `root/YYYY/MM/*.csv`
/// 搜索并用文件名日期过滤。起点向前放宽一段 guard（widen-only）：窗口首日的夜盘段落在
/// 上一个 session 的文件里，而上一个 session 可能隔着周末或长假，只放宽一天会把该段整段
/// 漏掉。guard 由扫到的文件名日集合推导（`derive_night_carry_guard_days`）；推出的
/// guard 若把起点推进更早的月目录，则补扫那些月份后重推，直到起点落在已扫月份内。
/// 结果追加到 `out`，函数结束前排序。
void append_tick_files_from_root(const std::string& root,
                                 uint32_t start_date,
                                 uint32_t end_date,
                                 std::vector<std::string>& out);

/// @brief 枚举 root 下 `YYYY/MM/*.csv` 的**全部**文件（不按窗口过滤）。
///
/// 与 `append_tick_files_from_root` 的有窗分支走同一套目录布局（4 位年 / 2 位月），
/// 只是不设日期边界，因此结果恰是「该布局下所有窗口扫描结果的并集」。分片库用它
/// 定 family 身份：身份必须与回放窗口无关，否则差一天的两个窗会各建一份全量缓存。
/// 结果追加到 `out`，函数结束前排序。
void append_all_tick_files_from_root(const std::string& root, std::vector<std::string>& out);

/// @brief 从候选 tick CSV 中读取第一个有效合约代码。
/// @return true 表示 `out` 已更新；false 表示文件不可读或没有有效 tick 行。
bool find_first_symbol_in_files(const std::vector<std::string>& files,
                                std::string& out,
                                double qty_base,
                                XtTickVolumeMode vol_mode);

/// @brief 主力推断（tick_auto_main）用的严格窗口文件列表。
///
/// root-scan 起点带 guard band，band 内（早于窗口起点）的文件常字典序排最前；主力推断
/// 取首个可解析 tick 的 symbol，band 文件领跑会在多合约语料下静默翻转推断结果。
/// 本函数按文件名日期剔除早于 `start_date`（原始窗口起点，非 band 起点）的文件；
/// 解析不出日期的文件保守保留（交由行级过滤兜底）。
/// @return `start_date` 为 0 或过滤后为空时返回原列表（推断不因过滤落空）。
std::vector<std::string> filter_files_for_auto_main(const std::vector<std::string>& files,
                                                    uint32_t start_date);

// tick 二进制 cache manifest。

/// @brief 计算多 source 文件 + 解析参数的 signature。
///
/// 除 (path, mtime, size) 外同时混入影响 bin 内容的解析参数（qty_base /
/// volume_mode）与建库交易日窗（start_day/end_day，建库按窗过滤后 cache 内容
/// 依赖窗口本身，manifest 必须自含该信息——文件名 range 可被重命名绕过）：
/// 任一变更同样失效 cache，避免静默读到旧口径解析的 bin。
/// vol_mode 传解析后枚举（非原始字串），"inc"/"incremental" 等价拼写不触发重建。
/// @return 64-bit FNV-1a signature；0 表示 source 列表为空或任一文件不可访问。
uint64_t compute_source_signature(const std::vector<std::string>& sources,
                                  double qty_base,
                                  XtTickVolumeMode vol_mode,
                                  uint32_t start_day,
                                  uint32_t end_day);

/// @brief 写 manifest sidecar（`bin_path + ".manifest"`）。
/// @return true 表示写入成功；false 表示调用方应跳过 cache 直接读取 CSV。
bool write_cache_manifest(const std::string& bin_path, uint64_t signature);

/// @brief 读取 manifest sidecar 并对比当前 sources + 解析参数的 signature。
/// @return true 表示 manifest 存在且 signature 一致；false 表示 cache 失效或不可验证
///         （含旧版 manifest：算法升级后 signature 必 mismatch → 自动重建，fail-safe）。
bool verify_cache_manifest(const std::string& bin_path,
                           const std::vector<std::string>& sources,
                           double qty_base,
                           XtTickVolumeMode vol_mode,
                           uint32_t start_day,
                           uint32_t end_day);
