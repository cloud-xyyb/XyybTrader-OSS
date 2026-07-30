#pragma once

/// @file
/// @brief tick 分片库的 per-root 闭包索引（交易日 → 有序源文件集）。
///
/// 分片按交易日切，CSV 源按自然日命名，两者不是一一对应：交易日 D 的夜盘行落在按
/// **上一个 session 的自然日**命名的文件里。索引把「要物化交易日 D，必须读哪些源文件、
/// 按什么顺序读」固化下来。
///
/// 闭包是**内容派生**的：每个源文件登记它实际含有的 trading_date 集合（清单），
/// closure(D) = 清单含 D 的那些文件。清单由建库读该文件时顺路记下（builder 反正逐行
/// 解析 trading_date），并由该文件的 stat 签名（mtime/size）背书——签名一变清单即作废，
/// 重扫后重新入册；签名失效前登记过的日集合降级为 `provisional_days`，只圈范围不定身份。
/// 文件名日期只在一处仍被使用：**未入册文件的候选圈定**（见
/// `XtClosureDayLookup::candidates_for_day`），且候选必须先读进来入册才谈得上闭包，
/// 故「语料里每个文件名日都是真实 session」这条假设不进入正确性链条。
///
/// 顺序是索引的一部分：`sources` 保留调用方给定的读取顺序，查询结果按下标升序返回。
/// 同一批文件换个顺序 → 查询结果顺序随之改变（tick 归并用稳定排序，同 key 的先后由
/// 读取顺序决定，见 `XtTickCollector`）。

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

/// @brief 源文件的新鲜度签名。
///
/// 与分片池的 `XtTickSourceStamp` 同域同语义（mtime + size，不做内容哈希）；索引侧
/// 单独定义以免闭包索引反过来依赖分片库头文件。
struct XtClosureStamp {
    uint64_t mtime_ns = 0;
    uint64_t size = 0;
};

/// @brief 索引里的一个源文件条目。
struct XtClosureSource {
    std::string path;
    XtClosureStamp stamp;
    uint32_t filename_day = 0;
    bool registered = false;
    std::vector<uint32_t> days;

    std::vector<uint32_t> provisional_days;
};

/// @brief per-root 闭包索引。
struct XtTickClosureIndex {

    static constexpr uint32_t kSchema = 2;

    std::string root;
    std::vector<XtClosureSource> sources;
};

XtTickClosureIndex build_closure_index(const std::string& root,
                                       const std::vector<std::string>& ordered_files,
                                       const std::vector<XtClosureStamp>& stamps = {});

void carry_over_registrations(const XtTickClosureIndex& prior, XtTickClosureIndex& index);

bool register_source_days(XtTickClosureIndex& index, const std::string& path,
                          std::vector<uint32_t> days);

std::vector<std::string> closure_files_for_range(const XtTickClosureIndex& index,
                                                 uint32_t start_day,
                                                 uint32_t end_day);

std::vector<uint32_t> closure_days(const XtTickClosureIndex& index);

std::vector<uint32_t> closure_candidate_days(const XtTickClosureIndex& index);

std::vector<std::string> undated_sources(const XtTickClosureIndex& index);

std::vector<std::string> unregistered_sources(const XtTickClosureIndex& index);

bool has_unregistered_undated_source(const XtTickClosureIndex& index);

/// @brief 单日查询视图。
///
/// 按日增量建库要为窗内每一天各查一次，而线性扫全部源在天数一多时整体是 O(days × srcs)。
/// 视图预先把清单倒排成一张表，单日查询只做一次哈希查找。结果与对应的自由函数逐元素相同。
/// @warning 只引用索引、不复制：视图的存活期必须短于其索引。
class XtClosureDayLookup {
public:
    explicit XtClosureDayLookup(const XtTickClosureIndex& index);

    std::vector<std::string> files_for_day(uint32_t day) const;

    std::vector<std::string> candidates_for_day(uint32_t day) const;

    std::vector<std::string> read_set_for_day(uint32_t day) const;

private:

    std::vector<std::string> resolve(std::vector<uint32_t> picked) const;

    void collect_candidates(uint32_t day, std::vector<uint32_t>& picked) const;

    uint32_t candidate_ceiling(uint32_t filename_day) const;

    const XtTickClosureIndex& index_;
    std::unordered_map<uint32_t, std::vector<uint32_t>> by_day_;

    std::unordered_map<uint32_t, std::vector<uint32_t>> provisional_by_day_;
    std::vector<uint32_t> known_days_;

    std::vector<std::pair<uint32_t, uint32_t>> pending_dated_;
    std::vector<uint32_t> pending_undated_;
    uint32_t guard_days_ = 0;
};

std::string serialize_closure_index(const XtTickClosureIndex& index);

bool parse_closure_index(const std::string& text, XtTickClosureIndex& out);

bool write_closure_index(const std::string& path, const XtTickClosureIndex& index);

bool read_closure_index(const std::string& path, XtTickClosureIndex& out);
