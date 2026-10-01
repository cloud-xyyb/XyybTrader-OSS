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
    uint64_t mtime_ns = 0;  ///< 最后修改时间（file_clock 纪元纳秒）
    uint64_t size = 0;      ///< 字节数
};

/// @brief 索引里的一个源文件条目。
struct XtClosureSource {
    std::string path;
    XtClosureStamp stamp;        ///< 入册时采到的签名；与当前签名不符即清单作废
    uint32_t filename_day = 0;   ///< 文件名解析出的自然日；0 = 文件名无可识别日期
    bool registered = false;     ///< `days` 是否由文件内容派生（false = 尚未入册）
    std::vector<uint32_t> days;  ///< 该文件实际含有的 trading_date，升序去重
    /// @brief 签名失效前该文件登记过的日集合（升序去重），仅未入册条目可能非空。
    ///
    /// 只圈范围、不定身份：参与交易日全集与候选圈定（故这些天照样被规划、照样进
    /// `known_days`），**绝不参与单日指纹**——文件已经变了，按旧清单给分片命名就是
    /// 名不副实。重扫入册后由实测日集合取代（见 `register_source_days`）。
    /// 不进 JSON：每轮由上一代索引现推（见 `carry_over_registrations`）。
    std::vector<uint32_t> provisional_days;
};

/// @brief per-root 闭包索引。
struct XtTickClosureIndex {
    /// @brief on-disk schema 版本；解析端只接受本值。
    static constexpr uint32_t kSchema = 2;

    std::string root;                      ///< 索引覆盖的数据根（forensic，用于识别错配）
    std::vector<XtClosureSource> sources;  ///< 有序源集；顺序即读取顺序
};

/// @brief 由有序文件名集合建索引骨架：登记路径与签名，清单一律留空（未入册）。
///
/// 只解析文件名、不读文件内容。重复路径按首次出现去重，不改变其余文件的相对顺序。
/// @param stamps 与 `ordered_files` 一一对应的签名；给空表示调用方不关心签名（此时全部
///               记为 0，`carry_over_registrations` 只会与同样为 0 的旧条目对上）。
XtTickClosureIndex build_closure_index(const std::string& root,
                                       const std::vector<std::string>& ordered_files,
                                       const std::vector<XtClosureStamp>& stamps = {});

/// @brief 用上一代索引的清单为本代续期：路径相同且签名一致的条目沿用其日集合。
///
/// 清单是 stat 寻址的（同分片池）：签名一致即视作同内容，故清单可跨 generation 复用，
/// 不必每轮重扫。签名不符则保持未入册等待重扫，但旧日集合转存为 `provisional_days`
/// ——那些天此刻只有这一份线索，丢掉就等于交易日全集缺口（某天只存在于被碰过的那个
/// 源里时，缺口会让建库撞上 `known_days` 闸而整批放弃）。旧条目本就未入册则无可续期。
void carry_over_registrations(const XtTickClosureIndex& prior, XtTickClosureIndex& index);

/// @brief 把某个源文件的实际交易日集合入册。
///
/// 实测即真相：入册同时清掉 `provisional_days`，旧线索到此让位。
/// @param days 该文件实际含有的 trading_date（可乱序含重复；函数内部升序去重）。
///             空集合是合法结果——非交易日文件（如空的周六文件）就该不进任何日闭包。
/// @return false 表示 `path` 不在索引里（调用方给错了源集）。
bool register_source_days(XtTickClosureIndex& index, const std::string& path,
                          std::vector<uint32_t> days);

/// @brief 查询交易日窗 [start_day, end_day] 的闭包文件集。
///
/// 边界为 0 表示该侧不设界（两侧皆 0 = 全部交易日）。只返回**已入册**且日集合与窗口
/// 有交集的源，按 `sources` 下标升序，即调用方应当采用的读取顺序。
std::vector<std::string> closure_files_for_range(const XtTickClosureIndex& index,
                                                 uint32_t start_day,
                                                 uint32_t end_day);

/// @brief 索引已入册部分覆盖的交易日升序列表。
std::vector<uint32_t> closure_days(const XtTickClosureIndex& index);

/// @brief 候选交易日全集：「这批源里可能有哪些交易日」的上界估计。
///
/// = 已入册源的真交易日 ∪ 未入册**有日期**源的 {文件名日, 其后第一个地标日}
/// ∪ 未入册源的 `provisional_days`。未入册源里藏着哪些交易日不读不知道，只能按文件名圈：
/// 它自己那天，加上夜盘段所属的下一个 session（拿索引里的下一个地标日顶，语料末尾则退回
/// 日历次日）；上一代登记过的日集合是比文件名更硬的线索，一并纳入。这只决定「要不要为
/// 这一天排个计划」，圈多了最多多一个空分片；闭包本身另由清单给出，不受此影响。未入册
/// **无日期**且无旧线索的源完全圈不住，调用方需另行兜底（见
/// `has_unregistered_undated_source`）。
std::vector<uint32_t> closure_candidate_days(const XtTickClosureIndex& index);

/// @brief 文件名无可识别日期的源（无论是否已入册）。
std::vector<std::string> undated_sources(const XtTickClosureIndex& index);

/// @brief 尚未入册的源（清单缺失或签名已失效）。
std::vector<std::string> unregistered_sources(const XtTickClosureIndex& index);

/// @brief 是否存在文件名无日期、且尚未入册的源。
///
/// 这类源里藏着哪些交易日既圈不出（无文件名日）又查不到（无清单），交易日全集因此
/// 失去依据——调用方只能整读一遍语料让数据自己说话。入册之后清单顶上，不再需要。
bool has_unregistered_undated_source(const XtTickClosureIndex& index);

/// @brief 单日查询视图。
///
/// 按日增量建库要为窗内每一天各查一次，而线性扫全部源在天数一多时整体是 O(days × srcs)。
/// 视图预先把清单倒排成一张表，单日查询只做一次哈希查找。结果与对应的自由函数逐元素相同。
/// @warning 只引用索引、不复制：视图的存活期必须短于其索引。
class XtClosureDayLookup {
public:
    explicit XtClosureDayLookup(const XtTickClosureIndex& index);

    /// @brief 交易日 `day` 的精确闭包（已入册源，按读取顺序）。
    std::vector<std::string> files_for_day(uint32_t day) const;

    /// @brief 可能含有交易日 `day` 的未入册源（按读取顺序）。
    ///
    /// 圈定只用文件名，且只用来决定「先读谁」：按自然日 d 命名的文件装的是 d 当天的行，
    /// 即 d 的日盘段（交易日 d）与 d 晚 21:00 起的夜盘段（交易日 = d 之后第一个 session）。
    /// 因此 d > day 的文件不可能含 day，而上界取「已入册源里大于 d 的最小交易日」——
    /// 已入册那部分是内容派生的真交易日集合，取其子集只会把上界抬高，不会抬低。整份索引
    /// 一天都没入册（冷启）时无从推断，退回本语料的夜盘跨度 guard（见
    /// `derive_night_carry_guard_days`）。文件名无日期的源无从圈定，一律纳入；带
    /// `provisional_days` 的源另按旧日集合纳入——那份线索比文件名硬，且丢了就没别处可查。
    /// @return 空集表示该日闭包已精确（无待入册的候选）。
    std::vector<std::string> candidates_for_day(uint32_t day) const;

    /// @brief 建交易日 `day` 必须读的源：精确闭包 ∪ 候选（按读取顺序，去重）。
    ///
    /// 候选多读进来不改变产出——它们若真不含 `day` 的行，就对该日分片没有任何贡献；
    /// 读完即入册，下一轮起该日闭包精确。
    std::vector<std::string> read_set_for_day(uint32_t day) const;

private:
    /// @brief 下标集 → 有序文件名集：升序去重后按 `sources` 取名。
    std::vector<std::string> resolve(std::vector<uint32_t> picked) const;
    /// @brief 候选下标追加到 `picked`（两个公开查询共用，保证口径一致）。
    void collect_candidates(uint32_t day, std::vector<uint32_t>& picked) const;

    /// @brief 文件名日 `filename_day` 的文件可能贡献到的最晚交易日。
    uint32_t candidate_ceiling(uint32_t filename_day) const;

    const XtTickClosureIndex& index_;
    std::unordered_map<uint32_t, std::vector<uint32_t>> by_day_;  ///< 交易日 → 已入册源下标
    /// @brief 交易日 → 带该日旧线索的未入册源下标（`provisional_days` 的倒排）。
    std::unordered_map<uint32_t, std::vector<uint32_t>> provisional_by_day_;
    std::vector<uint32_t> known_days_;  ///< 已入册源覆盖的交易日，升序（候选上界的依据）
    /// @brief 未入册有日期源，按 (文件名日, 下标) 升序——候选圈定按文件名日二分。
    std::vector<std::pair<uint32_t, uint32_t>> pending_dated_;
    std::vector<uint32_t> pending_undated_;  ///< 未入册无日期源下标，升序
    uint32_t guard_days_ = 0;  ///< 本语料推导出的夜盘跨度 guard（冷启候选上界用）
};

/// @brief 序列化为 JSON 文本（键序稳定，同一索引恒得同一字节串）。
std::string serialize_closure_index(const XtTickClosureIndex& index);

/// @brief 严格解析 JSON 文本。
///
/// schema 不等于 `kSchema`、字段类型不符、路径重复、日集合未严格升序，或未入册条目却
/// 带着日集合，都返回 false 且不触碰 `out`（fail-closed：宁可重扫入册，也不按半解析的
/// 清单去定闭包）。
bool parse_closure_index(const std::string& text, XtTickClosureIndex& out);

/// @brief 原子写入索引文件（tmp + rename）。
bool write_closure_index(const std::string& path, const XtTickClosureIndex& index);

/// @brief 读取并严格解析索引文件；文件缺失或解析失败返回 false。
bool read_closure_index(const std::string& path, XtTickClosureIndex& out);
