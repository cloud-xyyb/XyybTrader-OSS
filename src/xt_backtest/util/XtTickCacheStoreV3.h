#pragma once

/// @file
/// @brief RawShardV3 分片库的 family / generation 骨架。
///
/// 目录形态：
/// @code
///   <cache_root>/v3/fam-<16 hex>/          family：一组解析参数 + 有序源全集
///       .lock                              建库排他（flock，非阻塞）
///       CURRENT                            指向当前可读 generation 的文本指针
///       shards/                            分片池：stat 寻址、不可变、跨 generation 共享
///           shard_<YYYYMMDD>_<16 hex>.bin
///       gen-<epoch_ms>-<pid>/              不可变 generation（建好即只读）
///           manifest.json                  交易日 → 池内分片指纹（只记清单，不复制数据）
///           closure_index.json             各源文件的交易日清单（倒排即闭包）
/// @endcode
///
/// family 是「要什么」，generation 是「某次把它物化成了什么」。**两者都与回放窗口无关**：
/// family 身份取源全集（root 全扫或显式列表）与解析参数，窗口只是读端的挑片条件，
/// 差一天的两个窗因此落在同一 family 上、共用同一批分片。
///
/// 分片是 **stat 寻址**的：文件名带该日的身份指纹（见 `compute_day_shard_fingerprint`），
/// 指纹取闭包源的 path/mtime/size 而非内容哈希——与 DualV2 的 `.manifest` signature 同一
/// 信任级，同一指纹即视作同一内容，故可被任意 generation 引用而无需复制。建库因此是
/// 增量的——只建池里没有的那些天。generation 目录只写清单，最后一步原子改写 CURRENT：
/// 读者要么看到旧 generation，要么看到新的，不会看到半建好的目录。
///
/// 盲区随之与 V2 同域：保持 mtime 与 size 的原地替换（`touch -r` 回填、等长改写、
/// 某些 rsync/还原流程）签名不变，旧分片会被继续采用。要真正抵御这类替换，得整读语料
/// 算内容哈希——那比 cache 想省下的读一遍还贵，故刻意不做。
///
/// GC 只出计划不动手（`plan_gc` 永远 dry-run），删除由人执行；旧 generation 仍在引用的
/// 分片不会出现在回收清单里。

#include <cstdint>
#include <string>
#include <vector>

/// @brief family 身份的输入。
///
/// `ordered_sources` 是**源全集**（root 全扫或显式列表），与回放窗口无关——窗口进了
/// family 身份就意味着「1.21-2.17」与「1.21-2.18」各建一份全量缓存，正是分片库要消除的
/// 浪费。**不排序**：同一批文件换个顺序就是另一个 family。tick 归并用稳定排序，同 key
/// 记录的先后由读取顺序决定，故顺序是产出内容的一部分。
struct XtTickCacheFamilyKey {
    std::vector<std::string> ordered_sources;  ///< 有序源文件全集（顺序敏感，不含窗口）
    std::vector<std::string> symbols;          ///< 合约筛选集（顺序敏感，由调用方定序）
    double qty_base = 0.0;                     ///< simple CSV 缺量列时的默认数量
    int32_t volume_mode = 0;                   ///< XtTickVolumeMode 枚举值
    uint32_t codec_version = 0;                ///< 分片 on-disk 修订号
    uint32_t builder_rev = 0;                  ///< 建库语义修订号（非字节格式变更时 bump）
};

/// @brief 计算 family 指纹。
///
/// 字符串按长度前缀混入，消除拼接歧义；顺序参与哈希，故 {A,B} 与 {B,A} 必得不同值。
/// 刻意不混 mtime/size：那是「这份物化是否仍新鲜」的问题，属 generation 层，不改身份。
uint64_t compute_family_fingerprint(const XtTickCacheFamilyKey& key);

/// @brief family 目录名（`fam-` + 16 位十六进制指纹）。
std::string family_dir_name(uint64_t fingerprint);

/// @brief family 目录绝对/相对路径：`<cache_root>/v3/fam-xxxxxxxxxxxxxxxx`。
/// @return `cache_root` 为空时返回空串，表示调用方未启用分片库。
std::string family_dir_path(const std::string& cache_root, uint64_t fingerprint);

/// @brief 建 family 目录（含父目录）；已存在视为成功。
bool ensure_family_dir(const std::string& family_dir);

/// @brief family 级建库排他锁（flock，非阻塞）。
///
/// 同一 family 同时只允许一个建库者；读者不取锁——读者只跟着 CURRENT 走，而
/// generation 一旦建成即不可变。析构自动释放。
class XtTickCacheFamilyLock {
public:
    XtTickCacheFamilyLock() = default;
    ~XtTickCacheFamilyLock();

    XtTickCacheFamilyLock(const XtTickCacheFamilyLock&) = delete;
    XtTickCacheFamilyLock& operator=(const XtTickCacheFamilyLock&) = delete;
    XtTickCacheFamilyLock(XtTickCacheFamilyLock&& other) noexcept;
    XtTickCacheFamilyLock& operator=(XtTickCacheFamilyLock&& other) noexcept;

    /// @brief 尝试取锁；目录不存在会先创建。
    /// @return false 表示锁已被他人持有（或文件不可创建）——调用方应放弃本次建库。
    bool try_lock(const std::string& family_dir);

    bool held() const { return fd_ >= 0; }
    void unlock();

private:
    int fd_ = -1;
};

// ── 分片池（stat 寻址）───────────────────────────────────────────────────

/// @brief 单个源文件的新鲜度签名。
///
/// 只取 mtime + size：内容哈希要把 GB 级语料整读一遍，而 cache 失效判定必须比读数据
/// 本身便宜得多。取不到（文件已删/不可读）时两者为 0——指纹仍确定，坏文件由分片头与
/// payload CRC 兜底。
/// @warning mtime 与 size 都不变的原地替换检测不到（与 DualV2 manifest 同一盲区）。
struct XtTickSourceStamp {
    std::string path;
    uint64_t mtime_ns = 0;  ///< 最后修改时间（file_clock 纪元纳秒）
    uint64_t size = 0;      ///< 字节数
};

/// @brief 采集单个源文件的签名；不可访问时 `mtime_ns`/`size` 为 0。
XtTickSourceStamp stat_tick_source(const std::string& path);

/// @brief 单日分片的身份输入。
///
/// `ordered_sources` 是**贡献该交易日**的有序源（闭包，见 `XtTickClosureIndex`）——
/// 失效粒度因此是「日」而非「整个 family」：改一个源文件只让它参与的那几天换指纹，
/// 其余天的分片原样复用。
/// @pre 闭包必须涵盖该日的**全部**行来源（含夜盘段所在的上一 session 文件）：指纹只认
///      闭包，闭包漏一个源就会让两份内容不同的分片重名。闭包由各源文件的交易日清单
///      倒排而来，故清单未入册的源不得参与指纹计算（参见 `XtTickClosureIndex`）。
struct XtTickDayShardKey {
    uint32_t trading_day = 0;
    std::vector<XtTickSourceStamp> ordered_sources;
    std::vector<std::string> symbols;  ///< 合约筛选集（顺序敏感，由调用方定序）
    double qty_base = 0.0;
    int32_t volume_mode = 0;
    uint32_t codec_version = 0;
    uint32_t builder_rev = 0;
};

/// @brief 计算单日分片指纹（混入方式同 family：长度前缀 + 有序）。
uint64_t compute_day_shard_fingerprint(const XtTickDayShardKey& key);

/// @brief 分片池目录：`<family_dir>/shards`。
std::string shard_pool_dir(const std::string& family_dir);

/// @brief 池内分片路径：`<family_dir>/shards/shard_<YYYYMMDD>_<16 hex>.bin`。
std::string pooled_shard_path(const std::string& family_dir, uint32_t trading_day,
                              uint64_t fingerprint);

/// @brief 池内已有分片覆盖的交易日（升序去重）。
///
/// 文件名即索引：不必读 generation 也能知道这个 family 建过哪些天。只认严格形态
/// （`shard_<8 位日>_<16 hex>.bin`），认错一个就等于凭空多出一天。
std::vector<uint32_t> pooled_shard_days(const std::string& family_dir);

/// @brief 把建库产出的分片文件搬进池（同 family 目录内 rename，原子）。
/// @return false 表示源文件缺失或 rename 失败。
bool adopt_shard_into_pool(const std::string& staged_path, const std::string& family_dir,
                           uint32_t trading_day, uint64_t fingerprint);

/// @brief 在池里写一个空分片（该交易日确无数据）。
///
/// 「这天没数据」必须落成实体，否则每次装载都把它当缺片，反复重读一遍源文件。
/// tmp + rename 落盘：并发读者要么看到旧文件，要么看到新的。
bool write_empty_pooled_shard(const std::string& family_dir, uint32_t trading_day,
                              uint64_t fingerprint);

// ── generation 清单 ──────────────────────────────────────────────────────

/// @brief 清单里的一天：指向池中某个分片。
struct XtTickGenerationDay {
    uint32_t trading_day = 0;
    uint64_t shard_fingerprint = 0;
    uint64_t record_count = 0;  ///< 该日 tick 条数（forensic）
};

/// @brief generation 清单：这次建库把哪些交易日物化成了池里的哪些分片。
struct XtTickGenerationManifest {
    /// @brief on-disk schema 版本；解析端只接受本值。
    static constexpr uint32_t kSchema = 1;

    uint64_t family_fingerprint = 0;
    /// @brief 本次建库是否读了源全集（true 时 `days` 即该 family 的交易日全集）。
    bool full_coverage = false;
    std::vector<XtTickGenerationDay> days;  ///< 按 trading_day 升序
};

/// @brief generation 清单路径：`<generation_dir>/manifest.json`。
std::string generation_manifest_path(const std::string& generation_dir);

/// @brief generation 内闭包索引路径：`<generation_dir>/closure_index.json`。
std::string closure_index_path(const std::string& generation_dir);

/// @brief 序列化清单（键序稳定，同一清单恒得同一字节串）。
std::string serialize_generation_manifest(const XtTickGenerationManifest& manifest);

/// @brief 严格解析清单；schema 不符、字段类型不符或日条目未严格升序都返回 false
///        且不触碰 `out`（fail-closed：宁可重建，也不按半份清单去读数据）。
bool parse_generation_manifest(const std::string& text, XtTickGenerationManifest& out);

/// @brief 原子写入清单（tmp + rename）。
bool write_generation_manifest(const std::string& path,
                               const XtTickGenerationManifest& manifest);

/// @brief 读取并严格解析清单；文件缺失或解析失败返回 false。
bool read_generation_manifest(const std::string& path, XtTickGenerationManifest& out);

// ── generation ──────────────────────────────────────────────────────────

/// @brief 生成 generation 目录名：`gen-<epoch_ms>-<pid>-<seq>`。
///
/// 时钟与 pid 由调用方注入（测试可给定值，避免依赖真实时钟）。`seq` 是**同进程内**的
/// 递增序号：generation 目录不可复用（重名即拒建），而毫秒时钟的分辨率不足以区分同一
/// 进程里前后脚发生的两次建库——增量建库把每次建库压到毫秒级之后，这是常态而非边角。
std::string make_generation_name(uint64_t epoch_ms, int pid, uint64_t seq);

/// @brief 建 generation 目录；已存在返回 false（generation 不可复用、不可覆盖）。
/// @param out_path 成功时写入 generation 目录路径。
bool create_generation_dir(const std::string& family_dir,
                           const std::string& generation,
                           std::string& out_path);

/// @brief 原子改写 CURRENT 指针（tmp + rename），完成建库提交。
/// @pre generation 目录已建好且内容完整；本函数不校验内容。
bool commit_current(const std::string& family_dir, const std::string& generation);

/// @brief 读 CURRENT 指针。
/// @return false 表示 family 尚无提交过的 generation（首建未完成或从未建过）。
bool read_current(const std::string& family_dir, std::string& out_generation);

/// @brief GC 计划（永远 dry-run，不删任何东西）。
struct XtTickCacheGcPlan {
    std::string current;                 ///< CURRENT 指向的 generation；空串表示无指针
    std::vector<std::string> removable;  ///< 可回收的 generation 目录名，升序
    bool current_missing = false;        ///< CURRENT 指向的目录不存在（指针悬空）
    /// @brief 池内无人引用的分片文件名，升序。
    ///
    /// 「无人引用」= 任何 generation 的清单都没提到它。旧 generation 引用的分片一律
    /// 不列——回收 generation 与回收其分片是两步，人可以只做前一步。
    std::vector<std::string> orphan_shards;
};

/// @brief 扫描 family 目录并给出可回收 generation 与孤儿分片列表。
///
/// 只读：不删除、不重命名、不改 CURRENT。CURRENT 悬空时 `removable` 仍不含任何
/// 目录（`current_missing=true` 交人判断）——宁可留垃圾，不冒删掉唯一可用数据的风险；
/// 同理，任一 generation 清单解析不了时不报告孤儿分片（读不懂引用就不敢说没人引用）。
XtTickCacheGcPlan plan_gc(const std::string& family_dir);
