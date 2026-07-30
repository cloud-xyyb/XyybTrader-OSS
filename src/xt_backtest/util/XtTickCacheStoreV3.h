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
    std::vector<std::string> ordered_sources;
    std::vector<std::string> symbols;
    double qty_base = 0.0;
    int32_t volume_mode = 0;
    uint32_t codec_version = 0;
    uint32_t builder_rev = 0;
};

uint64_t compute_family_fingerprint(const XtTickCacheFamilyKey& key);

std::string family_dir_name(uint64_t fingerprint);

std::string family_dir_path(const std::string& cache_root, uint64_t fingerprint);

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

    bool try_lock(const std::string& family_dir);

    bool held() const { return fd_ >= 0; }
    void unlock();

private:
    int fd_ = -1;
};

/// @brief 单个源文件的新鲜度签名。
///
/// 只取 mtime + size：内容哈希要把 GB 级语料整读一遍，而 cache 失效判定必须比读数据
/// 本身便宜得多。取不到（文件已删/不可读）时两者为 0——指纹仍确定，坏文件由分片头与
/// payload CRC 兜底。
/// @warning mtime 与 size 都不变的原地替换检测不到（与 DualV2 manifest 同一盲区）。
struct XtTickSourceStamp {
    std::string path;
    uint64_t mtime_ns = 0;
    uint64_t size = 0;
};

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
    std::vector<std::string> symbols;
    double qty_base = 0.0;
    int32_t volume_mode = 0;
    uint32_t codec_version = 0;
    uint32_t builder_rev = 0;
};

uint64_t compute_day_shard_fingerprint(const XtTickDayShardKey& key);

std::string shard_pool_dir(const std::string& family_dir);

std::string pooled_shard_path(const std::string& family_dir, uint32_t trading_day,
                              uint64_t fingerprint);

std::vector<uint32_t> pooled_shard_days(const std::string& family_dir);

bool adopt_shard_into_pool(const std::string& staged_path, const std::string& family_dir,
                           uint32_t trading_day, uint64_t fingerprint);

bool write_empty_pooled_shard(const std::string& family_dir, uint32_t trading_day,
                              uint64_t fingerprint);

/// @brief 清单里的一天：指向池中某个分片。
struct XtTickGenerationDay {
    uint32_t trading_day = 0;
    uint64_t shard_fingerprint = 0;
    uint64_t record_count = 0;
};

/// @brief generation 清单：这次建库把哪些交易日物化成了池里的哪些分片。
struct XtTickGenerationManifest {

    static constexpr uint32_t kSchema = 1;

    uint64_t family_fingerprint = 0;

    bool full_coverage = false;
    std::vector<XtTickGenerationDay> days;
};

std::string generation_manifest_path(const std::string& generation_dir);

std::string closure_index_path(const std::string& generation_dir);

std::string serialize_generation_manifest(const XtTickGenerationManifest& manifest);

bool parse_generation_manifest(const std::string& text, XtTickGenerationManifest& out);

bool write_generation_manifest(const std::string& path,
                               const XtTickGenerationManifest& manifest);

bool read_generation_manifest(const std::string& path, XtTickGenerationManifest& out);

std::string make_generation_name(uint64_t epoch_ms, int pid, uint64_t seq);

bool create_generation_dir(const std::string& family_dir,
                           const std::string& generation,
                           std::string& out_path);

bool commit_current(const std::string& family_dir, const std::string& generation);

bool read_current(const std::string& family_dir, std::string& out_generation);

/// @brief GC 计划（永远 dry-run，不删任何东西）。
struct XtTickCacheGcPlan {
    std::string current;
    std::vector<std::string> removable;
    bool current_missing = false;

    std::vector<std::string> orphan_shards;
};

XtTickCacheGcPlan plan_gc(const std::string& family_dir);
