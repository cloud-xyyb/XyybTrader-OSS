#pragma once

#include <fstream>
#include <memory>
#include <queue>
#include <string>
#include <unordered_set>
#include <vector>

#include "api/IMarketDataReader.h"
#include "TickCsvUtil.h"

/// @brief 从一个或多个 CSV tick 文件输出 `XtRawTick` 的回测 reader。
///
/// 每个文件读取时解析表头 schema，并把可见行转换为 tick + CTP exchange-TD sort key。
/// 多文件模式按各文件预读 tick 的 key 做 heap merge；前提是单个文件内部 key 单调。
/// 文件内 key 回退只计数/告警，不重排、不拒绝。symbol filter 在本类执行；日期/session
/// 准入、skip-last 和 replay clock 由 `XtMarketDataSource` / `XtReplayer` 负责。
/// 单线程顺序读取，`reset()` 清空 stream、heap 和预读状态，不重写构造期配置。
class XtCsvTickReader : public IMarketDataReader {
public:
    // ── 配置模型 ──────────────────────────────────────────────
    /// @brief CSV tick reader 的构造期配置快照。
    struct Options {
        std::vector<std::string> paths;                 ///< CSV 文件路径；多文件模式按 tick key heap merge。
        std::unordered_set<std::string> symbol_filter;  ///< 空集表示不过滤 symbol。
        double qty_base = 100.0;                        ///< simple CSV 缺失数量列时使用。
        XtTickVolumeMode volume_mode = XtTickVolumeMode::Cumulative; ///< CSV volume 列解释方式。
    };

    // ── 生命周期 ──────────────────────────────────────────────
    explicit XtCsvTickReader(Options opts);

    // ── IMarketDataReader 接口实现 ──────────────────────────────────────────────
    /// @return true 表示至少有一个可打开文件；CSV 表头和内容延迟到读取时解析。
    bool init() override;
    /// @brief 清空 stream、heap、reader 预读状态和 sticky I/O 状态。
    void reset() override;
    /// @return true 表示 `out` 已写入下一条 tick；false 表示 EOF、过滤后无数据或 I/O 错误。
    bool next_tick(XtRawTick& out) override;

    /// @brief 文件内 tick key 回退（乱序）累计计数。
    ///
    /// stream 路径逐条透传不排序（memory 路径才 stable_sort）；多文件 heap merge
    /// 也以每个文件内部有序为前提。此计数只加可见性（warn + counter），不改序
    /// 不拒数据；>0 表示源 CSV 内部乱序，stream/memory 两模式回放序列可能不一致。
    uint64_t out_of_order_count() const { return out_of_order_count_; }

    /// @brief 逐行解析计数（consumed manifest 分类用）。
    ///
    /// `lines_read` = 送入 parse 的数据行总数（不含表头/空行/注释）；`symbol` /
    /// `bad_row` = parse 返回 false 的分类计数。三者满足
    /// `lines_read == 交付 tick 数 + symbol + bad_row`。reset() 清零。
    struct ReadStats {
        int64_t lines_read = 0;  ///< 送入 parse 的数据行总数。
        int64_t symbol     = 0;  ///< 合约白名单不命中丢弃。
        int64_t bad_row    = 0;  ///< 坏行丢弃。
    };
    const ReadStats& read_stats() const { return read_stats_; }

    /// @brief 每个源文件交付过的 trading_date（升序去重），下标与 `Options::paths` 对应。
    ///
    /// 只记**交付出去**的 tick：symbol 过滤掉的行不构成该文件对下游的贡献。分片库据此
    /// 把「这个文件实际含有哪些交易日」入册，替代按文件名猜（见 `XtTickClosureIndex`）。
    /// 集合只有读到 EOF 才完整——中途停止读取时，未读到的那部分交易日不会出现在这里。
    /// `reset()` 清空。
    const std::vector<std::vector<uint32_t>>& source_trading_days() const { return src_days_; }

private:
    // ── 内部读取结构 ──────────────────────────────────────────────
    /// @brief 单个 CSV 文件的 stream、schema 和一条预读 tick。
    struct Reader {
        std::unique_ptr<std::ifstream> in;   ///< 当前文件 stream；nullptr 表示未打开或已释放。
        XtTickCsvSpec spec;                  ///< 当前文件解析出的 CSV schema。
        XtRawTick tick;                      ///< 预读 tick；heap 弹出对应 Node 后写给调用方。
        uint64_t key = 0;                    ///< 预读 tick 的 CTP exchange-TD sort key。
        uint64_t seq = 0;                    ///< 同 key 下的全局稳定 tie-breaker。
        size_t path_idx = 0;                 ///< 对应 `Options::paths` 下标（打不开的文件不入 readers_）。
    };

    /// @brief 多文件 heap merge 中指向 reader 预读 tick 的排序节点。
    struct Node {
        uint64_t key = 0;    ///< heap 排序主键，对应 Reader::key。
        uint64_t seq = 0;    ///< heap 排序副键，对应 Reader::seq。
        size_t reader_idx = 0; ///< readers_ 下标；Node 不持有 stream。
    };

    /// @brief priority_queue 比较器；按 key/seq 形成最小堆语义。
    struct NodeCmp {
        bool operator()(const Node& a, const Node& b) const {
            if (a.key != b.key) return a.key > b.key;
            return a.seq > b.seq;
        }
    };

    // ── 内部流程函数 ──────────────────────────────────────────────
    bool next_single(XtRawTick& out);
    bool next_multi(XtRawTick& out);
    // 多文件模式为每个 reader 预读一条 tick；heap 中只保存 reader 索引和排序键。
    bool init_readers();
    bool read_next_tick(Reader& reader);
    bool read_next_parsed_line(std::ifstream& in, XtTickCsvSpec& spec, XtRawTick& out, uint64_t& key);
    // 乱序（key 回退）记账 + 限频 warn；不改序不拒数据。
    void note_key_regression_(const char* path_hint, uint64_t prev_key, uint64_t key);
    // 交付点记账：该源文件贡献了这个 trading_date（见 source_trading_days）。
    void note_source_day_(size_t path_idx, uint32_t trading_date);

    // ── 运行时状态 ──────────────────────────────────────────────
    Options opts_;          ///< 构造期传入的配置快照；reader 生命周期内不变。
    bool multi_ = false;     ///< true 表示 paths.size() > 1，使用 heap merge 路径。
    bool init_done_ = false; ///< true 表示 init() 已完成轻量可读性检查。
    uint64_t seq_ = 0;  ///< key 相同 tick 的稳定 tie-breaker，不参与时间语义。
    uint64_t last_single_key_ = 0;      ///< 单文件模式前一条 key（乱序检测）。
    uint64_t out_of_order_count_ = 0;   ///< 文件内 key 回退累计（见 out_of_order_count()）。
    ReadStats read_stats_;              ///< 逐行解析分类计数（见 read_stats()）。
    /// @brief 每个源文件交付过的 trading_date（升序去重）；见 source_trading_days()。
    std::vector<std::vector<uint32_t>> src_days_;
    std::unique_ptr<std::ifstream> in_; ///< 单文件模式 stream。
    XtTickCsvSpec spec_;                ///< 单文件模式 CSV schema。
    std::vector<Reader> readers_;       ///< 多文件模式 reader 状态；只保存已预读到 tick 的文件。
    std::priority_queue<Node, std::vector<Node>, NodeCmp> heap_; ///< 多文件模式下一条 tick 选择堆。
};
