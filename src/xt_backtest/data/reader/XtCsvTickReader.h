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

    struct Options {
        std::vector<std::string> paths;
        std::unordered_set<std::string> symbol_filter;
        double qty_base = 100.0;
        XtTickVolumeMode volume_mode = XtTickVolumeMode::Cumulative;
    };

    explicit XtCsvTickReader(Options opts);

    bool init() override;

    void reset() override;

    bool next_tick(XtRawTick& out) override;

    uint64_t out_of_order_count() const { return out_of_order_count_; }

    /// @brief 逐行解析计数（consumed manifest 分类用）。
    ///
    /// `lines_read` = 送入 parse 的数据行总数（不含表头/空行/注释）；`symbol` /
    /// `bad_row` = parse 返回 false 的分类计数。三者满足
    /// `lines_read == 交付 tick 数 + symbol + bad_row`。reset() 清零。
    struct ReadStats {
        int64_t lines_read = 0;
        int64_t symbol     = 0;
        int64_t bad_row    = 0;
    };
    const ReadStats& read_stats() const { return read_stats_; }

    const std::vector<std::vector<uint32_t>>& source_trading_days() const { return src_days_; }

private:

    struct Reader {
        std::unique_ptr<std::ifstream> in;
        XtTickCsvSpec spec;
        XtRawTick tick;
        uint64_t key = 0;
        uint64_t seq = 0;
        size_t path_idx = 0;
    };

    /// @brief 多文件 heap merge 中指向 reader 预读 tick 的排序节点。
    struct Node {
        uint64_t key = 0;
        uint64_t seq = 0;
        size_t reader_idx = 0;
    };

    /// @brief priority_queue 比较器；按 key/seq 形成最小堆语义。
    struct NodeCmp {
        bool operator()(const Node& a, const Node& b) const {
            if (a.key != b.key) return a.key > b.key;
            return a.seq > b.seq;
        }
    };

    bool next_single(XtRawTick& out);
    bool next_multi(XtRawTick& out);

    bool init_readers();
    bool read_next_tick(Reader& reader);
    bool read_next_parsed_line(std::ifstream& in, XtTickCsvSpec& spec, XtRawTick& out, uint64_t& key);

    void note_key_regression_(const char* path_hint, uint64_t prev_key, uint64_t key);

    void note_source_day_(size_t path_idx, uint32_t trading_date);

    Options opts_;
    bool multi_ = false;
    bool init_done_ = false;
    uint64_t seq_ = 0;
    uint64_t last_single_key_ = 0;
    uint64_t out_of_order_count_ = 0;
    ReadStats read_stats_;

    std::vector<std::vector<uint32_t>> src_days_;
    std::unique_ptr<std::ifstream> in_;
    XtTickCsvSpec spec_;
    std::vector<Reader> readers_;
    std::priority_queue<Node, std::vector<Node>, NodeCmp> heap_;
};
