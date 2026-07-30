#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "api/IMarketDataReader.h"
#include "xt_core/market/XtRawTick.h"
#include "xt_core/market/XtL2MarketStructs.h"
#include "xt_backtest/data/source/XtMarketDataSourceConfig.h"
#include "xt_backtest/data/legacy/XtBarModeDriver.h"

enum class XtReplayMode : uint8_t {
    Bar,
    Tick,
    EventStream
};

enum class XtReplayEventType : uint8_t {
    Tick = 0,

    OrderQueue = 1,
    OrderDetail = 2,
    Transaction = 3
};

/**
 * @brief 回测回放事件（backtest replay event）。
 *
 * 统一封装回放时序中的单个事件，由 `type` 指定本次事件的有效 payload 字段。
 * `key` 是统一 sort key，`seq` 是同 key 下的稳定序号，供多源事件合并排序使用。
 * 调用方必须先检查 `type`，再读取对应 payload 字段。
 */
struct XtReplayEvent {
    XtReplayEventType type = XtReplayEventType::Tick;
    uint64_t key = 0;
    uint64_t seq = 0;
    XtRawTick tick;
    XtOrderQueue order_queue;
    XtOrderDetail order_detail;
    XtTransaction transaction;
};

/// @brief 下一事件的时间头（non-consuming peek 用）。
///
/// sort key 只保证序（夜盘旋转编码，不可反解真实时刻）；event-axis 时间
/// 一律取自事件 payload 本头：`to_ns(action_date != 0 ? action_date : trading_date,
/// action_time)`。四类事件 payload 均携带三字段。
struct XtReplayEventHeader {
    uint64_t key = 0;
    uint32_t action_date = 0;
    uint32_t action_time = 0;
    uint32_t trading_date = 0;
};

enum class TickSource : uint8_t {
    None = 0,
    Csv,
    BinStream,
    ShardStream,
    Fbs,
    EventsParquet,
};

/// @brief 回测行情数据源（market data source for backtest）。
///
/// 按配置加载 tick/bar/orderflow 数据，并通过 `next_event()` 提供单消费者
/// 顺序读取接口。策略执行、账户更新和报告统计由 runtime/reporter 处理。
///
/// @note 内部维护读取游标和 1-event look-ahead 状态，不支持并发读取。
class XtMarketDataSource {
public:

    ~XtMarketDataSource();

    bool load(const XtMarketDataSourceConfig& cfg);

    void reset();

    bool next_event(XtReplayEvent& out);

    bool peek_next_key(uint64_t& out_key);

    bool peek_next_event_header(XtReplayEventHeader& out);

    XtReplayMode mode() const { return mode_; }
    bool streaming() const { return use_stream_; }

    uint64_t data_mode_ignored_count() const { return data_mode_ignored_count_; }

    bool has_io_error() const { return stream_io_error_; }

    const std::vector<std::string>& consumed_tick_files() const { return tick_.files; }

    const char* tick_source_name() const {
        switch (tick_.source) {
            case TickSource::Csv:           return "csv";
            case TickSource::BinStream:     return "bin_stream";
            case TickSource::ShardStream:   return "shard_stream";
            case TickSource::Fbs:           return "fbs";
            case TickSource::EventsParquet: return "events_parquet";
            default:                        return "none";
        }
    }
    bool bin_cache_enabled() const { return tick_.use_bin_cache; }

    bool bin_cache_hit() const { return tick_.bin_cache_hit; }

    std::string bin_cache_path() const {
        return tick_.bin_paths.empty() ? std::string() : tick_.bin_paths.front();
    }

    const std::vector<std::string>& shard_cache_paths() const { return tick_.shard_paths; }

    bool shard_cache_hit() const { return tick_.shard_cache_hit; }

    /// @brief RawShardV3 建库的复用/重建分解（增量是否真的生效，看这组数）。
    struct ShardCacheStats {
        uint64_t days_required = 0;
        uint64_t days_reused = 0;
        uint64_t days_built = 0;

        bool full_rebuild = false;

        uint64_t duplicate_sources = 0;

        uint64_t publish_failed = 0;

        uint64_t manifest_malformed = 0;
    };
    const ShardCacheStats& shard_cache_stats() const { return tick_.shard_stats; }

    /// @brief 载入→交付分类丢弃计数（consumed manifest 分解用）。
    ///
    /// `rows_loaded` = 交付前总读入行数（-1 = 该来源不可归因）；`window/session/
    /// skip_last/symbol/bad_row` = 数据源层丢弃分类。CSV/bin 路可精确归因
    /// （`loaded_trackable=true`）；fbs/parquet reader 内部预筛不可见 → 不可归因。
    /// 满足（trackable 时）：`rows_loaded == 交付 tick 数 + window + session +
    /// skip_last + symbol + bad_row`。stream 路在 next_event 消费期间累积，
    /// 故需在 replay 全部消费后读取。
    struct DropStats {
        int64_t rows_loaded = -1;
        int64_t window   = 0;
        int64_t session  = 0;
        int64_t skip_last = 0;
        int64_t symbol   = 0;
        int64_t bad_row  = 0;
        bool loaded_trackable = false;
    };
    const DropStats& drop_stats() const { return drop_stats_; }

private:

    struct DateRange {
        uint32_t start = 0;
        uint32_t end   = 0;
    };

    /// @brief tick mode 的配置快照、输入选择和 stream reader 状态。
    struct TickSourceState {
        double qty_base = 100.0;
        std::string volume_mode = "cumulative";
        std::string root;
        std::vector<std::string> files;
        std::vector<std::string> symbols;
        std::unordered_set<std::string> symbol_set;
        TickSource source = TickSource::None;
        std::string cache_dir = "data/ticks/cache";
        bool use_bin_cache = false;
        bool bin_cache_hit = false;
        std::vector<std::string> bin_paths;
        std::string cache_format = "v2";
        std::vector<std::string> shard_paths;
        bool shard_cache_hit = false;
        ShardCacheStats shard_stats;
        std::unique_ptr<IMarketDataReader> reader;
        bool reader_is_bar_simtick = false;

        std::string fbs_root;
        std::string events_parquet_root;
    };

    /// @brief memory data mode 的 tick 缓存与消费游标。
    struct MemoryState {
        size_t cursor = 0;
        std::vector<XtRawTick> ticks;
    };

    /// @brief skip_last_tick_per_day 流式实现的单日缓冲；reset_runtime_state 归零。
    struct DayBufferState {
        std::vector<XtRawTick> ready;
        size_t cursor = 0;
        XtRawTick pending{};
        bool has_pending = false;
        bool exhausted = false;
        bool seen_any = false;

    };

    /// @brief event_stream 模式的 Level-2 orderflow 输入文件集合。
    struct OrderFlowState {
        std::vector<std::string> order_queue_files;
        std::vector<std::string> order_detail_files;
        std::vector<std::string> transaction_files;
    };

    void apply_common_config(const XtMarketDataSourceConfig& cfg);
    bool collect_existing_tick_files(bool fail_on_missing);
    bool prepare_tick_mode(const XtMarketDataSourceConfig& cfg);
    void reset_runtime_state();
    void init_stream_reader();

    void load_memory_source();
    bool load_csvs_to_memory(const std::vector<std::string>& paths);
    bool build_tick_bin_from_csvs(const std::vector<std::string>& csv_paths, const std::string& bin_path);
    bool ensure_tick_bin_cache(const std::vector<std::string>& sources);

    bool ensure_tick_shard_cache(const std::vector<std::string>& all_sources,
                                 bool allow_undated_sources);
    bool load_tick_bin_into_memory();

    bool load_tick_shards_into_memory();
    bool build_event_stream_cache();

    bool next_tick(XtRawTick& out);
    bool read_next_event_uncached_(XtReplayEvent& out);

    bool admit_stream_tick_(XtRawTick& t);

    bool next_tick_day_buffered_(XtRawTick& out);

    void latch_reader_io_error_();

    void finalize_stream_load_stats_();

    void note_stream_forced_(const char* source_tag);

    XtReplayMode mode_      = XtReplayMode::Bar;
    bool         use_stream_ = true;
    bool         skip_last_tick_per_day_ = false;
    xt_core::Config session_;
    DateRange   range_;
    TickSourceState   tick_;
    MemoryState mem_;
    DayBufferState day_buf_;
    XtBarModeDriver bar_driver_;
    OrderFlowState  orderflow_;
    std::vector<XtReplayEvent> events_;
    size_t      event_cursor_   = 0;

    std::optional<XtReplayEvent> cached_next_event_;
    bool stream_io_error_ = false;
    uint64_t data_mode_ignored_count_ = 0;
    DropStats drop_stats_;
    int64_t stream_records_read_ = 0;
    bool stream_stats_finalized_ = false;

    bool at_start_ = false;
};
