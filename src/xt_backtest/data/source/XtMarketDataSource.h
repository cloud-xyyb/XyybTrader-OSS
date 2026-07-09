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

enum class TickSource : uint8_t {
    None = 0,
    Csv,
    BinStream,
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

    XtReplayMode mode() const { return mode_; }
    bool streaming() const { return use_stream_; }

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
        std::vector<std::string> bin_paths;
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
    bool load_tick_bin_into_memory();
    bool build_event_stream_cache();

    bool next_tick(XtRawTick& out);
    bool read_next_event_uncached_(XtReplayEvent& out);

    bool admit_stream_tick_(XtRawTick& t) const;

    bool next_tick_day_buffered_(XtRawTick& out);

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
};
