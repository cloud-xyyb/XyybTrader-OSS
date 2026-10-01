#pragma once

#include <cstdint>
#include <memory>
#include <queue>
#include <string>
#include <unordered_set>
#include <vector>

#include "api/IMarketDataReader.h"
#include "xt_backtest/data/event_stream/XtBookReplayer.h"
#include "TickCsvUtil.h"

namespace arrow {
class Array;
class Table;
}

/// @brief 从 canonical events parquet 构造 L5 tick 流的回测 reader。
///
/// 输入为 trades/book 两类 parquet：trade row 产生 tick，book row 通过 `XtBookReplayer`
/// 维护 L5 快照并填充 tick 深度。多个 exchange/symbol/day stream 按 exchange timestamp
/// heap merge；相同时间戳用内部 seq 保持稳定顺序。
/// 单线程顺序读取，`reset()` 只重置游标和 book replay state，不重新扫描磁盘。
class XtEventParquetReader : public IMarketDataReader {
public:
    /// @brief canonical events parquet reader 的查询配置。
    struct Options {
        std::string events_root;             ///< canonical events 根目录。
        std::vector<std::string> symbols;    ///< 空表示不过滤 symbol。
        uint32_t start_trading_day = 0;      ///< YYYYMMDD inclusive。
        uint32_t end_trading_day = 0;        ///< YYYYMMDD inclusive。
        XtTickVolumeMode volume_mode = XtTickVolumeMode::Cumulative; ///< trade quantity 输出为累计或增量。
    };

    explicit XtEventParquetReader(Options opts);

    /// @return true 表示至少加载到一个包含 trade 的 stream。
    bool init() override;
    /// @brief 重置 stream 游标、book replay state 与 heap；不重新扫描磁盘。
    void reset() override;
    /// @return true 表示 `out` 已写入下一条由 trade + L5 book 合成的 tick。
    bool next_tick(XtRawTick& out) override;

private:
    /// @brief trade parquet 中已归一化的一行成交。
    struct TradeRow {
        std::string exchange;   ///< vendor 原始 exchange；空时回退 Stream::exchange。
        std::string symbol;     ///< vendor 原始 symbol；空时回退 Stream::symbol。
        int64_t exch_ts_ms = 0; ///< exchange timestamp，epoch ms。
        double price = 0.0;     ///< 成交价。
        double quantity = 0.0;  ///< 单笔成交量。
    };

    /// @brief 单个 exchange/symbol/day parquet stream 的游标和 book replay 状态。
    struct Stream {
        std::string exchange;       ///< 路径或 trade row 推导出的 exchange fallback。
        std::string symbol;         ///< 路径/filter 限定值，或聚合文件按 trade row 分桶的桶 key；空 = 匿名单流。
        uint32_t trading_day = 0;   ///< stream 所属交易日，YYYYMMDD。
        std::vector<TradeRow> trades; ///< 已按 exch_ts_ms 稳定排序的 trade rows。
        std::size_t trade_pos = 0;  ///< 下一条待输出 trade 下标。
        std::size_t book_pos = 0;   ///< 下一条待应用 book row 下标。
        double cum_volume = 0.0;    ///< cumulative volume 模式下的 per-stream 累计成交量。
        XtBookReplayer replayer;    ///< 当前 stream 的 L5 book replay state。
        std::shared_ptr<arrow::Table> trades_table; ///< 持有 trade parquet table，保证列数组生命周期。
        std::shared_ptr<arrow::Table> book_table;   ///< 持有 book parquet table，保证列数组生命周期。
        std::shared_ptr<arrow::Array> book_ts;       ///< book.exch_ts_ms 列。
        std::shared_ptr<arrow::Array> book_event_type; ///< book.event_type 列。
        std::shared_ptr<arrow::Array> book_first;    ///< book.first_update_id 列。
        std::shared_ptr<arrow::Array> book_final;    ///< book.final_update_id 列。
        std::shared_ptr<arrow::Array> book_prev;     ///< book.prev_final_update_id 列。
        std::shared_ptr<arrow::Array> book_side;     ///< book.side 列。
        std::shared_ptr<arrow::Array> book_price;    ///< book.price 列。
        std::shared_ptr<arrow::Array> book_qty;      ///< book.quantity 列。
        std::shared_ptr<arrow::Array> book_symbol;   ///< book.symbol 列。
        std::size_t book_rows = 0;  ///< book_table 行数；book column arrays 以此为上界。
    };

    /// @brief 多 stream heap merge 中的下一条 trade 指针。
    struct HeapNode {
        int64_t ts_ms = 0;          ///< heap 排序主键，下一条 trade exch_ts_ms。
        std::size_t stream_idx = 0; ///< streams_ 下标；node 不持有 Stream。
        std::size_t seq = 0;        ///< 相同 timestamp 下的稳定 tie-breaker。
    };

    /// @brief priority_queue 比较器；按 timestamp/seq 形成最小堆语义。
    struct HeapCmp {
        bool operator()(const HeapNode& a, const HeapNode& b) const {
            if (a.ts_ms != b.ts_ms) return a.ts_ms > b.ts_ms;
            return a.seq > b.seq;
        }
    };

    bool load_streams();
    /// @brief 读取一对 trades/book parquet，按 symbol 分桶追加到 streams_（一对文件可产出多个 Stream）。
    /// @note `symbol` 非空（per-symbol 目录 / 单 symbol filter）退化为单 Stream；空（聚合文件）
    ///       按 trade row symbol 建桶，各 Stream 共享同一 table shared_ptr，book 行由
    ///       next_tick 按 stream.symbol skip 路由。
    bool load_one_stream(const std::string& exchange,
                         const std::string& symbol,
                         uint32_t trading_day,
                         const std::string& trades_path,
                         const std::string& book_path);
    bool push_stream_head(std::size_t stream_idx);

    Options opts_; ///< 构造期传入的 parquet 查询配置。
    std::vector<Stream> streams_; ///< 已加载且至少包含一条 trade 的 stream。
    std::priority_queue<HeapNode, std::vector<HeapNode>, HeapCmp> heap_; ///< 每个 stream 的下一条 trade。
    std::unordered_set<std::string> symbol_filter_; ///< 从 Options::symbols 派生；空集表示不过滤。
    std::size_t seq_ = 0;       ///< heap tie-breaker，reset() 后从 0 重新开始。
    bool initialized_ = false;  ///< true 表示 init() 已成功加载 streams_。
};
