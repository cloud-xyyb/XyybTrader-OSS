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
        std::string events_root;
        std::vector<std::string> symbols;
        uint32_t start_trading_day = 0;
        uint32_t end_trading_day = 0;
        XtTickVolumeMode volume_mode = XtTickVolumeMode::Cumulative;
    };

    explicit XtEventParquetReader(Options opts);

    bool init() override;

    void reset() override;

    bool next_tick(XtRawTick& out) override;

private:
    /// @brief trade parquet 中已归一化的一行成交。
    struct TradeRow {
        std::string exchange;
        std::string symbol;
        int64_t exch_ts_ms = 0;
        double price = 0.0;
        double quantity = 0.0;
    };

    /// @brief 单个 exchange/symbol/day parquet stream 的游标和 book replay 状态。
    struct Stream {
        std::string exchange;
        std::string symbol;
        uint32_t trading_day = 0;
        std::vector<TradeRow> trades;
        std::size_t trade_pos = 0;
        std::size_t book_pos = 0;
        double cum_volume = 0.0;
        XtBookReplayer replayer;
        std::shared_ptr<arrow::Table> trades_table;
        std::shared_ptr<arrow::Table> book_table;
        std::shared_ptr<arrow::Array> book_ts;
        std::shared_ptr<arrow::Array> book_event_type;
        std::shared_ptr<arrow::Array> book_first;
        std::shared_ptr<arrow::Array> book_final;
        std::shared_ptr<arrow::Array> book_prev;
        std::shared_ptr<arrow::Array> book_side;
        std::shared_ptr<arrow::Array> book_price;
        std::shared_ptr<arrow::Array> book_qty;
        std::shared_ptr<arrow::Array> book_symbol;
        std::size_t book_rows = 0;
    };

    /// @brief 多 stream heap merge 中的下一条 trade 指针。
    struct HeapNode {
        int64_t ts_ms = 0;
        std::size_t stream_idx = 0;
        std::size_t seq = 0;
    };

    /// @brief priority_queue 比较器；按 timestamp/seq 形成最小堆语义。
    struct HeapCmp {
        bool operator()(const HeapNode& a, const HeapNode& b) const {
            if (a.ts_ms != b.ts_ms) return a.ts_ms > b.ts_ms;
            return a.seq > b.seq;
        }
    };

    bool load_streams();

    bool load_one_stream(const std::string& exchange,
                         const std::string& symbol,
                         uint32_t trading_day,
                         const std::string& trades_path,
                         const std::string& book_path);
    bool push_stream_head(std::size_t stream_idx);

    Options opts_;
    std::vector<Stream> streams_;
    std::priority_queue<HeapNode, std::vector<HeapNode>, HeapCmp> heap_;
    std::unordered_set<std::string> symbol_filter_;
    std::size_t seq_ = 0;
    bool initialized_ = false;
};
