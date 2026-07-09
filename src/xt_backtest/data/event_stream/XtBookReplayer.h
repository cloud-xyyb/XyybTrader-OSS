#pragma once

#include <cstdint>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

/// @brief canonical book events 到 L5 snapshot 的本地 order book projector。
///
/// 供 `XtEventParquetReader` 在回放线程内串行使用：`process_event()` 接收归一化后的
/// book event，`l5_snapshot()` 输出当前 top-5 bid/ask，`reset()` 清空本地 book 状态。
/// 本类只维护 reader 侧的 replay book，不读取 parquet、不生成 tick，也不做并发保护。
class XtBookReplayer {
public:
    /// @brief 固定 L5 book snapshot。
    ///
    /// Bid levels 按价格从高到低排列，ask levels 按价格从低到高排列。
    /// 缺失档位的 price / qty 均填 quiet NaN。
    struct L5Snapshot {
        double bid_prices[5];
        double bid_qtys[5];
        double ask_prices[5];
        double ask_qtys[5];
    };

    explicit XtBookReplayer(std::size_t max_levels = 25, std::size_t warmup_events = 1000);

    void process_event(std::string_view side,
                       double price,
                       double qty,
                       std::string_view event_type = "update",
                       std::optional<int64_t> first_update_id = std::nullopt,
                       std::optional<int64_t> final_update_id = std::nullopt,
                       std::optional<int64_t> prev_final_update_id = std::nullopt);

    L5Snapshot l5_snapshot() const;

    void reset();

private:

    static constexpr double kPriceScale = 1e6;

    int64_t price_to_key(double price) const;
    double key_to_price(int64_t key) const;
    void process_snapshot_start(std::optional<int64_t> final_update_id);
    void apply_level(std::string_view side, double price, double qty);
    void clear_book();
    static bool equals_ignore_case(std::string_view lhs, std::string_view rhs);

    std::unordered_map<int64_t, double> bids_;
    std::unordered_map<int64_t, double> asks_;

    bool in_warmup_ = true;
    std::size_t events_seen_ = 0;
    std::size_t warmup_events_;
    std::optional<int64_t> snapshot_group_id_;
};
