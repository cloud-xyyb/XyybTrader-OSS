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
        double bid_prices[5]; ///< 买一到买五价格；缺档为 quiet NaN。
        double bid_qtys[5];   ///< 买一到买五数量；缺档为 quiet NaN。
        double ask_prices[5]; ///< 卖一到卖五价格；缺档为 quiet NaN。
        double ask_qtys[5];   ///< 卖一到卖五数量；缺档为 quiet NaN。
    };

    /// @brief 创建空 replay book。
    ///
    /// `warmup_events` 控制 update-only stream 何时离开 warmup；snapshot event 应用后立即离开
    /// warmup。`max_levels` 保留为构造参数以兼容 caller，导出的 snapshot 固定为 L5。
    explicit XtBookReplayer(std::size_t max_levels = 25, std::size_t warmup_events = 1000);

    /// @brief 应用一条归一化 book event。
    ///
    /// @param side update/snapshot event 使用，大小写不敏感，取值为 `"bid"` 或 `"ask"`。
    /// @param price 十进制价格；内部按 `round(price * 1e6)` 转为整数 key。
    /// @param qty 该价位数量；`qty <= 0` 表示删除该价位。
    /// @param event_type 大小写不敏感，取值为 `"update"`、`"snapshot"` 或 `"clear"`。
    /// @param first_update_id 仅为兼容输入 schema 接收，当前不消费。
    /// @param final_update_id snapshot group id；同一组 snapshot row 共用以触发 clear-then-apply。
    /// @param prev_final_update_id 仅为兼容输入 schema 接收，不做 gap validation。
    ///
    /// @throws std::invalid_argument `side` 或 `event_type` 不支持。
    void process_event(std::string_view side,
                       double price,
                       double qty,
                       std::string_view event_type = "update",
                       std::optional<int64_t> first_update_id = std::nullopt,
                       std::optional<int64_t> final_update_id = std::nullopt,
                       std::optional<int64_t> prev_final_update_id = std::nullopt);

    /// @brief 返回当前 top-5 book，不修改 replay state。
    L5Snapshot l5_snapshot() const;

    /// @brief 清空 bid/ask 两侧，并重置 warmup 和 snapshot grouping 状态。
    void reset();

private:
    // 十进制价格 key scale；与 canonical parquet feed 的 6 位小数精度一致。
    static constexpr double kPriceScale = 1e6;

    // ── 价格与事件处理 helper ──────────────────────────────────────────────
    int64_t price_to_key(double price) const;
    double key_to_price(int64_t key) const;
    void process_snapshot_start(std::optional<int64_t> final_update_id);
    void apply_level(std::string_view side, double price, double qty);
    void clear_book();
    static bool equals_ignore_case(std::string_view lhs, std::string_view rhs);

    // ── Book 状态 ──────────────────────────────────────────────
    // price_key -> qty；bid/ask 排序只在 l5_snapshot() 时派生，写入路径不维护有序容器。
    std::unordered_map<int64_t, double> bids_; ///< bid side level qty，key = round(price * kPriceScale)。
    std::unordered_map<int64_t, double> asks_; ///< ask side level qty，key = round(price * kPriceScale)。

    // ── Replay 进度 ──────────────────────────────────────────────
    bool in_warmup_ = true;      ///< update-only stream 未达到 warmup_events_ 前为 true。
    std::size_t events_seen_ = 0; ///< 自 reset 或 clear 后累计处理的 update/snapshot level 数。
    std::size_t warmup_events_;   ///< update-only stream 离开 warmup 的最小事件数。
    std::optional<int64_t> snapshot_group_id_;  ///< 相同 id 的多行累加为同一个 snapshot。
};
