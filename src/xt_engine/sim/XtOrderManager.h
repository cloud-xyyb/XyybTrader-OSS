#pragma once

#include <cstdint>
#include <deque>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "xt_core/market/XtRawTick.h"
#include "XtSimOrder.h"
#include "XtOrderBook.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/order/XtOffset.h"
#include "IQueueModel.h"

namespace YAML {
class Node;
}

/// @brief HFT 回测订单状态机与本地订单簿适配层。
///
/// `submit()` / `cancel()` 接收订单请求，`advance_clock()` 推进订单生命周期，
/// `set_depth_snapshot_fp()` 写入盘口，`match_tick()` 输出成交，
/// `drain_updates()` 输出订单状态增量。
///
/// 调用方按 replay worker 串行推进同一实例；本类不提供并发保护。
class XtOrderManager {
public:
    /// @brief 撮合状态机运行参数。
    struct RuntimeConfig {
        double queue_cancel_ratio = 0.0;
        uint64_t ack_delay_ticks = 0;
        uint64_t ttl_ticks = 0;

        uint64_t ack_delay_us = 0;
        uint64_t ttl_us = 0;

        double ofi_queue_scale = 0.0;

        bool market_impact_enabled = false;
        double market_impact_decay = 0.5;
    };

    void configure_runtime(const RuntimeConfig& cfg);

    void set_queue_model(const IQueueModel* m) noexcept { queue_model_ = m; }

    uint64_t alloc_order_id() { return next_id_++; }

    void advance_clock(uint64_t tick, int64_t event_now_ns,
                       uint32_t trading_date, uint32_t action_time_ms);

    void advance_clock(uint64_t tick, uint32_t date, uint32_t time_ms);

    void advance_clock_to(int64_t event_now_ns,
                          uint32_t trading_date, uint32_t action_time_ms);

    void advance_clock_to(int64_t event_now_ns);

    int64_t peek_earliest_due_ns() const noexcept { return earliest_due_event_ns_; }

    uint64_t event_clock_clamp_count() const noexcept { return event_clock_clamp_count_; }

    uint64_t submit(uint64_t order_id,
                    const char* code,
                    bool is_buy,
                    int64_t price_fp,
                    int64_t qty_fp,
                    XtTimeInForce tif = XtTimeInForce::GTC,
                    uint32_t order_date = 0,
                    uint32_t order_time_ms = 0,
                    std::string_view user_tag = "",
                    XtOffset close_offset = XtOffset::Open,
                    bool is_resubmit = false,
                    uint32_t ack_delay_ms_sampled = 0,
                    uint32_t fill_latency_ms_sampled = 0,
                    int64_t response_latency_ns_sampled = -1);

    bool cancel(uint64_t id, uint32_t cancel_delay_ms_sampled = 0);

    void emit_in_transit_canceled(uint64_t order_id, const char* code, bool is_buy,
                                  int64_t price_fp, int64_t qty_fp,
                                  XtTimeInForce tif, std::string_view user_tag,
                                  XtOffset close_offset);

    bool reject(uint64_t id, const char* reason_code = nullptr);

    void discard_eof_live_orders();

    /// @brief submit/book 容量与输入校验拒单计数。
    struct EngineDrops {
        uint64_t capacity_max_concurrent = 0;
        uint64_t blank_code              = 0;
        uint64_t zero_qty                = 0;
        uint64_t nonpos_price            = 0;
        uint64_t orders_per_level        = 0;
        uint64_t price_levels_full       = 0;
    };

    const EngineDrops& engine_drops() const noexcept { return engine_drops_; }

    void note_orders_per_level_drop() noexcept { ++engine_drops_.orders_per_level; }

    void note_price_levels_full_drop() noexcept { ++engine_drops_.price_levels_full; }

    void set_depth_snapshot_fp(const char* code,
                               const int64_t* bid_px_fp, const int64_t* ask_px_fp,
                               const int64_t* bid_qty_fp, const int64_t* ask_qty_fp,
                               int depth);

    void set_price_limits_fp(int64_t upper_fp, int64_t lower_fp) noexcept {
        upper_limit_fp_ = upper_fp;
        lower_limit_fp_ = lower_fp;
    }

    void match_tick(const char* code,
                    const int64_t* bid_px_fp,
                    const int64_t* ask_px_fp,
                    const int64_t* bid_qty_fp,
                    const int64_t* ask_qty_fp,
                    int depth,
                    double max_fill_qty,
                    std::vector<XtTradeRecord>& trades);

    const XtSimOrder* find(uint64_t id) const;

    const std::vector<XtSimOrder>& live_orders() const;

    void drain_updates(std::vector<XtOrderExecUpdate>& out);

private:
    /// @brief 单 tick 内按订单累计成交量；供 IOC 剩余撤单判断使用。
    struct FillRecord {
        uint64_t id = 0;
        int64_t qty_fp = 0;
    };
    static constexpr int kMaxFillsPerTick = 256;

    /// @brief 单 symbol 盘口快照与派生状态（queue model / OFI / impact carry）。
    ///
    /// 多 symbol 回放下 last/prev/carry 必须按 code 隔离：任意 symbol 的 tick
    /// 只滚动自己的 prev/last，否则 maker/taker 判定、queue erosion 与
    /// market impact carry 会被「最近任意 symbol」的盘口跨合约污染。
    struct DepthSnapshot {
        int64_t last_bid_px_fp[XtRawTick::DEPTH] = {0};
        int64_t last_ask_px_fp[XtRawTick::DEPTH] = {0};
        double  last_bid_qty[XtRawTick::DEPTH]   = {0};
        double  last_ask_qty[XtRawTick::DEPTH]   = {0};
        int64_t prev_bid_px_fp[XtRawTick::DEPTH] = {0};
        int64_t prev_ask_px_fp[XtRawTick::DEPTH] = {0};
        double  prev_bid_qty[XtRawTick::DEPTH]   = {0};
        double  prev_ask_qty[XtRawTick::DEPTH]   = {0};
        int     last_depth = 0;

        double consumed_ask_carry[XtRawTick::DEPTH] = {0};
        double consumed_bid_carry[XtRawTick::DEPTH] = {0};
    };

    /// @brief per-code 快照槽；线性小表（同 XtHftMatchEngine::VolumeEntry 惯例），
    ///        warm path 无 hash，槽一经建立身份不变、永不复用。
    struct DepthSlot {
        char code[32] = {};
        DepthSnapshot snap;
    };

    XtSimOrder* find_live(uint64_t id) noexcept {
        auto it = id_to_slot_.find(id);
        return (it != id_to_slot_.end()) ? &orders_[it->second] : nullptr;
    }
    const XtSimOrder* find_live(uint64_t id) const noexcept {
        auto it = id_to_slot_.find(id);
        return (it != id_to_slot_.end()) ? &orders_[it->second] : nullptr;
    }

    const DepthSnapshot* find_depth_(const char* code) const noexcept;

    DepthSnapshot& find_or_insert_depth_(const char* code);

    void refresh_queue_ahead(const char* code, const DepthSnapshot& snap, int depth);

    void apply_queue_cancel_erosion(const char* code, const DepthSnapshot& snap, int depth);

    bool is_board_price_(int64_t px_fp) const noexcept {
        return (upper_limit_fp_ > 0 && px_fp == upper_limit_fp_)
            || (lower_limit_fp_ > 0 && px_fp == lower_limit_fp_);
    }

    void apply_fill_updates(const std::vector<XtOrderBook::Fill>& fills,
                            std::vector<XtTradeRecord>& trades,
                            FillRecord* filled_buf, int& filled_count);

    void apply_canceled_ids(const std::vector<uint64_t>& ids);

    void apply_done_ids(const std::vector<uint64_t>& ids);

    void cancel_ioc_remainders(const FillRecord* filled_buf, int filled_count);

    bool cancel_now_(uint64_t id);

    void accumulate_market_impact(const std::vector<XtOrderBook::Fill>& fills,
                                  DepthSnapshot& snap);

    void archive_order(uint64_t id,
                       bool remove_from_book,
                       bool emit_update,
                       int64_t last_fill_fp);

    void push_update(const XtSimOrder& ord, int64_t last_fill_fp);

    int64_t clamp_event_now_(int64_t event_now_ns) noexcept;

    void note_event_deadline_(int64_t due_event_ns) noexcept {
        if (due_event_ns < earliest_due_event_ns_) earliest_due_event_ns_ = due_event_ns;
    }

    int64_t event_base_() const noexcept {
        return cur_event_ns_ != INT64_MIN ? cur_event_ns_ : 0;
    }

    void recompute_earliest_due_() noexcept;

    uint64_t next_id_ = 1;
    uint64_t cur_tick_ = 0;
    uint32_t cur_date_ = 0;
    uint32_t cur_time_ = 0;

    uint64_t ack_delay_ticks_ = 0;
    uint64_t ttl_ticks_ = 0;
    uint64_t ack_delay_us_ = 0;
    uint64_t ttl_us_ = 0;

    const IQueueModel* queue_model_ = nullptr;

    double queue_cancel_ratio_ = 0.0;
    double ofi_queue_scale_ = 0.0;
    bool   market_impact_enabled_ = false;
    double market_impact_decay_ = 0.5;
    int64_t upper_limit_fp_ = 0;
    int64_t lower_limit_fp_ = 0;

    static constexpr int kMaxConcurrentOrders = 8192;
    std::vector<XtSimOrder> orders_;
    EngineDrops engine_drops_;

    std::unordered_map<uint64_t, size_t> id_to_slot_;

    static constexpr size_t kDoneOrderCacheCap = 8192;
    std::deque<XtSimOrder> done_;
    std::vector<XtOrderExecUpdate> updates_;
    std::vector<uint64_t> pending_acks_;
    std::vector<uint64_t> pending_fills_;
    std::vector<uint64_t> pending_cancels_;

    std::vector<uint64_t> clock_scratch_;

    int64_t earliest_due_event_ns_ = INT64_MAX;
    int64_t cur_event_ns_ = INT64_MIN;
    uint64_t event_clock_clamp_count_ = 0;

    std::vector<DepthSlot> depth_table_;

    XtOrderBook order_book_;
};

namespace engine_config {

XtOrderManager::RuntimeConfig parse_order_manager_runtime_config(const YAML::Node& node);

}
