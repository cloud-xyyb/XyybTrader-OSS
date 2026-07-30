#pragma once

#include <cstdint>
#include <cstddef>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "xt_core/market/XtTick.h"
#include "XtOrderManager.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtOffset.h"
#include "HFTMatchEngineCfg.h"
#include "ILatencyModel.h"
#include "XtLatencyArith.h"
#include "IQueueModel.h"

namespace YAML {
class Node;
}

/// @brief HFT 回测撮合门面。
///
/// 调用方注入配置、latency model 和 queue model 后，通过 `advance_clock()`、
/// `submit()`、`cancel()` 与 `on_tick()` 推进订单生命周期。
/// 新增成交写入调用方提供的 trades，订单状态经 `drain_updates()` 输出。
///
/// 同一实例由 replay loop 串行调用；本类不提供并发保护。
class XtHftMatchEngine {
public:
    using Config = XtHftMatchEngineCfg;

    XtHftMatchEngine() = default;
    explicit XtHftMatchEngine(const Config& cfg) { set_config(cfg); }

    void set_config(const Config& cfg);

    void reset_volume_cache();

    void advance_clock(uint64_t tick_idx, int64_t event_now_ns,
                       uint32_t trading_date, uint32_t action_time_ms);

    void advance_clock(uint64_t tick_idx, uint32_t date, uint32_t time_ms);

    void advance_clock_to(int64_t event_now_ns,
                          uint32_t trading_date, uint32_t action_time_ms);

    void advance_clock(uint32_t date, uint32_t time_ms);

    void set_dispatch_clock(uint32_t date, uint32_t time_ms) noexcept {
        dispatch_date_ = date;
        dispatch_time_ms_ = time_ms;
    }

    void set_latency_model(std::unique_ptr<ILatencyModel> model) noexcept {
        latency_model_ = std::move(model);
    }

    bool has_latency_model() const noexcept { return latency_model_ != nullptr; }

    int64_t sample_response_latency_ns(int64_t event_ts_ns) const {
        return latency_model_
            ? xt_latency::validate_latency_ns(
                  latency_model_->response_latency_ns(event_ts_ns), "response")
            : 0;
    }

    void set_queue_model(std::unique_ptr<IQueueModel> model) noexcept {
        queue_model_ = std::move(model);
        order_manager_.set_queue_model(queue_model_.get());
    }

    bool has_queue_model() const noexcept { return queue_model_ != nullptr; }

    uint64_t alloc_order_id() { return order_manager_.alloc_order_id(); }

    uint64_t submit(uint64_t order_id,
                    const char* code,
                    bool is_buy,
                    int64_t price_fp,
                    int64_t qty_fp,
                    XtTimeInForce tif,
                    uint32_t order_date,
                    uint32_t order_time_ms,
                    std::string_view user_tag,
                    XtOffset close_offset,
                    bool is_resubmit = false);

    bool cancel(uint64_t id);

    void on_tick(const XtTick& tick, std::vector<XtTradeRecord>& trades);

    void drain_updates(std::vector<XtOrderExecUpdate>& out);

    size_t live_orders() const;

    const std::vector<XtSimOrder>& live_orders_view() const;

    const XtSimOrder* find(uint64_t id) const;

    void set_order_delay_ticks(uint32_t n);

    std::size_t delay_wheel_pending_count() const noexcept;

    void collect_in_transit_order_ids(std::vector<uint64_t>& out) const;

    std::uint64_t lost_in_transit_count() const noexcept { return lost_in_transit_count_; }

    std::size_t discard_in_transit_orders() noexcept;

    void discard_eof_live_orders();

private:
    /// @brief volume_limit 的 per-symbol 累计成交量缓存槽。
    struct VolumeEntry {
        char   code[32] = {};
        double volume   = 0.0;
    };

    /// @brief order delay wheel 中等待进入 XtOrderManager 的 submit 参数快照。
    ///
    /// submit() 入队时复制全部参数；到期后 submit_immediate_() 使用该快照补采样
    /// ack/fill latency 并提交。code/user_tag 使用固定缓冲，超长输入会被截断。
    struct DeferredOrder {
        uint64_t order_id;
        char code[32];
        bool is_buy;
        int64_t price_fp;
        int64_t qty_fp;
        XtTimeInForce tif;
        uint32_t order_date;
        uint32_t order_time_ms;
        char user_tag[32];
        XtOffset close_offset;
        bool is_resubmit;

        int64_t lookup_event_ns;
    };

    double next_rand01();

    double calc_max_fill_qty(const XtTick& tick);

    uint32_t sample_ack_delay_ms();

    uint32_t sample_fill_latency_ms(int64_t event_ts_ns);

    uint32_t sample_cancel_delay_ms(int64_t event_ts_ns);

    int64_t query_event_ts_() const;

    double& find_or_insert_volume(const char* code);

    void advance_wheel_and_flush_();

    void inject_cancel_errors_();

    void match_orders_on_tick_depth_(const XtTick& tick, std::vector<XtTradeRecord>& trades);

    void stamp_new_trade_times_(size_t first_new_trade, std::vector<XtTradeRecord>& trades);

    void split_new_fills_(size_t first_new_trade, std::vector<XtTradeRecord>& trades);

    uint64_t submit_immediate_(const DeferredOrder& d);

    XtOrderManager order_manager_;
    double error_rate_ = 0.0;
    double reject_rate_ = 0.0;
    bool split_fills_ = false;

    int64_t split_min_fp_  = 1'000'000;
    int64_t split_max_fp_  = 100'000'000;
    int64_t split_tick_fp_ = 1'000'000;
    bool volume_limit_ = true;
    bool cancel_latency_enabled_ = false;
    bool price_limit_enabled_ = false;

    uint32_t current_date_ = 0;
    uint32_t current_time_ms_ = 0;
    int64_t  cur_event_ns_ = INT64_MIN;
    uint32_t prev_date_ = 0;
    uint32_t prev_time_ms_ = 0;

    uint32_t dispatch_date_ = 0;
    uint32_t dispatch_time_ms_ = 0;

    std::mt19937 rng_;
    std::uniform_real_distribution<double> uniform_dist_{0.0, 1.0};
    uint64_t ack_delay_us_ = 0;
    uint64_t ack_delay_stddev_us_ = 0;
    std::normal_distribution<double> latency_dist_{0.0, 1.0};

    std::unique_ptr<ILatencyModel> latency_model_;
    std::unique_ptr<IQueueModel> queue_model_;

    std::vector<VolumeEntry> volume_table_;

    std::vector<int64_t> parts_buf_;
    std::vector<XtTradeRecord> expanded_buf_;

    uint32_t order_delay_ticks_ = 0;
    std::vector<std::vector<DeferredOrder>> delay_wheel_;
    uint32_t wheel_cursor_ = 0;

    std::uint64_t lost_in_transit_count_ = 0;
};

namespace engine_config {

XtHftMatchEngine::Config parse_match_engine_config(const YAML::Node& node);

}
