#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_util/stats/XtHistogramUtil.h"

namespace ct { namespace metrics { class XtMetricRegistry; } }
namespace prometheus { class Histogram; }

/// @brief 实盘订单路径延迟采样器，维护 submit/ack/fill rolling histogram。
///
/// 本类线程安全但不是 lock-free；SDK 回调、submit 回调与 snapshot() 可并发，
/// 内部 mutex 可能短暂竞争。采样只用于观测，不表达交易决策或风控语义。
class XtLiveLatencyTracker {
public:
    struct Config {
        bool enabled = true;
        uint64_t stale_ttl_us = 60000000ULL;

        uint64_t rolling_window_s = 600;
    };

    struct StageStats {
        uint64_t samples = 0;
        double avg_us = 0.0;
        uint64_t p50_us = 0;
        uint64_t p90_us = 0;
        uint64_t p95_us = 0;
        uint64_t p99_us = 0;
        uint64_t p999_us = 0;
        uint64_t max_us = 0;
    };

    struct Snapshot {
        bool enabled = false;
        uint64_t submit_attempts = 0;
        uint64_t submit_success = 0;
        uint64_t submit_failed = 0;
        uint64_t inflight_orders = 0;
        uint64_t stale_evicted = 0;
        StageStats submit_wire;
        StageStats strategy_processing;
        StageStats ack_path;
        StageStats submit_to_ack;
        StageStats submit_to_ack_paired;
        StageStats submit_to_trade_ack;
        StageStats uds_first_savings;
        StageStats submit_to_fill;
        StageStats ack_to_fill;
        StageStats event_to_ack;
        StageStats event_to_fill;
        StageStats cb_to_strategy;
    };

    explicit XtLiveLatencyTracker(const Config& cfg);

    void set_enabled(bool enabled);
    bool enabled() const;

    void set_stale_ttl_us(uint64_t ttl_us);
    uint64_t stale_ttl_us() const;

    void set_metric_registry(ct::metrics::XtMetricRegistry* registry,
                              std::string_view venue_label) noexcept;

    using MetaResolver =
        std::function<bool(uint64_t order_id, std::string& out_strategy, std::string& out_code)>;

    void set_meta_resolver(MetaResolver resolver);

    void on_submit_result(uint64_t order_id,
                          const std::string& trace_id,
                          uint64_t submit_begin_us,
                          uint64_t submit_end_us,
                          uint64_t trigger_tick_us = 0,
                          std::string_view strategy_id = {},
                          std::string_view code = {});

    void on_order_update(const XtOrderExecUpdate& upd);

    void on_trade(const XtTradeRecord& tr);

    void record_raw_trade_ack(uint64_t order_id, uint64_t ack_us);

    void record_raw_uds_arrival(uint64_t order_id, uint64_t uds_ts_us);

    void record_raw_trade_ack_unavailable(uint64_t order_id, uint64_t mark_us);

    Snapshot snapshot();

private:
    static constexpr size_t kHistBuckets = 32;

    static constexpr size_t kRollingSlots = 60;
    static constexpr size_t kCompletedOrderCacheCap = 4096;

    using Histogram = xt_util::LatencyHistogram<kHistBuckets>;
    using RollingHistogram = xt_util::RollingHistogram<kHistBuckets, kRollingSlots>;

    struct OrderTrack {
        uint64_t submit_begin_us = 0;
        uint64_t submit_end_us = 0;
        uint64_t trigger_tick_us = 0;
        uint64_t first_ack_us = 0;
        uint64_t first_fill_us = 0;
        uint64_t first_strategy_us = 0;
        uint64_t last_touch_us = 0;
        bool final_order_seen = false;

        bool submit_recorded = false;
        bool ack_latency_pushed = false;
        bool ack_path_pushed = false;
        bool fill_latency_pushed = false;
        bool ack_to_fill_pushed = false;
        bool strategy_processing_pushed = false;
        bool event_ack_pushed = false;
        bool event_fill_pushed = false;

        uint64_t raw_trade_ack_us = 0;
        bool raw_trade_ack_unavailable = false;
        bool raw_trade_ack_pushed = false;

        uint64_t raw_uds_us = 0;
        bool ack_paired_latency_pushed = false;
        bool uds_savings_pushed = false;
        std::string trace_id;

        std::string strategy;
        std::string code;
        bool meta_miss_counted = false;
    };

    struct StatsState {
        uint64_t submit_attempts = 0;
        uint64_t submit_success = 0;
        uint64_t submit_failed = 0;
        uint64_t stale_evicted = 0;

        RollingHistogram submit_wire;
        RollingHistogram strategy_processing;
        RollingHistogram ack_path;
        RollingHistogram submit_to_ack;
        RollingHistogram submit_to_ack_paired;
        RollingHistogram submit_to_trade_ack;
        RollingHistogram uds_first_savings;
        RollingHistogram submit_to_fill;
        RollingHistogram ack_to_fill;
        RollingHistogram event_to_ack;
        RollingHistogram event_to_fill;
        RollingHistogram cb_to_strategy;
    };

    static uint64_t now_us();
    static bool is_final_order_status(XtOrderStatus status);
    static bool parse_had_fill(const XtOrderExecUpdate& upd);

    static StageStats to_stage_stats(const RollingHistogram& ring);
    void cleanup_stale_unlocked(uint64_t now_us);

    bool ensure_emit_meta_unlocked(uint64_t order_id, OrderTrack& st);
    uint64_t resolve_order_id_unlocked(uint64_t order_id, const std::string& trace_id) const;
    void maybe_finish_order_unlocked(uint64_t order_id, const OrderTrack& st);
    static bool raw_latency_correlation_pending(const OrderTrack& st);
    bool completed_order_seen_unlocked(uint64_t order_id) const;
    void remember_completed_order_unlocked(uint64_t order_id);
    void push_strategy_processing_if_ready_unlocked(OrderTrack& st, uint64_t sample_ts_us);
    void push_ack_if_ready_unlocked(OrderTrack& st, uint64_t sample_ts_us);
    void push_paired_ack_and_savings_if_ready_unlocked(OrderTrack& st, uint64_t sample_ts_us);
    void push_trade_ack_if_ready_unlocked(OrderTrack& st, uint64_t sample_ts_us);
    void push_fill_if_ready_unlocked(OrderTrack& st, uint64_t sample_ts_us);
    void push_ack_to_fill_if_ready_unlocked(OrderTrack& st, uint64_t sample_ts_us);

    std::atomic<bool> enabled_{true};
    std::atomic<uint64_t> stale_ttl_us_{60000000ULL};

    uint64_t slot_dur_us_ = 10'000'000ULL;

    mutable std::mutex mu_;
    StatsState stats_;
    std::unordered_map<uint64_t, OrderTrack> orders_;
    std::unordered_map<std::string, uint64_t> trace_to_order_;
    std::unordered_set<uint64_t> completed_orders_;
    std::deque<uint64_t> completed_order_fifo_;

    ct::metrics::XtMetricRegistry* metric_registry_ = nullptr;
    std::string metric_venue_;
    MetaResolver meta_resolver_;
    struct LabelKey {
        std::string strategy;
        std::string book;
        std::string venue;
        std::string symbol;
        bool operator==(const LabelKey& o) const noexcept {
            return strategy == o.strategy && book == o.book
                && venue == o.venue && symbol == o.symbol;
        }
    };
    struct LabelKeyHash {
        size_t operator()(const LabelKey& k) const noexcept {
            size_t h = std::hash<std::string>{}(k.strategy);
            h ^= std::hash<std::string>{}(k.book)   + 0x9e3779b9 + (h << 6) + (h >> 2);
            h ^= std::hash<std::string>{}(k.venue)  + 0x9e3779b9 + (h << 6) + (h >> 2);
            h ^= std::hash<std::string>{}(k.symbol) + 0x9e3779b9 + (h << 6) + (h >> 2);
            return h;
        }
    };
    std::unordered_map<LabelKey, prometheus::Histogram*, LabelKeyHash> hist_ack_cache_;
    std::unordered_map<LabelKey, prometheus::Histogram*, LabelKeyHash> hist_fill_cache_;
    std::unordered_map<LabelKey, prometheus::Histogram*, LabelKeyHash> hist_paired_cache_;
    std::unordered_map<LabelKey, prometheus::Histogram*, LabelKeyHash> hist_strategy_cache_;
    std::unordered_map<LabelKey, prometheus::Histogram*, LabelKeyHash> hist_local_submit_cache_;
};
