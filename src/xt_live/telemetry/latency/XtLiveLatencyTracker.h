#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/container/XtLockFreeQueue.h"
#include "xt_core/sync/XtTransparentStringHash.h"
#include "xt_util/stats/XtHistogramUtil.h"
#include "detail/LatencyTap.h"

namespace ct { namespace metrics { class XtMetricRegistry; } }
namespace prometheus { class Histogram; class Counter; }

/// @brief 实盘订单路径延迟采样器，维护 submit/ack/fill rolling histogram。
///
/// 热路径（on_submit_result / on_order_update / on_trade / record_raw_*）无 mutex、
/// 无 unordered_map、无 string 构造：producer 只写定宽 POD tap 进 per-thread SPSC
/// ring（strings 走 SubmitMeta sidecar 冷路径），单一 assembler 线程 drain 后做全部
/// 归并、rolling histogram、Prometheus emit（V1 逻辑逐字迁移，单线程无锁）。
/// 公开 seam（类名 / Config / Snapshot / StageStats 形状 / 全部签名）与 V1 一致。
/// `snapshot()` 走 drain barrier：等待 assembler 消费越过捕获点后读 published_
/// （publish_mu_ 守护的单缓冲）发布值，外部观察与 V1 同步互斥读等价（超时 50ms
/// 兜底）。采样只用于观测，不表达交易
/// 决策或风控语义。
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
    ~XtLiveLatencyTracker();

    XtLiveLatencyTracker(const XtLiveLatencyTracker&) = delete;
    XtLiveLatencyTracker& operator=(const XtLiveLatencyTracker&) = delete;

    void set_enabled(bool enabled);
    bool enabled() const;

    void set_stale_ttl_us(uint64_t ttl_us);
    uint64_t stale_ttl_us() const;

    void set_metric_registry(ct::metrics::XtMetricRegistry* registry,
                              std::string_view venue_label) noexcept;

    using MetaResolver =
        std::function<bool(uint64_t order_id, std::string& out_strategy, std::string& out_code)>;

    void set_meta_resolver(MetaResolver resolver);

    void bind_producer_slot();

    void note_ack_eligible(std::string_view strategy, std::string_view book,
                           std::string_view symbol);

    void on_submit_result(uint64_t order_id,
                          const std::string& trace_id,
                          uint64_t submit_begin_us,
                          uint64_t submit_end_us,
                          uint64_t trigger_tick_us = 0,
                          std::string_view strategy_id = {},
                          std::string_view code = {});

    void on_order_update(const XtOrderExecUpdate& upd);

    void on_trade(const XtTradeRecord& tr, uint64_t order_id_override = 0);

    void record_raw_trade_ack(uint64_t order_id, uint64_t ack_us);

    void record_raw_uds_arrival(uint64_t order_id, uint64_t uds_ts_us);

    void record_raw_trade_ack_unavailable(uint64_t order_id, uint64_t mark_us);

    Snapshot snapshot();

    void stop();

private:

    friend class XtLiveLatencyTrackerTestPeer;

    void test_set_assembler_paused(bool paused);

    void test_set_seq_recorder(std::vector<uint64_t>* recorder);

    static constexpr size_t kHistBuckets = 32;
    static constexpr size_t kRollingSlots = 60;
    static constexpr size_t kCompletedOrderCacheCap = 4096;

    static constexpr size_t kSpscSlots       = 8;
    static constexpr size_t kSpscRingCap     = 4096;
    static constexpr size_t kDrainBatchCap   = 8192;
    static constexpr size_t kOverflowCap     = 1024;
    static constexpr size_t kSubmitMetaCap   = 8192;
    static constexpr size_t kOrdersCap       = 16384;
    static constexpr size_t kLabelCacheCap   = 512;
    static constexpr size_t kStrategyCap     = 64;
    static constexpr size_t kCodeCap         = 32;
    static constexpr size_t kTraceCap        = 128;

    using Histogram = xt_util::LatencyHistogram<kHistBuckets>;
    using RollingHistogram = xt_util::RollingHistogram<kHistBuckets, kRollingSlots>;
    using LatencyTap = xt_live::latency_detail::LatencyTap;

    // 字段布局按类型聚簇（全部 uint64 → 全部 bool → char array），消除 bool 交错
    // 造成的内部 padding，使 sizeof 定宽最小（§4.2 记账表按此 packed layout 计）。
    struct OrderTrack {
        uint64_t submit_begin_us = 0;
        uint64_t submit_end_us = 0;
        uint64_t trigger_tick_us = 0;
        uint64_t first_ack_us = 0;
        uint64_t first_fill_us = 0;
        uint64_t first_strategy_us = 0;
        uint64_t last_touch_us = 0;
        uint64_t effective_ack_us = 0;
        uint64_t pending_cb_us = 0;
        uint64_t pending_strategy_us = 0;
        uint64_t raw_trade_ack_us = 0;
        uint64_t raw_uds_us = 0;
        bool final_order_seen = false;
        bool submit_recorded = false;
        bool ack_latency_pushed = false;
        bool ack_path_pushed = false;
        bool fill_latency_pushed = false;
        bool ack_to_fill_pushed = false;
        bool strategy_processing_pushed = false;
        bool event_ack_pushed = false;
        bool event_fill_pushed = false;
        bool raw_trade_ack_unavailable = false;
        bool raw_trade_ack_pushed = false;
        bool ack_paired_latency_pushed = false;
        bool uds_savings_pushed = false;
        bool meta_miss_counted = false;
        bool has_trace = false;
        bool has_pending_cb = false;

        char trace[kTraceCap] = {0};
        char strategy[kStrategyCap] = {0};
        char code[kCodeCap] = {0};
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

    // SubmitMeta sidecar entry：strings 唯一通道（定宽 char array，零 per-entry heap）。
    struct MetaEntry {
        char strategy[kStrategyCap] = {0};
        char code[kCodeCap] = {0};
        char trace[kTraceCap] = {0};
    };

    void push_tap_(LatencyTap& tap, bool allow_auto_claim);
    int  tls_slot_for_() const;
    int  auto_claim_slot_();
    void write_submit_meta_(uint64_t key, std::string_view strategy,
                            std::string_view code, std::string_view trace);
    uint64_t next_synthetic_token_();
    void inc_tap_dropped_(const char* reason);

    void assembler_loop_();
    size_t drain_all_into_scratch_();
    void process_tap_(const LatencyTap& tap, uint64_t now);
    void refresh_config_if_needed_();
    void publish_stats_();

    void apply_submit_(const LatencyTap& tap, uint64_t now);
    void apply_order_update_(const LatencyTap& tap, uint64_t now);
    void apply_trade_(const LatencyTap& tap, uint64_t now);
    void apply_raw_ack_(const LatencyTap& tap, uint64_t now);
    void apply_raw_uds_(const LatencyTap& tap, uint64_t now);
    void apply_raw_ack_unavailable_(const LatencyTap& tap, uint64_t now);

    static uint64_t now_us();
    static bool is_final_order_status(XtOrderStatus status);
    static StageStats to_stage_stats(const RollingHistogram& ring);
    OrderTrack* get_or_create_order_(uint64_t order_id);
    void cleanup_stale_(uint64_t now);
    bool ensure_emit_meta_(uint64_t order_id, OrderTrack& st);
    uint64_t resolve_order_id_(uint64_t order_id, const char* trace, bool has_trace) const;
    void register_trace_(uint64_t order_id, OrderTrack& st, const char* trace);
    void maybe_finish_order_(uint64_t order_id, const OrderTrack& st);
    static bool raw_latency_correlation_pending(const OrderTrack& st);
    bool completed_order_seen_(uint64_t order_id) const;
    void remember_completed_order_(uint64_t order_id);
    void push_strategy_processing_if_ready_(OrderTrack& st, uint64_t sample_ts_us);
    void push_ack_if_ready_(OrderTrack& st, uint64_t sample_ts_us);
    void push_paired_ack_and_savings_if_ready_(OrderTrack& st, uint64_t sample_ts_us);
    void push_trade_ack_if_ready_(OrderTrack& st, uint64_t sample_ts_us);
    void push_fill_if_ready_(OrderTrack& st, uint64_t sample_ts_us);
    void push_ack_to_fill_if_ready_(OrderTrack& st, uint64_t sample_ts_us);

    void record_cb_to_strategy_(OrderTrack& st, uint64_t cb_us, uint64_t strategy_us, uint64_t now);

    std::atomic<bool> enabled_{true};
    std::atomic<uint64_t> stale_ttl_us_{60000000ULL};
    uint64_t slot_dur_us_ = 10'000'000ULL;
    const uint32_t generation_;

    std::thread assembler_;
    std::atomic<bool> assembler_run_{true};
    std::atomic<bool> stopped_{false};
    std::atomic<bool> assembler_paused_{false};
    std::atomic<bool> assembler_paused_ack_{false};
    std::once_flag stop_once_;

    std::vector<std::unique_ptr<xt_queue_detail::SpscQueue<LatencyTap>>> rings_;
    std::atomic<size_t> next_slot_{0};

    std::array<std::atomic<uint64_t>, kSpscSlots> ring_pushed_{};
    std::array<std::atomic<uint64_t>, kSpscSlots> ring_consumed_{};

    std::mutex overflow_mu_;
    std::vector<LatencyTap> overflow_ring_;
    size_t overflow_head_ = 0;
    size_t overflow_tail_ = 0;
    size_t overflow_size_ = 0;
    size_t overflow_dropped_pending_ = 0;
    std::atomic<uint64_t> overflow_pushed_{0};
    std::atomic<uint64_t> overflow_consumed_{0};

    std::atomic<uint64_t> enqueue_seq_{0};
    std::atomic<uint64_t> consume_generation_{0};
    std::atomic<uint64_t> synthetic_counter_{0};
    std::atomic<uint64_t> meta_evicted_pending_{0};
    std::atomic<int64_t>  snapshot_pending_{0};

    std::mutex meta_mu_;
    std::unordered_map<uint64_t, MetaEntry> submit_meta_;
    std::vector<uint64_t> meta_ring_;
    size_t meta_ring_pos_ = 0;
    size_t meta_ring_count_ = 0;

    std::mutex cfg_mu_;
    ct::metrics::XtMetricRegistry* metric_registry_ = nullptr;
    std::string metric_venue_;
    MetaResolver meta_resolver_;
    std::atomic<uint32_t> config_epoch_{0};

    std::mutex publish_mu_;
    Snapshot published_{};

    StatsState stats_;
    std::unordered_map<uint64_t, OrderTrack> orders_;

    std::vector<uint64_t> orders_ring_;
    size_t orders_ring_pos_ = 0;
    size_t orders_ring_count_ = 0;
    std::unordered_map<uint64_t, uint64_t> trace_to_order_;
    std::unordered_set<uint64_t> completed_orders_;
    std::deque<uint64_t> completed_order_fifo_;
    std::vector<LatencyTap> drain_scratch_;

    std::vector<LatencyTap> overflow_drain_;
    size_t overflow_drain_pos_ = 0;
    std::vector<uint64_t>* test_seq_recorder_ = nullptr;

    uint32_t seen_config_epoch_ = 0xFFFFFFFFu;
    ct::metrics::XtMetricRegistry* reg_local_ = nullptr;
    std::string venue_local_;
    MetaResolver resolver_local_;

    // 定宽 label key（零 per-entry heap，§4.2 记账终态化）：strategy/book 走 kStrategyCap，
    // symbol 走 kCodeCap，与 OrderTrack 同截断。venue 不进 key——它在 cache 生命周期内恒定
    // （config 变更即清 cache），只作 builder 参数（见 label_cache_get_ venue 形参）。
    // 零初始化 + copy_capped NUL 填充 → padding 恒 0，定宽 FNV-1a/memcmp 逐位确定。
    struct LabelKey {
        char strategy[kStrategyCap] = {0};
        char book[kStrategyCap] = {0};
        char symbol[kCodeCap] = {0};
        bool operator==(const LabelKey& o) const noexcept {
            return std::memcmp(strategy, o.strategy, kStrategyCap) == 0
                && std::memcmp(book, o.book, kStrategyCap) == 0
                && std::memcmp(symbol, o.symbol, kCodeCap) == 0;
        }
    };
    struct LabelKeyHash {
        size_t operator()(const LabelKey& k) const noexcept {
            uint64_t h = xt_core::sync::fnv1a_bytes(k.strategy, kStrategyCap);
            h = xt_core::sync::fnv1a_update(h, k.book, kStrategyCap);
            h = xt_core::sync::fnv1a_update(h, k.symbol, kCodeCap);
            return static_cast<size_t>(h);
        }
    };

    static LabelKey make_label_key(std::string_view strategy, std::string_view book,
                                   std::string_view symbol) noexcept;
    std::unordered_map<LabelKey, prometheus::Histogram*, LabelKeyHash> hist_ack_cache_;
    std::unordered_map<LabelKey, prometheus::Histogram*, LabelKeyHash> hist_fill_cache_;
    std::unordered_map<LabelKey, prometheus::Histogram*, LabelKeyHash> hist_paired_cache_;
    std::unordered_map<LabelKey, prometheus::Histogram*, LabelKeyHash> hist_strategy_cache_;
    std::unordered_map<LabelKey, prometheus::Histogram*, LabelKeyHash> hist_local_submit_cache_;
    prometheus::Histogram* label_cache_get_(
        std::unordered_map<LabelKey, prometheus::Histogram*, LabelKeyHash>& cache,
        const LabelKey& key,
        const std::string& venue,
        prometheus::Histogram* (*builder)(ct::metrics::XtMetricRegistry*,
                                          const std::string&, const std::string&,
                                          const std::string&, const std::string&));

    std::mutex eligible_cache_mu_;
    ct::metrics::XtMetricRegistry* eligible_cache_reg_ = nullptr;
    std::unordered_map<LabelKey, prometheus::Counter*, LabelKeyHash> eligible_cache_;

    std::atomic<uint64_t> snapshot_barrier_timeout_{0};
};

/// @brief RAII：作用域退出时 stop() 关联 tracker（若非空）。
///
/// 三 runner 在 boot struct 声明之后紧跟声明本 guard（析构先于 boot 任何成员 →
/// 覆盖异常 unwind / cleanup_and_fail / 一切 return 路径）；tracker 的 meta_resolver
/// 捕获 &order_book，guard 保证 assembler 停 + resolver 清先于 order_book 析构，
/// 消灭 teardown 期 resolver-after-order_book-death 的 UAF 窗口。
class TrackerStopGuard {
public:
    explicit TrackerStopGuard(std::unique_ptr<XtLiveLatencyTracker>& tracker) noexcept
        : tracker_(tracker) {}
    ~TrackerStopGuard() {
        if (tracker_) tracker_->stop();
    }
    TrackerStopGuard(const TrackerStopGuard&) = delete;
    TrackerStopGuard& operator=(const TrackerStopGuard&) = delete;

private:
    std::unique_ptr<XtLiveLatencyTracker>& tracker_;
};
