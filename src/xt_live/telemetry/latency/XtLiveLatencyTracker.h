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
        // rolling_window_s=0 退化为累计统计，不推荐用于告警。
        uint64_t rolling_window_s = 600;
    };

    struct StageStats {
        uint64_t samples = 0;
        double avg_us = 0.0;
        uint64_t p50_us = 0;  // 之前 HftRunnerControl 用 avg_us 当 p50 上报 (长尾分布下失真)
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
        StageStats submit_to_ack;          ///< first-of-arrival (UDS-first dedup 后)，策略推进基准
        StageStats submit_to_ack_paired;   ///< only orders with both raw UDS and raw trade ACK; strict min(raw_uds, raw_ack)
        StageStats submit_to_trade_ack;    ///< raw transport ACK 时刻，**不经 UDS-first**；用于对比看收益
        StageStats uds_first_savings;      ///< raw_trade_ack - effective per order；直接显示 UDS-first 提早量（绕开 histogram log2 bucketing artifact）
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

    /** @brief 运行时启用/关闭延迟采样。 */
    void set_enabled(bool enabled);
    bool enabled() const;

    /** @brief 设置过期订单淘汰阈值（stale TTL，微秒）。 */
    void set_stale_ttl_us(uint64_t ttl_us);
    uint64_t stale_ttl_us() const;

    /// @brief 注入 prometheus metric registry. nullptr 默认 → backtest / unit-test no-op (bit-identical).
    /// @pre 事件流启动前注入；v2 起 assembler 只在启动后只读该值。
    void set_metric_registry(ct::metrics::XtMetricRegistry* registry,
                              std::string_view venue_label) noexcept;

    /// @brief order_id → (strategy, code) 反查；返回 true 时两个输出均已填非空值。
    using MetaResolver =
        std::function<bool(uint64_t order_id, std::string& out_strategy, std::string& out_code)>;

    /// @brief 注入 emit meta 反查 fallback（order book hazard-snapshot 读）。
    ///
    /// submit 路径未带 strategy/code（gateway restart 后 recovery 单等）时，assembler
    /// emit 前用 resolver 回填；回填仍失败计 xt_latency_meta_miss_total（每订单一次）。
    /// @pre 事件流启动前注入；resolver 由 assembler 单线程调用，不得回调 tracker。
    /// @note stop() 首步即清 resolver，final drain 不再调用 → teardown UAF 类整体消灭。
    void set_meta_resolver(MetaResolver resolver);

    /// @brief 热线程在自身线程函数体首部显式绑定一个 SPSC producer slot。
    ///
    /// HFT/CTA event-loop worker、SEL scheduler driver 调用；幂等（已绑同 generation
    /// 即 no-op）。8 槽耗尽 → 该线程后续 tap 落 overflow + slot_exhausted 计数。
    /// TLS 缓存 (generation, slot) 防 tracker 重建错配。
    void bind_producer_slot();

    /// @brief 记录一次 ack-eligible（gateway submit 成功处调用，每单一次天然去重）。
    ///
    /// 用作 submit_to_ack coverage 分母 xt_latency_eligible_total{stage="ack",...}。
    /// strategy/symbol 必 known（空则不计）。同步 emit（冷路径，per-order）。
    void note_ack_eligible(std::string_view strategy, std::string_view book,
                           std::string_view symbol);

    /** @brief 记录下单提交结果及耗时（submit wire latency）。 */
    /// @param strategy_id 策略 id（StrategySidRegistry name，prom label `strategy`）；空 → skip emit
    /// @param code        instrument code（prom label `symbol`）；空 → skip emit
    void on_submit_result(uint64_t order_id,
                          const std::string& trace_id,
                          uint64_t submit_begin_us,
                          uint64_t submit_end_us,
                          uint64_t trigger_tick_us = 0,
                          std::string_view strategy_id = {},
                          std::string_view code = {});
    /** @brief 处理订单状态回报，更新 ack/fill 延迟采样（order update latency）。 */
    void on_order_update(const XtOrderExecUpdate& upd);
    /** @brief 处理成交回报，更新 fill 延迟采样（trade latency）。 */
    /// @param order_id_override 非 0 时以此键替代 tr.order_id 落 tracker（L-2 fill-link：
    ///        physical fill 以 logical clord 键与 gateway submit track 配对）；0 = 用 tr.order_id。
    ///        零拷贝入口——caller 不需为改键复制整条 record（避免 broker-ID string heap 分配，I-4）。
    void on_trade(const XtTradeRecord& tr, uint64_t order_id_override = 0);
    /// @brief 记录 raw transport ACK 时刻，不经 UDS-first dedup。
    void record_raw_trade_ack(uint64_t order_id, uint64_t ack_us);
    /// @brief 记录 raw UDS 到达时刻，不经 UDS-first dedup。
    void record_raw_uds_arrival(uint64_t order_id, uint64_t uds_ts_us);
    /// @brief 标记 raw transport ACK 不会到达（transport error / timeout）。
    void record_raw_trade_ack_unavailable(uint64_t order_id, uint64_t mark_us);

    /** @brief 生成延迟统计快照（latency stats snapshot）。 */
    /// @note drain barrier：等待 assembler 消费越过捕获点后读 published_（publish_mu_
    ///       单缓冲）；assembler 已 stop 时跳 barrier 直读（final drain 保证 stop 前样本已 publish）。
    Snapshot snapshot();

    /// @brief 停止 assembler 并做 final drain+publish（幂等）。
    ///
    /// 序列：① 锁下清 meta_resolver_（final drain 不调 resolver）→ ② 停 producer 接收
    /// （enabled=false）→ ③ final drain + aggregate + publish 到 published_（单缓冲）→ ④ join assembler
    /// → ⑤ 锁下清 metric_registry_。析构自动调用兜底。
    void stop();

private:
    // ── 测试专用 seam（prod 无功能影响；经 friend XtLiveLatencyTrackerTestPeer 桥接访问，
    //    不污染公开 API surface；见 @note）──────────────────────────────────────
    friend class XtLiveLatencyTrackerTestPeer;
    /// @brief 暂停/恢复 assembler drain。
    /// @note test-only：暂停下 producer 可把 backlog 堆到 ring（>kDrainBatchCap）后再恢复，
    ///       用于确定性验证 k-way merge 的两批场景。stop() 会强制放行（不因暂停挂 join）。
    void test_set_assembler_paused(bool paused);
    /// @brief 注入处理序 seq 记录容器（非空时 assembler 每处理一个 tap 追加其 seq）。
    /// @note test-only：验证 drain 处理序按 seq 全序；prod 恒 nullptr，热路径仅一次 null 判。
    ///       仅在暂停态或事件流静默时设置/读取（assembler 单写，无并发保护）。
    void test_set_seq_recorder(std::vector<uint64_t>* recorder);

    static constexpr size_t kHistBuckets = 32;
    static constexpr size_t kRollingSlots = 60;
    static constexpr size_t kCompletedOrderCacheCap = 4096;

    // ── 容器上界（逐容器 cap，诚实上界 ≤12 MiB，见 spec §4.2 表）─────────────
    static constexpr size_t kSpscSlots       = 8;
    static constexpr size_t kSpscRingCap     = 4096;
    static constexpr size_t kDrainBatchCap   = 8192;  ///< assembler 单批 drain 上限（F7）；scratch 记账定宽
    static constexpr size_t kOverflowCap     = 1024;
    static constexpr size_t kSubmitMetaCap   = 8192;
    static constexpr size_t kOrdersCap       = 16384;
    static constexpr size_t kLabelCacheCap   = 512;   ///< 6 定宽 label cache 共用 cap（512 保 §4.2 总和 ≤12 MiB）
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
        uint64_t effective_ack_us = 0;      ///< ACK finalize 时的统一 effective 端点（F1）；fill 侧 ack_to_fill 复用同端点，未 finalize 时 0
        uint64_t pending_cb_us = 0;         ///< callback-before-submit 暂存 cb_us（F5-a），apply_submit_ 到达后 retro-push
        uint64_t pending_strategy_us = 0;   ///< callback-before-submit 暂存 local_strategy_us（F5-a）
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
        bool has_pending_cb = false;        ///< pending_cb_us/pending_strategy_us 有效（F5-a）
        // 定宽 char array（零 per-entry heap，H-6）：trace 完整存放供碰撞复核。
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

    // ── producer 侧：tap 入队路由 ──────────────────────────────────────────
    void push_tap_(LatencyTap& tap, bool allow_auto_claim);
    int  tls_slot_for_() const;
    int  auto_claim_slot_();
    void write_submit_meta_(uint64_t key, std::string_view strategy,
                            std::string_view code, std::string_view trace);
    uint64_t next_synthetic_token_();
    void inc_tap_dropped_(const char* reason);

    // ── assembler 侧：单线程处理（无锁；orders_/trace_to_order_/stats_ 私有）──
    void assembler_loop_();
    size_t drain_all_into_scratch_();       ///< 9 路 k-way merge（8 SPSC ring + overflow 第 9 路，按 seq）进 scratch，返回条数
    void process_tap_(const LatencyTap& tap, uint64_t now);
    void refresh_config_if_needed_();       ///< config_epoch_ 变更时锁下重读 registry/resolver
    void publish_stats_();                  ///< 计算 Snapshot 到 published_（publish_mu_ 单缓冲）+ bump consume_generation

    // V1 逻辑逐字迁移（assembler 单线程，去锁版本）。
    void apply_submit_(const LatencyTap& tap, uint64_t now);
    void apply_order_update_(const LatencyTap& tap, uint64_t now);
    void apply_trade_(const LatencyTap& tap, uint64_t now);
    void apply_raw_ack_(const LatencyTap& tap, uint64_t now);
    void apply_raw_uds_(const LatencyTap& tap, uint64_t now);
    void apply_raw_ack_unavailable_(const LatencyTap& tap, uint64_t now);

    static uint64_t now_us();
    static bool is_final_order_status(XtOrderStatus status);
    static StageStats to_stage_stats(const RollingHistogram& ring);
    OrderTrack* get_or_create_order_(uint64_t order_id);   ///< 含 cap 驱逐
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
    /// @brief cb_to_strategy 记录：submit 已到直接 push；submit 未到（callback-before-submit）
    ///        则暂存待 apply_submit_ retro-push（F5-a）。第一个 cb 定型（first_strategy_us 非 0 即忽略）。
    void record_cb_to_strategy_(OrderTrack& st, uint64_t cb_us, uint64_t strategy_us, uint64_t now);

    // ── 配置 / 生命周期 ────────────────────────────────────────────────────
    std::atomic<bool> enabled_{true};
    std::atomic<uint64_t> stale_ttl_us_{60000000ULL};
    uint64_t slot_dur_us_ = 10'000'000ULL;
    const uint32_t generation_;                 ///< 本 tracker 实例 id（TLS slot 键）

    std::thread assembler_;
    std::atomic<bool> assembler_run_{true};
    std::atomic<bool> stopped_{false};          ///< stop() 已完成（snapshot 跳 barrier 直读）
    std::atomic<bool> assembler_paused_{false}; ///< test-only：暂停 drain（run==false 时被绕过）
    std::atomic<bool> assembler_paused_ack_{false}; ///< test-only：assembler 已 park 在暂停分支
    std::once_flag stop_once_;

    // ── producer→assembler 传输面 ──────────────────────────────────────────
    std::vector<std::unique_ptr<xt_queue_detail::SpscQueue<LatencyTap>>> rings_;
    std::atomic<size_t> next_slot_{0};          ///< 已分配 SPSC slot 数（bind/auto-claim）

    // per-ring 发布游标（F2）：producer push 成功后 pushed_ ++（真实入队才计，seq 已取
    // 但未 push 的空洞不进），assembler drain pop 后 consumed_ += popped。snapshot barrier
    // 捕获全部 pushed_ 快照、等待对应 consumed_ 越过，杜绝 post-capture 事件顶替 pre-capture
    // 未发布空洞提前放行。全序由 drain 的 9 路 k-way merge 保证（seq 排序键），无独立 sort pass。
    std::array<std::atomic<uint64_t>, kSpscSlots> ring_pushed_{};
    std::array<std::atomic<uint64_t>, kSpscSlots> ring_consumed_{};

    std::mutex overflow_mu_;
    std::vector<LatencyTap> overflow_ring_;      ///< mutex 环形（cap kOverflowCap）
    size_t overflow_head_ = 0;
    size_t overflow_tail_ = 0;
    size_t overflow_size_ = 0;
    size_t overflow_dropped_pending_ = 0;        ///< overflow 满丢弃待计数（overflow_mu_ 守）
    std::atomic<uint64_t> overflow_pushed_{0};   ///< overflow 成功入队累计（barrier 发布游标；overflow_mu_ 内 ++）
    std::atomic<uint64_t> overflow_consumed_{0}; ///< overflow 已 merge 出累计（barrier 消费游标，per-item-merged；assembler single-writer）

    std::atomic<uint64_t> enqueue_seq_{0};       ///< 全局单调序号（tap.seq 全序 merge 排序键）
    std::atomic<uint64_t> consume_generation_{0};///< 每 publish 后 bump（barrier）
    std::atomic<uint64_t> synthetic_counter_{0}; ///< order_id==0 事件 synthetic token 计数
    std::atomic<uint64_t> meta_evicted_pending_{0}; ///< SubmitMeta 驱逐待计数（producer 标记）
    std::atomic<int64_t>  snapshot_pending_{0};  ///< 在途 snapshot waiter 数（gate assembler 空闲 publish）

    // SubmitMeta sidecar（strings 唯一通道；冷路径 per-order）。逐旧驱逐用固定环形
    // buffer（零 per-op heap；deque 会因 assembler 消费 map 而 fifo 无界增长 + chunk
    // 分配污染 p99）。ring 满时覆盖最旧 key，若仍 live 则从 map 驱逐 + 计数。
    std::mutex meta_mu_;
    std::unordered_map<uint64_t, MetaEntry> submit_meta_;
    std::vector<uint64_t> meta_ring_;            ///< 固定 kSubmitMetaCap 槽（插入序）
    size_t meta_ring_pos_ = 0;                   ///< 下一写位置
    size_t meta_ring_count_ = 0;                 ///< 已占槽数（饱和到 cap）

    // ── 配置注入面（cfg_mu_ 守，assembler 经 config_epoch_ 只读缓存）──────────
    std::mutex cfg_mu_;
    ct::metrics::XtMetricRegistry* metric_registry_ = nullptr;
    std::string metric_venue_;
    MetaResolver meta_resolver_;
    std::atomic<uint32_t> config_epoch_{0};

    // ── snapshot 发布面：published_ + mutex 单缓冲（非双缓冲 swap）──────────
    // assembler publish_stats_ 在 publish_mu_ 下整体覆盖 published_，snapshot 在同锁下
    // 读取；barrier 的可见性由 consume_generation_ 排序保证，不依赖 buffer 双份。
    std::mutex publish_mu_;
    Snapshot published_{};

    // ── assembler 私有状态（单线程无锁）────────────────────────────────────
    StatsState stats_;
    std::unordered_map<uint64_t, OrderTrack> orders_;
    // orders_ 逐旧驱逐用固定环形（同 meta_ring_ 理由：order 终态/stale 由 assembler
    // 带外 erase orders_ 但不动 fifo → deque 会无界增长 + chunk 分配）。ring 满时覆盖
    // 最旧 id，若仍 live 则驱逐 + tombstone + 计数。
    std::vector<uint64_t> orders_ring_;
    size_t orders_ring_pos_ = 0;
    size_t orders_ring_count_ = 0;
    std::unordered_map<uint64_t, uint64_t> trace_to_order_;  ///< FNV-1a(trace) → order_id
    std::unordered_set<uint64_t> completed_orders_;
    std::deque<uint64_t> completed_order_fifo_;
    std::vector<LatencyTap> drain_scratch_;       ///< assembler drain 复用缓冲（无 per-pass alloc）
    // overflow 第 9 路暂存（S1）：mutex ring 里多 producer 到达序的 tap drain 出后按 seq 排序，
    // 作为 k-way merge 第 9 路参与全序归并；未消费项跨批留存（refill-only-when-empty，上界
    // kOverflowCap，seq 单调保 leftover seq < 任何新到 tap → 不补新项不破坏全序）。assembler
    // single-writer，无锁。
    std::vector<LatencyTap> overflow_drain_;
    size_t overflow_drain_pos_ = 0;               ///< overflow_drain_ merge 游标
    std::vector<uint64_t>* test_seq_recorder_ = nullptr;  ///< test-only：处理序 seq 记录（assembler 单写；prod 恒 null）

    // assembler 本地 config 缓存（config_epoch_ 变更时 refresh）。
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
    /// @brief 从 string_view 三元组构造定宽 LabelKey（零 heap，NUL 填充保 hash/eq 确定）。
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
        const std::string& venue,   ///< venue 不在 key，miss 时透传 builder（cache 生命周期内恒定）
        prometheus::Histogram* (*builder)(ct::metrics::XtMetricRegistry*,
                                          const std::string&, const std::string&,
                                          const std::string&, const std::string&));

    // eligible Counter* label cache（F6-c，与 5 个 histogram cache 同 cap（kLabelCacheCap）
    // 与定宽 LabelKey，但 note_ack_eligible 是 producer 侧多线程冷路径 → 自持 mutex；label 集
    // {strategy, book, symbol} 与 histogram 一致以保 PromQL vector matching）。
    std::mutex eligible_cache_mu_;
    ct::metrics::XtMetricRegistry* eligible_cache_reg_ = nullptr;  ///< cache 构建所针对的 registry（换即清）
    std::unordered_map<LabelKey, prometheus::Counter*, LabelKeyHash> eligible_cache_;

    // snapshot barrier 超时计数（诊断）。
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
