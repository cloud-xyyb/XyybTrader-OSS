#include "XtLiveLatencyTracker.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>

#include "xt_util/time/XtSteadyClock.h"
#include "XtMetricRegistry.h"
#include <prometheus/counter.h>
#include <prometheus/histogram.h>

namespace {

using xt_live::latency_detail::LatencyTap;
using xt_live::latency_detail::TapKind;
using xt_live::latency_detail::TriggerKind;
namespace tf = xt_live::latency_detail::tap_flags;

// ── 内部工具 ────────────────────────────────────────────────────────────────

// 定宽 char array 拷贝（截断到 cap-1 + NUL）。
void copy_capped(char* dst, size_t cap, std::string_view src) {
    const size_t n = std::min(src.size(), cap - 1);
    std::memcpy(dst, src.data(), n);
    dst[n] = '\0';
}

// trace 内部 hash（非 wire / 持久化契约；碰撞由完整 trace 复核，故本函数非承重）。
uint64_t fnv1a(const char* s) {
    uint64_t h = 14695981039346656037ULL;
    for (; *s; ++s) {
        h ^= static_cast<uint8_t>(*s);
        h *= 1099511628211ULL;
    }
    return h;
}

template <typename RollingHistogram>
bool rolling_add_elapsed(RollingHistogram& hist,
                         uint64_t end_us,
                         uint64_t begin_us,
                         uint64_t sample_ts_us,
                         uint64_t slot_dur_us) {
    if (begin_us == 0 || end_us == 0 || end_us <= begin_us) return false;
    xt_util::rolling_add(hist, end_us - begin_us, sample_ts_us, slot_dur_us);
    return true;
}

template <typename StatsState>
void rotate_latency_windows(StatsState& stats, uint64_t now_us, uint64_t slot_dur_us) {
    xt_util::rolling_rotate_if_needed(stats.submit_wire, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.strategy_processing, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.ack_path, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.submit_to_ack, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.submit_to_ack_paired, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.submit_to_trade_ack, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.uds_first_savings, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.submit_to_fill, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.ack_to_fill, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.event_to_ack, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.event_to_fill, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.cb_to_strategy, now_us, slot_dur_us);
}

// per-tracker generation id（TLS slot 键，防 tracker 重建错配）。
std::atomic<uint32_t> g_tracker_generation{0};

// TLS：(tracker_generation, slot_idx)。slot_idx == kUnbound 表示未绑，
// kOverflowBound 表示已试且落 overflow（8 槽耗尽，不再重试 claim）。
constexpr int kUnbound = -1;
constexpr int kOverflowBound = -2;
thread_local uint32_t tl_tracker_gen = 0xFFFFFFFFu;
thread_local int tl_slot = kUnbound;

using XtMetricRegistry = ct::metrics::XtMetricRegistry;

// 5 个 histogram builder（函数指针；label 集 {strategy, book, symbol}，venue 由
// prom-agent external_labels 注入，native 不加——G-12）。
prometheus::Histogram* build_ack(XtMetricRegistry* r, const std::string& s,
                                 const std::string& b, const std::string& v,
                                 const std::string& sym) {
    return r->get_submit_to_ack_us_histogram(s, b, v, sym);
}
prometheus::Histogram* build_fill(XtMetricRegistry* r, const std::string& s,
                                  const std::string& b, const std::string& v,
                                  const std::string& sym) {
    return r->get_submit_to_fill_us_histogram(s, b, v, sym);
}
prometheus::Histogram* build_paired(XtMetricRegistry* r, const std::string& s,
                                    const std::string& b, const std::string& v,
                                    const std::string& sym) {
    return r->get_submit_to_ack_paired_us_histogram(s, b, v, sym);
}
prometheus::Histogram* build_strategy(XtMetricRegistry* r, const std::string& s,
                                      const std::string& b, const std::string& v,
                                      const std::string& sym) {
    return r->get_strategy_us_histogram(s, b, v, sym);
}
prometheus::Histogram* build_local_submit(XtMetricRegistry* r, const std::string& s,
                                          const std::string& b, const std::string& v,
                                          const std::string& sym) {
    return r->get_local_submit_us_histogram(s, b, v, sym);
}

}  // namespace

// ── 构造 / 析构 / 生命周期 ──────────────────────────────────────────────────

XtLiveLatencyTracker::XtLiveLatencyTracker(const Config& cfg)
    : enabled_(cfg.enabled),
      stale_ttl_us_(cfg.stale_ttl_us > 0 ? cfg.stale_ttl_us : 60000000ULL),
      generation_(g_tracker_generation.fetch_add(1, std::memory_order_relaxed) + 1) {
    const uint64_t win_s = cfg.rolling_window_s;
    if (win_s > 0) {
        const uint64_t win_us = win_s * 1'000'000ULL;
        slot_dur_us_ = win_us / kRollingSlots;
        if (slot_dur_us_ == 0) slot_dur_us_ = 1;
    } else {
        slot_dur_us_ = 0;
    }
    rings_.reserve(kSpscSlots);
    for (size_t i = 0; i < kSpscSlots; ++i) {
        rings_.push_back(
            std::make_unique<xt_queue_detail::SpscQueue<LatencyTap>>(kSpscRingCap));
    }
    overflow_ring_.resize(kOverflowCap);
    submit_meta_.reserve(kSubmitMetaCap);   // reserve==cap，锁内零 rehash（G-3/G-8/G-9）
    meta_ring_.resize(kSubmitMetaCap);      // 固定环形 buffer，锁内零 heap
    orders_.reserve(kOrdersCap);
    orders_ring_.resize(kOrdersCap);        // 固定环形，assembler 侧零 per-op heap
    drain_scratch_.reserve(kDrainBatchCap); // 单批上限 kDrainBatchCap，drain 不 shrink（F7）
    overflow_drain_.reserve(kOverflowCap);  // overflow 第 9 路暂存，clear() 保容量、drain 不 shrink（S1）
    published_.enabled = enabled_.load(std::memory_order_relaxed);

    // 诚实内存上界（逐容器 cap × per-entry 定宽 struct，零 per-entry heap）≤ 12 MiB。
    // 任何 cap / struct 变更须保持。kNodeOverhead = libstdc++ node-based 容器每 entry 的
    // 非 payload 开销 = next ptr(8) + cached hash(8) + bucket slot(8) = 24（该值即注释所列
    // 三项之和；此前写作 32 含 8 字节未说明 slack，现对齐三项组成保诚实上界）。
    constexpr size_t kNodeOverhead = 24;
    constexpr size_t kEstBound =
        kSpscSlots * kSpscRingCap * sizeof(LatencyTap)                 // 8× SPSC ring
        + kOverflowCap * sizeof(LatencyTap)                            // overflow
        + kSubmitMetaCap * (sizeof(MetaEntry) + 8 + kNodeOverhead)     // SubmitMeta map
        + kSubmitMetaCap * sizeof(uint64_t)                            // meta_ring_
        + kOrdersCap * (sizeof(OrderTrack) + 8 + kNodeOverhead)        // orders_（packed layout）
        + kOrdersCap * sizeof(uint64_t)                                // orders_ring_
        + kOrdersCap * (8 + 8 + kNodeOverhead)                         // trace_to_order_
        + kCompletedOrderCacheCap * (8 + kNodeOverhead + sizeof(uint64_t))  // completed set+fifo
        + 6 * kLabelCacheCap * (sizeof(LabelKey) + 8 + kNodeOverhead)  // 6 定宽 label cache（零 per-entry heap）
        + kOverflowCap * sizeof(LatencyTap)                            // overflow_drain_（第 9 路暂存, S1）
        + kDrainBatchCap * sizeof(LatencyTap);                         // drain_scratch_（chunked, F7）
    static_assert(kEstBound <= 12ull * 1024 * 1024,
                  "latency tracker container upper bound exceeds honest 12 MiB budget");

    assembler_ = std::thread([this]() { assembler_loop_(); });
}

XtLiveLatencyTracker::~XtLiveLatencyTracker() {
    stop();
}

void XtLiveLatencyTracker::stop() {
    std::call_once(stop_once_, [this]() {
        // ① 锁下清 resolver（final drain 不调 resolver → teardown UAF 消灭）。
        {
            std::lock_guard<std::mutex> guard(cfg_mu_);
            meta_resolver_ = nullptr;
            config_epoch_.fetch_add(1, std::memory_order_release);
        }
        // ② 停 producer 接收。
        enabled_.store(false, std::memory_order_release);
        // ③ 请求 assembler 退出（final drain + aggregate + publish 在 loop 尾完成）。
        assembler_run_.store(false, std::memory_order_release);
        // ④ join（assembler 循环无阻塞调用、每轮查 stop flag → join 有界）。
        if (assembler_.joinable()) assembler_.join();
        // ⑤ 锁下清 registry。
        {
            std::lock_guard<std::mutex> guard(cfg_mu_);
            metric_registry_ = nullptr;
        }
        stopped_.store(true, std::memory_order_release);
    });
}

// ── 简单 setter ─────────────────────────────────────────────────────────────

void XtLiveLatencyTracker::set_enabled(bool enabled) {
    enabled_.store(enabled, std::memory_order_release);
}
bool XtLiveLatencyTracker::enabled() const {
    return enabled_.load(std::memory_order_acquire);
}
void XtLiveLatencyTracker::set_stale_ttl_us(uint64_t ttl_us) {
    stale_ttl_us_.store(ttl_us > 0 ? ttl_us : 60000000ULL, std::memory_order_release);
}
uint64_t XtLiveLatencyTracker::stale_ttl_us() const {
    return stale_ttl_us_.load(std::memory_order_acquire);
}

void XtLiveLatencyTracker::set_metric_registry(ct::metrics::XtMetricRegistry* reg,
                                               std::string_view venue) noexcept {
    std::lock_guard<std::mutex> guard(cfg_mu_);
    metric_registry_ = reg;
    metric_venue_.assign(venue.data(), venue.size());
    config_epoch_.fetch_add(1, std::memory_order_release);
}

void XtLiveLatencyTracker::set_meta_resolver(MetaResolver resolver) {
    std::lock_guard<std::mutex> guard(cfg_mu_);
    meta_resolver_ = std::move(resolver);
    config_epoch_.fetch_add(1, std::memory_order_release);
}

void XtLiveLatencyTracker::test_set_assembler_paused(bool paused) {
    assembler_paused_.store(paused, std::memory_order_release);
    if (!paused) return;
    // 等 assembler park 在暂停分支再返回：保证 test 后续 producer push 时 drain 已停，
    // 处理序不被「pause 生效前的早 drain」污染。stop() 后（run==false）不挂等。
    while (assembler_run_.load(std::memory_order_acquire) &&
           !assembler_paused_ack_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
}

void XtLiveLatencyTracker::test_set_seq_recorder(std::vector<uint64_t>* recorder) {
    test_seq_recorder_ = recorder;  // assembler 单读；test 于暂停态/静默态设置
}

// ── static helper ───────────────────────────────────────────────────────────

uint64_t XtLiveLatencyTracker::now_us() { return XtSteadyClock::steady_now_us(); }
bool XtLiveLatencyTracker::is_final_order_status(XtOrderStatus status) {
    return order_status_is_final(status);
}

XtLiveLatencyTracker::StageStats
XtLiveLatencyTracker::to_stage_stats(const RollingHistogram& ring) {
    const Histogram hist = xt_util::rolling_merge(ring);
    StageStats out;
    out.samples = hist.samples;
    out.avg_us = hist.samples > 0
                     ? static_cast<double>(hist.sum_us) / static_cast<double>(hist.samples)
                     : 0.0;
    const std::array<xt_util::QRatio, 5> queries = {{
        {50, 100}, {90, 100}, {95, 100}, {99, 100}, {999, 1000}
    }};
    std::array<uint64_t, 5> percs{};
    xt_util::hist_percentile_multi(hist, queries, percs);
    out.p50_us  = percs[0];
    out.p90_us  = percs[1];
    out.p95_us  = percs[2];
    out.p99_us  = percs[3];
    out.p999_us = percs[4];
    out.max_us = hist.max_us;
    return out;
}

// ── producer 侧：slot 路由 + tap 入队 ───────────────────────────────────────

int XtLiveLatencyTracker::tls_slot_for_() const {
    if (tl_tracker_gen != generation_) return kUnbound;  // 别的 tracker / 未绑
    return tl_slot;
}

int XtLiveLatencyTracker::auto_claim_slot_() {
    const size_t idx = next_slot_.fetch_add(1, std::memory_order_relaxed);
    if (idx >= kSpscSlots) {
        // 8 槽耗尽 → overflow，且不再重试 claim。
        tl_tracker_gen = generation_;
        tl_slot = kOverflowBound;
        inc_tap_dropped_("slot_exhausted");
        return kOverflowBound;
    }
    tl_tracker_gen = generation_;
    tl_slot = static_cast<int>(idx);
    return tl_slot;
}

void XtLiveLatencyTracker::bind_producer_slot() {
    const int cur = tls_slot_for_();
    if (cur != kUnbound) return;  // 已绑（含 overflow-bound），幂等。
    (void)auto_claim_slot_();
}

void XtLiveLatencyTracker::inc_tap_dropped_(const char* reason) {
    // 冷路径：仅在物理满 / 驱逐时命中。经 registry 计 xt_latency_tap_dropped_total{reason}。
    ct::metrics::XtMetricRegistry* reg = nullptr;
    {
        std::lock_guard<std::mutex> guard(cfg_mu_);
        reg = metric_registry_;
    }
    if (reg) reg->inc_latency_tap_dropped_total(reason);
}

void XtLiveLatencyTracker::push_tap_(LatencyTap& tap, bool allow_auto_claim) {
    // 全局单调 seq：drain 9 路 k-way merge 全序排序键唯一来源（barrier 走 per-ring/overflow 游标）。
    tap.seq = enqueue_seq_.fetch_add(1, std::memory_order_relaxed);

    int slot = tls_slot_for_();
    if (slot == kUnbound && allow_auto_claim) {
        slot = auto_claim_slot_();
    }
    if (slot >= 0) {
        if (rings_[static_cast<size_t>(slot)]->push(tap)) {
            // 发布成功后才 ++ pushed_（barrier 只等真实入队的 tap，seq 已取但 push 失败的
            // 空洞不计），F2。
            ring_pushed_[static_cast<size_t>(slot)].fetch_add(1, std::memory_order_release);
        } else {
            inc_tap_dropped_("ring_full");
        }
        return;
    }
    // 未绑 / overflow-bound → overflow mutex ring。
    std::lock_guard<std::mutex> guard(overflow_mu_);
    if (overflow_size_ >= kOverflowCap) {
        // overflow_mu_ 与 cfg_mu_ 无固定锁序，此处不调 inc_tap_dropped_（会取 cfg_mu_），
        // 改计数后由 assembler 在 drain 锁外 emit（每 drop 一计，不丢精度）。
        ++overflow_dropped_pending_;
        return;
    }
    overflow_ring_[overflow_head_] = tap;
    overflow_head_ = (overflow_head_ + 1) % kOverflowCap;
    ++overflow_size_;
    overflow_pushed_.fetch_add(1, std::memory_order_release);  // 发布游标（barrier）
}

uint64_t XtLiveLatencyTracker::next_synthetic_token_() {
    // token counter 2^62 进程生命周期不可达（wrap 论证：即便 1e9 tap/s 亦需 ~146 年）。
    const uint64_t n = synthetic_counter_.fetch_add(1, std::memory_order_relaxed);
    return xt_live::latency_detail::kSyntheticTokenBit | (n & (xt_live::latency_detail::kSyntheticTokenBit - 1));
}

void XtLiveLatencyTracker::write_submit_meta_(uint64_t key, std::string_view strategy,
                                              std::string_view code, std::string_view trace) {
    if (strategy.empty() && code.empty() && trace.empty()) return;
    // 超长 trace 不截断入表而是放弃 trace 注册 + trace_oversize 计数（截断会错关联）。
    const bool trace_oversize = trace.size() >= kTraceCap;
    std::string_view trace_eff = trace_oversize ? std::string_view{} : trace;
    {
        std::lock_guard<std::mutex> guard(meta_mu_);
        auto it = submit_meta_.find(key);
        if (it == submit_meta_.end()) {
            // 固定环形：满时覆盖最旧 key，若仍 live（未被 assembler 消费）则驱逐 + 计数。
            if (meta_ring_count_ == kSubmitMetaCap) {
                const uint64_t old = meta_ring_[meta_ring_pos_];
                if (submit_meta_.erase(old) > 0) {
                    meta_evicted_pending_.fetch_add(1, std::memory_order_relaxed);
                }
            } else {
                ++meta_ring_count_;
            }
            meta_ring_[meta_ring_pos_] = key;
            meta_ring_pos_ = (meta_ring_pos_ + 1) % kSubmitMetaCap;
            MetaEntry e;
            copy_capped(e.strategy, kStrategyCap, strategy);
            copy_capped(e.code, kCodeCap, code);
            copy_capped(e.trace, kTraceCap, trace_eff);
            submit_meta_.emplace(key, e);
        } else {
            // merge：非空字段 last-writer-wins（同 order 的多写，strategy/code 一致）。
            if (!strategy.empty()) copy_capped(it->second.strategy, kStrategyCap, strategy);
            if (!code.empty()) copy_capped(it->second.code, kCodeCap, code);
            if (!trace_eff.empty()) copy_capped(it->second.trace, kTraceCap, trace_eff);
        }
    }
    if (trace_oversize) inc_tap_dropped_("trace_oversize");
}

void XtLiveLatencyTracker::note_ack_eligible(std::string_view strategy,
                                             std::string_view book,
                                             std::string_view symbol) {
    if (strategy.empty() || symbol.empty()) return;
    ct::metrics::XtMetricRegistry* reg = nullptr;
    {
        std::lock_guard<std::mutex> guard(cfg_mu_);
        reg = metric_registry_;  // venue 由 external label 注入，native 不加（G-12）
    }
    if (!reg) return;
    // F6-b：与 OrderTrack.strategy[64]/code[32] 同截断（cap-1=63/31），保 eligible(分母) 与
    // histogram(分子) label 逐字一致，否则 PromQL vector matching 断链。histogram book label
    // 取自 st.strategy（book==strategy，单 book 模式）→ book 按 strategy cap 截断。
    char strat[kStrategyCap];
    char bk[kStrategyCap];
    char sym[kCodeCap];
    copy_capped(strat, kStrategyCap, strategy);
    copy_capped(bk, kStrategyCap, book);
    copy_capped(sym, kCodeCap, symbol);
    // F6-c：eligible Counter* label cache（cap kLabelCacheCap，与 histogram cache 同上界），
    // producer 多线程冷路径 → eligible_cache_mu_ 守；registry 变更即清 cache。
    prometheus::Counter* ctr = nullptr;
    bool over_cap = false;
    {
        std::lock_guard<std::mutex> guard(eligible_cache_mu_);
        if (eligible_cache_reg_ != reg) {
            eligible_cache_.clear();
            eligible_cache_reg_ = reg;
        }
        LabelKey key = make_label_key(strat, bk, sym);  // venue 不进 key（native 不加）
        auto it = eligible_cache_.find(key);
        if (it != eligible_cache_.end()) {
            ctr = it->second;
        } else if (eligible_cache_.size() >= kLabelCacheCap) {
            over_cap = true;  // cap：跳过 emit（不插新 label）+ label_cache_full 计数（锁外）
        } else {
            ctr = reg->get_latency_eligible_counter("ack", strat, bk, sym);
            eligible_cache_.emplace(std::move(key), ctr);
        }
    }
    if (ctr) ctr->Increment();
    else if (over_cap) inc_tap_dropped_("label_cache_full");
}

// ── producer entry points ───────────────────────────────────────────────────

void XtLiveLatencyTracker::on_submit_result(uint64_t order_id,
                                            const std::string& trace_id,
                                            uint64_t submit_begin_us,
                                            uint64_t submit_end_us,
                                            uint64_t trigger_tick_us,
                                            std::string_view strategy_id,
                                            std::string_view code) {
    if (!enabled()) return;
    const uint64_t begin = submit_begin_us > 0 ? submit_begin_us : submit_end_us;
    const uint64_t end = submit_end_us > 0 ? submit_end_us : now_us();

    const bool has_meta = order_id != 0 &&
                          (!strategy_id.empty() || !code.empty() || !trace_id.empty());
    if (has_meta) {
        write_submit_meta_(order_id, strategy_id, code,
                           std::string_view(trace_id));
    }

    LatencyTap tap;
    tap.kind = static_cast<uint8_t>(TapKind::Submit);
    tap.order_id = order_id;
    tap.ts_us = begin;
    tap.aux_us = end;
    tap.trigger_us = trigger_tick_us;
    tap.trigger_kind = static_cast<uint8_t>(
        trigger_tick_us > 0 ? TriggerKind::HftTick : TriggerKind::Unknown);
    if (has_meta) tap.flags |= tf::kHasMeta;
    if (order_id == 0) tap.flags |= tf::kSubmitFailed;
    push_tap_(tap, /*allow_auto_claim=*/false);
}

void XtLiveLatencyTracker::on_order_update(const XtOrderExecUpdate& upd) {
    if (!enabled()) return;
    if (upd.id == 0) return;
    const uint64_t event_ts = upd.local_cb_us > 0 ? upd.local_cb_us : now_us();

    LatencyTap tap;
    tap.kind = static_cast<uint8_t>(TapKind::OrderUpdate);
    tap.order_id = upd.id;
    tap.ts_us = event_ts;
    tap.aux_us = upd.local_strategy_us;
    tap.status = static_cast<uint8_t>(upd.status);
    if (upd.local_cb_us > 0) tap.flags |= tf::kCbPresent;
    // parse_had_fill 需 last_fill_fp/filled_fp/status，producer 侧算好置 flag。
    const bool had_fill = upd.last_fill_fp > 0 || upd.filled_fp > 0 ||
                          upd.status == XtOrderStatus::PartiallyFilled ||
                          upd.status == XtOrderStatus::Filled;
    if (had_fill) tap.flags |= tf::kHadFill;
    push_tap_(tap, /*allow_auto_claim=*/false);
}

void XtLiveLatencyTracker::on_trade(const XtTradeRecord& tr, uint64_t order_id_override) {
    if (!enabled()) return;
    const uint64_t event_ts = tr.local_cb_us > 0 ? tr.local_cb_us : now_us();

    // order_id_override 非 0 时替代 tr.order_id 作 tracker 键（L-2 fill-link）；0 时用 tr.order_id。
    // 其余字段（local_cb_us / strategy_id / code / trace_id / submit info）一律取自 tr，
    // 故等价于「拷贝 record 仅改 order_id 后传入」，但零拷贝（I-4）。
    uint64_t key = order_id_override != 0 ? order_id_override : tr.order_id;
    bool synthetic = false;
    if (key == 0) {
        // trace 无法解析则 assembler 会 drop，但仍需 synthetic token 承载 trace。
        key = next_synthetic_token_();
        synthetic = true;
    }
    const bool has_meta = !tr.strategy_id.empty() || !tr.code.empty() || !tr.trace_id.empty();
    if (has_meta) {
        write_submit_meta_(key, std::string_view(tr.strategy_id),
                           std::string_view(tr.code), std::string_view(tr.trace_id));
    }

    LatencyTap tap;
    tap.kind = static_cast<uint8_t>(TapKind::Trade);
    tap.order_id = key;
    tap.ts_us = event_ts;
    if (synthetic) tap.flags |= tf::kIsSynthetic;
    if (tr.local_cb_us > 0) tap.flags |= tf::kCbPresent;
    if (has_meta) tap.flags |= tf::kHasMeta;
    if (tr.local_submit_begin_us > 0) {
        tap.flags |= tf::kHasSubmitInfo;
        tap.aux_us = tr.local_submit_begin_us;
        tap.trigger_us = tr.local_submit_end_us;
        tap.aux2_us = tr.local_strategy_us;  // F5-b：submit-info trade 仍携 strategy_us 供 cb_to_strategy
    } else {
        tap.aux_us = tr.local_strategy_us;
    }
    push_tap_(tap, /*allow_auto_claim=*/false);
}

void XtLiveLatencyTracker::record_raw_trade_ack(uint64_t order_id, uint64_t ack_us) {
    if (!enabled() || order_id == 0 || ack_us == 0) return;
    LatencyTap tap;
    tap.kind = static_cast<uint8_t>(TapKind::RawAck);
    tap.order_id = order_id;
    tap.ts_us = ack_us;
    push_tap_(tap, /*allow_auto_claim=*/true);
}

void XtLiveLatencyTracker::record_raw_uds_arrival(uint64_t order_id, uint64_t uds_ts_us) {
    if (!enabled() || order_id == 0 || uds_ts_us == 0) return;
    LatencyTap tap;
    tap.kind = static_cast<uint8_t>(TapKind::RawUds);
    tap.order_id = order_id;
    tap.ts_us = uds_ts_us;
    push_tap_(tap, /*allow_auto_claim=*/true);
}

void XtLiveLatencyTracker::record_raw_trade_ack_unavailable(uint64_t order_id, uint64_t mark_us) {
    if (!enabled() || order_id == 0) return;
    LatencyTap tap;
    tap.kind = static_cast<uint8_t>(TapKind::RawAckUnavailable);
    tap.order_id = order_id;
    tap.ts_us = mark_us;  // 0 时 assembler 用 now
    push_tap_(tap, /*allow_auto_claim=*/true);
}

// ── assembler：drain + 处理 + publish ───────────────────────────────────────

void XtLiveLatencyTracker::assembler_loop_() {
    while (true) {
        const bool run = assembler_run_.load(std::memory_order_acquire);
        // test-only 暂停闸：仅在仍 run 时生效，让 test 把 backlog 堆过 kDrainBatchCap；
        // stop() 置 run=false 绕过本闸，暂停态也不挂 join。ack 供 test 确认已 park。
        if (run && assembler_paused_.load(std::memory_order_acquire)) {
            assembler_paused_ack_.store(true, std::memory_order_release);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        assembler_paused_ack_.store(false, std::memory_order_release);
        const size_t n = drain_all_into_scratch_();
        if (n > 0) {
            const uint64_t now = now_us();
            // drain_all_into_scratch_ 的 9 路 k-way merge 输出已按 seq 升序（每路输入自身有序：
            // 每 SPSC ring 单 producer seq 递增、overflow 第 9 路 drain 后显式排序），故此处无需
            // 独立 sort pass。
            cleanup_stale_(now);
            for (size_t i = 0; i < n; ++i) {
                process_tap_(drain_scratch_[i], now);
            }
        }
        // publish：有活 / 有 snapshot waiter / 停止收尾时；否则空闲不重算。
        const bool pending_snapshot = snapshot_pending_.load(std::memory_order_acquire) > 0;
        if (n > 0 || pending_snapshot || !run) {
            publish_stats_();
        }
        // stop 请求且本轮 ring 已排空 → 收尾（stop 后仍会有 enabled=false 前入队的
        // in-flight tap，逐轮 drain 直到 n==0 才退出，不丢样）。
        if (!run && n == 0) break;
        if (n == 0 && !pending_snapshot) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        } else if (n == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        // n > 0：不 sleep，立即再 drain（stop 期尽快排空）。
    }
    // final publish 确保 stop 前最后一批样本落 published_（snapshot stopped 直读）。
    publish_stats_();
}

size_t XtLiveLatencyTracker::drain_all_into_scratch_() {
    drain_scratch_.clear();

    // (1) overflow 第 9 路准备：仅当上批已把 overflow_drain_ merge 排空时，从 mutex ring 全量补入
    //     并按 seq 排序（overflow 是多 producer 到达序、非 seq 序）。refill-only-when-empty 保
    //     overflow_drain_ 上界为 kOverflowCap（不跨批累积无界增长）。
    //     契约边界（W5R4/R5 收窄）：seq 在 overflow_mu_ 之外分配 → 取号后长停顿的 producer 可让
    //     leftover 中出现比后续新 overflow 项更大的 seq；该角落按 late-arrival 语义处理（下批
    //     merge 吸收）。样本「值」取自 tap 内时间戳，处理序不改写已 push 样本；但存在一个已知
    //     单样本级例外：order 终态处理先于晚发布的 raw ACK/UDS 时（tombstone 后 raw 被忽略），
    //     该单 effective-ACK 取 callback 时刻而非 raw 时刻——取值随处理序、且该样本不「自愈」
    //     （与 V1 mutex 到达序下同类晚到结局一致，telemetry 单样本级，接受为契约）。overflow 路
    //     的跨批序与 barrier 可见性为 best-effort，不提供精确前缀保证；精确前缀保证仅限 8 个
    //     SPSC ring 路（per-ring published 游标）。
    size_t overflow_dropped = 0;
    if (overflow_drain_pos_ >= overflow_drain_.size()) {
        overflow_drain_.clear();
        overflow_drain_pos_ = 0;
        {
            std::lock_guard<std::mutex> guard(overflow_mu_);
            while (overflow_size_ > 0) {
                overflow_drain_.push_back(overflow_ring_[overflow_tail_]);
                overflow_tail_ = (overflow_tail_ + 1) % kOverflowCap;
                --overflow_size_;
            }
            overflow_dropped = overflow_dropped_pending_;
            overflow_dropped_pending_ = 0;
        }
        if (overflow_drain_.size() > 1) {
            std::sort(overflow_drain_.begin(), overflow_drain_.end(),
                      [](const LatencyTap& a, const LatencyTap& b) { return a.seq < b.seq; });
        }
    }

    // (2) 9 路 k-way merge：8 个 SPSC ring 队头（每 ring 单 producer → ring 内 seq 升序）+
    //     overflow_drain_ 游标（已排序）。每次比对九路队头取最小 seq 进 scratch，批容量
    //     kDrainBatchCap 统一约束九路。公平性由「永远取最小 seq」结构性保证（无路因 8192 截断
    //     饥饿、overflow 晚 seq 不插队）。**序契约（W5R4 终态）**：per-producer FIFO 精确（ring 内
    //     seq 升序 + per-ring merge）；跨 producer 为 seq 最优努力序——取号与 publish 之间被抢占的
    //     in-flight tap、以及 overflow leftover 角落，均按 late-arrival 走 apply_* 状态机吸收
    //     （callback-before-submit / retro-push / dup-earliest），样本值取自 tap 内时间戳不受处理序
    //     影响。消费侧（rolling/Prom）不依赖跨 producer 全序。ring pop → ring_consumed_（barrier，
    //     精确 per-ring 前缀）；overflow 项 merge 出 → overflow_consumed_（barrier 对 overflow 路为
    //     best-effort，见 (1) 契约边界）。
    std::array<LatencyTap, kSpscSlots> heads;
    std::array<bool, kSpscSlots> has_head{};
    std::array<uint64_t, kSpscSlots> popped_per_ring{};
    for (size_t s = 0; s < rings_.size(); ++s) {
        has_head[s] = rings_[s]->peek(heads[s]);
    }
    uint64_t overflow_merged = 0;
    while (drain_scratch_.size() < kDrainBatchCap) {
        // 8 个 ring 头中的最小 seq。
        int min_slot = -1;
        uint64_t min_seq = 0;
        for (size_t s = 0; s < rings_.size(); ++s) {
            if (!has_head[s]) continue;
            if (min_slot < 0 || heads[s].seq < min_seq) {
                min_slot = static_cast<int>(s);
                min_seq = heads[s].seq;
            }
        }
        // overflow 第 9 路队头（已排序数组游标）参与比较：seq 更小则先取。
        const bool overflow_avail = overflow_drain_pos_ < overflow_drain_.size();
        if (overflow_avail &&
            (min_slot < 0 || overflow_drain_[overflow_drain_pos_].seq < min_seq)) {
            drain_scratch_.push_back(overflow_drain_[overflow_drain_pos_]);
            ++overflow_drain_pos_;
            ++overflow_merged;
            continue;
        }
        if (min_slot < 0) break;  // 全部 ring + overflow 排空
        const size_t s = static_cast<size_t>(min_slot);
        LatencyTap tap;
        rings_[s]->pop(tap);  // 消费已 peek 的 min-seq 队头（single consumer → 与 peek 同值）
        drain_scratch_.push_back(tap);
        ++popped_per_ring[s];
        has_head[s] = rings_[s]->peek(heads[s]);  // 补下一队头（未消费的队头留在 ring，下批再取）
    }
    for (size_t s = 0; s < rings_.size(); ++s) {
        if (popped_per_ring[s]) {
            ring_consumed_[s].fetch_add(popped_per_ring[s], std::memory_order_release);
        }
    }
    if (overflow_merged) {
        overflow_consumed_.fetch_add(overflow_merged, std::memory_order_release);
    }

    for (size_t i = 0; i < overflow_dropped; ++i) inc_tap_dropped_("overflow_full");
    // SubmitMeta 驱逐计数搬运到 tap_dropped（producer 标记，assembler emit）。
    uint64_t ev = meta_evicted_pending_.exchange(0, std::memory_order_relaxed);
    for (uint64_t i = 0; i < ev; ++i) inc_tap_dropped_("meta_evicted");
    return drain_scratch_.size();
}

void XtLiveLatencyTracker::refresh_config_if_needed_() {
    const uint32_t epoch = config_epoch_.load(std::memory_order_acquire);
    if (epoch == seen_config_epoch_) return;
    {
        std::lock_guard<std::mutex> guard(cfg_mu_);
        reg_local_ = metric_registry_;
        venue_local_ = metric_venue_;
        resolver_local_ = meta_resolver_;
    }
    seen_config_epoch_ = epoch;
    hist_ack_cache_.clear();
    hist_fill_cache_.clear();
    hist_paired_cache_.clear();
    hist_strategy_cache_.clear();
    hist_local_submit_cache_.clear();
}

void XtLiveLatencyTracker::process_tap_(const LatencyTap& tap, uint64_t now) {
    refresh_config_if_needed_();
    if (test_seq_recorder_ != nullptr) test_seq_recorder_->push_back(tap.seq);  // test-only（prod null）
    switch (static_cast<TapKind>(tap.kind)) {
        case TapKind::Submit:            apply_submit_(tap, now); break;
        case TapKind::OrderUpdate:       apply_order_update_(tap, now); break;
        case TapKind::Trade:             apply_trade_(tap, now); break;
        case TapKind::RawAck:            apply_raw_ack_(tap, now); break;
        case TapKind::RawUds:            apply_raw_uds_(tap, now); break;
        case TapKind::RawAckUnavailable: apply_raw_ack_unavailable_(tap, now); break;
    }
}

void XtLiveLatencyTracker::publish_stats_() {
    const uint64_t now = now_us();
    rotate_latency_windows(stats_, now, slot_dur_us_);
    Snapshot out;
    out.enabled = enabled_.load(std::memory_order_relaxed);
    out.submit_attempts = stats_.submit_attempts;
    out.submit_success = stats_.submit_success;
    out.submit_failed = stats_.submit_failed;
    out.stale_evicted = stats_.stale_evicted;
    out.inflight_orders = orders_.size();
    out.submit_wire = to_stage_stats(stats_.submit_wire);
    out.strategy_processing = to_stage_stats(stats_.strategy_processing);
    out.ack_path = to_stage_stats(stats_.ack_path);
    out.submit_to_ack = to_stage_stats(stats_.submit_to_ack);
    out.submit_to_ack_paired = to_stage_stats(stats_.submit_to_ack_paired);
    out.submit_to_trade_ack = to_stage_stats(stats_.submit_to_trade_ack);
    out.uds_first_savings = to_stage_stats(stats_.uds_first_savings);
    out.submit_to_fill = to_stage_stats(stats_.submit_to_fill);
    out.ack_to_fill = to_stage_stats(stats_.ack_to_fill);
    out.event_to_ack = to_stage_stats(stats_.event_to_ack);
    out.event_to_fill = to_stage_stats(stats_.event_to_fill);
    out.cb_to_strategy = to_stage_stats(stats_.cb_to_strategy);
    {
        std::lock_guard<std::mutex> guard(publish_mu_);
        published_ = out;
    }
    consume_generation_.fetch_add(1, std::memory_order_release);
}

// ── snapshot：drain barrier ─────────────────────────────────────────────────

XtLiveLatencyTracker::Snapshot XtLiveLatencyTracker::snapshot() {
    if (stopped_.load(std::memory_order_acquire)) {
        // assembler 已停：final drain+publish 先于 join，直读 published_（单缓冲）。
        std::lock_guard<std::mutex> guard(publish_mu_);
        return published_;
    }

    snapshot_pending_.fetch_add(1, std::memory_order_acq_rel);
    // 捕获全部 ring/overflow 发布游标快照（published prefix）：只等真实入队的 tap，
    // seq 已取但尚未 push 的空洞不在 pushed_ 内 → 不会被 post-capture 事件顶替提前放行（F2）。
    std::array<uint64_t, kSpscSlots> captured_pushed;
    for (size_t s = 0; s < kSpscSlots; ++s) {
        captured_pushed[s] = ring_pushed_[s].load(std::memory_order_acquire);
    }
    const uint64_t captured_overflow = overflow_pushed_.load(std::memory_order_acquire);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(50);

    bool observed_consumed = false;
    uint64_t gen_at_consumed = 0;
    while (true) {
        if (!observed_consumed) {
            bool all = overflow_consumed_.load(std::memory_order_acquire) >= captured_overflow;
            for (size_t s = 0; all && s < kSpscSlots; ++s) {
                if (ring_consumed_[s].load(std::memory_order_acquire) < captured_pushed[s]) all = false;
            }
            if (all) {
                observed_consumed = true;
                gen_at_consumed = consume_generation_.load(std::memory_order_acquire);
            }
        } else {
            // 发布顺序钉死：assembler publish stats 后 bump consume_generation；
            // 唤醒读到的版本必含捕获批的 **ring 路前缀**（per-ring 游标精确）；
            // overflow 路为 best-effort（累计计数可被后到项顶替，见 drain (1) 契约边界）。
            if (consume_generation_.load(std::memory_order_acquire) > gen_at_consumed) break;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            snapshot_barrier_timeout_.fetch_add(1, std::memory_order_relaxed);
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    snapshot_pending_.fetch_sub(1, std::memory_order_acq_rel);

    std::lock_guard<std::mutex> guard(publish_mu_);
    return published_;
}

// ── assembler-only 内部逻辑（V1 去锁迁移）──────────────────────────────────

XtLiveLatencyTracker::OrderTrack* XtLiveLatencyTracker::get_or_create_order_(uint64_t order_id) {
    auto it = orders_.find(order_id);
    if (it != orders_.end()) return &it->second;
    // 固定环形逐旧驱逐：满时覆盖最旧 id，若仍 live（未 finish/stale-evict）则从 orders_
    // 驱逐 + tombstone（防重建）+ 计数；已消失的 stale id 覆盖为 no-op。
    if (orders_ring_count_ == kOrdersCap) {
        const uint64_t old = orders_ring_[orders_ring_pos_];
        auto oit = orders_.find(old);
        if (oit != orders_.end()) {
            if (oit->second.has_trace) {
                const uint64_t h = fnv1a(oit->second.trace);
                auto tit = trace_to_order_.find(h);
                if (tit != trace_to_order_.end() && tit->second == old) trace_to_order_.erase(tit);
            }
            orders_.erase(oit);
            remember_completed_order_(old);
            inc_tap_dropped_("orders_evicted");
        }
    } else {
        ++orders_ring_count_;
    }
    orders_ring_[orders_ring_pos_] = order_id;
    orders_ring_pos_ = (orders_ring_pos_ + 1) % kOrdersCap;
    auto res = orders_.emplace(order_id, OrderTrack{});
    return &res.first->second;
}

void XtLiveLatencyTracker::cleanup_stale_(uint64_t now) {
    if (orders_.empty()) return;
    const uint64_t ttl = stale_ttl_us_.load(std::memory_order_relaxed);
    if (ttl == 0) return;
    for (auto it = orders_.begin(); it != orders_.end();) {
        const OrderTrack& st = it->second;
        const uint64_t last_touch = st.last_touch_us > 0 ? st.last_touch_us : st.submit_end_us;
        if (last_touch > 0 && now > last_touch && (now - last_touch) > ttl) {
            if (st.has_trace) {
                const uint64_t h = fnv1a(st.trace);
                auto tit = trace_to_order_.find(h);
                if (tit != trace_to_order_.end() && tit->second == it->first) trace_to_order_.erase(tit);
            }
            it = orders_.erase(it);
            ++stats_.stale_evicted;
        } else {
            ++it;
        }
    }
}

uint64_t XtLiveLatencyTracker::resolve_order_id_(uint64_t order_id, const char* trace,
                                                 bool has_trace) const {
    if (order_id > 0 && (order_id & xt_live::latency_detail::kSyntheticTokenBit) == 0) {
        return order_id;
    }
    if (!has_trace || trace[0] == '\0') return 0;
    const uint64_t h = fnv1a(trace);
    auto it = trace_to_order_.find(h);
    if (it == trace_to_order_.end()) return 0;
    // 碰撞复核：完整 trace 相等才认（key = FNV-1a hash，H-6）。
    auto oit = orders_.find(it->second);
    if (oit == orders_.end()) return 0;
    if (std::strcmp(oit->second.trace, trace) != 0) return 0;  // hash 碰撞，拒绝。
    return it->second;
}

void XtLiveLatencyTracker::register_trace_(uint64_t order_id, OrderTrack& st, const char* trace) {
    if (trace == nullptr || trace[0] == '\0') return;
    if (st.has_trace) {
        // 已有 trace：同 trace 幂等（map 已含该 hash→order_id）；不同 trace → 跳过 map 写入
        // （否则同 order 注入第二个 hash 条目，orders_ 驱逐只清 st.trace 的 hash、第二条
        // 泄漏出 orders_ cap 外）+ trace_conflict 计数（F6-a；trace_to_order_ 随 orders_ 16384
        // 上界）。用 assembler 缓存的 reg_local_ 直发，避免 apply_submit_ 下 meta_mu_→cfg_mu_ 嵌套。
        if (std::strcmp(st.trace, trace) != 0 && reg_local_ != nullptr) {
            reg_local_->inc_latency_tap_dropped_total("trace_conflict");
        }
        return;
    }
    copy_capped(st.trace, kTraceCap, trace);
    st.has_trace = true;
    trace_to_order_[fnv1a(trace)] = order_id;
}

void XtLiveLatencyTracker::maybe_finish_order_(uint64_t order_id, const OrderTrack& st) {
    if (!st.final_order_seen) return;
    if (!st.submit_recorded) return;
    if (raw_latency_correlation_pending(st)) return;
    if (st.has_trace) {
        const uint64_t h = fnv1a(st.trace);
        auto tit = trace_to_order_.find(h);
        if (tit != trace_to_order_.end() && tit->second == order_id) trace_to_order_.erase(tit);
    }
    remember_completed_order_(order_id);
    orders_.erase(order_id);
}

bool XtLiveLatencyTracker::raw_latency_correlation_pending(const OrderTrack& st) {
    if (!st.final_order_seen) return false;
    if (st.raw_uds_us == 0) return false;
    if (st.raw_trade_ack_us > 0 || st.raw_trade_ack_unavailable) return false;
    return true;
}

bool XtLiveLatencyTracker::completed_order_seen_(uint64_t order_id) const {
    return completed_orders_.find(order_id) != completed_orders_.end();
}

void XtLiveLatencyTracker::remember_completed_order_(uint64_t order_id) {
    if (completed_orders_.insert(order_id).second) {
        completed_order_fifo_.push_back(order_id);
    }
    while (completed_orders_.size() > kCompletedOrderCacheCap && !completed_order_fifo_.empty()) {
        const uint64_t old = completed_order_fifo_.front();
        completed_order_fifo_.pop_front();
        completed_orders_.erase(old);
    }
}

bool XtLiveLatencyTracker::ensure_emit_meta_(uint64_t order_id, OrderTrack& st) {
    if (st.strategy[0] != '\0' && st.code[0] != '\0') return true;
    if (resolver_local_) {
        std::string strategy, code;
        if (resolver_local_(order_id, strategy, code)) {
            if (st.strategy[0] == '\0' && !strategy.empty())
                copy_capped(st.strategy, kStrategyCap, strategy);
            if (st.code[0] == '\0' && !code.empty())
                copy_capped(st.code, kCodeCap, code);
        }
    }
    if (st.strategy[0] != '\0' && st.code[0] != '\0') return true;
    if (!st.meta_miss_counted && reg_local_ != nullptr) {
        st.meta_miss_counted = true;
        reg_local_->inc_latency_meta_miss_total(venue_local_);
    }
    return false;
}

XtLiveLatencyTracker::LabelKey
XtLiveLatencyTracker::make_label_key(std::string_view strategy, std::string_view book,
                                     std::string_view symbol) noexcept {
    LabelKey k;  // 成员初值 {0} 零填充 → copy_capped 只写前缀+NUL，padding 恒 0（hash/eq 确定）。
    copy_capped(k.strategy, kStrategyCap, strategy);
    copy_capped(k.book, kStrategyCap, book);
    copy_capped(k.symbol, kCodeCap, symbol);
    return k;
}

prometheus::Histogram* XtLiveLatencyTracker::label_cache_get_(
    std::unordered_map<LabelKey, prometheus::Histogram*, LabelKeyHash>& cache,
    const LabelKey& key,
    const std::string& venue,
    prometheus::Histogram* (*builder)(ct::metrics::XtMetricRegistry*,
                                      const std::string&, const std::string&,
                                      const std::string&, const std::string&)) {
    auto it = cache.find(key);
    if (it != cache.end()) return it->second;
    if (cache.size() >= kLabelCacheCap) {
        // 数值 cap：超出 skip emit（不插新 label）+ label_cache_full 计数。
        inc_tap_dropped_("label_cache_full");
        return nullptr;
    }
    // char[] key 字段 → const std::string& builder 形参：miss 冷路径构造临时 string（零热路径）。
    prometheus::Histogram* h =
        builder(reg_local_, key.strategy, key.book, venue, key.symbol);
    cache.emplace(key, h);
    return h;
}

// ── push_*_if_ready_（V1 逻辑逐字迁移，去锁 + char array label + reg_local_）──

void XtLiveLatencyTracker::push_strategy_processing_if_ready_(OrderTrack& st, uint64_t sample_ts_us) {
    if (st.strategy_processing_pushed) return;
    if (st.trigger_tick_us == 0 || st.submit_begin_us == 0) return;
    if (rolling_add_elapsed(stats_.strategy_processing, st.submit_begin_us,
                            st.trigger_tick_us, sample_ts_us, slot_dur_us_)) {
        st.strategy_processing_pushed = true;
        if (reg_local_ != nullptr && st.strategy[0] != '\0' && st.code[0] != '\0'
            && st.submit_begin_us > st.trigger_tick_us) {
            LabelKey key = make_label_key(st.strategy, st.strategy, st.code);
            auto* h = label_cache_get_(hist_strategy_cache_, key, venue_local_, &build_strategy);
            if (h) h->Observe(static_cast<double>(st.submit_begin_us - st.trigger_tick_us));
        }
    }
}

void XtLiveLatencyTracker::push_ack_if_ready_(OrderTrack& st, uint64_t sample_ts_us) {
    if (st.first_ack_us == 0 || !st.submit_recorded) return;
    const bool has_uds = st.raw_uds_us > 0;
    const bool has_ack = st.raw_trade_ack_us > 0;
    const bool ack_closed = has_ack || st.raw_trade_ack_unavailable;
    const bool raw_used = has_uds || has_ack || st.raw_trade_ack_unavailable;
    uint64_t effective_end_us = st.first_ack_us;
    if (raw_used) {
        if (has_uds && ack_closed) {
            effective_end_us = has_ack ? std::min(st.raw_uds_us, st.raw_trade_ack_us)
                                       : st.raw_uds_us;
        } else if (st.final_order_seen) {
            if (has_ack && has_uds) {
                effective_end_us = std::min(st.raw_uds_us, st.raw_trade_ack_us);
            } else if (has_ack) {
                effective_end_us = st.raw_trade_ack_us;
            } else if (has_uds) {
                effective_end_us = st.raw_uds_us;
            } else {
                return;
            }
        } else {
            return;
        }
    }
    // 统一 ACK 端点（F1）：gate 通过即定型 effective_ack_us（首次），供 fill 侧 ack_to_fill
    // 复用同端点。ack_path / event_to_ack 亦切 effective_end_us（不再用 raw first_ack_us），
    // 与 submit_to_ack 四 stage 共用同一端点（CSV 端点 first-ACK→effective-ACK 已披露 §8.1）。
    if (st.effective_ack_us == 0) st.effective_ack_us = effective_end_us;
    if (!st.ack_latency_pushed) {
        if (rolling_add_elapsed(stats_.submit_to_ack, effective_end_us,
                                st.submit_begin_us, sample_ts_us, slot_dur_us_)) {
            st.ack_latency_pushed = true;
            if (reg_local_ && st.strategy[0] != '\0' && st.code[0] != '\0'
                && effective_end_us > st.submit_begin_us) {
                LabelKey key = make_label_key(st.strategy, st.strategy, st.code);
                auto* h = label_cache_get_(hist_ack_cache_, key, venue_local_, &build_ack);
                if (h) h->Observe(static_cast<double>(effective_end_us - st.submit_begin_us));
            }
        }
    }
    if (!st.ack_path_pushed && st.submit_end_us > 0) {
        if (rolling_add_elapsed(stats_.ack_path, effective_end_us, st.submit_end_us,
                                sample_ts_us, slot_dur_us_)) {
            st.ack_path_pushed = true;
        }
    }
    if (!st.event_ack_pushed && st.trigger_tick_us > 0) {
        if (rolling_add_elapsed(stats_.event_to_ack, effective_end_us, st.trigger_tick_us,
                                sample_ts_us, slot_dur_us_)) {
            st.event_ack_pushed = true;
        }
    }
}

void XtLiveLatencyTracker::push_paired_ack_and_savings_if_ready_(OrderTrack& st, uint64_t sample_ts_us) {
    if (!st.submit_recorded) return;
    if (st.raw_uds_us == 0 || st.raw_trade_ack_us == 0) return;
    const uint64_t effective_end_us = std::min(st.raw_uds_us, st.raw_trade_ack_us);
    if (!st.ack_paired_latency_pushed) {
        if (rolling_add_elapsed(stats_.submit_to_ack_paired, effective_end_us,
                                st.submit_begin_us, sample_ts_us, slot_dur_us_)) {
            st.ack_paired_latency_pushed = true;
            if (reg_local_ && st.strategy[0] != '\0' && st.code[0] != '\0'
                && effective_end_us > st.submit_begin_us) {
                LabelKey key = make_label_key(st.strategy, st.strategy, st.code);
                auto* h = label_cache_get_(hist_paired_cache_, key, venue_local_, &build_paired);
                if (h) h->Observe(static_cast<double>(effective_end_us - st.submit_begin_us));
            }
        }
    }
    if (!st.uds_savings_pushed) {
        const uint64_t savings_us = st.raw_trade_ack_us > effective_end_us
                                        ? st.raw_trade_ack_us - effective_end_us : 0;
        xt_util::rolling_add(stats_.uds_first_savings, savings_us, sample_ts_us, slot_dur_us_);
        st.uds_savings_pushed = true;
    }
}

void XtLiveLatencyTracker::push_trade_ack_if_ready_(OrderTrack& st, uint64_t sample_ts_us) {
    if (st.raw_trade_ack_us == 0 || !st.submit_recorded) return;
    if (st.raw_trade_ack_pushed) return;
    if (rolling_add_elapsed(stats_.submit_to_trade_ack, st.raw_trade_ack_us,
                            st.submit_begin_us, sample_ts_us, slot_dur_us_)) {
        st.raw_trade_ack_pushed = true;
    }
}

void XtLiveLatencyTracker::push_ack_to_fill_if_ready_(OrderTrack& st, uint64_t sample_ts_us) {
    if (st.ack_to_fill_pushed) return;
    // I-1：统一 submit gate（V1 此路径无 submit 检查）。无 submit 的重建/新建 track
    // 结构性 push-inert，tombstone 纯为 churn 抑制、正确性零依赖。
    if (!st.submit_recorded) return;
    if (st.first_ack_us == 0 || st.first_fill_us == 0) return;
    // F1：ack 端点用 push_ack_if_ready_ 定型的 effective_ack_us（与 submit_to_ack/ack_path/
    // event_to_ack 共用同端点）；未 finalize（effective_ack_us==0）则 fallback first_ack_us（维持现行为）。
    const uint64_t ack_end = st.effective_ack_us > 0 ? st.effective_ack_us : st.first_ack_us;
    if (rolling_add_elapsed(stats_.ack_to_fill, st.first_fill_us, ack_end,
                            sample_ts_us, slot_dur_us_)) {
        st.ack_to_fill_pushed = true;
    }
}

void XtLiveLatencyTracker::record_cb_to_strategy_(OrderTrack& st, uint64_t cb_us,
                                                  uint64_t strategy_us, uint64_t now) {
    if (strategy_us == 0) return;
    if (st.first_strategy_us != 0) return;  // 第一个 cb 定型（已 push 或已暂存）。
    if (st.submit_recorded) {
        // submit 已到 → 直接 push（V1 语义）。
        st.first_strategy_us = strategy_us;
        rolling_add_elapsed(stats_.cb_to_strategy, strategy_us, cb_us, now, slot_dur_us_);
    } else if (!st.has_pending_cb) {
        // callback-before-submit（合法乱序）：暂存首个 cb/strategy，apply_submit_ 到达后
        // retro-push（F5-a）——I-1 披露的 delta 只覆盖「submit 永不到」的单，不覆盖此类。
        st.pending_cb_us = cb_us;
        st.pending_strategy_us = strategy_us;
        st.has_pending_cb = true;
    }
}

void XtLiveLatencyTracker::push_fill_if_ready_(OrderTrack& st, uint64_t sample_ts_us) {
    if (st.first_fill_us == 0) return;
    if (!st.fill_latency_pushed && st.submit_recorded) {
        if (rolling_add_elapsed(stats_.submit_to_fill, st.first_fill_us,
                                st.submit_begin_us, sample_ts_us, slot_dur_us_)) {
            st.fill_latency_pushed = true;
            if (reg_local_ && st.strategy[0] != '\0' && st.code[0] != '\0'
                && st.first_fill_us > st.submit_begin_us) {
                LabelKey key = make_label_key(st.strategy, st.strategy, st.code);
                auto* h = label_cache_get_(hist_fill_cache_, key, venue_local_, &build_fill);
                if (h) h->Observe(static_cast<double>(st.first_fill_us - st.submit_begin_us));
            }
        }
    }
    if (!st.event_fill_pushed && st.trigger_tick_us > 0) {
        if (rolling_add_elapsed(stats_.event_to_fill, st.first_fill_us,
                                st.trigger_tick_us, sample_ts_us, slot_dur_us_)) {
            st.event_fill_pushed = true;
        }
    }
    push_ack_to_fill_if_ready_(st, sample_ts_us);
}

// ── apply_*（producer 入口对应的 assembler 处理）────────────────────────────

void XtLiveLatencyTracker::apply_submit_(const LatencyTap& tap, uint64_t /*now*/) {
    const uint64_t begin = tap.ts_us;
    const uint64_t end = tap.aux_us;
    ++stats_.submit_attempts;
    rolling_add_elapsed(stats_.submit_wire, end, begin, end, slot_dur_us_);
    if (tap.order_id == 0 || (tap.flags & tf::kSubmitFailed)) {
        ++stats_.submit_failed;
        return;
    }
    ++stats_.submit_success;
    const uint64_t order_id = tap.order_id;
    if (completed_order_seen_(order_id)) return;  // 已终态 tombstone，重建防护
    OrderTrack& st = *get_or_create_order_(order_id);
    st.submit_begin_us = begin;
    st.submit_end_us = end;
    if (tap.trigger_us > 0) st.trigger_tick_us = tap.trigger_us;
    st.submit_recorded = true;

    // meta（strings）从 SubmitMeta sidecar 取出合入 OrderTrack 后 erase。
    if (tap.flags & tf::kHasMeta) {
        std::lock_guard<std::mutex> guard(meta_mu_);
        auto it = submit_meta_.find(order_id);
        if (it != submit_meta_.end()) {
            if (st.strategy[0] == '\0' && it->second.strategy[0] != '\0')
                std::memcpy(st.strategy, it->second.strategy, kStrategyCap);
            if (st.code[0] == '\0' && it->second.code[0] != '\0')
                std::memcpy(st.code, it->second.code, kCodeCap);
            if (it->second.trace[0] != '\0') register_trace_(order_id, st, it->second.trace);
            submit_meta_.erase(it);
        }
    }
    st.last_touch_us = end;
    ensure_emit_meta_(order_id, st);

    // F5-a：callback-before-submit 流的 cb/strategy 在 submit 未到时已暂存，此处 retro-push
    // （与其它 retro-push 同模式），恢复合法乱序样本。
    if (st.has_pending_cb && st.first_strategy_us == 0) {
        st.first_strategy_us = st.pending_strategy_us;
        rolling_add_elapsed(stats_.cb_to_strategy, st.pending_strategy_us, st.pending_cb_us,
                            end, slot_dur_us_);
        st.has_pending_cb = false;
    }

    push_strategy_processing_if_ready_(st, end);
    push_ack_if_ready_(st, end);
    push_trade_ack_if_ready_(st, end);
    push_paired_ack_and_savings_if_ready_(st, end);
    push_fill_if_ready_(st, end);

    // prom emit: local_submit_us（submit_wire = end-begin）。
    if (reg_local_ != nullptr && st.strategy[0] != '\0' && st.code[0] != '\0' && end > begin) {
        LabelKey key = make_label_key(st.strategy, st.strategy, st.code);
        auto* h = label_cache_get_(hist_local_submit_cache_, key, venue_local_, &build_local_submit);
        if (h) h->Observe(static_cast<double>(end - begin));
    }
    maybe_finish_order_(order_id, st);
}

void XtLiveLatencyTracker::apply_order_update_(const LatencyTap& tap, uint64_t now) {
    const uint64_t order_id = tap.order_id;
    if (order_id == 0) return;
    if (completed_order_seen_(order_id)) return;
    const uint64_t event_ts = tap.ts_us;
    OrderTrack& st = *get_or_create_order_(order_id);
    if (st.submit_begin_us == 0) st.submit_begin_us = st.submit_end_us;
    if (st.submit_end_us == 0) st.submit_end_us = event_ts;
    st.last_touch_us = event_ts;
    ensure_emit_meta_(order_id, st);

    const XtOrderStatus status = static_cast<XtOrderStatus>(tap.status);
    const bool is_terminal_now = is_final_order_status(status);
    if (is_terminal_now) st.final_order_seen = true;

    // (1) submit → ack
    if (status != XtOrderStatus::Submitted &&
        (st.first_ack_us == 0 ||
         (!st.ack_latency_pushed &&
          ((st.submit_begin_us > 0 && st.first_ack_us <= st.submit_begin_us) ||
           event_ts < st.first_ack_us)))) {
        st.first_ack_us = event_ts;
        if (st.submit_recorded) push_ack_if_ready_(st, now);
        push_ack_to_fill_if_ready_(st, now);
    }
    // (2) submit → fill
    const bool had_fill = (tap.flags & tf::kHadFill) != 0;
    if (had_fill &&
        (st.first_fill_us == 0 ||
         (!st.fill_latency_pushed &&
          ((st.submit_begin_us > 0 && st.first_fill_us <= st.submit_begin_us) ||
           event_ts < st.first_fill_us)))) {
        st.first_fill_us = event_ts;
        if (st.submit_recorded) push_fill_if_ready_(st, now);
        push_ack_to_fill_if_ready_(st, now);
    }
    // (3) submit → strategy（cb_to_strategy）。submit 已到直接 push；未到（callback-before-submit）
    //     则暂存待 apply_submit_ retro-push（F5-a）。local_cb == event_ts 仅当 cb_present；
    //     否则 begin=0 语义。
    const bool cb_present = (tap.flags & tf::kCbPresent) != 0;
    record_cb_to_strategy_(st, cb_present ? event_ts : 0, tap.aux_us, now);

    if (is_terminal_now) {
        if (st.submit_recorded) push_ack_if_ready_(st, now);
        maybe_finish_order_(order_id, st);
    }
}

void XtLiveLatencyTracker::apply_trade_(const LatencyTap& tap, uint64_t now) {
    const uint64_t event_ts = tap.ts_us;
    const bool synthetic = (tap.flags & tf::kIsSynthetic) != 0;

    // meta（strings）取出。synthetic key 用于反查 trace → real order。
    MetaEntry meta;
    bool has_meta_entry = false;
    if (tap.flags & tf::kHasMeta) {
        std::lock_guard<std::mutex> guard(meta_mu_);
        auto it = submit_meta_.find(tap.order_id);
        if (it != submit_meta_.end()) {
            meta = it->second;
            has_meta_entry = true;
            submit_meta_.erase(it);
        }
    }

    uint64_t order_id;
    if (synthetic) {
        const char* trace = has_meta_entry ? meta.trace : "";
        order_id = resolve_order_id_(0, trace, trace[0] != '\0');
    } else {
        order_id = tap.order_id;
    }
    if (order_id == 0) return;
    if (completed_order_seen_(order_id)) return;
    OrderTrack& st = *get_or_create_order_(order_id);

    // CTP 补位 submit 时间戳（trade 内嵌 submit 信息）。
    if (tap.flags & tf::kHasSubmitInfo) {
        if (st.submit_begin_us == 0 && tap.aux_us > 0) {
            st.submit_begin_us = tap.aux_us;
            st.submit_recorded = true;
        }
        if (st.submit_end_us == 0 && tap.trigger_us > 0) st.submit_end_us = tap.trigger_us;
    }
    st.last_touch_us = event_ts;
    // trace 注册（submit 缺时由 trade 补）。
    if (has_meta_entry && meta.trace[0] != '\0') register_trace_(order_id, st, meta.trace);
    // 回填 strategy/code（submit 缺 label 时）。
    if (has_meta_entry) {
        if (st.strategy[0] == '\0' && meta.strategy[0] != '\0')
            std::memcpy(st.strategy, meta.strategy, kStrategyCap);
        if (st.code[0] == '\0' && meta.code[0] != '\0')
            std::memcpy(st.code, meta.code, kCodeCap);
    }
    ensure_emit_meta_(order_id, st);

    // submit → fill
    if (st.first_fill_us == 0 ||
        (!st.fill_latency_pushed &&
         ((st.submit_begin_us > 0 && st.first_fill_us <= st.submit_begin_us) ||
          event_ts < st.first_fill_us))) {
        st.first_fill_us = event_ts;
        if (st.submit_recorded) push_fill_if_ready_(st, now);
        push_ack_to_fill_if_ready_(st, now);
    }
    // cb_to_strategy：无 submit-info 时 aux_us 携带 local_strategy_us；有 submit-info 时
    // aux2_us 携带 local_strategy_us（F5-b，对拍 V1 on_trade 同时设 submit 与 push cb）。
    // 上面 kHasSubmitInfo 分支已置 submit_recorded → record_cb_to_strategy_ 走直接 push 分支。
    const bool has_submit_info = (tap.flags & tf::kHasSubmitInfo) != 0;
    const uint64_t strategy_us = has_submit_info ? tap.aux2_us : tap.aux_us;
    const bool cb_present = (tap.flags & tf::kCbPresent) != 0;
    record_cb_to_strategy_(st, cb_present ? event_ts : 0, strategy_us, now);

    maybe_finish_order_(order_id, st);
}

void XtLiveLatencyTracker::apply_raw_ack_(const LatencyTap& tap, uint64_t now) {
    const uint64_t order_id = tap.order_id;
    const uint64_t ack_us = tap.ts_us;
    if (completed_order_seen_(order_id)) return;
    OrderTrack& st = *get_or_create_order_(order_id);
    if (st.raw_trade_ack_us == 0 || ack_us < st.raw_trade_ack_us) st.raw_trade_ack_us = ack_us;
    if (st.first_ack_us == 0 || ack_us < st.first_ack_us) st.first_ack_us = ack_us;
    st.last_touch_us = ack_us;
    ensure_emit_meta_(order_id, st);
    push_trade_ack_if_ready_(st, now);
    push_ack_if_ready_(st, now);
    push_paired_ack_and_savings_if_ready_(st, now);
    maybe_finish_order_(order_id, st);
}

void XtLiveLatencyTracker::apply_raw_uds_(const LatencyTap& tap, uint64_t now) {
    const uint64_t order_id = tap.order_id;
    const uint64_t uds_ts_us = tap.ts_us;
    if (completed_order_seen_(order_id)) return;
    OrderTrack& st = *get_or_create_order_(order_id);
    if (st.raw_uds_us == 0 || uds_ts_us < st.raw_uds_us) st.raw_uds_us = uds_ts_us;
    if (st.first_ack_us == 0 || uds_ts_us < st.first_ack_us) st.first_ack_us = uds_ts_us;
    st.last_touch_us = uds_ts_us;
    ensure_emit_meta_(order_id, st);
    push_ack_if_ready_(st, now);
    push_paired_ack_and_savings_if_ready_(st, now);
    maybe_finish_order_(order_id, st);
}

void XtLiveLatencyTracker::apply_raw_ack_unavailable_(const LatencyTap& tap, uint64_t now) {
    const uint64_t order_id = tap.order_id;
    if (completed_order_seen_(order_id)) return;
    OrderTrack& st = *get_or_create_order_(order_id);
    st.raw_trade_ack_unavailable = true;
    st.last_touch_us = tap.ts_us > 0 ? tap.ts_us : now;
    ensure_emit_meta_(order_id, st);
    push_ack_if_ready_(st, now);
    maybe_finish_order_(order_id, st);
}
