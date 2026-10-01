#pragma once

#include "xt_core/market/XtBarStruct.h"
#include "xt_core/market/XtL2MarketStructs.h"
#include "xt_core/container/XtRingBuffer.h"
#include "xt_core/market/XtTick.h"
#include "market/XtRuntimeBarSeriesView.h"
#include "xt_core/sync/XtTransparentStringHash.h"
#include "xt_util/sync/XtHazardDomain.h"
#include "xt_util/sync/XtSnapshotPool.h"

#include <atomic>
#include <cstddef>
#include <string>
#include <thread>
#include <vector>

namespace xt_runtime {

/// @brief XtRuntimeMarketCache 的启动期配置。
///
/// 调用方在首笔行情写入前通过 `configure()` 注入；已创建的历史窗口不会因后续
/// 配置调用重新分配。
struct XtRuntimeMarketCacheConfig {
    size_t tick_history_cap      = 2048;  ///< tick history ring 容量
    size_t bar_history_cap       = 512;   ///< 默认 bar 历史容量（0 → 512）
    size_t orderflow_history_cap = 2048;  ///< orderflow 三类事件 ring 容量
    bool   bar_from_tick         = true;  ///< true: 内部 tick→bar 聚合；false: 外部注入 bar（CTA）
};

/// @brief 策略可见的行情缓存：每个品种的最新 tick、tick/bar 历史和 L2 orderflow。
///
/// 引擎/回放线程每 tick 写入，策略经 runtime view 在同一 worker 线程读取；
/// configure()/pre_warm() 等容量设置会在热路径开始前完成，消除首 tick 堆分配。
///
/// 线程契约：写入与直读必须在同一 worker 线程；跨线程直读 fail-closed
/// （返 nullptr/0 并计数）。跨线程要看最新 tick，走 snapshot_last_tick()
/// 的 Hazard Pointer 快照路径。
class XtRuntimeMarketCache {
public:
    /// @brief 一次性注入配置；应在首次写入行情前调用（之后调用不 resize 已建 buffer）。
    void configure(const XtRuntimeMarketCacheConfig& cfg);
    /// 设置指定品种+周期的 bar 历史容量；仅在 series 首次创建前生效。
    void set_bar_capacity(const char* code, const char* period, size_t max);

    /// 预分配指定品种的 last_tick 和 tick_history 槽位，消除热路径首 tick 堆分配。
    /// 冷路径，交易开始前单线程调用。
    void pre_warm(const std::vector<std::string>& symbols);

    /// @brief 注册允许 off-worker 读取 latest tick 快照的合约集合。
    ///
    /// 冷路径、worker 启动前单线程调用。注册完成后交易期不再插入新 key，
    /// 避免 publish_quote_ 和 snapshot_last_tick() 并发 find 时触发 rehash。
    /// @note 未注册的合约 snapshot_last_tick() 返回 false；本方法不预热
    ///       last_ticks_，worker 侧 last_tick() 仍由 pre_warm()/行情写入决定。
    void register_published_symbols(const std::vector<std::string>& symbols);

    /// @brief 绑定 worker（写 + 直读）线程；此后直读闩以该 tid 为权威。
    ///
    /// 冷路径、装配期单次调用（runner main 在 event-loop worker tid 已知后）。
    /// 绑定覆盖任何先行 first-touch 结果。
    /// @pre 单次调用；调用时不得有并发的直读/写入在途。
    void bind_worker_thread(std::thread::id tid);

    /// @brief 解绑 + 重置 first-touch 绑定（test fixture seam）。
    ///
    /// 供 test fixture 在 worker stop/join 后调用，让后续主线程直读按
    /// first-touch 重新绑定。生产路径不调用。
    /// @pre 调用时不得有并发的直读/写入在途。
    void unbind_worker_thread();

    // 行情写入
    /// @brief 写入完整 tick：更新 latest tick/price、追加 tick history，并推进内部 bar 聚合。
    ///
    /// 首次合约可能触发 map 插入和 ring buffer 初始化；pre_warm 后常规路径不再分配。
    void on_tick(const XtTick& tick);

    /// @brief 仅更新 last_tick 快照，不追加历史。
    void update_last_only(const XtTick& tick);

    /// @brief 写入 order_queue 事件到历史窗口。
    void on_order_queue(const XtOrderQueue& oq);

    /// @brief 写入 order_detail 事件到历史窗口。
    void on_order_detail(const XtOrderDetail& od);

    /// @brief 写入 transaction 事件到历史窗口。
    void on_transaction(const XtTransaction& tr);

    // CTA 扩展写入
    /// @brief 存储 tick，并按 bar_from_tick_ 决定是否推进内部 bar 聚合。
    ///
    /// @note 内部 bar 聚合行为由 bar_from_tick_ 标志控制（默认 true）：
    /// true 表示 HFT/internal 模式；false 表示 CTA tick 模式，bar 由外部 BarAggregator 注入。
    void store_tick(const XtTick& tick);

    /// 直接注入外部完成 bar（不走 tick 聚合，CTA bar aggregator 用）。
    void inject_bar(const char* code, const char* period, const XtBar& bar);

    // ── 直读查询（worker-only，release fail-closed 线程闩）──────────────────
    // 本区所有查询直读 worker-only 的 mutable map，与写入无同步；跨线程调用被
    // is_worker_thread_allowed_ 拦截，fail-closed 返 nullptr/0 并累加
    // xt_runtime_market_drops::cross_thread_read_blocked()（+ prom counter）。
    // off-worker 消费者一律走 snapshot_last_tick()。

    /// @brief 返回指定合约最新 tick 指针；无数据时返回 nullptr。
    ///
    /// @warning worker（写）线程专用；跨线程调用 fail-closed 返 nullptr + 计数，
    ///          off-worker 须改用 snapshot_last_tick()（HP 发布的 immutable 拷贝）。
    [[nodiscard]] const XtTick* last_tick(const char* code) const noexcept;

    /// @brief off-worker 安全读取指定合约最新 tick 的 immutable 拷贝（Hazard Pointer 保护）。
    ///
    /// worker 在 on_tick/update_last_only 把 pre_warm 过的合约 tick 发布到 HP 保护的快照；
    /// control report emit / gRPC panel 等非 worker 线程经本接口读取，避免直读 last_ticks_ 的
    /// data race（rehash-during-find UB + torn read）。
    /// @param out  命中时写入 tick 拷贝；未命中（非 pre_warm 合约 / 尚无 tick）保持不变。
    /// @return true=命中并已拷贝；false=未命中。
    [[nodiscard]] bool snapshot_last_tick(const char* code, XtTick& out) const noexcept;

    /// @brief off-worker 发布 pool 两试仍耗尽、跳过发布的累计次数（观测 / 测试用）。
    /// 稳态应恒 0（acquire-fail→try_scan→retry 自愈）；持续 >0 表示 pool 偏小或 reader 长持 guard。
    [[nodiscard]] uint64_t quote_publish_skipped() const noexcept {
        return quote_publish_skipped_.load(std::memory_order_relaxed);
    }

    /// @brief 拷贝最近 N 条 tick 到调用方缓冲区。
    size_t ticks(const char* code, XtTick* out, size_t max) const;

    /// @brief 获取 bar 序列，写入调用方缓冲区。
    size_t bars(const char* code, const char* period, XtBar* out, size_t max);

    // orderflow 三类事件当前只保留写侧缓存面；若新增读端，必须经
    // XtRuntimeView 暴露拷贝接口，不得返回内部容器引用。

    /// 查询最新价（fp）；仅返回 last_prices_fp_ 中的显式缓存值。
    /// @warning worker（写）线程专用；跨线程调用 fail-closed 返 0 + 计数。
    [[nodiscard]] int64_t last_price_fp(const char* code) const noexcept;

    /// Best bid/ask 价格（level 0, fp x1e6）；无 quote 时返回 0。
    /// @warning worker（写）线程专用；跨线程调用 fail-closed 返 0 + 计数。
    ///          off-worker 经 snapshot_last_tick() 取拷贝后读 bid_price_fp[0]/ask_price_fp[0]。
    [[nodiscard]] int64_t bid_price_fp(const char* code) const noexcept;
    [[nodiscard]] int64_t ask_price_fp(const char* code) const noexcept;

private:
    void store_tick_without_bar_(const XtTick& tick);

    /// @brief 直读线程契约检查（release 也生效）。
    ///
    /// bound 时以 worker_tid_ 为权威；未绑定时 first-touch 绑定当前线程
    /// （test-then-CAS：稳态只付 load+cmp，仅首次读 CAS——BT/单测 unbound 常态
    /// 下避免 per-call lock cmpxchg 开销）。返回 false = 跨线程直读，
    /// caller 必须 fail-closed。
    [[nodiscard]] bool is_worker_thread_allowed_() const noexcept;

    /// @brief 直读契约失败的统一出口：计数 + metric + log-once；caller 随后返 nullptr/0。
    void on_cross_thread_read_blocked_() const noexcept;

    /// @brief worker 把 tick 发布到 HP 保护的 per-symbol 快照（仅 pre_warm 过的合约）。
    /// pool 耗尽时跳过发布（reader 读到上一版，stale 不崩）并累加 quote_publish_skipped_。
    void publish_quote_(const XtTick& tick);
    /// @brief SnapshotPool 归还回调（HazardDomain retire_raw 用）。
    static void release_quote_(void* ctx, const void* ptr) noexcept;

    /// off-worker 发布快照池容量；pre_warm 合约的 live + retired-pending + reader-held 之和上限。
    static constexpr std::size_t kQuoteSnapshotPoolSize = 64;

    size_t tick_history_cap_      = 2048;
    size_t orderflow_history_cap_ = 2048;

    // last_tick：按值存储，避免 unique_ptr 的间接寻址与首次堆分配。
    // 透明 hash：find(const char*) 零临时 string 构造。
    xt_core::sync::TransparentStringMap<XtTick> last_ticks_;

    // 历史窗口：ring-buffer 替代 deque，内存连续，copy_tail cache 友好。
    xt_core::sync::TransparentStringMap<XtRingBuffer<XtTick>>              tick_histories_;
    xt_core::sync::TransparentStringMap<XtRingBuffer<XtOrderQueue>>  order_queue_histories_;
    xt_core::sync::TransparentStringMap<XtRingBuffer<XtOrderDetail>> order_detail_histories_;
    xt_core::sync::TransparentStringMap<XtRingBuffer<XtTransaction>> transaction_histories_;

    market::XtRuntimeBarSeriesView bar_view_;
    bool bar_from_tick_ = true;     ///< store_tick 是否内部聚合 tick->bar。
    bool first_tick_seen_ = false;  ///< debug-only: 检测 configure() 是否在写入行情后误调。

    // CTA 最新价缓存（fp）：支持仅有价格、没有完整 XtTick 的调用路径。
    xt_core::sync::TransparentStringMap<int64_t> last_prices_fp_;

    // ── off-worker 安全发布快照（Hazard Pointer）──────────────────────────────
    // worker 写 last_ticks_ 的同时，把 pre_warm 过的合约 tick 发布到 HP 保护的 per-symbol 槽，
    // 供 control report emit / gRPC panel 等非 worker 线程经 snapshot_last_tick() 安全读取。
    // published_tick_slots_ 在 pre_warm（冷路径、单线程）建好后交易期不再插入 → 交易期并发
    // find() 安全（仅值 atomic 变化，map 结构冻结）。
    // 声明序：quote_hp_domain_ 最后声明 → 析构最先 → retire 记录回收进仍存活的 quote_pool_。
    xt_core::sync::TransparentStringMap<std::atomic<const XtTick*>> published_tick_slots_;
    std::atomic<uint64_t> quote_publish_skipped_{0};  ///< pool 耗尽跳过发布次数（观测）。
    xt_util::sync::XtSnapshotPool<XtTick, kQuoteSnapshotPoolSize> quote_pool_;
    mutable xt_util::sync::HazardDomain quote_hp_domain_;

    // ── 直读线程闩（fail-closed）────────────────────────────────────────────
    // worker_tid_ 经 worker_bound_ release/acquire 发布；正确性依赖 bind 单次 +
    // unbind 仅 test-seam quiescent 期调用（与 XtOrderRuntimeBook marshal 同契约）。
    std::atomic<bool> worker_bound_{false};           ///< bind_worker_thread 已调
    std::thread::id   worker_tid_{};                  ///< 权威 worker tid
    mutable std::atomic<std::thread::id> read_first_touch_tid_{};  ///< 未绑定时 first-touch
};

} // namespace xt_runtime

/// @brief XtRuntimeMarketCache 直读闩 process-global 观测 counter。
///
/// 独立于 XtMetricRegistry（xt_runtime 不强依赖 metrics target），单调递增；
/// prod 永不重置，测试经 reset_for_test 归零。
namespace xt_runtime_market_drops {

/// @brief 非 worker 线程调直读查询被 fail-closed 拦截的次数。
///
/// 正常 prod 调用图（策略 ctx / engine roll 均在 worker）应恒为 0；> 0 表示
/// 有代码从 panel/metrics 等线程误用直读接口（被拦截前是 data race）。
std::atomic<uint64_t>& cross_thread_read_blocked();

#ifdef XT_BUILD_TESTS
/// @brief test-only 计数器重置 helper（process-global，测试 SetUp 显式重置防串扰）。
inline void reset_for_test() {
    cross_thread_read_blocked().store(0, std::memory_order_relaxed);
}
#endif
}  // namespace xt_runtime_market_drops
