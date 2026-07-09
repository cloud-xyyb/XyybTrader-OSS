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
    size_t tick_history_cap      = 2048;
    size_t bar_history_cap       = 512;
    size_t orderflow_history_cap = 2048;
    bool   bar_from_tick         = true;
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

    void configure(const XtRuntimeMarketCacheConfig& cfg);

    void set_bar_capacity(const char* code, const char* period, size_t max);

    void pre_warm(const std::vector<std::string>& symbols);

    void register_published_symbols(const std::vector<std::string>& symbols);

    void bind_worker_thread(std::thread::id tid);

    void unbind_worker_thread();

    void on_tick(const XtTick& tick);

    void update_last_only(const XtTick& tick);

    void on_order_queue(const XtOrderQueue& oq);

    void on_order_detail(const XtOrderDetail& od);

    void on_transaction(const XtTransaction& tr);

    void store_tick(const XtTick& tick);

    void inject_bar(const char* code, const char* period, const XtBar& bar);

    [[nodiscard]] const XtTick* last_tick(const char* code) const noexcept;

    [[nodiscard]] bool snapshot_last_tick(const char* code, XtTick& out) const noexcept;

    [[nodiscard]] uint64_t quote_publish_skipped() const noexcept {
        return quote_publish_skipped_.load(std::memory_order_relaxed);
    }

    size_t ticks(const char* code, XtTick* out, size_t max) const;

    size_t bars(const char* code, const char* period, XtBar* out, size_t max);

    [[nodiscard]] int64_t last_price_fp(const char* code) const noexcept;

    [[nodiscard]] int64_t bid_price_fp(const char* code) const noexcept;
    [[nodiscard]] int64_t ask_price_fp(const char* code) const noexcept;

private:
    void store_tick_without_bar_(const XtTick& tick);

    [[nodiscard]] bool is_worker_thread_allowed_() const noexcept;

    void on_cross_thread_read_blocked_() const noexcept;

    void publish_quote_(const XtTick& tick);

    static void release_quote_(void* ctx, const void* ptr) noexcept;

    static constexpr std::size_t kQuoteSnapshotPoolSize = 64;

    size_t tick_history_cap_      = 2048;
    size_t orderflow_history_cap_ = 2048;

    xt_core::sync::TransparentStringMap<XtTick> last_ticks_;

    xt_core::sync::TransparentStringMap<XtRingBuffer<XtTick>>              tick_histories_;
    xt_core::sync::TransparentStringMap<XtRingBuffer<XtOrderQueue>>  order_queue_histories_;
    xt_core::sync::TransparentStringMap<XtRingBuffer<XtOrderDetail>> order_detail_histories_;
    xt_core::sync::TransparentStringMap<XtRingBuffer<XtTransaction>> transaction_histories_;

    market::XtRuntimeBarSeriesView bar_view_;
    bool bar_from_tick_ = true;
    bool first_tick_seen_ = false;

    xt_core::sync::TransparentStringMap<int64_t> last_prices_fp_;

    xt_core::sync::TransparentStringMap<std::atomic<const XtTick*>> published_tick_slots_;
    std::atomic<uint64_t> quote_publish_skipped_{0};
    xt_util::sync::XtSnapshotPool<XtTick, kQuoteSnapshotPoolSize> quote_pool_;
    mutable xt_util::sync::HazardDomain quote_hp_domain_;

    std::atomic<bool> worker_bound_{false};
    std::thread::id   worker_tid_{};
    mutable std::atomic<std::thread::id> read_first_touch_tid_{};
};

}

namespace xt_runtime_market_drops {

std::atomic<uint64_t>& cross_thread_read_blocked();

#ifdef XT_BUILD_TESTS

inline void reset_for_test() {
    cross_thread_read_blocked().store(0, std::memory_order_relaxed);
}
#endif
}
