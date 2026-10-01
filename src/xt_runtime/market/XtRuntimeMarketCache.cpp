#include "XtRuntimeMarketCache.h"
#include "xt_core/XtFixedPoint.h"
#include "xt_runtime/market/detail/XtTransparentMapSlot.h"

#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif

#include <cassert>
#include <cstdio>

namespace xt_runtime_market_drops {

std::atomic<uint64_t>& cross_thread_read_blocked() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

}  // namespace xt_runtime_market_drops

namespace xt_runtime {

using market::detail::slot_for;

void XtRuntimeMarketCache::configure(const XtRuntimeMarketCacheConfig& cfg) {
    assert(!first_tick_seen_ && "configure() must run before the first market write");
    tick_history_cap_      = cfg.tick_history_cap;
    orderflow_history_cap_ = cfg.orderflow_history_cap;
    bar_view_.set_history_cap(cfg.bar_history_cap);  // 0 → 512 由 set_history_cap 兜底
    bar_from_tick_         = cfg.bar_from_tick;
}

void XtRuntimeMarketCache::set_bar_capacity(const char* code, const char* period, size_t max) {
    bar_view_.set_series_capacity(code, period, max);
}

void XtRuntimeMarketCache::register_published_symbols(const std::vector<std::string>& symbols) {
    // off-worker 发布槽：冷路径建好后交易期冻结（不再插入），保证并发 find() 安全（无 rehash）。
    for (const auto& sym : symbols) {
        if (sym.empty()) continue;
        published_tick_slots_.try_emplace(sym, nullptr);
    }
}

void XtRuntimeMarketCache::pre_warm(const std::vector<std::string>& symbols) {
    for (const auto& sym : symbols) {
        if (sym.empty()) continue;
        last_ticks_.emplace(sym, XtTick{});
        if (tick_history_cap_ > 0 && tick_histories_.find(sym) == tick_histories_.end()) {
            auto& rb = tick_histories_[sym];
            rb.set_cap(tick_history_cap_);
        }
        if (orderflow_history_cap_ > 0) {
            if (order_queue_histories_.find(sym) == order_queue_histories_.end()) {
                order_queue_histories_[sym].set_cap(orderflow_history_cap_);
            }
            if (order_detail_histories_.find(sym) == order_detail_histories_.end()) {
                order_detail_histories_[sym].set_cap(orderflow_history_cap_);
            }
            if (transaction_histories_.find(sym) == transaction_histories_.end()) {
                transaction_histories_[sym].set_cap(orderflow_history_cap_);
            }
        }
    }
}

void XtRuntimeMarketCache::bind_worker_thread(std::thread::id tid) {
    worker_tid_ = tid;
    worker_bound_.store(true, std::memory_order_release);
}

void XtRuntimeMarketCache::unbind_worker_thread() {
    worker_bound_.store(false, std::memory_order_release);
    read_first_touch_tid_.store(std::thread::id{}, std::memory_order_release);
}

bool XtRuntimeMarketCache::is_worker_thread_allowed_() const noexcept {
    const std::thread::id self = std::this_thread::get_id();
    if (worker_bound_.load(std::memory_order_acquire)) {
        return self == worker_tid_;
    }
    // 未绑定（BT/单测）：test-then-CAS —— per-tick 读的稳态只付 load+cmp，
    // 仅首个直读线程 CAS 一次（无条件 CAS 会给 unbound 常态加 ~4-7ns/call）。
    std::thread::id seen = read_first_touch_tid_.load(std::memory_order_acquire);
    if (seen == std::thread::id{}) {
        if (read_first_touch_tid_.compare_exchange_strong(
                seen, self, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return true;
        }
        // CAS 失败：seen 已被竞争胜者 tid 覆盖，落到下方同线程比较。
    }
    return seen == self;
}

void XtRuntimeMarketCache::on_cross_thread_read_blocked_() const noexcept {
    // 跨线程直读 mutable map 是 data race（rehash-during-find UB + torn read）；
    // fail-closed 返 nullptr/0 比 race 出垃圾行情喂给策略安全（读不到 = 保守不动作）。
    xt_runtime_market_drops::cross_thread_read_blocked().fetch_add(
        1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
    ct::metrics::XtMetricRegistry::instance().inc_runtime_market_drop_total(
        "cross_thread_direct_read");
#endif
    static std::atomic<bool> warned{false};
    if (!warned.exchange(true)) {
        std::fprintf(stderr,
                     "[MarketCache][error] cross-thread direct read blocked — "
                     "last_tick/ticks/bars/last_price_fp must run on the worker "
                     "thread; off-worker readers use snapshot_last_tick()\n");
    }
}

void XtRuntimeMarketCache::on_tick(const XtTick& tick) {
    bar_view_.on_tick(tick);
    store_tick_without_bar_(tick);
}

void XtRuntimeMarketCache::store_tick_without_bar_(const XtTick& tick) {
    const char* code = tick.code;
    // 空 code tick 不更新任何缓存，避免脏 "" key（reader 一律 guard 空 code）。
    if (code[0] == '\0') return;
#ifndef NDEBUG
    first_tick_seen_ = true;
#endif
    slot_for(last_ticks_, code) = tick;
    // 统一写入 last_prices_fp_（HFT + CTA 共用单一价格源）。
    if (tick.price_fp != 0) slot_for(last_prices_fp_, code) = tick.price_fp;
    // 发布 off-worker 快照（在 tick_history early-return 之前，保证恒发布）。
    publish_quote_(tick);

    if (tick_history_cap_ == 0) return;
    auto& rb = slot_for(tick_histories_, code);
    // 未 pre_warm 的合约在首次有效 tick 时懒初始化 ring buffer。
    if (rb.cap() == 0) rb.set_cap(tick_history_cap_);
    rb.push(tick);
}

void XtRuntimeMarketCache::update_last_only(const XtTick& tick) {
    const char* code = tick.code;
    if (!code || code[0] == '\0') return;
#ifndef NDEBUG
    first_tick_seen_ = true;  // configure-after-write 检测需覆盖所有行情写入路径
#endif
    slot_for(last_ticks_, code) = tick;
    publish_quote_(tick);
}

void XtRuntimeMarketCache::on_order_queue(const XtOrderQueue& oq) {
    if (oq.code[0] == '\0' || orderflow_history_cap_ == 0) return;
    auto& rb = slot_for(order_queue_histories_, oq.code);
    if (rb.cap() == 0) rb.set_cap(orderflow_history_cap_);
    rb.push(oq);
}

void XtRuntimeMarketCache::on_order_detail(const XtOrderDetail& od) {
    if (od.code[0] == '\0' || orderflow_history_cap_ == 0) return;
    auto& rb = slot_for(order_detail_histories_, od.code);
    if (rb.cap() == 0) rb.set_cap(orderflow_history_cap_);
    rb.push(od);
}

void XtRuntimeMarketCache::on_transaction(const XtTransaction& tr) {
    if (tr.code[0] == '\0' || orderflow_history_cap_ == 0) return;
    auto& rb = slot_for(transaction_histories_, tr.code);
    if (rb.cap() == 0) rb.set_cap(orderflow_history_cap_);
    rb.push(tr);
}

void XtRuntimeMarketCache::store_tick(const XtTick& tick) {
    if (bar_from_tick_) {
        on_tick(tick);
    } else {
        // CTA tick 模式由外部 BarAggregator 注入完成 bar，只保留 tick/price/history。
        store_tick_without_bar_(tick);
    }
}

void XtRuntimeMarketCache::inject_bar(const char* code, const char* period, const XtBar& bar) {
    bar_view_.inject_completed_bar(code, period, bar);
    // 完成 bar 的 close 价是 CTA bar 模式下 last_price_fp 的唯一价格来源。
    if (code && code[0] != '\0' && bar.close > 0.0)
        slot_for(last_prices_fp_, code) = xt_fp::to_fp(bar.close);
}

// 直读查询统一结构：degenerate 参数检查先于线程闩（无意义调用不触发
// first-touch 绑定，对齐 XtOrderRuntimeBook::collect_open_orders 顺序）。

const XtTick* XtRuntimeMarketCache::last_tick(const char* code) const noexcept {
    if (!code || code[0] == '\0') return nullptr;
    if (!is_worker_thread_allowed_()) {
        on_cross_thread_read_blocked_();
        return nullptr;
    }
    auto it = last_ticks_.find(code);
    return (it != last_ticks_.end()) ? &it->second : nullptr;
}

size_t XtRuntimeMarketCache::ticks(const char* code, XtTick* out, size_t max) const {
    if (!code || code[0] == '\0' || !out || max == 0) return 0;
    if (!is_worker_thread_allowed_()) {
        on_cross_thread_read_blocked_();
        return 0;
    }
    auto it = tick_histories_.find(code);
    return (it != tick_histories_.end()) ? it->second.copy_tail(out, max) : 0;
}

size_t XtRuntimeMarketCache::bars(const char* code, const char* period, XtBar* out, size_t max) {
    // get_bars miss 时会 emplace series（伪装成查询的写）；闩必须在委托之前。
    if (!code || code[0] == '\0' || !out || max == 0) return 0;
    if (!is_worker_thread_allowed_()) {
        on_cross_thread_read_blocked_();
        return 0;
    }
    return bar_view_.get_bars(code, period, out, max);
}

// orderflow 读端必须经 XtRuntimeView 拷贝接口新增；写侧见 on_order_queue/on_order_detail/on_transaction。

int64_t XtRuntimeMarketCache::last_price_fp(const char* code) const noexcept {
    if (!code || code[0] == '\0') return 0;
    if (!is_worker_thread_allowed_()) {
        on_cross_thread_read_blocked_();
        return 0;
    }
    auto it = last_prices_fp_.find(code);
    return (it != last_prices_fp_.end()) ? it->second : 0;
}

int64_t XtRuntimeMarketCache::bid_price_fp(const char* code) const noexcept {
    if (!code || code[0] == '\0') return 0;
    if (!is_worker_thread_allowed_()) {
        on_cross_thread_read_blocked_();
        return 0;
    }
    auto it = last_ticks_.find(code);
    if (it == last_ticks_.end()) return 0;
    return it->second.bid_price_fp[0];
}

int64_t XtRuntimeMarketCache::ask_price_fp(const char* code) const noexcept {
    if (!code || code[0] == '\0') return 0;
    if (!is_worker_thread_allowed_()) {
        on_cross_thread_read_blocked_();
        return 0;
    }
    auto it = last_ticks_.find(code);
    if (it == last_ticks_.end()) return 0;
    return it->second.ask_price_fp[0];
}

// ── off-worker 安全发布快照（Hazard Pointer）─────────────────────────────────

void XtRuntimeMarketCache::publish_quote_(const XtTick& tick) {
    // 仅发布 register_published_symbols 注册过的合约：published_tick_slots_ 交易期冻结（不插入），
    // 避免 rehash 与并发 reader find() 撞 UB。未注册合约不发布（off-worker reader 得 false）。
    auto it = published_tick_slots_.find(tick.code);
    if (it == published_tick_slots_.end()) return;
    XtTick* buf = quote_pool_.acquire();
    if (!buf) {
        // 自愈：先扫回收无 hazard 保护的 retired buffer，再试一次（参照 XtOrderRuntimeBook
        // acquire_order_book_snapshot）。省略此步会因 scan_threshold_（随注册线程数增长，
        // = max(32×records, 16)）≥ pool size → pool 先于 try_scan_if_due 耗尽 → 永久 stall。
        quote_hp_domain_.try_scan();
        buf = quote_pool_.acquire();
    }
    if (!buf) {
        quote_publish_skipped_.fetch_add(1, std::memory_order_relaxed);
        return;  // 两试仍耗尽：保留上一发布版本（reader 读到 stale，不崩）。
    }
    *buf = tick;
    const XtTick* old = it->second.exchange(buf, std::memory_order_acq_rel);
    if (old) quote_hp_domain_.retire_raw(old, &quote_pool_, &release_quote_);
    quote_hp_domain_.try_scan_if_due();
}

void XtRuntimeMarketCache::release_quote_(void* ctx, const void* ptr) noexcept {
    auto* pool =
        static_cast<xt_util::sync::XtSnapshotPool<XtTick, kQuoteSnapshotPoolSize>*>(ctx);
    pool->release(const_cast<XtTick*>(static_cast<const XtTick*>(ptr)));
}

bool XtRuntimeMarketCache::snapshot_last_tick(const char* code, XtTick& out) const noexcept {
    if (!code || code[0] == '\0') return false;
    auto it = published_tick_slots_.find(code);
    if (it == published_tick_slots_.end()) return false;
    auto guard = quote_hp_domain_.protect(it->second);
    if (!guard) return false;
    out = *guard;  // HP 保护下拷贝 immutable tick
    return true;
}

} // namespace xt_runtime
