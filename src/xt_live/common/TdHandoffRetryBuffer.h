#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <utility>

namespace xt_live {

inline constexpr size_t kTdHandoffDefaultCapacity = 256;

/// @brief must_not_drop 事件的 TD-queue handoff 重投缓冲（bounded store-and-forward）。
///
/// 修复 dedup/ledger「commit-before-enqueue」在队列拒绝后精确事件不可重放的缺口
/// （BIN-D13，venue-agnostic）：enqueue 失败时不丢 payload，持入有界 deque 进入
/// store-and-forward（S&F）模式；模式内后续 must_not_drop 事件一律追加队尾、
/// drain 自队首重投——must_not_drop 事件间全序保持（快照失败入缓冲后，后续
/// fill/终态不得超车快照）。排空即退出 S&F。溢出时新事件被拒绝（kOverflow），
/// 由调用方落入既有 critical drop 计数与 fail-closed 停机语义；**已缓冲事件
/// 永不因溢出被丢**。
///
/// 并发模型（定稿契约，plan §4.5）：下游 TD queue 是 MPSC（producer=venue
/// SDK/ws/reconcile/control 线程；consumer=worker；worker 兼职 producer 合法）。
/// 本组件自带一把 mutex，序列化两个复合操作于同一临界区：
///   (a) producer 侧「S&F 模式检查 + enqueue-or-buffer 决策」（offer）；
///   (b) drain 侧「批量出队重投 + 模式清除」（try_drain / offer 内联 drain）。
/// 由此杜绝「drain 清模式瞬间另一 producer 读到 mode=false 直发超车缓冲事件」：
/// 直发只可能发生在锁内确认缓冲为空之后，而缓冲清空与模式退出同锁完成。
/// 所有经由本组件的下游 push 都在锁内执行 → must_not_drop 事件进入 MPSC 队列
/// 的顺序 == offer 顺序（缓冲序）。非 must_not_drop 事件不经本组件（可超车，
/// 维持既有丢弃语义）。
///
/// PushFn 契约：`bool push(Event&)` —— 尝试向下游队列投递；返回 true 表示已
/// 投递（**可以** move-from 实参）；返回 false 表示未投递且**必须**保持实参
/// 完好可重投。适配层负责满足该契约：下游签名若是 consume-on-failure 的
/// 按值接口（如 `XtHftEventLoop::push_td_event`），适配 lambda 须先拷贝再投
/// （copy-probe）；下游若失败不消费（如 `MpscRingQueue::push(T&&)` 满队即返，
/// 不 forward），可直接 `push(std::move(e))` 零拷贝。
///
/// @note 进程内必达承诺以进程存活为界：停机/析构时缓冲内未排空事件随对象
///       销毁丢弃（与既有停止期丢弃语义一致；crash → venue rebuild）。
/// @note capacity == 0 = 功能关闭（回滚开关，对齐 `<=0 关闭` 惯例）：offer
///       退化为直发、失败即 kOverflow（等价旧的失败即丢语义）。
template <typename Event>
class TdHandoffRetryBuffer {
public:
    static constexpr size_t kDefaultCapacity = kTdHandoffDefaultCapacity;

    enum class Offer : uint8_t {
        kEnqueued,
        kBuffered,
        kOverflow,
    };

    /// @brief 观测快照；counter 单调递增，depth/store_and_forward 为瞬时值。
    struct Stats {
        uint64_t buffered_total = 0;
        uint64_t redelivered_total = 0;
        uint64_t overflow_total = 0;
        size_t depth = 0;
        bool store_and_forward = false;
    };

    explicit TdHandoffRetryBuffer(size_t capacity = kDefaultCapacity)
        : cap_(capacity) {}

    template <typename PushFn>
    Offer offer(Event ev, PushFn&& push) {
        std::lock_guard<std::mutex> lk(mu_);
        if (cap_ == 0) {

            if (push(ev)) {
                return Offer::kEnqueued;
            }
            overflow_total_.fetch_add(1, std::memory_order_relaxed);
            return Offer::kOverflow;
        }
        if (!buf_.empty()) {
            drain_locked_(push);
        }
        if (buf_.empty()) {
            if (push(ev)) {
                return Offer::kEnqueued;
            }

            buf_.push_back(std::move(ev));
            saf_hint_.store(true, std::memory_order_relaxed);
            buffered_total_.fetch_add(1, std::memory_order_relaxed);
            return Offer::kBuffered;
        }
        if (buf_.size() >= cap_) {
            overflow_total_.fetch_add(1, std::memory_order_relaxed);
            return Offer::kOverflow;
        }
        buf_.push_back(std::move(ev));
        buffered_total_.fetch_add(1, std::memory_order_relaxed);
        return Offer::kBuffered;
    }

    template <typename PushFn>
    size_t try_drain(PushFn&& push) {
        std::lock_guard<std::mutex> lk(mu_);
        return drain_locked_(push);
    }

    [[nodiscard]] bool in_store_and_forward() const noexcept {
        return saf_hint_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] Stats stats() const {
        std::lock_guard<std::mutex> lk(mu_);
        Stats s;
        s.buffered_total = buffered_total_.load(std::memory_order_relaxed);
        s.redelivered_total = redelivered_total_.load(std::memory_order_relaxed);
        s.overflow_total = overflow_total_.load(std::memory_order_relaxed);
        s.depth = buf_.size();
        s.store_and_forward = !buf_.empty();
        return s;
    }

    void set_capacity(size_t capacity) {
        std::lock_guard<std::mutex> lk(mu_);
        cap_ = capacity;
    }

private:

    template <typename PushFn>
    size_t drain_locked_(PushFn&& push) {
        size_t n = 0;
        while (!buf_.empty()) {

            if (!push(buf_.front())) break;
            buf_.pop_front();
            ++n;
        }
        if (n > 0) {
            redelivered_total_.fetch_add(n, std::memory_order_relaxed);
        }
        if (buf_.empty()) {
            saf_hint_.store(false, std::memory_order_relaxed);
        }
        return n;
    }

    mutable std::mutex mu_;
    std::deque<Event> buf_;
    size_t cap_;

    std::atomic<bool> saf_hint_{false};
    std::atomic<uint64_t> buffered_total_{0};
    std::atomic<uint64_t> redelivered_total_{0};
    std::atomic<uint64_t> overflow_total_{0};
};

}
