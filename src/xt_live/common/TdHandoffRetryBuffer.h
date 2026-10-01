#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <utility>

namespace xt_live {

/// 重投缓冲默认容量；覆盖 TD 队列瞬时满的突发窗口（must_not_drop 为低频事件）。
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

    /// @brief offer 结局；kOverflow 之外 payload 均未丢失。
    enum class Offer : uint8_t {
        kEnqueued,  ///< 已直接投递下游队列（缓冲为空且 push 成功）。
        kBuffered,  ///< 已持入重投缓冲（进程内必达承诺生效）。
        kOverflow,  ///< 缓冲已满，新事件被拒绝——调用方按既有 drop 语义计数。
    };

    /// @brief 观测快照；counter 单调递增，depth/store_and_forward 为瞬时值。
    struct Stats {
        uint64_t buffered_total = 0;     ///< 累计持入缓冲的事件数
        uint64_t redelivered_total = 0;  ///< 累计自缓冲重投成功的事件数
        uint64_t overflow_total = 0;     ///< 累计因缓冲满被拒绝的事件数
        size_t depth = 0;                ///< 当前缓冲深度
        bool store_and_forward = false;  ///< 当前是否处于 S&F 模式
    };

    explicit TdHandoffRetryBuffer(size_t capacity = kDefaultCapacity)
        : cap_(capacity) {}

    /// @brief 投递一个 must_not_drop 事件：先试排空缓冲（enqueue 调用点即 drain
    ///        时机），缓冲空则直发，直发失败或模式内则按序持入缓冲。
    /// @param ev   事件 payload；kOverflow 时随本次调用丢弃（调用方已在调用前
    ///             提取计数/分类所需信息）。
    /// @param push 下游投递函数，见类注释 PushFn 契约；在本组件锁内执行，必须
    ///             非阻塞（对齐 MPSC try-push 语义）。
    /// @return 见 Offer；kOverflow 才算 drop。
    template <typename PushFn>
    Offer offer(Event ev, PushFn&& push) {
        std::lock_guard<std::mutex> lk(mu_);
        if (cap_ == 0) {
            // 功能关闭：直发，失败即视为溢出（回退旧的失败即丢语义）；
            // kOverflow 返回值与 overflow_total 保持 1:1，与 enabled 路径同口径。
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
            // 下游拒绝（队列满/已停）→ 进入 store-and-forward。
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

    /// @brief 自队首按序重投缓冲事件；排空即退出 S&F 模式（同一临界区）。
    /// @return 本次成功重投的事件数。
    /// @note 供周期 hook（1Hz frame-tail / worker 每轮）与 enqueue 调用点共用；
    ///       缓冲为空时 O(1) 返回。任意线程可调。
    template <typename PushFn>
    size_t try_drain(PushFn&& push) {
        std::lock_guard<std::mutex> lk(mu_);
        return drain_locked_(push);
    }

    /// @brief S&F 模式 hint（lock-free relaxed 读）。
    /// @note 仅作廉价预检（周期 hook / 非关键事件路径避免每次取锁）；权威判定
    ///       在 offer/try_drain 锁内完成，hint 短暂滞后无碍正确性。
    [[nodiscard]] bool in_store_and_forward() const noexcept {
        return saf_hint_.load(std::memory_order_relaxed);
    }

    /// @brief 观测快照（冷路径，取锁读 depth 保证与 counter 一致视图）。
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

    /// @brief 运行时调整容量；0 = 关闭（已缓冲事件保留，仍可被 try_drain 排空）。
    void set_capacity(size_t capacity) {
        std::lock_guard<std::mutex> lk(mu_);
        cap_ = capacity;
    }

private:
    /// @pre 调用方持有 mu_。
    template <typename PushFn>
    size_t drain_locked_(PushFn&& push) {
        size_t n = 0;
        while (!buf_.empty()) {
            // PushFn 失败不消费实参（契约）→ 队首原地保留，保序等待下轮。
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

    mutable std::mutex mu_;   ///< 序列化 offer 决策与 drain+模式清除（见类注释）
    std::deque<Event> buf_;   ///< S&F 缓冲；非空 ⇔ S&F 模式（锁内权威判定）
    size_t cap_;              ///< 容量上限；0 = 功能关闭；mu_ 保护

    std::atomic<bool> saf_hint_{false};            ///< S&F 模式 lock-free hint
    std::atomic<uint64_t> buffered_total_{0};      ///< 累计持入缓冲数
    std::atomic<uint64_t> redelivered_total_{0};   ///< 累计重投成功数
    std::atomic<uint64_t> overflow_total_{0};      ///< 累计溢出拒绝数
};

}  // namespace xt_live
