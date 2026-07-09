#pragma once
#include <atomic>
#include <cstddef>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

#if defined(__linux__)
#  include <sys/mman.h>
#endif

/// @file
/// @brief SPSC/MPSC 有界无锁队列与统一 facade。
///
/// `push` 满队列时返回 false，调用方据此统计 drop/backpressure。可选 huge-page
/// advice 只在 Linux 上通过 `madvise(MADV_HUGEPAGE)` 请求 THP，失败不改变队列语义。
namespace xt_queue_detail {

inline void advise_huge_pages(void* ptr, size_t bytes) noexcept {
#if defined(__linux__)
    if (ptr && bytes > 0) {
        ::madvise(ptr, bytes, MADV_HUGEPAGE);
    }
#else
    (void)ptr; (void)bytes;
#endif
}

using HugePageAdviceFn = void (*)(void*, size_t);

inline HugePageAdviceFn& huge_page_advice_fn() noexcept {
    static HugePageAdviceFn fn = &advise_huge_pages;
    return fn;
}

inline void maybe_advise_huge_pages(bool use_huge_pages, void* ptr, size_t bytes) noexcept {
    if (use_huge_pages) {
        huge_page_advice_fn()(ptr, bytes);
    }
}

}

namespace xt_queue_detail {

/// @brief 单生产者单消费者有界 ring buffer。
///
/// head_ 仅 producer 写，tail_ 仅 consumer 写；两者显式 cache-line 隔离，
/// 避免 SPSC 热字段互相 false sharing。
/// @warning 只能有一个 producer 和一个 consumer；多 producer 必须使用 MpscRingQueue。
template <typename T>
class SpscQueue {
public:
    explicit SpscQueue(size_t capacity, bool use_huge_pages = false)
        : cap_(next_pow2(capacity)),
          mask_(cap_ - 1),
          buf_(cap_) {
        xt_queue_detail::maybe_advise_huge_pages(use_huge_pages, buf_.data(), cap_ * sizeof(T));
    }

    bool push(const T& v) {
        const size_t head = head_.load(std::memory_order_relaxed);
        const size_t next = (head + 1) & mask_;
        if (__builtin_expect(next == tail_.load(std::memory_order_acquire), 0)) return false;
        buf_[head] = v;
        head_.store(next, std::memory_order_release);
        return true;
    }

    bool push(T&& v) {
        const size_t head = head_.load(std::memory_order_relaxed);
        const size_t next = (head + 1) & mask_;
        if (__builtin_expect(next == tail_.load(std::memory_order_acquire), 0)) return false;
        buf_[head] = std::move(v);
        head_.store(next, std::memory_order_release);
        return true;
    }

    bool pop(T& out) {
        const size_t tail = tail_.load(std::memory_order_relaxed);
        if (__builtin_expect(tail == head_.load(std::memory_order_acquire), 0)) return false;
        out = buf_[tail];
        tail_.store((tail + 1) & mask_, std::memory_order_release);
        return true;
    }

private:
    static size_t next_pow2(size_t v) {
        size_t n = 1;
        while (n < v) n <<= 1;
        return n < 2 ? 2 : n;
    }

    const size_t cap_;
    const size_t mask_;
    std::vector<T> buf_;

    alignas(64) std::atomic<size_t> head_{0};
    char _pad_head_[64 - sizeof(std::atomic<size_t>)];

    alignas(64) std::atomic<size_t> tail_{0};
    char _pad_tail_[64 - sizeof(std::atomic<size_t>)];
};

/**
 * @brief 多生产者单消费者无锁队列（MPSC lock-free ring queue）。
 *
 * @details
 * - 基于序号环（sequence-based ring buffer）实现，push 端 CAS 竞争，pop 端单线程。
 * - 容量向上取整至 2 的幂，满时 push 返回 false（背压语义）。
 * - 析构时自动 drain 并析构所有残留元素。
 * @warning 只能有一个 consumer；多个 consumer 会破坏 `deq_pos_` 单写者不变量。
 */
template <typename T>
class MpscRingQueue {
public:
    explicit MpscRingQueue(size_t capacity, bool use_huge_pages = false)
        : cap_(round_pow2(capacity > 1 ? capacity : 1024)),
          mask_(cap_ - 1),
          buffer_(cap_) {
        for (size_t i = 0; i < cap_; ++i) {
            buffer_[i].seq.store(i, std::memory_order_relaxed);
        }
        xt_queue_detail::maybe_advise_huge_pages(use_huge_pages, buffer_.data(), cap_ * sizeof(Cell));
    }

    ~MpscRingQueue() {
        while (pop_discard()) {}
    }

    bool push(const T& v) { return emplace(v); }
    bool push(T&& v) { return emplace(std::move(v)); }

    bool pop(T& out) {
        Cell& cell = buffer_[deq_pos_ & mask_];
        const size_t seq = cell.seq.load(std::memory_order_acquire);
        const intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(deq_pos_ + 1);
        if (diff != 0) return false;

        T* ptr = reinterpret_cast<T*>(&cell.storage);
        out = std::move(*ptr);
        ptr->~T();
        cell.seq.store(deq_pos_ + cap_, std::memory_order_release);
        ++deq_pos_;
        return true;
    }

    size_t pop_bulk(T* out, size_t max) {
        if (!out || max == 0) return 0;
        size_t n = 0;
        while (n < max && pop(out[n])) {
            ++n;
        }
        return n;
    }

private:
    struct Cell {
        std::atomic<size_t> seq{0};
        typename std::aligned_storage<sizeof(T), alignof(T)>::type storage;
    };

    static size_t round_pow2(size_t v) {
        size_t n = 1;
        while (n < v) n <<= 1;
        return n;
    }

    template <typename U>
    bool emplace(U&& value) {
        size_t pos = enq_pos_.load(std::memory_order_relaxed);
        for (;;) {
            Cell& cell = buffer_[pos & mask_];
            const size_t seq = cell.seq.load(std::memory_order_acquire);
            const intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
            if (diff == 0) {
                if (enq_pos_.compare_exchange_weak(
                        pos, pos + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    new (&cell.storage) T(std::forward<U>(value));
                    cell.seq.store(pos + 1, std::memory_order_release);
                    return true;
                }
            } else if (diff < 0) {
                return false;
            } else {
                pos = enq_pos_.load(std::memory_order_relaxed);
            }
        }
    }

    bool pop_discard() {
        Cell& cell = buffer_[deq_pos_ & mask_];
        const size_t seq = cell.seq.load(std::memory_order_acquire);
        const intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(deq_pos_ + 1);
        if (diff != 0) return false;
        T* ptr = reinterpret_cast<T*>(&cell.storage);
        ptr->~T();
        cell.seq.store(deq_pos_ + cap_, std::memory_order_release);
        ++deq_pos_;
        return true;
    }

    const size_t cap_;
    const size_t mask_;
    std::vector<Cell> buffer_;

    alignas(64) std::atomic<size_t> enq_pos_{0};
    char _pad_enq_[64 - sizeof(std::atomic<size_t>)];

    alignas(64) size_t deq_pos_ = 0;
    char _pad_deq_[64 - sizeof(size_t)];
};

}

enum class QueueMode {
    SPSC,
    MPSC
};

/**
 * @brief 统一无锁队列封装（queue facade）。
 *
 * @details
 * - 根据 `QueueMode` 选择底层实现（backend selection），实例生命周期内模式固定。
 * - `push/pop/pop_bulk` 均为非阻塞接口。
 * - 背压语义（backpressure）：`push=false` 表示元素未入队，上层可统计 drop。
 * @warning `QueueMode::SPSC` 继承单 producer/单 consumer 约束。
 */
template <typename T>
class XtLockFreeQueue {
public:

    explicit XtLockFreeQueue(QueueMode mode = QueueMode::MPSC, size_t capacity = 1024,
                             bool use_huge_pages = false) {
        mode_ = mode;
        const size_t cap = capacity > 0 ? capacity : 1024;
        if (mode_ == QueueMode::SPSC) {
            spsc_ = std::make_unique<xt_queue_detail::SpscQueue<T>>(cap, use_huge_pages);
        } else {
            mpsc_ = std::make_unique<xt_queue_detail::MpscRingQueue<T>>(cap, use_huge_pages);
        }
    }

    bool push(const T& v) {
        if (mode_ == QueueMode::SPSC) {
            return spsc_->push(v);
        }
        return mpsc_->push(v);
    }

    bool push(T&& v) {
        if (mode_ == QueueMode::SPSC) {
            return spsc_->push(std::move(v));
        }
        return mpsc_->push(std::move(v));
    }

    bool pop(T& out) {
        if (mode_ == QueueMode::SPSC) {
            return spsc_->pop(out);
        }
        return mpsc_->pop(out);
    }

    size_t pop_bulk(T* out, size_t max) {
        if (!out || max == 0) return 0;
        if (mode_ == QueueMode::SPSC) {
            size_t n = 0;
            while (n < max && spsc_->pop(out[n])) {
                ++n;
            }
            return n;
        }
        return mpsc_ ? mpsc_->pop_bulk(out, max) : 0;
    }

private:
    QueueMode mode_ = QueueMode::MPSC;

    std::unique_ptr<xt_queue_detail::SpscQueue<T>> spsc_;
    std::unique_ptr<xt_queue_detail::MpscRingQueue<T>> mpsc_;
};
