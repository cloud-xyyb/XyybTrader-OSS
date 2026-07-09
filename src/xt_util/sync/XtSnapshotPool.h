#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>

namespace xt_util {
namespace sync {

/// @brief lock-free bitset 管理的固定容量对象池。
///
/// T 应可默认构造且适合被调用方反复覆盖写入。
/// acquire() 的调用方必须完整填充其业务可见字段
/// release() 前必须确认没有 reader 仍持有该 slot
template <typename T, std::size_t N>
class XtSnapshotPool {
public:
    static_assert(N > 0, "XtSnapshotPool requires a non-empty pool");

    XtSnapshotPool() noexcept {
        for (auto& word : free_mask_) {
            word.store(0, std::memory_order_relaxed);
        }
        for (std::size_t i = 0; i < N; ++i) {
            const std::size_t word = i / 64;
            const uint64_t bit = uint64_t{1} << (i % 64);
            free_mask_[word].fetch_or(bit, std::memory_order_relaxed);
        }
    }

    T* acquire() noexcept {
        for (std::size_t word = 0; word < kWordCount; ++word) {
            uint64_t mask = free_mask_[word].load(std::memory_order_acquire);
            while (mask != 0) {
                const unsigned bit_index = first_set_bit_(mask);
                const uint64_t bit = uint64_t{1} << bit_index;
                const uint64_t desired = mask & ~bit;
                if (free_mask_[word].compare_exchange_weak(
                        mask, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
                    const std::size_t index = word * 64 + bit_index;
                    if (index < N) {
                        return &ring_[index];
                    }

                    release_bit_(word, bit);
                    break;
                }
            }
        }
        return nullptr;
    }

    void release(T* ptr) noexcept {
        if (!ptr) {
            return;
        }
        assert(ptr >= ring_.data() && ptr < ring_.data() + N);
        const std::size_t index = static_cast<std::size_t>(ptr - ring_.data());
        const std::size_t word = index / 64;
        const uint64_t bit = uint64_t{1} << (index % 64);
        const uint64_t old = free_mask_[word].fetch_or(bit, std::memory_order_release);
        assert((old & bit) == 0 && "XtSnapshotPool double release");
    }

    std::size_t capacity() const noexcept {
        return N;
    }

private:
    static constexpr std::size_t kWordCount = (N + 63) / 64;

    static unsigned first_set_bit_(uint64_t mask) noexcept {
        for (unsigned i = 0; i < 64; ++i) {
            if (mask & (uint64_t{1} << i)) {
                return i;
            }
        }
        return 0;
    }

    void release_bit_(std::size_t word, uint64_t bit) noexcept {
        free_mask_[word].fetch_or(bit, std::memory_order_release);
    }

    std::array<T, N> ring_{};
    std::array<std::atomic<uint64_t>, kWordCount> free_mask_{};
};

}
}
