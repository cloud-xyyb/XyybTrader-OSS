#pragma once

/// @file
/// @brief 单写多读共享内存广播 ring（seqlock + 逻辑序号编码）。
///
/// 一块调用方提供的 mmap 段上承载一个广播 ring：单 writer publish，N 个 reader
/// 各自持有 cursor 零拷贝读。writer 永不被慢 reader 反压；ring 满时覆盖最旧
/// slot，慢 reader 被 lap 后跳到仍安全的位置并累加 gap。
///
/// 撕裂保护：per-slot seq 奇=写入中/偶=稳定，writer/reader 两侧 atomic_signal_fence
/// （x86 编译器栅栏；x86 硬件不重排 load-load/store-store，故充分）。
/// 逻辑位置编码：seq = ((logical_index+1)<<1)（稳定）/ |1（写入中）/ 0（空）——
/// 纯 odd/even seqlock 在 lap 下会把 cursor+cap 的新数据当成 cursor，编码 index 修正之。
/// ring 不拥有内存：view over 调用方 mmap 的段（owner 调 init_ctrl 初始化）。

#include <atomic>
#include <cstdint>
#include <type_traits>

#include "xt_ipc/XtMdWireContract.h"

namespace xt_ipc {

// seqlock 的 plain payload 访问在 C++ 抽象机下形式上是 data race；x86 +
// atomic_signal_fence 下由 seq 协议保护，但 TSan 仍会报告。no_sanitize 只包
// payload copy，让 TSan 继续检查 seq/head 等真实同步状态。
#if defined(__SANITIZE_THREAD__)
#define XT_IPC_NO_TSAN __attribute__((no_sanitize("thread")))
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define XT_IPC_NO_TSAN __attribute__((no_sanitize("thread")))
#else
#define XT_IPC_NO_TSAN
#endif
#else
#define XT_IPC_NO_TSAN
#endif

template <class T>
XT_IPC_NO_TSAN inline void seqlock_copy_payload(T& dst, const T& src) noexcept {
    dst = src;  // seqlock 窗口内的 plain copy（撕裂由 seq 奇偶 + signal_fence 保护）
}

/// @brief ring slot。seq 与 payload 各独占 cache line（避免 payload 写 invalidate seq 读行）。
template <class T>
struct MdRingSlot {
    alignas(64) std::atomic<uint64_t> seq;  ///< 0=空；((idx+1)<<1)=稳定持有 logical idx；|1=写入中
    alignas(64) T payload;
};

/// @brief 单写多读共享内存广播 ring（view，不持有内存）。
template <class T>
class XtShmRing {
    static_assert(std::is_trivially_copyable_v<T> && std::is_standard_layout_v<T>,
                  "ring 元素必须 trivially_copyable + standard_layout（跨进程 memcpy + offset 一致）");

public:
    using Slot = MdRingSlot<T>;
    static constexpr uint64_t kSlotStride = sizeof(Slot);
    /// @brief wire 元素字节数（== init_ctrl 写入 element_size，validate_ring 的 typed 期望值）。
    static constexpr uint32_t kElementSize = static_cast<uint32_t>(sizeof(T));

    /// @brief attach 到已 mmap 的段（reader/writer 共用；校验另走 validate_ring）。
    explicit XtShmRing(void* base) noexcept
        : ctrl_(reinterpret_cast<RingControlBlock*>(base)),
          slots_(reinterpret_cast<Slot*>(static_cast<char*>(base) + sizeof(RingControlBlock))),
          mask_(ctrl_->capacity - 1) {}

    uint64_t capacity() const noexcept { return ctrl_->capacity; }
    uint64_t head() const noexcept { return ctrl_->head.load(std::memory_order_acquire); }
    const RingControlBlock* ctrl() const noexcept { return ctrl_; }

    /// @brief owner 在 owner-create（零初始化）的段上写控制块 + 清 slot。
    /// @pre capacity 是 2 的幂；段已 ftruncate 到 segment_bytes(capacity, kSlotStride)。
    static void init_ctrl(void* base, uint64_t capacity) noexcept {
        auto* c = reinterpret_cast<RingControlBlock*>(base);
        c->magic = kMdRingMagic;
        c->wire_version = XT_MD_WIRE_VERSION;
        c->element_size = static_cast<uint32_t>(sizeof(T));
        c->capacity = capacity;
        c->slot_stride = kSlotStride;
        c->head.store(0, std::memory_order_relaxed);
        auto* s = reinterpret_cast<Slot*>(static_cast<char*>(base) + sizeof(RingControlBlock));
        for (uint64_t i = 0; i < capacity; ++i) s[i].seq.store(0, std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_release);  // slot 清零对 reader 可见
    }

    /// @brief 单写者 publish（不阻塞，满则覆盖最旧）。
    /// @pre 同一 ring 同时只能有一个 writer 线程/进程调用。
    void publish(const T& v) noexcept {
        const uint64_t h = ctrl_->head.load(std::memory_order_relaxed);
        Slot& s = slots_[h & mask_];
        const uint64_t token = (h + 1) << 1;                       // 稳定 token = 持有 logical h
        s.seq.store(token | 1, std::memory_order_release);         // 写入中（奇）
        std::atomic_signal_fence(std::memory_order_acq_rel);
        seqlock_copy_payload(s.payload, v);                        // plain copy（seqlock 保护）
        std::atomic_signal_fence(std::memory_order_acq_rel);
        s.seq.store(token, std::memory_order_release);             // 完成（偶）
        ctrl_->head.store(h + 1, std::memory_order_release);       // 发布
    }

    /// @brief reader 游标。每个 reader 一个，互不影响，绝不反压 writer。
    class Reader {
    public:
        explicit Reader(const XtShmRing* ring) noexcept
            : ring_(ring), cursor_(ring->head()) {}  // attach 时从当前 head 起，不回放存量

        uint64_t cursor() const noexcept { return cursor_; }
        uint64_t gap_count() const noexcept { return gap_count_; }

        /// @brief 读下一条逻辑 tick。
        /// @return true=拿到一条；false=暂无新数据或本次竞争自旋耗尽。
        bool next(T& out) noexcept {
            const uint64_t cap = ring_->capacity();
            for (int spin = 0; spin < kMaxSpins; ++spin) {
                const uint64_t h = ring_->ctrl_->head.load(std::memory_order_acquire);
                if (cursor_ >= h) return false;                    // 无新数据
                if (h - cursor_ >= cap) {                          // 落后被 lap，cursor 不再安全
                    const uint64_t nc = h - cap + 1;               // 最旧仍安全可读（留 1 格余量避正写 slot）
                    gap_count_ += (nc - cursor_);
                    cursor_ = nc;
                }
                const Slot& s = ring_->slots_[cursor_ & ring_->mask_];
                const uint64_t s1 = s.seq.load(std::memory_order_acquire);
                if (s1 & 1) continue;                              // 写入中 → 重试
                if (s1 != ((cursor_ + 1) << 1)) continue;          // slot 不持有 cursor_（被覆盖/竞态）→ 顶部重判
                seqlock_copy_payload(out, s.payload);              // plain copy（seqlock 保护）
                std::atomic_signal_fence(std::memory_order_acq_rel);
                const uint64_t s2 = s.seq.load(std::memory_order_acquire);
                if (s2 != s1) continue;                            // 读中途被写 → 重试
                ++cursor_;
                return true;
            }
            return false;  // 自旋上限：writer 持续覆盖该位，本次放弃（gap 已计），下次再来
        }

    private:
        static constexpr int kMaxSpins = 1024;
        const XtShmRing* ring_;
        uint64_t cursor_;
        uint64_t gap_count_ = 0;
    };

    Reader make_reader() const noexcept { return Reader(this); }

private:
    RingControlBlock* ctrl_;
    Slot* slots_;
    uint64_t mask_;
};

}  // namespace xt_ipc
