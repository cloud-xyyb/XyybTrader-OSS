#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <type_traits>
#include <vector>

namespace xt_util {
namespace sync {

/// @brief hazard pointer 管理器，用来保护“读者正在看的旧 snapshot 指针”
class HazardDomain {
public:
    static constexpr std::size_t kMaxHazardsPerThread = 16;

    /// @brief type-erased 回收函数。
    /// @param ctx 调用方传入的回收上下文，常见为 SnapshotPool 指针。
    /// @param ptr retire 时提交的对象地址；调用时已确认不在任何 hazard slot 中。
    using ReclaimFn = void (*)(void* ctx, const void* ptr) noexcept;

private:
    /// 单条待回收记录；reclaim 可为 delete 或 pool release。
    struct RetiredRecord {
        const void* ptr = nullptr;
        void* ctx = nullptr;
        ReclaimFn reclaim = nullptr;
    };

    struct ThreadRecord {
        explicit ThreadRecord(const HazardDomain* owner_domain) noexcept : domain(owner_domain) {
            for (auto& hazard : hazards) {
                hazard.store(nullptr, std::memory_order_relaxed);
            }
            slot_in_use.fill(false);
        }

        const HazardDomain* domain = nullptr;
        std::thread::id owner_thread{};
        std::array<std::atomic<const void*>, kMaxHazardsPerThread> hazards;
        std::array<bool, kMaxHazardsPerThread> slot_in_use;
        std::vector<RetiredRecord> retired;
    };

public:
    HazardDomain();
    ~HazardDomain();

    HazardDomain(const HazardDomain&) = delete;
    HazardDomain& operator=(const HazardDomain&) = delete;

    template <typename T>
    class Guard {
    public:
        using Element = typename std::remove_const<T>::type;

        Guard() noexcept = default;

        ~Guard() noexcept {
            reset();
        }

        Guard(Guard&& other) noexcept {
            move_from(other);
        }

        Guard& operator=(Guard&& other) noexcept {
            if (this != &other) {
                reset();
                move_from(other);
            }
            return *this;
        }

        Guard(const Guard&) = delete;
        Guard& operator=(const Guard&) = delete;

        const Element* get() const noexcept {
            return protected_;
        }

        const Element* operator->() const noexcept {
            return protected_;
        }

        const Element& operator*() const noexcept {
            return *protected_;
        }

        explicit operator bool() const noexcept {
            return protected_ != nullptr;
        }

        /// @brief 清空 hazard slot，并释放本线程内的 slot 占用。
        void reset() noexcept {
            if (record_) {
                record_->hazards[slot_index_].store(nullptr, std::memory_order_release);
                record_->slot_in_use[slot_index_] = false;
                record_ = nullptr;
                protected_ = nullptr;
                slot_index_ = 0;
            }
        }

    private:
        friend class HazardDomain;

        Guard(ThreadRecord* record, std::size_t slot_index, const Element* protected_ptr) noexcept
            : record_(record), slot_index_(slot_index), protected_(protected_ptr) {}

        void move_from(Guard& other) noexcept {
            record_ = other.record_;
            slot_index_ = other.slot_index_;
            protected_ = other.protected_;
            other.record_ = nullptr;
            other.slot_index_ = 0;
            other.protected_ = nullptr;
        }

        ThreadRecord* record_ = nullptr;
        std::size_t slot_index_ = 0;
        const Element* protected_ = nullptr;
    };

    /// @brief 保护一次 atomic snapshot 指针读取。
    ///
    /// @return 非空 Guard 表示读取到稳定指针；src 为 nullptr 时返回空 Guard 且不占用 hazard slot。
    /// @note 同一线程在同一 domain 中同时持有的 Guard 数不能超过 kMaxHazardsPerThread。
    template <typename T>
    Guard<const T> protect(const std::atomic<const T*>& src) const noexcept {
        auto* record = local_record_();
        const std::size_t slot_index = acquire_slot_(*record);

        const T* protected_ptr = nullptr;
        for (;;) {
            protected_ptr = src.load(std::memory_order_acquire);
            record->hazards[slot_index].store(protected_ptr, std::memory_order_release);
            std::atomic_thread_fence(std::memory_order_seq_cst);
            if (src.load(std::memory_order_acquire) == protected_ptr) {
                break;
            }
        }

        if (!protected_ptr) {
            record->hazards[slot_index].store(nullptr, std::memory_order_release);
            record->slot_in_use[slot_index] = false;
            return Guard<const T>();
        }
        return Guard<const T>(record, slot_index, protected_ptr);
    }

    /// @brief 延迟 delete 一个普通堆对象；ptr 为 nullptr 时 no-op。
    template <typename T>
    void retire(const T* ptr) noexcept {
        if (!ptr) {
            return;
        }
        retire_raw(ptr, nullptr, &delete_reclaim_<T>);
    }

    /// @brief 延迟执行自定义回收函数。
    ///
    /// 常用于把 snapshot slot 归还给 XtSnapshotPool。reclaim 必须 noexcept，且不能依赖被回收对象仍可被读者访问。
    void retire_raw(const void* ptr, void* ctx, ReclaimFn reclaim) noexcept;

    /// @brief 扫描所有线程 hazard slot，并回收当前线程 retire list 中未受保护的对象。
    void try_scan() noexcept;

    /// @brief 当前线程 retire list 达到阈值时才触发 try_scan()。
    void try_scan_if_due() noexcept;

    /// @brief 测试/诊断入口：当前线程 retire list 的待回收数量。
    std::size_t retired_count_for_current_thread() const noexcept;

    /// @brief 测试/诊断入口：已懒注册的线程记录数量。
    std::size_t thread_record_count() const noexcept;

private:
    template <typename T>
    static void delete_reclaim_(void*, const void* ptr) noexcept {
        delete static_cast<const T*>(ptr);
    }

    ThreadRecord* local_record_() const noexcept;
    static std::size_t acquire_slot_(ThreadRecord& record) noexcept;
    std::size_t scan_threshold_() const noexcept;

    /// @pre 调用方已经停止所有使用该 domain 的读线程，并释放所有 Guard。
    void force_reclaim_all_() noexcept;

    mutable std::atomic<bool> shutting_down_{false};
    mutable std::vector<ThreadRecord*> records_;
    mutable std::atomic_flag records_lock_ = ATOMIC_FLAG_INIT;
};

template <typename T>
using HazardGuard = HazardDomain::Guard<T>;

}  // namespace sync
}  // namespace xt_util
