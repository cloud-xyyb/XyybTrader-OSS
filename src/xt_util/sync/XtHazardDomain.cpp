#include "xt_util/sync/XtHazardDomain.h"

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <new>
#include <unordered_set>

namespace xt_util {
namespace sync {
namespace {

// 每个线程按 domain 懒注册一个 ThreadRecord；vector 规模等于线程接触过的 domain 数，通常很小。
struct HazardDomainTlsEntry {
    const HazardDomain* domain = nullptr;
    void* record = nullptr;
};

thread_local std::vector<HazardDomainTlsEntry> tls_hazard_records;

/// @brief 用 atomic_flag 保护 records_ 注册表。
///
/// 该锁不在 reader dereference 路径持有；protect() 只有首次注册 ThreadRecord 时会进入。
class SpinLockGuard {
public:
    explicit SpinLockGuard(std::atomic_flag& flag) noexcept : flag_(flag) {
        while (flag_.test_and_set(std::memory_order_acquire)) {
        }
    }

    ~SpinLockGuard() noexcept {
        flag_.clear(std::memory_order_release);
    }

    SpinLockGuard(const SpinLockGuard&) = delete;
    SpinLockGuard& operator=(const SpinLockGuard&) = delete;

private:
    std::atomic_flag& flag_;
};

// 只移除当前线程 TLS 中指向 domain 的 entry；其他线程的 record 由 force_reclaim_all_ 统一处理。
void remove_current_thread_tls_record(const HazardDomain* domain) noexcept {
    tls_hazard_records.erase(
        std::remove_if(tls_hazard_records.begin(), tls_hazard_records.end(),
                       [domain](const HazardDomainTlsEntry& entry) {
                           return entry.domain == domain;
                       }),
        tls_hazard_records.end());
}

}  // namespace

HazardDomain::HazardDomain() = default;

HazardDomain::~HazardDomain() {
    shutting_down_.store(true, std::memory_order_release);
    remove_current_thread_tls_record(this);
    // 析构要求外部已经让读线程 quiescent；这里不等待其他线程释放 hazard slot。
    force_reclaim_all_();
}

HazardDomain::ThreadRecord* HazardDomain::local_record_() const noexcept {
    for (const auto& entry : tls_hazard_records) {
        if (entry.domain == this) {
            return static_cast<ThreadRecord*>(entry.record);
        }
    }

    auto* record = new (std::nothrow) ThreadRecord(this);
    if (!record) {
        std::abort();
    }
    record->owner_thread = std::this_thread::get_id();
    // reserve 只发生在线程首次进入 domain 时，避免常规 retire push 频繁扩容。
    record->retired.reserve(kMaxHazardsPerThread * 4);

    {
        SpinLockGuard guard(records_lock_);
        records_.push_back(record);
    }

    tls_hazard_records.push_back({this, record});
    return record;
}

std::size_t HazardDomain::acquire_slot_(ThreadRecord& record) noexcept {
    for (std::size_t i = 0; i < kMaxHazardsPerThread; ++i) {
        if (!record.slot_in_use[i]) {
            record.slot_in_use[i] = true;
            record.hazards[i].store(nullptr, std::memory_order_relaxed);
            return i;
        }
    }

    assert(false && "HazardDomain per-thread hazard slots exhausted");
    std::abort();
}

void HazardDomain::retire_raw(const void* ptr, void* ctx, ReclaimFn reclaim) noexcept {
    if (!ptr) {
        return;
    }
    auto* record = local_record_();
    record->retired.push_back(RetiredRecord{ptr, ctx, reclaim});
    if (record->retired.size() >= scan_threshold_()) {
        try_scan();
    }
}

void HazardDomain::try_scan_if_due() noexcept {
    auto* record = local_record_();
    if (record->retired.size() >= scan_threshold_()) {
        try_scan();
    }
}

void HazardDomain::try_scan() noexcept {
    auto* record = local_record_();
    if (record->retired.empty()) {
        return;
    }

    // scan 是 writer/GC 路径：用临时 set 收集所有已发布 hazard，允许发生短期堆分配。
    std::unordered_set<const void*> hazards;

    {
        SpinLockGuard guard(records_lock_);
        // records_.size() 的 reserve 提示也必须在锁内读：local_record_() 注册新线程时
        // 在锁内 push_back(records_)，锁外读 size() 会与之 data race（TSAN 报，且属 UB——
        // 即便仅作 reserve 容量提示）。持 records_lock_ 保护 records_ 的 size 读与迭代；
        // hazard slot 本身通过 acquire load 读取。
        hazards.reserve(records_.size() * kMaxHazardsPerThread);
        for (const auto* thread_record : records_) {
            if (!thread_record) {
                continue;
            }
            for (const auto& slot : thread_record->hazards) {
                const void* protected_ptr = slot.load(std::memory_order_acquire);
                if (protected_ptr) {
                    hazards.insert(protected_ptr);
                }
            }
        }
    }

    auto write = record->retired.begin();
    for (auto read = record->retired.begin(); read != record->retired.end(); ++read) {
        if (hazards.find(read->ptr) != hazards.end()) {
            // 仍被读者保护的对象留在当前线程 retire list，等待后续 scan。
            if (write != read) {
                *write = *read;
            }
            ++write;
            continue;
        }
        if (read->reclaim) {
            read->reclaim(read->ctx, read->ptr);
        }
    }
    record->retired.erase(write, record->retired.end());
}

std::size_t HazardDomain::retired_count_for_current_thread() const noexcept {
    return local_record_()->retired.size();
}

std::size_t HazardDomain::thread_record_count() const noexcept {
    SpinLockGuard guard(records_lock_);
    return records_.size();
}

std::size_t HazardDomain::scan_threshold_() const noexcept {
    const std::size_t records = std::max<std::size_t>(thread_record_count(), 1);
    // 阈值随线程数增长，避免 reader 多时 writer 过于频繁地全域扫描。
    return std::max<std::size_t>(2 * records * kMaxHazardsPerThread, kMaxHazardsPerThread);
}

void HazardDomain::force_reclaim_all_() noexcept {
    std::vector<ThreadRecord*> owned;
    {
        SpinLockGuard guard(records_lock_);
        owned.swap(records_);
    }

    for (auto* record : owned) {
        if (!record) {
            continue;
        }
        // Domain 析构期外部应已停止 reader；清 slot 只是避免诊断残留，不提供同步等待。
        for (auto& hazard : record->hazards) {
            hazard.store(nullptr, std::memory_order_release);
        }
        for (const auto& retired : record->retired) {
            if (retired.reclaim) {
                retired.reclaim(retired.ctx, retired.ptr);
            }
        }
        delete record;
    }
}

}  // namespace sync
}  // namespace xt_util
