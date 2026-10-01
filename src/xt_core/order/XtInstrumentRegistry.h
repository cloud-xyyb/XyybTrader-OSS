#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

#include "xt_core/sync/XtTransparentStringHash.h"

namespace xt_core {

/// 合约整数 ID；0 永远表示无效/未注册。
using XtInstrumentId = uint32_t;

static constexpr XtInstrumentId kInvalidInstrumentId = 0;  ///< 0 保留为无效 ID
static constexpr uint32_t kMaxInstruments = 8192;           ///< 单 registry 可分配 ID 数（1..kMaxInstruments 全可用）
/// intern 探测表槽数：2 的幂且 ≥ 2×kMaxInstruments，保证负载 ≤50%、线性探测链必有空槽终止。
static constexpr size_t kInstrumentSlotCount = 16384;

static_assert(kInstrumentSlotCount >= 2u * kMaxInstruments,
              "intern slot count must keep load factor <= 50% for linear probing");
static_assert((kInstrumentSlotCount & (kInstrumentSlotCount - 1)) == 0,
              "intern slot count must be a power of two (mask-based wrap)");

namespace instrument_registry_drops {

/// @brief intern 表容量耗尽导致 `register_or_get` 返回无效 ID 的次数（critical）。
///
/// 进程级计数，跨所有 registry 实例累计。正常应恒为 0；>0 表示某条 code 拿不到
/// instrument id——BT 侧由 `AccountStateStore` fail-fast throw 兜底，live 侧走
/// quarantine/拒单路径，两者都不允许静默丢弃。
inline std::atomic<uint64_t>& intern_overflow() noexcept {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

}  // namespace instrument_registry_drops

/// @brief 合约代码字符串与整数 ID 的双向注册表（定容 append-only intern 表）。
///
/// 在热路径上到处传 `std::string` 合约代码（比较、hash、拷贝）太贵，本注册表把每个 code
/// intern 成一个稳定的 `XtInstrumentId`（从 1 起分配），此后热路径只传这个整数，需要时再用
/// `code_of()` O(1) 反查回代码。ID 0 保留给无效/未注册。
///
/// 并发契约：
/// - 写（`register_or_get`）由 registry 内部 mutex 串行，**不依赖任何外部串行假设**；
/// - 读（`id_of` / `code_of`）无锁：探测表逐槽 acquire load，id→code 先 acquire `next_id_`
///   取上界再索引定长数组；
/// - entry 一经分配永不释放/搬迁（`std::deque` 持有），槽位无删除、无 tombstone；
/// - 读者可能在「写者已构造 entry、尚未发布槽指针」的窗口内读到 miss（该 code 此刻确实
///   尚未发布），不会读到半构造 entry。
///
/// @warning `reset()` 不可与任何并发读写同时发生（bootstrap/测试边界专用）。
class XtInstrumentRegistry {
public:
    XtInstrumentRegistry() = default;
    /// @brief 转移 intern 内容；探测表按新宿主的 entry 地址重建。
    /// @pre 两侧对象都不得有并发读写在途（move 只发生在装配/测试边界）。
    XtInstrumentRegistry(XtInstrumentRegistry&& other) noexcept { adopt_(other); }
    XtInstrumentRegistry& operator=(XtInstrumentRegistry&& other) noexcept {
        if (this != &other) adopt_(other);
        return *this;
    }
    XtInstrumentRegistry(const XtInstrumentRegistry&) = delete;
    XtInstrumentRegistry& operator=(const XtInstrumentRegistry&) = delete;

    /// @brief 注册 code 或返回既有 ID。
    ///
    /// cold path 使用；首次注册在内部 mutex 内追加一条永久 entry。
    /// 空指针/空字符串返回 `kInvalidInstrumentId`；容量耗尽（已分配满
    /// `kMaxInstruments` 个 ID）同样返回无效 ID 并累加
    /// `instrument_registry_drops::intern_overflow()`。
    XtInstrumentId register_or_get(const char* code) {
        if (!code || code[0] == '\0') return kInvalidInstrumentId;
        const std::string_view sv(code);
        // 无锁快路径：已发布的 code 直接命中探测链，重复注册不进锁。
        if (const XtInstrumentId hit = lookup_(sv); hit != kInvalidInstrumentId) return hit;

        std::lock_guard<std::mutex> lk(mu_);
        // 锁内重走探测链：快路径与取锁之间可能有另一写者发布了同一 code。
        // 写者被 mutex 串行 ⇒ 锁内可用 relaxed 读自己的写。
        size_t free_slot = kInstrumentSlotCount;
        const size_t home = slot_home_(sv);
        for (size_t probe = 0; probe < kInstrumentSlotCount; ++probe) {
            const size_t k = (home + probe) & kSlotMask;
            const InternEntry* e = slots_[k].load(std::memory_order_relaxed);
            if (!e) {
                free_slot = k;
                break;
            }
            if (e->code == sv) return e->id;
        }

        const uint32_t id = next_id_.load(std::memory_order_relaxed);
        // 边界：`>` 而非 `>=`——next_id_ 是下一个可分配 ID 且自 1 起，故 ID 1..kMaxInstruments
        // 全可用（第 kMaxInstruments 个注册成功、第 kMaxInstruments+1 个失败）。
        // free_slot 落空在 ≤50% 负载下结构性不可达，与容量耗尽同路处理。
        if (id > kMaxInstruments || free_slot == kInstrumentSlotCount) {
            instrument_registry_drops::intern_overflow().fetch_add(1, std::memory_order_relaxed);
            return kInvalidInstrumentId;
        }

        entries_.push_back(InternEntry{std::string(sv), id});
        // 发布序：先写 id→code 反查表 → release 递增 next_id_ → release 发布槽指针。
        // 读者拿到槽指针即已看到 next_id_ 与反查表内容（release/acquire 传递）。
        write_code_(id, sv);
        next_id_.store(id + 1, std::memory_order_release);
        slots_[free_slot].store(&entries_.back(), std::memory_order_release);
        return id;
    }

    /// @brief 查找已注册 code 的 ID；不修改 registry。
    ///
    /// 无锁；未注册、空指针或空字符串返回 `kInvalidInstrumentId`。
    XtInstrumentId id_of(const char* code) const noexcept {
        if (!code || code[0] == '\0') return kInvalidInstrumentId;
        return lookup_(std::string_view(code));
    }

    /// @brief O(1) 数组反查 ID 对应的 code。
    ///
    /// 无效 ID 返回空字符串。返回指针归 registry 持有，生命周期到下一次 `reset()` 或对象析构。
    const char* code_of(XtInstrumentId id) const noexcept {
        // 先 acquire 取 next_id_ 作上界：它的 release 写发生在 id_to_code_ 写之后，
        // 故通过上界的 id 其反查内容已可见。
        if (id == kInvalidInstrumentId || id >= next_id_.load(std::memory_order_acquire)) return "";
        return id_to_code_[id];
    }

    /// @brief 清空注册表并释放 entry 持有的字符串。
    ///
    /// 该操作不能与 `register_or_get()`/`id_of()`/`code_of()` 并发；调用方应只在
    /// bootstrap/test 重置边界使用。
    void reset() {
        const uint32_t used = next_id_.load(std::memory_order_relaxed);
        for (auto& slot : slots_) slot.store(nullptr, std::memory_order_relaxed);
        // 保留数组容量，只清已分配 ID 的内容。
        for (uint32_t i = 1; i < used; ++i) id_to_code_[i][0] = '\0';
        entries_.clear();
        next_id_.store(1, std::memory_order_relaxed);
    }

private:
    // code_of() 使用固定数组返回，超过该长度的 code 会在反查侧截断；entry 保留完整字符串。
    static constexpr size_t kMaxCodeLen = 32;
    static constexpr size_t kSlotMask = kInstrumentSlotCount - 1;

    /// @brief 一条永久 intern 记录；发布后 immutable，地址永不失效。
    struct InternEntry {
        std::string code;   ///< 完整合约代码（探测链比较的权威值）
        XtInstrumentId id;  ///< 分配到的稳定 ID
    };

    static size_t slot_home_(std::string_view sv) noexcept {
        // 取模前先把高位混进低位：FNV 的低位随最后几个字节线性变化，直接截低 14 位
        // 会让同前缀、尾部相近的合约码（ag2601 / ag2602 / …）落进相邻槽连成长探测链。
        uint64_t h = xt_core::sync::TransparentStringHash{}(sv);
        h ^= h >> 33;
        return static_cast<size_t>(h) & kSlotMask;
    }

    /// @brief 无锁线性探测；空槽即探测链终点。
    XtInstrumentId lookup_(std::string_view sv) const noexcept {
        const size_t home = slot_home_(sv);
        for (size_t probe = 0; probe < kInstrumentSlotCount; ++probe) {
            const InternEntry* e = slots_[(home + probe) & kSlotMask].load(std::memory_order_acquire);
            if (!e) return kInvalidInstrumentId;
            if (e->code == sv) return e->id;
        }
        return kInvalidInstrumentId;
    }

    void write_code_(XtInstrumentId id, std::string_view sv) noexcept {
        const size_t n = sv.size() < kMaxCodeLen - 1 ? sv.size() : kMaxCodeLen - 1;
        std::memcpy(id_to_code_[id], sv.data(), n);
        id_to_code_[id][n] = '\0';
    }

    /// @brief move 的公共实现：搬 entry 后按新地址重建探测表，不依赖 deque move 的地址语义。
    void adopt_(XtInstrumentRegistry& other) noexcept {
        entries_ = std::move(other.entries_);
        id_to_code_ = other.id_to_code_;
        next_id_.store(other.next_id_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        for (auto& slot : slots_) slot.store(nullptr, std::memory_order_relaxed);
        for (const auto& e : entries_) {
            const size_t home = slot_home_(e.code);
            for (size_t probe = 0; probe < kInstrumentSlotCount; ++probe) {
                const size_t k = (home + probe) & kSlotMask;
                if (!slots_[k].load(std::memory_order_relaxed)) {
                    slots_[k].store(&e, std::memory_order_relaxed);
                    break;
                }
            }
        }
        other.reset();
    }

    mutable std::mutex mu_;               ///< 只串行写者；读路径不取。
    std::deque<InternEntry> entries_;     ///< 永久 entry 存储（引用/指针一经发布永不失效）。
    /// open-addressing 探测表；nullptr = 空槽（探测链终点），只由持锁写者填充。
    std::array<std::atomic<const InternEntry*>, kInstrumentSlotCount> slots_{};
    // id → code 的 O(1) 反查表，id 从 1 开始。
    std::array<char[kMaxCodeLen], kMaxInstruments + 1> id_to_code_{};
    std::atomic<uint32_t> next_id_{1};  ///< 下一个可分配 ID；0 保留给 kInvalidInstrumentId。
};

}  // namespace xt_core
