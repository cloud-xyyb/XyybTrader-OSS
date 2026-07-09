#pragma once

/// @file
/// @brief MD fan-out 共享内存 wire 契约（常量、layout 与 attach 校验）。
///
/// writer 进程与 reader 进程共同 include 本头，以同一份常量、layout 和
/// 校验逻辑约束共享内存 ring ABI。只支持同机、同架构 POSIX shm；
/// 不提供 endianness、字长或跨主机 wire 兼容。
///
/// Wire element = XtRawTick 原样（pack(8)，sizeof==512），不 repack；
/// 字段顺序、语义或 sizeof 变化必须 bump XT_MD_WIRE_VERSION。

#include <atomic>
#include <cstdint>
#include <type_traits>

#include "xt_core/market/XtRawTick.h"

namespace xt_ipc {

inline constexpr uint64_t kMdRingMagic = 0x58544D44524E4731ULL;

inline constexpr uint32_t XT_MD_WIRE_VERSION = 1;

static_assert(std::is_trivially_copyable_v<XtRawTick>,
              "wire element 必须 trivially_copyable（memcpy 跨进程）");
static_assert(std::is_standard_layout_v<XtRawTick>,
              "wire element 必须 standard_layout（两进程 offset 一致）");
static_assert(sizeof(XtRawTick) == 512,
              "XtRawTick wire size 钉死 512；任何变更必须 bump XT_MD_WIRE_VERSION");

static_assert(sizeof(void*) == 8, "xt_ipc shm ring targets 64-bit x86");

/// @brief ring 控制块 —— 位于 shm 段 offset 0。固定宽度类型，跨进程 layout 稳定。
///
/// head 单独占一条 cache line（alignas(64)），与元数据隔离避免 writer 推 head
/// 时反复 invalidate reader 读元数据的行。
struct RingControlBlock {
    uint64_t magic;
    uint32_t wire_version;
    uint32_t element_size;
    uint64_t capacity;
    uint64_t slot_stride;
    char _pad0[24];
    alignas(64) std::atomic<uint64_t> head;
    char _pad1[56];
};
static_assert(sizeof(RingControlBlock) == 128,
              "RingControlBlock layout 钉死；变更必须 bump XT_MD_WIRE_VERSION");
static_assert(std::atomic<uint64_t>::is_always_lock_free, "head 必须 lock-free");

inline constexpr uint64_t segment_bytes(uint64_t capacity, uint64_t slot_stride) {
    return sizeof(RingControlBlock) + capacity * slot_stride;
}

inline const char* validate_ring(const RingControlBlock* ctrl,
                                 uint64_t mapped_bytes,
                                 uint64_t slot_stride) noexcept {

    if (mapped_bytes < sizeof(RingControlBlock)) return "mapped segment smaller than control block";
    if (ctrl->magic != kMdRingMagic) return "magic mismatch";
    if (ctrl->wire_version != XT_MD_WIRE_VERSION) return "wire_version mismatch";
    if (ctrl->element_size != sizeof(XtRawTick)) return "element_size mismatch";
    if (ctrl->slot_stride != slot_stride) return "slot_stride mismatch";
    const uint64_t cap = ctrl->capacity;
    if (cap == 0 || (cap & (cap - 1)) != 0) return "capacity not power-of-two";
    if (mapped_bytes < segment_bytes(cap, slot_stride)) return "mapped segment too small";
    return nullptr;
}

}
