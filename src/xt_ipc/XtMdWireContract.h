#pragma once

/// @file
/// @brief MD fan-out 共享内存 wire 契约（常量、layout 与 attach 校验）。
///
/// writer 进程与 reader 进程共同 include 本头，以同一份常量、layout 和
/// 校验逻辑约束共享内存 ring ABI。只支持同机、同架构 POSIX shm；
/// 不提供 endianness、字长或跨主机 wire 兼容。
///
/// Wire element = XtMdWireSlot { capture_ns + XtRawTick }（不 pack，自然对齐）；
/// XtRawTick 原样嵌入（pack(8)，sizeof==832），不 repack。capture_ns 是 connector
/// publish 前 stamp 的 CLOCK_MONOTONIC steady 时刻（同机跨进程可比），供 reader 端
/// 上溯 md_ingress→submit 起点。任何字段顺序、语义或 sizeof 变化必须 bump
/// XT_MD_WIRE_VERSION。

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "xt_core/market/XtRawTick.h"

namespace xt_ipc {

/// @brief ring 段魔数（"XTMDRNG1" 的 ASCII，认段 + 防误 attach 无关共享内存）。
inline constexpr uint64_t kMdRingMagic = 0x58544D44524E4731ULL;

/// @brief wire 协议版本。XtMdWireSlot / XtRawTick / RingControlBlock 任何 layout 变更都 bump。
/// v2：XtRawTick DEPTH 10→20（sizeof 512→832）。
/// v3：wire element 从 XtRawTick 改为 XtMdWireSlot（前置 int64 capture_ns，sizeof 832→840）。
/// writer/reader 必须同批部署，attach 校验（wire_version + element_size）对错配 fail-closed。
inline constexpr uint32_t XT_MD_WIRE_VERSION = 3;

/// @brief MD fan-out ring 的 wire 元素：capture 时戳 + XtRawTick 原样。
///
/// @note **禁 pack(1)**：XtRawTick 已 pack(8)，此处保持自然对齐，让 int64 capture_ns
///       落 offset 0、XtRawTick 落 offset 8（alignof(XtRawTick)==8），跨进程 offset
///       稳定且不破坏 XtRawTick 内部 layout 与 atomic slot 协议。
/// @note capture_ns==0 语义 = 未 stamp（如旧写方或非 SHM 路径）；reader/下游按 0 fallback。
struct XtMdWireSlot {
    int64_t capture_ns = 0;  ///< connector publish 前 stamp 的 CLOCK_MONOTONIC steady ns（0=未 stamp）
    XtRawTick tick;          ///< 行情 tick 原样（pack(8)，sizeof==832）
};

// ── Wire element invariants ────────────────────────────────────────────
static_assert(std::is_trivially_copyable_v<XtRawTick>,
              "wire element 必须 trivially_copyable（memcpy 跨进程）");
static_assert(std::is_standard_layout_v<XtRawTick>,
              "wire element 必须 standard_layout（两进程 offset 一致）");
static_assert(sizeof(XtRawTick) == 832,
              "XtRawTick wire size 钉死 832；任何变更必须 bump XT_MD_WIRE_VERSION");

// XtRawTick 字段级 layout 钉：总大小不变的字段换序/改宽（如 price/open 互换）不改
// sizeof，validate_ring 与 sizeof 钉全过而 reader 读错字段——关键字段 offsetof 锚点
// 让此类漂移在编译期爆炸（漂移必须显式 bump XT_MD_WIRE_VERSION 并同批部署）。
static_assert(offsetof(XtRawTick, code) == 10, "XtRawTick.code offset 漂移");
static_assert(offsetof(XtRawTick, price) == 48, "XtRawTick.price offset 漂移");
static_assert(offsetof(XtRawTick, open) == 56, "XtRawTick.open offset 漂移");
static_assert(offsetof(XtRawTick, total_volume) == 104, "XtRawTick.total_volume offset 漂移");
static_assert(offsetof(XtRawTick, trading_date) == 152, "XtRawTick.trading_date offset 漂移");
static_assert(offsetof(XtRawTick, action_time) == 160, "XtRawTick.action_time offset 漂移");
static_assert(offsetof(XtRawTick, bid_prices) == 192, "XtRawTick.bid_prices offset 漂移");
static_assert(offsetof(XtRawTick, ask_prices) == 352, "XtRawTick.ask_prices offset 漂移");
static_assert(offsetof(XtRawTick, bid_qty) == 512, "XtRawTick.bid_qty offset 漂移");
static_assert(offsetof(XtRawTick, ask_qty) == 672, "XtRawTick.ask_qty offset 漂移");

static_assert(std::is_trivially_copyable_v<XtMdWireSlot>,
              "wire slot 必须 trivially_copyable（memcpy 跨进程）");
static_assert(std::is_standard_layout_v<XtMdWireSlot>,
              "wire slot 必须 standard_layout（两进程 offset 一致）");
static_assert(sizeof(XtMdWireSlot) == 840,
              "XtMdWireSlot wire size 钉死 840；任何变更必须 bump XT_MD_WIRE_VERSION");
static_assert(offsetof(XtMdWireSlot, tick) == 8,
              "XtRawTick 必须落 offset 8（capture_ns 之后、自然对齐，非 pack(1)）");
static_assert(alignof(XtMdWireSlot) == 8,
              "wire slot alignof 钉死 8（禁 pack(1)，随 XtRawTick 8-byte 对齐）");

// x86-64 内存模型前提：seqlock 用 atomic_signal_fence（编译器栅栏），依赖
// x86 硬件不重排 load-load / store-store。其他架构需要重新审查 fence 语义。
static_assert(sizeof(void*) == 8, "xt_ipc shm ring targets 64-bit x86");

/// @brief ring 控制块 —— 位于 shm 段 offset 0。固定宽度类型，跨进程 layout 稳定。
///
/// head 单独占一条 cache line（alignas(64)），与元数据隔离避免 writer 推 head
/// 时反复 invalidate reader 读元数据的行。
struct RingControlBlock {
    uint64_t magic;          ///< == kMdRingMagic
    uint32_t wire_version;   ///< == XT_MD_WIRE_VERSION
    uint32_t element_size;   ///< == sizeof(wire 元素 T)（MD ring 为 XtMdWireSlot；typed validator 据此校验）
    uint64_t capacity;       ///< slot 数（2 的幂）
    uint64_t slot_stride;    ///< 单 slot 字节数（writer 写、reader 校验，防 ABI 漂移）
    char _pad0[24];          ///< 凑满到 64
    alignas(64) std::atomic<uint64_t> head;  ///< 单调递增发布序号（逻辑 index）
    char _pad1[56];
};
static_assert(sizeof(RingControlBlock) == 128,
              "RingControlBlock layout 钉死；变更必须 bump XT_MD_WIRE_VERSION");
static_assert(std::atomic<uint64_t>::is_always_lock_free, "head 必须 lock-free");

/// @brief 段总字节 = 控制块 + capacity × slot_stride。两边用同一公式算段大小。
inline constexpr uint64_t segment_bytes(uint64_t capacity, uint64_t slot_stride) {
    return sizeof(RingControlBlock) + capacity * slot_stride;
}

/// @brief reader attach 时的 fail-closed 校验（typed：expected_element_size 由调用方按
///        自身编译出的 wire 元素类型传入，不硬编码 sizeof(XtRawTick)）。
/// @param ctrl                 段头（已 mmap）
/// @param mapped_bytes         实际 mmap 的段字节（防物理段比声明小 → 越界 SIGBUS）
/// @param slot_stride          reader 侧编译出的 slot stride（须与 writer 写入的一致）
/// @param expected_element_size reader 侧 wire 元素 sizeof（如 sizeof(XtMdWireSlot)；由
///        XtShmRing<T>::kElementSize 提供）。**不得**硬编码 sizeof(XtRawTick)——否则 v3
///        wrapper（element_size==840）会被误判 mismatch，全 PF fail-closed 起不来。
/// @return nullptr = ok；否则返回错误原因字符串（reader 应拒绝读并非零退出）。
inline const char* validate_ring(const RingControlBlock* ctrl,
                                 uint64_t mapped_bytes,
                                 uint64_t slot_stride,
                                 uint32_t expected_element_size) noexcept {
    // 先验段足够大再解引用控制块（否则截断/部分初始化段读 ctrl 字段会越过文件末页 → SIGBUS）。
    if (mapped_bytes < sizeof(RingControlBlock)) return "mapped segment smaller than control block";
    if (ctrl->magic != kMdRingMagic) return "magic mismatch";
    if (ctrl->wire_version != XT_MD_WIRE_VERSION) return "wire_version mismatch";
    if (ctrl->element_size != expected_element_size) return "element_size mismatch";
    if (ctrl->slot_stride != slot_stride) return "slot_stride mismatch";
    const uint64_t cap = ctrl->capacity;
    if (cap == 0 || (cap & (cap - 1)) != 0) return "capacity not power-of-two";
    if (mapped_bytes < segment_bytes(cap, slot_stride)) return "mapped segment too small";
    return nullptr;
}

}  // namespace xt_ipc
