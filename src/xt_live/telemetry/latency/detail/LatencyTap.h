#pragma once

#include <cstdint>
#include <type_traits>

/// @file
/// @brief 延迟采样热路径的 POD tap event（producer→assembler 单向传递）。
///
/// tap 不携带任何 string（strategy/code/trace 走 SubmitMeta sidecar 单独通道），
/// trivially_copyable + 定宽 ≤64B，直接 memcpy 进 SPSC ring。全部时间量纲 µs
/// （steady clock，与 V1 同源）。`seq` 是全局单调序号，assembler 按其排序还原
/// producer 程序序（单 producer 无空洞；多 producer 近似 real-time 到达序，与 V1
/// mutex 到达序同口径）。snapshot drain barrier 不按 seq 计数，改用 per-ring published
/// 游标前缀（ring_pushed_/ring_consumed_）判定消费是否越过捕获点。

namespace xt_live { namespace latency_detail {

enum class TapKind : uint8_t {
    Submit = 0,
    OrderUpdate,
    Trade,
    RawAck,
    RawUds,
    RawAckUnavailable,
};

enum class TriggerKind : uint8_t {
    Unknown = 0,
    HftTick,
    HftTimer,
    HftOffthread,
    CtaTick,
    CtaBar,
    SelSchedule,
    Manual,
    Panel,
    Recovery,
    ForceExit,
};

namespace tap_flags {
    constexpr uint16_t kHasMeta       = 1u << 0;
    constexpr uint16_t kSubmitFailed  = 1u << 1;
    constexpr uint16_t kIsSynthetic   = 1u << 2;
    constexpr uint16_t kCbPresent     = 1u << 3;
    constexpr uint16_t kHadFill       = 1u << 4;
    constexpr uint16_t kHasSubmitInfo = 1u << 5;
}

/// @brief 单条延迟 tap（POD，跨 producer→assembler 边界）。
struct LatencyTap {
    uint64_t seq         = 0;
    uint64_t order_id    = 0;
    uint64_t ts_us       = 0;
    uint64_t aux_us      = 0;
    uint64_t trigger_us  = 0;
    uint64_t aux2_us     = 0;
    uint8_t  kind        = 0;
    uint8_t  trigger_kind = 0;
    uint16_t flags       = 0;
    uint8_t  status      = 0;
    uint8_t  _pad[3]     = {0, 0, 0};
};

static_assert(std::is_trivially_copyable<LatencyTap>::value,
              "LatencyTap must be trivially copyable for lock-free ring transport");
static_assert(sizeof(LatencyTap) <= 64,
              "LatencyTap must fit in 64 bytes (cache-line friendly, no strings)");

constexpr uint64_t kSyntheticTokenBit = (1ULL << 63);

}}
