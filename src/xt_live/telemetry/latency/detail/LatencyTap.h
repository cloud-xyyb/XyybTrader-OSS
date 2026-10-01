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

/// @brief tap 事件类型（对应 tracker 各 producer 入口）。
enum class TapKind : uint8_t {
    Submit = 0,          ///< on_submit_result
    OrderUpdate,         ///< on_order_update
    Trade,               ///< on_trade
    RawAck,              ///< record_raw_trade_ack
    RawUds,              ///< record_raw_uds_arrival
    RawAckUnavailable,   ///< record_raw_trade_ack_unavailable
};

/// @brief 触发源分类（tap 字段；**不入 Prom label**，供 CSV / 后续使用）。
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

/// @brief tap flags 位定义。
namespace tap_flags {
    constexpr uint16_t kHasMeta       = 1u << 0;  ///< SubmitMeta 有此 key 的 strategy/code/trace 条目
    constexpr uint16_t kSubmitFailed  = 1u << 1;  ///< Submit: order_id==0（提交失败，不建 track）
    constexpr uint16_t kIsSynthetic   = 1u << 2;  ///< order_id 是 synthetic token（order_id==0 事件；权威判据，见 H-5）
    constexpr uint16_t kCbPresent     = 1u << 3;  ///< OrderUpdate/Trade: 原始 local_cb_us > 0
    constexpr uint16_t kHadFill       = 1u << 4;  ///< OrderUpdate: parse_had_fill(upd) 为真
    constexpr uint16_t kHasSubmitInfo = 1u << 5;  ///< Trade: aux_us/trigger_us 携带 submit_begin/end（CTP 补位）
}

/// @brief 单条延迟 tap（POD，跨 producer→assembler 边界）。
struct LatencyTap {
    uint64_t seq         = 0;   ///< 全局单调序号（全序 merge 排序键；barrier 走 per-ring/overflow 游标，不用 seq）
    uint64_t order_id    = 0;   ///< 真 order_id 或 synthetic token（bit63 set；权威判据见 flags.kIsSynthetic）
    uint64_t ts_us       = 0;   ///< 语义时间：Submit=submit_begin / Update|Trade=event_ts / Raw=raw ts
    uint64_t aux_us      = 0;   ///< Submit=submit_end / Update|Trade=local_strategy_us / Trade+submit_info=submit_begin
    uint64_t trigger_us  = 0;   ///< Submit=trigger_tick_us / Trade+submit_info=submit_end
    uint64_t aux2_us     = 0;   ///< Trade+submit_info=local_strategy_us（cb_to_strategy 用；F5-b。其它 kind 不用）
    uint8_t  kind        = 0;   ///< TapKind
    uint8_t  trigger_kind = 0;  ///< TriggerKind
    uint16_t flags       = 0;   ///< tap_flags 位或
    uint8_t  status      = 0;   ///< OrderUpdate: XtOrderStatus 原始值
    uint8_t  _pad[3]     = {0, 0, 0};
};

static_assert(std::is_trivially_copyable<LatencyTap>::value,
              "LatencyTap must be trivially copyable for lock-free ring transport");
static_assert(sizeof(LatencyTap) <= 64,
              "LatencyTap must fit in 64 bytes (cache-line friendly, no strings)");

/// @brief synthetic token 高位标记（order_id==0 事件专用 key，进程生命周期不可达 wrap）。
constexpr uint64_t kSyntheticTokenBit = (1ULL << 63);

}}  // namespace xt_live::latency_detail
