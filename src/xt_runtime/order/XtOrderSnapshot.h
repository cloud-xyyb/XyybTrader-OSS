#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "xt_core/order/XtInstrumentRegistry.h"
#include "xt_core/order/XtOffset.h"

/// @file
/// @brief `XtOrderRuntimeBook` 发布给跨线程 reader 的 immutable order snapshot。
///
/// 本文件定义固定容量 POD snapshot 和 code helper

namespace xt_runtime {
namespace order {

using xt_core::XtInstrumentId;
using xt_core::kInvalidInstrumentId;

inline constexpr std::size_t kOrderSnapshotMaxOrders = 512;
inline constexpr std::size_t kOrderSnapshotMaxCodes = 512;
inline constexpr std::size_t kOrderSnapshotMaxCloseOutstanding = 512;
inline constexpr std::size_t kOrderSnapshotCodeLen = 64;

/// @brief 单笔在途订单的 immutable snapshot 条目。
///
/// 由 XtOrderRuntimeBook 单写者发布，跨线程 reader 只能在 HazardGuard 生命周期内读取。
/// `id=0` 表示空槽；所有 qty/price 字段使用 order fixed-point scale。
struct OrderSnapshot {
    uint64_t id = 0;
    XtInstrumentId instrument_id = kInvalidInstrumentId;
    char code[kOrderSnapshotCodeLen] = {};
    bool is_buy = true;
    bool is_close = false;
    XtOffset close_offset = XtOffset::Close;
    int64_t price_fp = 0;
    int64_t leaves_qty_fp = 0;
    int64_t locked_today_fp = 0;
    int64_t locked_yesterday_fp = 0;
    int64_t filled_today_fp = 0;
    int64_t filled_yesterday_fp = 0;
    uint64_t seq = 0;
    uint32_t sid_id = 0;
};

/// @brief 已跟踪合约或 force-flat pending 合约的 snapshot 条目。
///
/// `tracked_codes` 只使用 instrument/code；`force_flat_pending_codes` 额外携带
/// force-flat 发起方和 genuine 标志，供 panel/closeout 区分宽窄口径。
struct OrderTrackedCodeSnapshot {
    XtInstrumentId instrument_id = kInvalidInstrumentId;
    char code[kOrderSnapshotCodeLen] = {};

    uint32_t force_flat_sid_id = 0;

    bool is_genuine_force_flat = false;
};

/// @brief 平仓在途冻结聚合 snapshot。
///
/// `is_buy` 继承订单方向语义：true 表示买入平空，false 表示卖出平多。
struct CloseOutstandingSnapshot {
    XtInstrumentId instrument_id = kInvalidInstrumentId;
    char code[kOrderSnapshotCodeLen] = {};
    bool is_buy = true;
    int64_t today_fp = 0;
    int64_t yesterday_fp = 0;
};

/// @brief XtOrderRuntimeBook 发布给跨线程 reader 的完整 immutable snapshot。
///
/// `*_count` 指示对应 fixed array 的有效前缀长度；reader 不应扫描 count 之后的条目。
struct OrderBookSnapshot {
    int64_t generation = 0;
    std::size_t order_count = 0;
    std::size_t tracked_code_count = 0;
    std::size_t force_flat_pending_count = 0;
    std::size_t close_outstanding_count = 0;
    std::array<OrderSnapshot, kOrderSnapshotMaxOrders> orders{};
    std::array<OrderTrackedCodeSnapshot, kOrderSnapshotMaxCodes> tracked_codes{};
    std::array<OrderTrackedCodeSnapshot, kOrderSnapshotMaxCodes> force_flat_pending_codes{};
    std::array<CloseOutstandingSnapshot, kOrderSnapshotMaxCloseOutstanding> close_outstanding{};
};

inline void copy_snapshot_code(char (&dst)[kOrderSnapshotCodeLen], const char* src) noexcept {
    dst[0] = '\0';
    if (!src || src[0] == '\0') {
        return;
    }
    std::strncpy(dst, src, kOrderSnapshotCodeLen - 1);
    dst[kOrderSnapshotCodeLen - 1] = '\0';
}

inline bool snapshot_code_equals(const char* lhs, const char* rhs) noexcept {
    if (!lhs || lhs[0] == '\0' || !rhs || rhs[0] == '\0') {
        return false;
    }
    return std::strncmp(lhs, rhs, kOrderSnapshotCodeLen) == 0;
}

static_assert(std::is_trivially_copyable<OrderSnapshot>::value,
              "OrderSnapshot must stay trivially copyable");
static_assert(std::is_trivially_copyable<OrderTrackedCodeSnapshot>::value,
              "OrderTrackedCodeSnapshot must stay trivially copyable");
static_assert(std::is_trivially_copyable<CloseOutstandingSnapshot>::value,
              "CloseOutstandingSnapshot must stay trivially copyable");
static_assert(std::is_trivially_copyable<OrderBookSnapshot>::value,
              "OrderBookSnapshot must stay trivially copyable");

}
}
