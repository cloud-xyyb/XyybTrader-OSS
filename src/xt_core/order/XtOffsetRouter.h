#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>

#include "xt_core/CloseOffsetCore.h"
#include "xt_core/order/XtOffsetPolicy.h"
#include "xt_core/order/XtOrderRequest.h"

/// @file
/// @brief 解析策略提交中的 Auto/Open 开平意图。
///
/// 策略单可以用 `XtOffset::Auto` 表达“由系统决定开平”；提交入口通过
/// `resolve_submit_offset()` 先把它解析成 Open 意图。
/// 人工、强平和恢复来源不能使用 Auto；误传时入口直接拒单。
/// 对解析后的策略 Open 请求，`route_open_intent()` 再决定原样开仓、先平反向仓，
/// 或返回 defer，由提交入口按 `OppositeOpenPending` 本地拒单。

namespace xt_offset_router {

[[nodiscard]] inline std::optional<XtOffset> resolve_submit_offset(
    XtOffset offset, xt_core::XtOrderSource source) noexcept {
    if (offset != XtOffset::Auto) return offset;
    if (source != xt_core::XtOrderSource::Strategy) return std::nullopt;
    return XtOffset::Open;
}

/// @brief 路由输入；全部数量为非负 fixed-point（调用方已校验 req_qty_fp > 0）。
struct OffsetRouteInput {
    const char* code = nullptr;
    XtOffsetPolicy policy = XtOffsetPolicy::OpenAllowed;
    int64_t req_qty_fp = 0;

    int64_t opposite_closeable_today_fp = 0;
    int64_t opposite_closeable_yesterday_fp = 0;
    int64_t opposite_close_outstanding_today_fp = 0;
    int64_t opposite_close_outstanding_yesterday_fp = 0;

    int64_t opposite_open_outstanding_fp = 0;
};

/// @brief 路由决策。kRewrite 时 close_slices[0..close_slice_count) + 余量 Open。
struct OffsetRouteDecision {
    enum class Action : uint8_t {
        kPassOpen = 0,
        kRewrite  = 1,
        kDefer    = 2,
    };
    Action action = Action::kPassOpen;
    close_offset_core::CloseSlice close_slices[2];
    int close_slice_count = 0;
    int64_t open_remainder_fp = 0;
};

inline OffsetRouteDecision route_open_intent(const OffsetRouteInput& in) {
    OffsetRouteDecision d;
    if (in.policy != XtOffsetPolicy::CloseFirst || in.req_qty_fp <= 0) {
        return d;
    }

    if (in.opposite_open_outstanding_fp > 0) {
        d.action = OffsetRouteDecision::Action::kDefer;
        return d;
    }

    const int64_t eff_today = std::max<int64_t>(
        0, in.opposite_closeable_today_fp - in.opposite_close_outstanding_today_fp);
    const int64_t eff_yesterday = std::max<int64_t>(
        0, in.opposite_closeable_yesterday_fp - in.opposite_close_outstanding_yesterday_fp);
    const int64_t eff_total = eff_today + eff_yesterday;
    if (eff_total <= 0) {
        return d;
    }

    const int64_t close_qty_fp = std::min(in.req_qty_fp, eff_total);
    const int n = close_offset_core::resolve_close_slices(
        in.code, close_qty_fp, eff_today, eff_yesterday,
         false, d.close_slices);
    if (n <= 0) {
        return d;
    }
    d.action = OffsetRouteDecision::Action::kRewrite;
    d.close_slice_count = n;
    d.open_remainder_fp = in.req_qty_fp - close_qty_fp;
    return d;
}

}
