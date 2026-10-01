#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>

#include "xt_core/CloseOffsetCore.h"
#include "xt_core/order/XtOffsetPolicy.h"
#include "xt_core/order/XtOrderRequest.h"  // XtOrderSource（resolve_submit_offset）

/// @file
/// @brief 解析策略提交中的 Auto/Open 开平意图。
///
/// 策略单可以用 `XtOffset::Auto` 表达“由系统决定开平”；提交入口通过
/// `resolve_submit_offset()` 先把它解析成 Open 意图。
/// 人工、强平和恢复来源不能使用 Auto；误传时入口直接拒单。
/// 对解析后的策略 Open 请求，`route_open_intent()` 再决定原样开仓、先平反向仓，
/// 或返回 defer，由提交入口按 `OppositeOpenPending` 本地拒单。

namespace xt_offset_router {

/// @brief XtOffset::Auto 提交入口解析（纯函数，BT/live 同路径）。
///
/// Auto = submit-time sentinel：Strategy 源 → Open 意图（close_first 品种随后
/// 由 route_open_intent 改写/defer，open_allowed 等价 Open）；非策略源
/// （Manual/Closeout/Recovery）无 Auto 语义 → nullopt，caller 必须 fail-closed 拒
/// （防未解析 Auto 泄漏进风控 gate/venue 映射）。非 Auto 原样透传。
[[nodiscard]] inline std::optional<XtOffset> resolve_submit_offset(
    XtOffset offset, xt_core::XtOrderSource source) noexcept {
    if (offset != XtOffset::Auto) return offset;
    if (source != xt_core::XtOrderSource::Strategy) return std::nullopt;
    return XtOffset::Open;
}

/// @brief 路由输入；全部数量为非负 fixed-point（调用方已校验 req_qty_fp > 0）。
struct OffsetRouteInput {
    const char* code = nullptr;       ///< 拆分形态由 needs_close_today_split(code) 内部决定
    XtOffsetPolicy policy = XtOffsetPolicy::OpenAllowed;  ///< 已消解（非 Inherit）
    int64_t req_qty_fp = 0;           ///< 策略 Open 请求量
    // 对侧（与请求方向相反的持仓腿）可平与在途占用，分今昨：
    int64_t opposite_closeable_today_fp = 0;
    int64_t opposite_closeable_yesterday_fp = 0;
    int64_t opposite_close_outstanding_today_fp = 0;
    int64_t opposite_close_outstanding_yesterday_fp = 0;
    /// 对侧在途 Open 总量（book open_no_trade_qty_fp(code, !is_buy)）。
    int64_t opposite_open_outstanding_fp = 0;
};

/// @brief 路由决策。kRewrite 时 close_slices[0..close_slice_count) + 余量 Open。
struct OffsetRouteDecision {
    enum class Action : uint8_t {
        kPassOpen = 0,  ///< 按原始 Open 直通（现状路径）
        kRewrite  = 1,  ///< 改写：先平对侧可平量，余量保持 Open
        kDefer    = 2,  ///< 本地拒 OppositeOpenPending（策略下 tick 重试）
    };
    Action action = Action::kPassOpen;
    close_offset_core::CloseSlice close_slices[2];
    int close_slice_count = 0;
    int64_t open_remainder_fp = 0;  ///< kRewrite 时未被可平量吸收的 Open 余量（可为 0）
};

/// @brief Open 意图路由（纯函数）。
///
/// CloseFirst 先检查对侧在途 Open：存在则 defer，避免在未知 fill 状态下赌可平量。
/// 否则按 `max(0, closeable - close outstanding)` 计算有效可平腿，先平昨再平今；
/// 剩余数量保留 Open。无有效可平腿时按原 Open 直通。
inline OffsetRouteDecision route_open_intent(const OffsetRouteInput& in) {
    OffsetRouteDecision d;
    if (in.policy != XtOffsetPolicy::CloseFirst || in.req_qty_fp <= 0) {
        return d;  // kPassOpen
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
        return d;  // kPassOpen：无可平腿，真开仓
    }

    const int64_t close_qty_fp = std::min(in.req_qty_fp, eff_total);
    const int n = close_offset_core::resolve_close_slices(
        in.code, close_qty_fp, eff_today, eff_yesterday,
        /*prefer_today=*/false, d.close_slices);
    if (n <= 0) {
        return d;  // 防御：resolve 失败回退直通（close_qty<=eff_total 理论不可达）
    }
    d.action = OffsetRouteDecision::Action::kRewrite;
    d.close_slice_count = n;
    d.open_remainder_fp = in.req_qty_fp - close_qty_fp;
    return d;
}

}  // namespace xt_offset_router
