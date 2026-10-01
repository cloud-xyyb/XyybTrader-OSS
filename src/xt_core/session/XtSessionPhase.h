#pragma once
/// @file
/// @brief Venue-neutral 交易时段 phase 分类 helper。
///
/// A 股按 HHMM 划分集合竞价、连续竞价、午休和盘后；中国期货与 24h venue
/// 在此层只区分为 Continuous，具体多段交易窗口由上层 session filter 判断。

#include <cstdint>

namespace xt_core {

/// @brief 交易时段 phase 分类。
enum class XtSessionPhase {
    PreOpen          = 0,  ///< 盘前 / 撮合空隙
    OpenCallAuction  = 1,  ///< 开盘集合竞价（A 股 9:15-9:25）
    Continuous       = 2,  ///< 连续竞价（期货 / 加密始终此 phase）
    Lunch            = 3,  ///< 午休（A 股 11:30-13:00）
    CloseCallAuction = 4,  ///< 收盘集合竞价（A 股 14:57-15:00）
    PostClose        = 5,  ///< 盘后
};

/// @brief Venue 类别；决定 session phase 分类模板。
enum class XtVenueKind {
    Unknown       = -1, ///< 未能派生 venue；分类为 PostClose。
    AShare        = 0,  ///< 沪深主板 / 创业板（含集合竞价）
    Continuous24h = 1,  ///< 加密永续（始终 Continuous）
    FuturesCN     = 2,  ///< 中国期货（多 segment 由上层 session filter 判断）
};

/// @brief 推断 A 股 session phase。
///
/// `hhmm` 是小时分钟数，例如 935 表示 9:35。本 helper 不解析交易日历和节假日；
/// 非交易窗口或超出正常范围的值会保守归入 PreOpen/PostClose/Lunch 等不可连续竞价 phase。
[[nodiscard]] constexpr XtSessionPhase classify_phase_a_share(uint32_t hhmm) noexcept {
    if (hhmm < 915)   return XtSessionPhase::PreOpen;
    if (hhmm < 925)   return XtSessionPhase::OpenCallAuction;
    if (hhmm < 930)   return XtSessionPhase::PreOpen;        // 9:25-9:30 撮合空隙
    if (hhmm < 1130)  return XtSessionPhase::Continuous;
    if (hhmm < 1300)  return XtSessionPhase::Lunch;
    if (hhmm < 1457)  return XtSessionPhase::Continuous;
    if (hhmm < 1500)  return XtSessionPhase::CloseCallAuction;
    return XtSessionPhase::PostClose;
}

/// @brief 通用 dispatch：按 venue 选择 phase 模板。
///
/// Unknown venue 使用 PostClose 作为 fail-safe，避免上层把无法识别的 venue 当作可交易窗口。
[[nodiscard]] constexpr XtSessionPhase classify_phase(XtVenueKind venue,
                                                       uint32_t hhmm) noexcept {
    switch (venue) {
        case XtVenueKind::Unknown: return XtSessionPhase::PostClose;
        case XtVenueKind::AShare:  return classify_phase_a_share(hhmm);
        case XtVenueKind::Continuous24h: return XtSessionPhase::Continuous;
        case XtVenueKind::FuturesCN:     return XtSessionPhase::Continuous;
    }
    return XtSessionPhase::PostClose;
}

/// @brief 是否在集合竞价时段（IOC/FOK 应拒单）。
[[nodiscard]] constexpr bool is_call_auction(XtSessionPhase phase) noexcept {
    return phase == XtSessionPhase::OpenCallAuction
        || phase == XtSessionPhase::CloseCallAuction;
}

/// @brief 从 HHMMSSmmm packed 整数（如 93000000 = 9:30:00.000）提取 HHMM (= 930)。
///
/// 仅做整数截断，不校验秒/毫秒字段范围。
[[nodiscard]] constexpr uint32_t extract_hhmm_from_packed(uint32_t hhmmssmmm) noexcept {
    // hhmmssmmm = HH × 10^7 + MM × 10^5 + SS × 10^3 + mmm
    return hhmmssmmm / 100000;  // 截断到 HHMM
}

}  // namespace xt_core
