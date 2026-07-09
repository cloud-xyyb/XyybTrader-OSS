#pragma once
/// @file
/// @brief Venue-neutral 交易时段 phase 分类 helper。
///
/// A 股按 HHMM 划分集合竞价、连续竞价、午休和盘后；中国期货与 24h venue
/// 在此层只区分为 Continuous，具体多段交易窗口由上层 session filter 判断。

#include <cstdint>

namespace xt_core {

enum class XtSessionPhase {
    PreOpen          = 0,
    OpenCallAuction  = 1,
    Continuous       = 2,
    Lunch            = 3,
    CloseCallAuction = 4,
    PostClose        = 5,
};

enum class XtVenueKind {
    Unknown       = -1,
    AShare        = 0,
    Continuous24h = 1,
    FuturesCN     = 2,
};

[[nodiscard]] constexpr XtSessionPhase classify_phase_a_share(uint32_t hhmm) noexcept {
    if (hhmm < 915)   return XtSessionPhase::PreOpen;
    if (hhmm < 925)   return XtSessionPhase::OpenCallAuction;
    if (hhmm < 930)   return XtSessionPhase::PreOpen;
    if (hhmm < 1130)  return XtSessionPhase::Continuous;
    if (hhmm < 1300)  return XtSessionPhase::Lunch;
    if (hhmm < 1457)  return XtSessionPhase::Continuous;
    if (hhmm < 1500)  return XtSessionPhase::CloseCallAuction;
    return XtSessionPhase::PostClose;
}

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

[[nodiscard]] constexpr bool is_call_auction(XtSessionPhase phase) noexcept {
    return phase == XtSessionPhase::OpenCallAuction
        || phase == XtSessionPhase::CloseCallAuction;
}

[[nodiscard]] constexpr uint32_t extract_hhmm_from_packed(uint32_t hhmmssmmm) noexcept {

    return hhmmssmmm / 100000;
}

}
