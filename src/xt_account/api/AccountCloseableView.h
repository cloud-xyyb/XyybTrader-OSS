#pragma once

/// @file
/// @brief 单侧平仓可用量视图：由 PositionSnapshot 的 sell/cover 字段投影得到。
///
/// order / runtime / risk 侧只需要这组投影 helper，不需要 store 的 hazard 发布帧。

#include <cstdint>
#include <type_traits>

#include "xt_core/order/XtOffset.h"
#include "xt_account/api/AccountSnapshot.h"

namespace xt_account {

/// @brief 单侧平仓可用量视图。
///
/// today_fp 与 total_fp 会 clamp 至非负；yesterday_fp = max(total_fp - today_fp, 0)。
struct CloseableView {
    int64_t today_fp     = 0;
    int64_t yesterday_fp = 0;
    int64_t total_fp     = 0;
};

inline int64_t clamp_qty_nonneg(int64_t v) noexcept {
    return v > 0 ? v : 0;
}

inline CloseableView make_closeable_view(int64_t today_raw, int64_t total_raw) noexcept {
    CloseableView out;
    out.today_fp = clamp_qty_nonneg(today_raw);
    out.total_fp = clamp_qty_nonneg(total_raw);
    out.yesterday_fp = out.total_fp > out.today_fp ? (out.total_fp - out.today_fp) : 0;
    return out;
}

inline CloseableView closeable_view(const PositionSnapshot& pos, bool is_buy_to_close) noexcept {
    return is_buy_to_close
        ? make_closeable_view(pos.available_cover_today_qty, pos.available_cover_total_qty)
        : make_closeable_view(pos.available_sell_today_qty,  pos.available_sell_total_qty);
}

inline int64_t closeable_for_offset(const CloseableView& v, XtOffset offset) noexcept {
    switch (offset) {
    case XtOffset::CloseToday:     return v.today_fp;
    case XtOffset::CloseYesterday: return v.yesterday_fp;
    case XtOffset::Close:
    default:                       return v.total_fp;
    }
}

static_assert(std::is_trivially_copyable<CloseableView>::value,
              "CloseableView must stay trivially copyable");

}
