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
/// 构造后置条件（`make_closeable_view` / `closeable_view` 返回那一刻成立）：三个字段
/// 都非负，且 `today_fp + yesterday_fp == total_fp`。total_fp 是 venue 口径的总可平量
/// （唯一权威），今昨两分量是它按持仓今昨结构的拆分。消费者既可按 offset 取单个分量，
/// 也可把两分量相加还原 total（`XtLiveRiskGuardContext::pick_closeable_by_offset` 的
/// `XtOffset::Close` 分支、`IAccountView::pos_detail` 的下游求和都依赖该等式）。
///
/// @note 该等式不是实例级不变量：把本视图当可变余量账本用的消费者
///       （`XtOrderRuntimeBook::fillup_alloc_close_locks_` 原地扣减今昨分量）会让两
///       分量之和小于 total_fp。那类 caller 自负余量语义，不得再对求和取总可平量。
struct CloseableView {
    int64_t today_fp     = 0;
    int64_t yesterday_fp = 0;
    int64_t total_fp     = 0;
};

/// @brief 将 producer 侧异常负数防御性折回 0。
inline int64_t clamp_qty_nonneg(int64_t v) noexcept {
    return v > 0 ? v : 0;
}

/// @brief 由今仓持仓量、总可平量与该方向的毛持仓量构造 CloseableView。
///
/// @param today_holding_raw 该方向今仓**持仓量**（非可平量）。
/// @param total_raw         venue 口径总可平量（T+1 账本下已扣今仓）。
/// @param gross_raw         该方向毛持仓量（long_qty / short_qty）。
///
/// 昨仓可平量 = min(总可平量, 昨仓持仓量)，今仓可平量 = 总可平量 − 昨仓可平量。
/// 这样拆分对 T+1 与非 T+1 两种账本同时成立，且恒满足 today + yesterday == total：
/// - **非 T+1**（期货 / 未开今昨拆分的股票账本）：total = gross，昨仓可平 =
///   gross − today_holding、今仓可平 = today_holding，与旧式 `total − today` 逐位相同。
/// - **T+1**（A 股）：total = gross − today_holding 本身已是昨仓，昨仓可平 = total、
///   今仓可平 = 0（当日买入不可卖）。旧式 `yesterday = total − today` 在此把今仓减了
///   两次（昨仓低估），而 `today` 又把不可卖的今仓报成可平——两个错误在求和时互相
///   抵消才让 total 看着对，单独取任一分量都是错的。
inline CloseableView make_closeable_view(int64_t today_holding_raw,
                                         int64_t total_raw,
                                         int64_t gross_raw) noexcept {
    CloseableView out;
    out.total_fp = clamp_qty_nonneg(total_raw);
    const int64_t today_holding = clamp_qty_nonneg(today_holding_raw);
    const int64_t gross = clamp_qty_nonneg(gross_raw);
    const int64_t yesterday_holding = gross > today_holding ? (gross - today_holding) : 0;
    out.yesterday_fp = out.total_fp < yesterday_holding ? out.total_fp : yesterday_holding;
    out.today_fp = out.total_fp - out.yesterday_fp;
    return out;
}

/// @brief 无毛持仓量信息时的构造（gross == total，即非 T+1 口径）。
/// @note 仅供「total 就是毛持仓」的合成场景（测试夹具、无今昨拆分的 venue）。
///       T+1 快照必须走三参形式，否则今昨分量会退化为旧的双减口径。
inline CloseableView make_closeable_view(int64_t today_holding_raw, int64_t total_raw) noexcept {
    return make_closeable_view(today_holding_raw, total_raw, total_raw);
}

/// @brief 从 PositionSnapshot 按 close 方向构造 CloseableView。
///
/// @param pos              持仓快照（已经 immutable hazard snapshot 一致视图）
/// @param is_buy_to_close  true = 买平空（cover），false = 卖平多（sell）
/// @note 今昨分量取原始持仓字段（long/short 的 qty 与 today_qty），总量取 venue
///       可平口径（available_*_total）；三者同帧，不跨快照。
inline CloseableView closeable_view(const PositionSnapshot& pos, bool is_buy_to_close) noexcept {
    return is_buy_to_close
        ? make_closeable_view(pos.short_today_qty, pos.available_cover_total_qty, pos.short_qty)
        : make_closeable_view(pos.long_today_qty,  pos.available_sell_total_qty,  pos.long_qty);
}

/// @brief 按 close offset 选择今仓、昨仓或总可平量。
/// @note XtOffset::Open/default 返回 total_fp；调用方应只在平仓路径使用。
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

}  // namespace xt_account
