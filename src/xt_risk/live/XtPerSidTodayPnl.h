#pragma once

#include <cstdint>
#include <string_view>

/// @file
/// @brief per-sid today realized PnL 的统一计算 helper。

namespace xt_risk {
namespace live {

/// @brief per-sid today realized PnL fp = aggregator lifetime realized − 日初 baseline。
///
/// baseline 缺失/未 ready 时退化为 lifetime realized，避免启动早期扩大拒单范围。
/// 与 `XtLiveRiskGuardContext::make_snapshot` 的 `per_strategy_today_pnl_fp` **同口径**——
/// reject 层（submit 热路径）与 auto-liquidation trigger（1Hz frame loop）必须共用本 helper，
/// 否则 recovery / baseline-未-ready 时两层对 breach 的判断会分歧。
///
/// 模板参数仅约束接口：`Aggr::realized_fp_for_sid(string_view) const`、
/// `Baseline::ready() const` + `Baseline::realized_fp_for_sid(string_view) const`。
///
/// @pre aggr 非空、sid 非空（调用方负责 sid>0 guard + acquire-load 指针 + sid 反查）。
/// @param baseline 可为 nullptr（退化 lifetime）。
template <typename Aggr, typename Baseline>
inline int64_t per_sid_today_realized_fp(const Aggr* aggr,
                                         const Baseline* baseline,
                                         std::string_view sid) {
    int64_t pnl_fp = aggr->realized_fp_for_sid(sid);
    if (baseline && baseline->ready()) {
        pnl_fp -= baseline->realized_fp_for_sid(sid);
    }
    return pnl_fp;
}

}  // namespace live
}  // namespace xt_risk
