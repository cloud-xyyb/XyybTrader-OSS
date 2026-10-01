#pragma once

#include <algorithm>
#include <cstdint>

#include "api/XtRiskSnapshot.h"
#include "xt_account/api/AccountSnapshot.h"

/// @file
/// @brief 账户快照 → 风控快照的纯投影。
///
/// 投影公式属风控口径知识（净盈亏怎么算、昨仓怎么反解），账户模块只负责发布
/// 自洽的 account / position 帧，由风控侧按自己的口径折算。

namespace xt_risk {

/// @brief 把同一发布边界的账户与持仓快照投影为风控快照。
///
/// @param account 账户资金快照。
/// @param pos 同帧的单 symbol 持仓快照。
/// @return 已填充账户/持仓派生字段的风控快照；caller-builds 字段保持 0。
///
/// @note caller-builds 契约：`today_pnl_fp` / `limit_up_fp` / `limit_down_fp` /
///       `starting_balance_fp` / `per_strategy_today_pnl_fp` / `strategy_killed`
///       由 IRiskGuardContext::make_snapshot 的实现层按各自数据源覆写；本函数
///       留 0，对应 XtRiskEngine 各检查的 fail-safe bypass。
/// @pre account 与 pos 必须来自同一发布边界（见
///      AccountStateStore::snapshot_account_and_position）。跨帧组合会让权益与
///      持仓分属不同时刻，风控判据失去自洽性。
[[nodiscard]] inline XtRiskSnapshot project_risk_snapshot(
    const xt_account::AccountSnapshot& account,
    const xt_account::PositionSnapshot& pos) noexcept {
    XtRiskSnapshot out;
    out.cash_available_fp = account.available_cash;
    out.equity_fp = account.equity;
    // process_net_pnl 是交易净盈亏口径：realized - fees + unrealized。
    // cashflow 不进入该值，annual-loss 由 XtRiskEngine 的 nmf_offset 跨重启连续化。
    out.process_net_pnl_fp = account.realized_pnl - account.fees + account.unrealized_pnl;
    out.position_qty_fp = pos.qty;
    out.available_close_qty_fp = pos.available_sell_total_qty;
    // 昨仓判据 = min(总可平量, 毛持仓 − 今仓)，与 xt_account::closeable_view 的昨仓
    // 分量同一拆分口径：venue 收窄总可平量时两处给同一答案，不出现双推导分叉。
    // 不用 available_sell_total − available_sell_today：T+1 账本下 available_sell_total
    // 本身已是「总仓 − 今仓」，再减一次今仓会低估昨仓（engine 的
    // sell ≤ yesterday_long_qty_fp 判据据此假拒可卖昨仓）。
    // 三个分量先 clamp 非负再相减：坏帧不会下溢成巨大正值，减法也不触 signed overflow UB。
    // in-tree producer（AccountStateStore::build_position_snapshot_）恒满足
    // 0 ≤ long_today ≤ long_qty 且 available_sell_total = long_qty − (t1 ? long_today : 0)，
    // 两个账本分支下 avail 都不小于 gross − today，取值恒为 long_qty − long_today。
    // 本投影只做持仓口径拆分，不扣 broker frozen——frozen 双源去重在
    // XtLiveRiskGuardContext 的 close 判据里做。
    const int64_t gross = std::max<int64_t>(0, pos.long_qty);
    const int64_t today = std::max<int64_t>(0, pos.long_today_qty);
    const int64_t yesterday_holding = gross > today ? (gross - today) : 0;
    const int64_t avail = std::max<int64_t>(0, pos.available_sell_total_qty);
    out.yesterday_long_qty_fp = yesterday_holding < avail ? yesterday_holding : avail;
    return out;
}

}  // namespace xt_risk
