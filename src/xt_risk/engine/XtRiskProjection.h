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

[[nodiscard]] inline XtRiskSnapshot project_risk_snapshot(
    const xt_account::AccountSnapshot& account,
    const xt_account::PositionSnapshot& pos) noexcept {
    XtRiskSnapshot out;
    out.cash_available_fp = account.available_cash;
    out.equity_fp = account.equity;

    out.process_net_pnl_fp = account.realized_pnl - account.fees + account.unrealized_pnl;
    out.position_qty_fp = pos.qty;
    out.available_close_qty_fp = pos.available_sell_total_qty;
    out.yesterday_long_qty_fp =
        std::max<int64_t>(0, pos.available_sell_total_qty - pos.available_sell_today_qty);
    return out;
}

}
