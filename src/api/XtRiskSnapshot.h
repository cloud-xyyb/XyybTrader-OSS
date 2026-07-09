#pragma once

#include <cstdint>
#include <type_traits>

/** @brief 风控快照输入（snapshot input）。 */
struct XtRiskSnapshot {
    int64_t cash_available_fp = 0;

    int64_t today_pnl_fp = 0;
    int64_t position_qty_fp = 0;
    int64_t available_close_qty_fp = 0;

    int64_t equity_fp = 0;

    int64_t process_net_pnl_fp = 0;

    int64_t yesterday_long_qty_fp = 0;

    int64_t limit_up_fp = 0;

    int64_t limit_down_fp = 0;

    int64_t per_strategy_today_pnl_fp = 0;

    int64_t starting_balance_fp = 0;

    bool strategy_killed = false;

};

static_assert(sizeof(XtRiskSnapshot) == 96,
              "XtRiskSnapshot ABI size changed (sync AccountSnapshot.h guard + AccountSnapshotTypesTest)");
static_assert(std::is_trivially_copyable<XtRiskSnapshot>::value,
              "XtRiskSnapshot must stay trivially_copyable (cross-module POD)");
