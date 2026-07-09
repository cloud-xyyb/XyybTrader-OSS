#pragma once

#include <cstdint>
#include <utility>

#include "xt_risk/engine/XtRiskEngine.h"

/// @file
/// @brief session-begin 时同步年度止损基准的共享 helper。
///
/// 年度止损需要「今年年初的权益」作基准；live 和 HFT 回测在每个 session
/// 开始时都经这里检查年份是否变化，跨年才取一次 PnL 快照并让 engine 重置基准。

namespace xt_risk {

template <typename SnapshotPnlFn>
inline void arm_annual_loss_on_session_begin(XtRiskEngine& engine,
                                             uint32_t trading_date,
                                             int64_t equity_fp,
                                             SnapshotPnlFn&& snapshot_pnl) noexcept {
    if (trading_date == 0) return;
    const int year = static_cast<int>(trading_date / 10000);

    if (engine.current_year() == year) return;
    int64_t realized_fp = 0;
    int64_t fees_fp = 0;
    int64_t unrealized_fp = 0;
    std::forward<SnapshotPnlFn>(snapshot_pnl)(realized_fp, fees_fp, unrealized_fp);
    engine.on_year_change(year, equity_fp, realized_fp - fees_fp + unrealized_fp);
}

}
