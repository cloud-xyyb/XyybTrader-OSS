#pragma once

#include <cstdint>
#include <string_view>

/// @file
/// @brief per-sid today realized PnL 的统一计算 helper。

namespace xt_risk {
namespace live {

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

}
}
