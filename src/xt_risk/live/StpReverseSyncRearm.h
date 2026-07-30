#pragma once

#include <vector>

#include "api/AliveInflightOrder.h"
#include "live/XtSymbolPriceBoundSTP.h"

inline void stp_rearm_on_reverse_sync(XtSymbolPriceBoundSTP& stp,
                                      const std::vector<AliveInflightOrder>& orders,
                                      bool is_cold_start) {

    if (!is_cold_start) return;

    stp.clear_all_active();
    for (const auto& o : orders)
        stp.seed_active_order(o.code, o.local_id, o.is_buy, o.price_fp);
}
