#pragma once

/// @file
/// @brief 重启恢复时把交易所在途单灌回引擎的 helper。
///
/// 各 venue gateway 从交易所查到在途单列表后，都经这里统一发给引擎，
/// 顺带记 metric 和日志。空列表也照发，让下游知道「恢复完成、确实没有在途单」。

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>

#include "api/AliveInflightOrder.h"
#include "api/ITradeGateway.h"
#include "xt_logger/XtLogger.h"

#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif

namespace xyybtrader::recovery {

inline void emit_reverse_sync_batch_with_metric(
    std::vector<AliveInflightOrder> orders,
    bool is_cold_start,
    std::string_view venue,
    const ITradeGateway::ReverseSyncBatchHandler& handler)
{
    const std::size_t n = orders.size();

#ifdef XT_HAVE_METRICS
    ct::metrics::XtMetricRegistry::instance()
        .inc_recovery_rebuild_alive_total(venue, "success");
#else
    (void)venue;
#endif

    XtLogger::info_m("ReverseSync",
        "event=recovery_rebuild_alive_emit venue={} orders={} is_cold_start={}",
        venue, n, is_cold_start ? 1 : 0);

    if (handler) {
        handler(orders, is_cold_start);
    }
}

}
