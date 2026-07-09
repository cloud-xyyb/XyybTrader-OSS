#include "RunnerCommon.h"

#include "XtLogger.h"
#include "xt_strategy_engine/util/XtRolloverHelper.h"

#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif

namespace xt_live::common {

void stop_metrics_exposer_() {
#ifdef XT_HAVE_METRICS
    ct::metrics::XtMetricRegistry::instance().stop_exposer();
#endif
}

#ifdef XT_HAVE_METRICS
namespace {

void position_aggregator_jump_bridge_(const char* sid,
                                      const char* code,
                                      int64_t prev_qty_fp,
                                      int64_t new_qty_fp) {
    (void)prev_qty_fp;
    (void)new_qty_fp;
    ct::metrics::XtMetricRegistry::instance().inc_position_aggregator_jump_total(
        sid ? sid : "",
        code ? code : "");
}
}
#endif

void register_position_aggregator_jump_reporter_() {
#ifdef XT_HAVE_METRICS
    xt_rollover::PositionAggregator::set_jump_reporter(&position_aggregator_jump_bridge_);
#else

#endif
}

}
