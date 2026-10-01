#include "RunnerCommon.h"

#include "XtLogger.h"
#include "xt_strategy_engine/util/XtRolloverHelper.h"  // PositionAggregator::set_jump_reporter

#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif

namespace xt_live::common {

// stop 顺序由 runner 控制：先关 exposer，再关 logger，避免 shutdown 末尾丢失诊断日志。
// exposer 启动由三 runner 共享 runner_process_init（RunnerBootCommon）负责。
void stop_metrics_exposer_() {
#ifdef XT_HAVE_METRICS
    ct::metrics::XtMetricRegistry::instance().stop_exposer();
#endif
}

#ifdef XT_HAVE_METRICS
namespace {

// PositionAggregator 只把跳变归因维度交给 Prometheus；原始仓位数值由
// account/position telemetry 保留，避免高频 label 或样本膨胀。
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
}  // namespace
#endif

void register_position_aggregator_jump_reporter_() {
#ifdef XT_HAVE_METRICS
    xt_rollover::PositionAggregator::set_jump_reporter(&position_aggregator_jump_bridge_);
#else
    // metrics 未编译时保持 hook 为空，PositionAggregator::on_update 跳过 emit。
#endif
}

}  // namespace xt_live::common
