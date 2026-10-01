#include "XtBacktestReporterObserver.h"

#include "xt_report/util/XtReportEventBuilder.h"

void XtBacktestReporterFillObserver::on_trade(const XtTradeRecord& tr,
                                              XtLatencyEligibility /*elig*/,
                                              uint64_t /*logical_order_id*/) {
    // BT reporter 只做报告输出，不喂 latency tracker；忽略 elig/logical。
    if (!reporter_) return;
    // 在 engine.on_trade 完成 domain 处理后触发：tr.realized_pnl_fp 已由引擎盖章。
    // build_fill(tr) 携带 strategy_id/symbol/realized_pnl；reporter 同时收 fill row 和
    // 原始成交，便于报表与诊断使用同一事件源。
    reporter_->on_fill(xt_report::XtReportEventBuilder::build_fill(tr), tr);
}
