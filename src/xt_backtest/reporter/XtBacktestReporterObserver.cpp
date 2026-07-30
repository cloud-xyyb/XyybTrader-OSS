#include "XtBacktestReporterObserver.h"

#include "xt_report/util/XtReportEventBuilder.h"

void XtBacktestReporterFillObserver::on_trade(const XtTradeRecord& tr,
                                              XtLatencyEligibility  ,
                                              uint64_t  ) {

    if (!reporter_) return;

    reporter_->on_fill(xt_report::XtReportEventBuilder::build_fill(tr), tr);
}
