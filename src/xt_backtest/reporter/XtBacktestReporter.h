#pragma once
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "xt_report/core/XtReportTypes.h"
#include "xt_report/core/IReportSink.h"
#include "api/IRejectSink.h"
#include "XtBacktestDailyBreakdown.h"
#include "XtEventStreamSink.h"
#include "xt_backtest/infra/XtReplayer.h"

namespace xt_core {
struct XtOrder;
}
struct XtRiskResult;
struct XtRawTick;
struct XtTradeRecord;

/// @brief 回测报告聚合器。
///
/// 由 replay 线程串行调用，汇总 tick、成交、订单与 reject 事件。
/// `IReportSink` 承担批量表写出；`XtEventStreamSink` 承担事件流写出。
class XtBacktestReporter : public IReplayReporter, public IRejectSink {
public:
    XtBacktestReporter() = default;

    bool init(xt_report::IReportSink& report_sink,
              std::unique_ptr<XtEventStreamSink> event_stream_sink);

    void on_tick_pre(int64_t ts, const XtRawTick& t) override;

    void on_tick_post(int64_t ts, size_t active_orders) override;

    bool flush();

    int trade_count() const { return trade_count_; }

    uint64_t sink_drop_count() const noexcept { return drop_count_; }

    bool write_daily_breakdown(const std::string& out_path,
                               uint32_t cfg_start_date,
                               uint32_t cfg_end_date) const;

    /// @brief 日终账户快照（equity/cash/frozen，人类单位 double）。
    struct AccountEod {
        double equity = 0.0;
        double cash = 0.0;
        double frozen = 0.0;
    };

    void set_account_probe(std::function<AccountEod()> probe) {
        account_probe_ = std::move(probe);
    }

    void on_fill(const xt_report::XtFillRow& fill, const XtTradeRecord& raw);

    void on_order_update(const XtOrderExecUpdate& u);

    void on_order_submit(int64_t ts,
                         uint64_t id,
                         const char* code,
                         bool is_buy,
                         int64_t price_fp,
                         int64_t qty_fp,
                         std::string_view user_tag,
                         bool is_close,
                         bool is_today);

    void on_risk_reject(int64_t ts,
                        const char* code,
                        bool is_buy,
                        int64_t price_fp,
                        int64_t qty_fp,
                        const std::string& reason);

    void on_margin_reject(const xt_core::XtOrder& req,
                          const XtRiskResult& decision,
                          int64_t now_ms,
                          std::string_view strategy_id = std::string_view{}) override;

    void on_reject(const xt_core::XtOrder& req,
                   const XtRiskResult& decision,
                   std::string_view strategy_id = std::string_view{}) override;

private:

    xt_report::IReportSink* sink_ = nullptr;
    std::unique_ptr<XtEventStreamSink> event_writer_;

    std::vector<xt_report::XtFillRow> fills_;

    std::unordered_map<std::string, int> risk_rejects_;
    int trade_count_ = 0;

    XtBacktestDailyBreakdown daily_breakdown_;

    uint32_t current_trading_date_ = 0;

    std::function<AccountEod()> account_probe_;

    int64_t last_tick_ts_ = 0;

    uint64_t drop_count_ = 0;
};
