#pragma once

#include <cstdint>
#include <string_view>

#include "api/IHftEngineObserver.h"
#include "xt_backtest/reporter/XtBacktestReporter.h"
#include "xt_core/order/XtTradeRecord.h"

/// @brief HFT 回测 gateway-side 的订单生命周期报告 observer。
///
/// XtBacktestReporterObserver把订单提交和订单状态更新转发给 `XtBacktestReporter`
/// 成交事件由 `XtBacktestReporterFillObserver` 处理。
/// `reporter` 为 non-owning，可为空；为空时丢弃所有报告事件。
class XtBacktestReporterObserver final : public xt_runtime::IHftEngineObserver {
public:
    /// @param r non-owning；允许为空，表示关闭本 observer 的 reporter 输出。
    explicit XtBacktestReporterObserver(XtBacktestReporter* r) : reporter_(r) {}

    void on_order_update(const XtOrderExecUpdate& u,
                         XtLatencyEligibility /*elig*/ = XtLatencyEligibility::kEligible) override {
        // BT reporter 只做报告输出，不喂 latency tracker；忽略 elig。
        if (reporter_) reporter_->on_order_update(u);
    }

    void on_order_submit(int64_t ts,
                         uint64_t id,
                         const char* code,
                         bool is_buy,
                         int64_t price_fp,
                         int64_t qty_fp,
                         std::string_view tag,
                         bool is_close,
                         bool is_today) override {
        if (reporter_) {
            reporter_->on_order_submit(ts, id, code, is_buy, price_fp, qty_fp,
                                       tag, is_close, is_today);
        }
    }
    // HFT backtest reporter 不消费 position/account snapshot observer 回调。

private:
    XtBacktestReporter* reporter_;  ///< non-owning；允许为空
};

/// @brief HFT 回测 engine-side 的成交报告 observer。
///
/// Runner 将本 observer 注册到 engine；`on_trade()` 把 domain 处理后的成交转成
/// reporter 的 fill 行。`reporter` 为 non-owning，可为空；为空时关闭 fill 上报。
class XtBacktestReporterFillObserver final : public xt_runtime::IHftEngineObserver {
public:
    /// @param r non-owning；允许为空，表示关闭 fill 上报。
    explicit XtBacktestReporterFillObserver(XtBacktestReporter* r) : reporter_(r) {}

    void on_trade(const XtTradeRecord& tr,
                  XtLatencyEligibility elig = XtLatencyEligibility::kEligible,
                  uint64_t logical_order_id = 0) override;

private:
    XtBacktestReporter* reporter_;  ///< non-owning；允许为空
};
