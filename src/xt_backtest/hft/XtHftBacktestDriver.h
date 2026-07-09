#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "api/IReplayRuntime.h"
#include "api/ITradeGateway.h"
#include "xt_core/order/XtTradeRecord.h"

class XtHftEngine;
class XtHftBacktestOrderGateway;

namespace xt_backtest::hft {

/// @brief HFT 回测的 runtime adapter：让通用 Replayer 不需要知道 HFT 的 engine + gateway 协作细节。
class XtHftBacktestDriver final : public IReplayRuntime {
public:
    XtHftBacktestDriver(XtHftEngine& engine, XtHftBacktestOrderGateway& gw)
        : engine_(&engine), gw_(&gw) {}

    void on_tick(const XtTick& tick) override;

    void on_order_queue(const XtOrderQueue& oq) override;
    void on_order_detail(const XtOrderDetail& od) override;
    void on_transaction(const XtTransaction& tr) override;

    void on_clock(uint32_t date, uint32_t time_ms) override;

    size_t active_orders() const override;

    void flush_pending(std::uint32_t grace_ms);

private:

    XtHftEngine*                engine_ = nullptr;
    XtHftBacktestOrderGateway*  gw_     = nullptr;
};

}
