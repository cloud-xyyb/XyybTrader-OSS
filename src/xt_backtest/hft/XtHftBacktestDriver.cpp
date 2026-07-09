#include "XtHftBacktestDriver.h"

#include "xt_backtest/hft/XtHftBacktestOrderGateway.h"
#include "xt_engine/hft/XtHftEngine.h"
#include "xt_util/time/XtTime.h"
#include "XtLogger.h"

namespace xt_backtest::hft {

void XtHftBacktestDriver::on_tick(const XtTick& tick) {
    engine_->on_tick(tick);
}

void XtHftBacktestDriver::on_order_queue(const XtOrderQueue& oq) {
    engine_->on_order_queue(oq);
}

void XtHftBacktestDriver::on_order_detail(const XtOrderDetail& od) {
    engine_->on_order_detail(od);
}

void XtHftBacktestDriver::on_transaction(const XtTransaction& tr) {
    engine_->on_transaction(tr);
}

void XtHftBacktestDriver::on_clock(uint32_t date, uint32_t time_ms) {

    gw_->advance_clock_only(date, time_ms);
    thread_local std::vector<XtTradeRecord> fills;
    fills.clear();
    gw_->drain_fills(date, time_ms, fills);
    for (auto& tr : fills) engine_->on_trade(tr, ITradeGateway::TdState::Live);
}

size_t XtHftBacktestDriver::active_orders() const {
    return gw_->live_orders();
}

void XtHftBacktestDriver::flush_pending(std::uint32_t grace_ms) {
    thread_local std::vector<XtTradeRecord> fills;
    fills.clear();

    const std::uint32_t last_d = gw_->last_clock_date();
    const std::uint32_t last_t = gw_->last_clock_time_ms();
    if (last_d == 0 && last_t == 0) {

        gw_->drop_pending_fills();
        return;
    }

    std::uint32_t eof_d = last_d;
    std::uint32_t eof_t = last_t;
    if (grace_ms > 0) {
        const std::int64_t last_ns = xt_time::to_ns(last_d, last_t);
        const std::int64_t eof_ns  = last_ns +
                                     static_cast<std::int64_t>(grace_ms) * 1'000'000LL;
        xt_time::from_ns(eof_ns, eof_d, eof_t);
    }

    gw_->advance_clock_only(eof_d, eof_t);

    gw_->drain_fills(eof_d, eof_t, fills);
    for (auto& tr : fills) engine_->on_trade(tr, ITradeGateway::TdState::Live);

    const std::size_t dropped = gw_->drop_pending_fills();

    const std::size_t lost_intransit = gw_->discard_in_transit_orders();

    if (dropped > 0 || lost_intransit > 0) {
        XtLogger::info_m("BT_EOF",
                         "EOF strict grace_ms={}: drained {} fill, dropped {} "
                         "unmatured pending fill(s), lost_in_transit={} order(s) (cum={})",
                         grace_ms, fills.size(), dropped,
                         lost_intransit, gw_->lost_in_transit_count());
    }
}

}
