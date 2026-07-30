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

void XtHftBacktestDriver::on_clock(uint32_t action_date, uint32_t action_time_ms,
                                   uint32_t trading_date) {

    gw_->advance_clock_only(action_date, action_time_ms, trading_date);

    thread_local std::vector<XtOrderExecUpdate> upds;
    upds.clear();
    gw_->drain_order_updates(upds);
    for (auto& u : upds) engine_->on_order_update(u);
    thread_local std::vector<XtTradeRecord> fills;
    fills.clear();

    gw_->drain_fills(action_date != 0 ? action_date : trading_date, action_time_ms, fills);
    for (auto& tr : fills) engine_->on_trade(tr, ITradeGateway::TdState::Live);
}

size_t XtHftBacktestDriver::active_orders() const {
    return gw_->live_orders();
}

void XtHftBacktestDriver::flush_pending(std::uint32_t grace_ms) {

    gw_->begin_eof_closing();

    thread_local std::vector<XtTradeRecord> fills;
    fills.clear();

    const std::uint32_t last_d  = gw_->last_clock_date();
    const std::uint32_t last_t  = gw_->last_clock_time_ms();
    const std::uint32_t last_ad = gw_->last_clock_action_date();
    if (last_d == 0 && last_t == 0) {

        gw_->drop_pending_fills();
        return;
    }

    std::uint32_t eof_ad = last_ad;
    std::uint32_t eof_t  = last_t;
    if (grace_ms > 0) {
        const std::int64_t last_ns = xt_time::to_ns(last_ad, last_t);
        const std::int64_t eof_ns  = last_ns +
                                     static_cast<std::int64_t>(grace_ms) * 1'000'000LL;
        xt_time::from_ns(eof_ns, eof_ad, eof_t);
    }

    gw_->advance_clock_only(eof_ad, eof_t, last_d);

    thread_local std::vector<XtOrderExecUpdate> upds;
    upds.clear();
    gw_->drain_order_updates(upds);
    for (auto& u : upds) engine_->on_order_update(u);
    gw_->drain_fills(eof_ad, eof_t, fills);
    const std::size_t drained = fills.size();
    for (auto& tr : fills) engine_->on_trade(tr, ITradeGateway::TdState::Live);

    fills.clear();
    const std::size_t completed = gw_->complete_eof_delivered_terminal_fills(fills);
    for (auto& tr : fills) engine_->on_trade(tr, ITradeGateway::TdState::Live);

    const std::size_t dropped = gw_->drop_pending_fills();

    const std::size_t lost_intransit = gw_->discard_in_transit_orders();
    if (lost_intransit > 0) {

        upds.clear();
        gw_->drain_order_updates(upds);
        for (auto& u : upds) engine_->on_order_update(u);
    }

    const std::size_t resting_canceled = gw_->synthesize_eof_delivered_cancels();
    if (resting_canceled > 0) {
        upds.clear();
        gw_->drain_order_updates(upds);
        for (auto& u : upds) engine_->on_order_update(u);
    }

    if (dropped > 0 || lost_intransit > 0 || resting_canceled > 0 || completed > 0) {
        XtLogger::info_m("BT_EOF",
                         "EOF strict grace_ms={}: drained {} fill, completed {} "
                         "delivered-terminal fill(s), dropped {} unmatured pending fill(s), "
                         "lost_in_transit={} order(s) (cum={}), resting_canceled={} order(s)",
                         grace_ms, drained, completed, dropped,
                         lost_intransit, gw_->lost_in_transit_count(),
                         resting_canceled);
    }
}

}
