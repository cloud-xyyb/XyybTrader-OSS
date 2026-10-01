#include "XtHftBacktestDriver.h"

#include "xt_backtest/hft/XtHftBacktestOrderGateway.h"
#include "xt_engine/hft/XtHftEngine.h"
#include "xt_util/time/XtTime.h"  // xt_time::to_ns / from_ns
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
    // Grid clock 只表示时间流逝；这里必须走 no-tick 路径，避免消耗
    // ack_delay_ticks / ttl_ticks 等按 market tick 计数的预算。
    gw_->advance_clock_only(action_date, action_time_ms, trading_date);
    // 到期的 fill-driven update 先于同 visible_at 的 fill 消费（live 回报序同款）。
    thread_local std::vector<XtOrderExecUpdate> upds;
    upds.clear();
    gw_->drain_order_updates(upds);
    for (auto& u : upds) engine_->on_order_update(u);
    thread_local std::vector<XtTradeRecord> fills;
    fills.clear();
    // pending 可见性轴 = event 轴（P3 已切）：drain 截止用自然日对。
    gw_->drain_fills(action_date != 0 ? action_date : trading_date, action_time_ms, fills);
    for (auto& tr : fills) engine_->on_trade(tr, ITradeGateway::TdState::Live);
}

size_t XtHftBacktestDriver::active_orders() const {
    return gw_->live_orders();
}

void XtHftBacktestDriver::flush_pending(std::uint32_t grace_ms) {
    // EOF 收尾栅栏：置位后 submit 一律本地拒。合成 Canceled 终态经 on_order 可触发
    // 策略重挂，而 EOF 后无 tick 可撮合，新单会重新冻结并在 flush 返回时留活单。
    gw_->begin_eof_closing();

    thread_local std::vector<XtTradeRecord> fills;
    fills.clear();

    // Gateway 维护统一 clock；market tick 与 grid clock 都会更新这个截止基准。
    const std::uint32_t last_d  = gw_->last_clock_date();
    const std::uint32_t last_t  = gw_->last_clock_time_ms();
    const std::uint32_t last_ad = gw_->last_clock_action_date();
    if (last_d == 0 && last_t == 0) {
        // 未推进过 clock 时没有可判定的 EOF window；直接清理 pending fills。
        gw_->drop_pending_fills();
        return;
    }

    // grace_ms 只扩展 EOF cutoff；0 表示严格停在最后一次 gateway clock。
    // grace 数学在 event 轴（自然日）上做——与 pending 可见性同轴，跨午夜不回绕。
    std::uint32_t eof_ad = last_ad;
    std::uint32_t eof_t  = last_t;
    if (grace_ms > 0) {
        const std::int64_t last_ns = xt_time::to_ns(last_ad, last_t);
        const std::int64_t eof_ns  = last_ns +
                                     static_cast<std::int64_t>(grace_ms) * 1'000'000LL;
        xt_time::from_ns(eof_ns, eof_ad, eof_t);
    }

    // EOF 收尾也只推进时间，不消耗 gateway 的 market tick 预算；
    // trading 戳保持最后交易日（grace 不跨交易日）。
    gw_->advance_clock_only(eof_ad, eof_t, last_d);

    // 只派发 EOF cutoff 内已经可见的 update/fills（update 先，同 live 回报序）。
    thread_local std::vector<XtOrderExecUpdate> upds;
    upds.clear();
    gw_->drain_order_updates(upds);
    for (auto& u : upds) engine_->on_order_update(u);
    gw_->drain_fills(eof_ad, eof_t, fills);
    const std::size_t drained = fills.size();
    for (auto& tr : fills) engine_->on_trade(tr, ITradeGateway::TdState::Live);

    // 机制 3（EOF 补交付）：盘中已派终态（TTL/延迟撤单于 EOF fence 置位前的普通 clock
    // 成熟）的单，其未可见 fill 越过 cutoff 补交付到 engine，完成 late-fill after terminal
    // 的账本闭合（否则该 fill 被下方 strict-drop、策略/账户收终态却无对应 fill = 分裂）。
    // EOF 栅栏已置位 → 补交付触发的策略重挂被拒（F3 既有）。
    fills.clear();
    const std::size_t completed = gw_->complete_eof_delivered_terminal_fills(fills);
    for (auto& tr : fills) engine_->on_trade(tr, ITradeGateway::TdState::Live);

    // cutoff 外 fill 不进入 strategy / reporter / PnL。
    const std::size_t dropped = gw_->drop_pending_fills();

    // order-delay wheel 内尚未到达 match engine 的 order 只计 lost-in-transit。
    const std::size_t lost_intransit = gw_->discard_in_transit_orders();
    if (lost_intransit > 0) {
        // wheel 单的合成 Canceled 终态必须派发（frozen 释放链），
        // 区别于未成熟 fill 的 strict-drop。
        upds.clear();
        gw_->drain_order_updates(upds);
        for (auto& u : upds) engine_->on_order_update(u);
    }

    // 尚未收到 delivered 终态的在途单（resting GTC + 终态回报被 strict-drop 的
    // 全成交/部分成交单）EOF 后无终态 → 策略收不到挂单结局、frozen 不释放；逐单按
    // delivered 口径合成 Canceled 派发（filled = 已派发累计，leaves = qty − delivered）。
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

} // namespace xt_backtest::hft
