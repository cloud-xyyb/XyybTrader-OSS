#include "state/AccountReducer.h"

#include "xt_logger/XtLogger.h"

static inline bool has_valid_code(const XtAccountEvent& ev) noexcept {
    return ev.code && ev.code[0] != '\0';
}

void apply_event(AccountState& account, const XtAccountEvent& ev) {
    using T = XtAccountEvent::Type;

    switch (ev.type) {

    case T::Invalid:
        return;

    case T::SessionBegin:
        account.on_session_begin(ev.trading_date);
        return;

    case T::Reset:

        account.reset_positions();
        return;

    case T::SetCash:
        account.set_cash(ev.payload.cash.amount);
        return;

    case T::SetPosition:
        if (!has_valid_code(ev)) return;

        account.set_position(ev.code,
                             ev.payload.set_position.long_qty,
                             ev.payload.set_position.long_avg_price,
                             ev.payload.set_position.short_qty,
                             ev.payload.set_position.short_avg_price,
                             ev.payload.set_position.long_today,
                             ev.payload.set_position.short_today,
                             ev.payload.set_position.broker_long_frozen_fp,
                             ev.payload.set_position.broker_short_frozen_fp,
                             ev.payload.set_position.broker_frozen_last_update_ms);
        return;

    case T::OrderAccepted:
        account.on_order_accepted(ev.order_id,
                                  ev.payload.order_accepted.required_margin,
                                  ev.payload.order_accepted.per_unit_margin);
        return;

    case T::OrderExecUpdate:
        account.on_order_update(ev.order_id,
                                ev.payload.order_state.filled,
                                ev.payload.order_state.remaining,
                                ev.payload.order_state.is_final);
        return;

    case T::OrderFilled:
    case T::OrderCanceled:
    case T::OrderRejected:
        account.on_order_update(ev.order_id,
                                ev.payload.order_state.filled,
                                ev.payload.order_state.remaining,
                                 true);
        return;

    case T::DailySettlement:
        account.on_daily_settlement(ev.payload.daily_settlement.realized_delta,
                                    ev.payload.daily_settlement.settlement_fee);
        return;

    case T::CashTransfer:
        account.on_cash_transfer(ev.payload.cash.amount);
        return;

    case T::TradeEx:
        if (!has_valid_code(ev)) return;
        account.on_trade_ex(ev.code,
                            ev.payload.trade_ex.is_buy,
                            ev.payload.trade_ex.qty,
                            ev.payload.trade_ex.price,
                            ev.payload.trade_ex.fee,
                            ev.payload.trade_ex.is_close,
                            ev.payload.trade_ex.is_today);
        return;

    case T::MarketTick:

        return;

    }
}
