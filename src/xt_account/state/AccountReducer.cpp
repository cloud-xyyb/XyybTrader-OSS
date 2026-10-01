#include "state/AccountReducer.h"

#include "xt_logger/XtLogger.h"

// AccountState 的持仓 key 必须是非空 code；nullptr/空串事件在 reducer 边界丢弃，
// 避免异常回报向 positions map 写入空键。
static inline bool has_valid_code(const XtAccountEvent& ev) noexcept {
    return ev.code && ev.code[0] != '\0';
}

void apply_event(AccountState& account, const XtAccountEvent& ev) {
    using T = XtAccountEvent::Type;

    switch (ev.type) {

    case T::Invalid:
        return;

    // -----------------------------------------------------------------------
    // 会话与账户初始化（session & account initialization）
    // -----------------------------------------------------------------------
    case T::SessionBegin:
        account.on_session_begin();
        return;

    case T::Reset:
        // 仓位 recovery 擦除；账务域（cash/realized/fees）跨 recovery 保留。
        account.reset_positions();
        return;

    case T::SetCash:
        account.set_cash(ev.payload.cash.amount);
        return;

    case T::SetPosition:
        if (!has_valid_code(ev)) return;
        // last_price 不进 AccountState：估值口径的 mark 与成本价 fallback 都归
        // xt_account::AccountStateStore 的 MarkPriceTable 管，账本只存持仓与均价。
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

    // -----------------------------------------------------------------------
    // 订单生命周期（order lifecycle）
    // -----------------------------------------------------------------------
    case T::OrderAccepted:
        account.on_order_accepted(ev.order_id,
                                  ev.payload.order_accepted.required_margin,
                                  ev.payload.order_accepted.per_unit_margin);
        return;

    case T::OrderExecUpdate:
        account.on_order_update(ev.order_id,
                                ev.payload.order_state.filled,
                                ev.payload.order_state.is_final);
        return;

    // -----------------------------------------------------------------------
    // 结算与资金变动（settlement & cash movement）
    // -----------------------------------------------------------------------
    case T::DailySettlement:
        account.on_daily_settlement(ev.payload.daily_settlement.realized_delta,
                                    ev.payload.daily_settlement.settlement_fee);
        return;

    case T::CashTransfer:
        account.on_cash_transfer(ev.payload.cash.amount);
        return;

    // -----------------------------------------------------------------------
    // 成交入账（trade fill accounting）
    // -----------------------------------------------------------------------
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

    // -----------------------------------------------------------------------
    // store 级消费事件（reducer no-op）
    // -----------------------------------------------------------------------
    case T::MarketTick:
        // 盯市价格由 AccountStateStore::on_event_before_apply 写入 store 级
        // MarkPriceTable；账本内核不直接消费行情事件，此分支为 no-op。
        return;

    } // switch
}
