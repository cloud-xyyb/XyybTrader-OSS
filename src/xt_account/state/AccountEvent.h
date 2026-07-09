#pragma once

#include <cstdint>

/// @brief reducer 写路径的账户事件 DTO。
///
/// payload 使用 reducer 侧 fixed-point 口径；TD 回报由 ingest/AccountEventCodec.h
/// 转换成本类型，store 内部入口也会直接构造它。
///
/// @warning `code` 是 non-owning 指针；带 code 的事件只允许在源对象存活期内同步
///          apply，不得跨线程、跨队列或跨对象生命周期保存。
struct XtAccountEvent {
    enum class Type : uint8_t {
        Invalid,
        SessionBegin,
        Reset,
        SetCash,
        SetPosition,
        MarketTick,
        OrderAccepted,
        OrderExecUpdate,
        OrderFilled,
        OrderCanceled,
        OrderRejected,
        DailySettlement,
        CashTransfer,
        TradeEx,
    };

    Type type = Type::Invalid;
    const char* code = nullptr;
    uint64_t order_id = 0;

    uint32_t trading_date = 0;

    union {
        // SetCash / CashTransfer：资金事件；SetCash 写现金绝对值，CashTransfer 累加现金变动。
        struct {
            int64_t amount;
        } cash;

        // SetPosition：broker/recovery 持仓真值；last_price 只作为 MarkPriceTable 的 mark hint。
        struct {
            int64_t long_qty;
            int64_t long_avg_price;
            int64_t short_qty;
            int64_t short_avg_price;
            int64_t last_price;
            int64_t long_today;
            int64_t short_today;
            int64_t broker_long_frozen_fp;
            int64_t broker_short_frozen_fp;
            int64_t broker_frozen_last_update_ms;
        } set_position;

        // MarketTick：store-level 盯市事件；reducer 不消费，AccountStateStore 用它更新 mark。
        struct {
            int64_t price;
        } tick;

        // OrderAccepted：订单通过风控后创建冻结保证金账本项。
        struct {
            int64_t required_margin;
            int64_t per_unit_margin;
        } order_accepted;

        // OrderExecUpdate / OrderFilled / OrderCanceled / OrderRejected：订单状态回报；终态释放冻结。
        struct {
            int64_t filled;
            int64_t remaining;
            bool is_final;
        } order_state;

        // DailySettlement：结算确认；同时调整 realized、cash 和 fees。
        struct {
            int64_t realized_delta;
            int64_t settlement_fee;
        } daily_settlement;

        // TradeEx：成交入账；更新持仓、均价、手续费和 realized PnL。
        struct {
            bool is_buy;
            bool is_close;
            bool is_today;
            int64_t qty;
            int64_t price;
            int64_t fee;
        } trade_ex;
    } payload = {};

    static XtAccountEvent session_begin(uint32_t date) {
        XtAccountEvent ev;
        ev.type = Type::SessionBegin;
        ev.trading_date = date;
        return ev;
    }

    static XtAccountEvent reset() {
        XtAccountEvent ev;
        ev.type = Type::Reset;
        return ev;
    }

    static XtAccountEvent set_cash(int64_t cash) {
        XtAccountEvent ev;
        ev.type = Type::SetCash;
        ev.payload.cash.amount = cash;
        return ev;
    }

    static XtAccountEvent set_position(const char* code,
                                       int64_t long_qty,
                                       int64_t long_avg_price,
                                       int64_t short_qty,
                                       int64_t short_avg_price,
                                       int64_t last_price,
                                       int64_t long_today,
                                       int64_t short_today,
                                       int64_t broker_long_frozen_fp  = 0,
                                       int64_t broker_short_frozen_fp = 0,
                                       int64_t broker_frozen_last_update_ms = 0) {
        XtAccountEvent ev;
        ev.type = Type::SetPosition;
        ev.code = code;
        ev.payload.set_position.long_qty        = long_qty;
        ev.payload.set_position.long_avg_price  = long_avg_price;
        ev.payload.set_position.short_qty       = short_qty;
        ev.payload.set_position.short_avg_price = short_avg_price;
        ev.payload.set_position.last_price      = last_price;
        ev.payload.set_position.long_today      = long_today;
        ev.payload.set_position.short_today     = short_today;
        ev.payload.set_position.broker_long_frozen_fp  = broker_long_frozen_fp;
        ev.payload.set_position.broker_short_frozen_fp = broker_short_frozen_fp;
        ev.payload.set_position.broker_frozen_last_update_ms = broker_frozen_last_update_ms;
        return ev;
    }

    static XtAccountEvent market_tick(const char* code, int64_t price) {
        XtAccountEvent ev;
        ev.type = Type::MarketTick;
        ev.code = code;
        ev.payload.tick.price = price;
        return ev;
    }

    static XtAccountEvent order_accepted(uint64_t order_id,
                                         int64_t required_margin,
                                         int64_t per_unit_margin) {
        XtAccountEvent ev;
        ev.type = Type::OrderAccepted;
        ev.order_id = order_id;
        ev.payload.order_accepted.required_margin = required_margin;
        ev.payload.order_accepted.per_unit_margin = per_unit_margin;
        return ev;
    }

    static XtAccountEvent order_update(uint64_t order_id,
                                       int64_t filled,
                                       int64_t remaining,
                                       bool is_final) {
        XtAccountEvent ev;
        ev.type = Type::OrderExecUpdate;
        ev.order_id = order_id;
        ev.payload.order_state.filled    = filled;
        ev.payload.order_state.remaining = remaining;
        ev.payload.order_state.is_final  = is_final;
        return ev;
    }

    static XtAccountEvent order_filled(uint64_t order_id,
                                       int64_t filled,
                                       int64_t remaining) {
        XtAccountEvent ev;
        ev.type = Type::OrderFilled;
        ev.order_id = order_id;
        ev.payload.order_state.filled    = filled;
        ev.payload.order_state.remaining = remaining;
        ev.payload.order_state.is_final  = true;
        return ev;
    }

    static XtAccountEvent order_canceled(uint64_t order_id,
                                         int64_t filled,
                                         int64_t remaining) {
        XtAccountEvent ev;
        ev.type = Type::OrderCanceled;
        ev.order_id = order_id;
        ev.payload.order_state.filled    = filled;
        ev.payload.order_state.remaining = remaining;
        ev.payload.order_state.is_final  = true;
        return ev;
    }

    static XtAccountEvent order_rejected(uint64_t order_id,
                                         int64_t filled,
                                         int64_t remaining) {
        XtAccountEvent ev;
        ev.type = Type::OrderRejected;
        ev.order_id = order_id;
        ev.payload.order_state.filled    = filled;
        ev.payload.order_state.remaining = remaining;
        ev.payload.order_state.is_final  = true;
        return ev;
    }

    static XtAccountEvent daily_settlement(int64_t realized_delta, int64_t settlement_fee) {
        XtAccountEvent ev;
        ev.type = Type::DailySettlement;
        ev.payload.daily_settlement.realized_delta = realized_delta;
        ev.payload.daily_settlement.settlement_fee = settlement_fee;
        return ev;
    }

    static XtAccountEvent cash_transfer(int64_t amount) {
        XtAccountEvent ev;
        ev.type = Type::CashTransfer;
        ev.payload.cash.amount = amount;
        return ev;
    }

    static XtAccountEvent trade_ex(const char* code,
                                   bool is_buy,
                                   int64_t qty,
                                   int64_t price,
                                   int64_t fee,
                                   bool is_close,
                                   bool is_today) {
        XtAccountEvent ev;
        ev.type = Type::TradeEx;
        ev.code = code;
        ev.payload.trade_ex.is_buy   = is_buy;
        ev.payload.trade_ex.is_close = is_close;
        ev.payload.trade_ex.is_today = is_today;
        ev.payload.trade_ex.qty      = qty;
        ev.payload.trade_ex.price    = price;
        ev.payload.trade_ex.fee      = fee;
        return ev;
    }
};
