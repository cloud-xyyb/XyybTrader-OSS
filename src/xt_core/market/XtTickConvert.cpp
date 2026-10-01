#include "XtTickConvert.h"

#include <cstring>

// to_core() 是 MD 线程热路径；固定数组来自外部数据源，复制后必须强制补 null terminator。
// to_raw() 只用于 debug/export/replay 边界。

namespace xt_tick_convert {

XtTick to_core(const XtRawTick& raw) {
    XtTick out;
    std::memcpy(out.exchg, raw.exchg, sizeof(out.exchg));
    std::memcpy(out.code, raw.code, sizeof(out.code));
    out.exchg[sizeof(out.exchg) - 1] = '\0';
    out.code[sizeof(out.code) - 1] = '\0';

    out.price_fp = xt_tick_fp::to_fp(raw.price, xt_tick_fp::kPriceScale);
    out.open_fp = xt_tick_fp::to_fp(raw.open, xt_tick_fp::kPriceScale);
    out.high_fp = xt_tick_fp::to_fp(raw.high, xt_tick_fp::kPriceScale);
    out.low_fp = xt_tick_fp::to_fp(raw.low, xt_tick_fp::kPriceScale);
    out.settle_fp = xt_tick_fp::to_fp(raw.settle, xt_tick_fp::kPriceScale);

    out.upper_limit_fp = xt_tick_fp::to_fp(raw.upper_limit, xt_tick_fp::kPriceScale);
    out.lower_limit_fp = xt_tick_fp::to_fp(raw.lower_limit, xt_tick_fp::kPriceScale);

    out.total_volume_fp = xt_tick_fp::to_fp(raw.total_volume, xt_tick_fp::kQtyScale);
    out.volume_fp = xt_tick_fp::to_fp(raw.volume, xt_tick_fp::kQtyScale);
    out.total_turnover_fp = xt_tick_fp::to_fp(raw.total_turnover, xt_tick_fp::kTurnoverScale);
    out.turnover_fp = xt_tick_fp::to_fp(raw.turnover, xt_tick_fp::kTurnoverScale);
    out.open_interest_fp = xt_tick_fp::to_fp(raw.open_interest, xt_tick_fp::kInterestScale);
    out.diff_interest_fp = xt_tick_fp::to_fp(raw.diff_interest, xt_tick_fp::kInterestScale);

    out.trading_date = raw.trading_date;
    out.action_date = raw.action_date;
    out.action_time = raw.action_time;
    out.kind = raw.kind;

    out.pre_close_fp = xt_tick_fp::to_fp(raw.pre_close, xt_tick_fp::kPriceScale);
    out.pre_settle_fp = xt_tick_fp::to_fp(raw.pre_settle, xt_tick_fp::kPriceScale);
    out.pre_interest_fp = xt_tick_fp::to_fp(raw.pre_interest, xt_tick_fp::kInterestScale);

    for (int i = 0; i < XtTick::DEPTH; ++i) {
        out.bid_price_fp[i] = xt_tick_fp::to_fp(raw.bid_prices[i], xt_tick_fp::kPriceScale);
        out.ask_price_fp[i] = xt_tick_fp::to_fp(raw.ask_prices[i], xt_tick_fp::kPriceScale);
        out.bid_qty_fp[i] = xt_tick_fp::to_fp(raw.bid_qty[i], xt_tick_fp::kQtyScale);
        out.ask_qty_fp[i] = xt_tick_fp::to_fp(raw.ask_qty[i], xt_tick_fp::kQtyScale);
    }

    return out;
}

XtRawTick to_raw(const XtTick& core) {
    XtRawTick out;
    std::memcpy(out.exchg, core.exchg, sizeof(out.exchg));
    std::memcpy(out.code, core.code, sizeof(out.code));
    out.exchg[sizeof(out.exchg) - 1] = '\0';
    out.code[sizeof(out.code) - 1] = '\0';

    out.price = xt_tick_fp::from_fp(core.price_fp, xt_tick_fp::kPriceScale);
    out.open = xt_tick_fp::from_fp(core.open_fp, xt_tick_fp::kPriceScale);
    out.high = xt_tick_fp::from_fp(core.high_fp, xt_tick_fp::kPriceScale);
    out.low = xt_tick_fp::from_fp(core.low_fp, xt_tick_fp::kPriceScale);
    out.settle = xt_tick_fp::from_fp(core.settle_fp, xt_tick_fp::kPriceScale);

    out.upper_limit = xt_tick_fp::from_fp(core.upper_limit_fp, xt_tick_fp::kPriceScale);
    out.lower_limit = xt_tick_fp::from_fp(core.lower_limit_fp, xt_tick_fp::kPriceScale);

    out.total_volume = xt_tick_fp::from_fp(core.total_volume_fp, xt_tick_fp::kQtyScale);
    out.volume = xt_tick_fp::from_fp(core.volume_fp, xt_tick_fp::kQtyScale);
    out.total_turnover = xt_tick_fp::from_fp(core.total_turnover_fp, xt_tick_fp::kTurnoverScale);
    out.turnover = xt_tick_fp::from_fp(core.turnover_fp, xt_tick_fp::kTurnoverScale);
    out.open_interest = xt_tick_fp::from_fp(core.open_interest_fp, xt_tick_fp::kInterestScale);
    out.diff_interest = xt_tick_fp::from_fp(core.diff_interest_fp, xt_tick_fp::kInterestScale);

    out.trading_date = core.trading_date;
    out.action_date = core.action_date;
    out.action_time = core.action_time;
    out.kind = core.kind;

    out.pre_close = xt_tick_fp::from_fp(core.pre_close_fp, xt_tick_fp::kPriceScale);
    out.pre_settle = xt_tick_fp::from_fp(core.pre_settle_fp, xt_tick_fp::kPriceScale);
    out.pre_interest = xt_tick_fp::from_fp(core.pre_interest_fp, xt_tick_fp::kInterestScale);

    for (int i = 0; i < XtTick::DEPTH; ++i) {
        out.bid_prices[i] = xt_tick_fp::from_fp(core.bid_price_fp[i], xt_tick_fp::kPriceScale);
        out.ask_prices[i] = xt_tick_fp::from_fp(core.ask_price_fp[i], xt_tick_fp::kPriceScale);
        out.bid_qty[i] = xt_tick_fp::from_fp(core.bid_qty_fp[i], xt_tick_fp::kQtyScale);
        out.ask_qty[i] = xt_tick_fp::from_fp(core.ask_qty_fp[i], xt_tick_fp::kQtyScale);
    }
    return out;
}

}  // namespace xt_tick_convert
