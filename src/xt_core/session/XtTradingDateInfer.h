#pragma once

#include <cassert>
#include <cstdint>
#include <functional>
#include <utility>

#include "xt_util/time/XtTime.h"

/// @brief 从自然日行情时间推断交易所 trading_day。
///
/// `infer()` 接收 tick/bar 的自然日 `action_date` 和 `action_time`；
/// 夜盘起点之后的事件归入下一交易日，日盘事件归入当天。
/// 下一交易日由 `init()` 注入的交易日历回调提供。
///
/// 已经带有 broker TradingDay 的数据应直接透传，不走本推断器。
class XtTradingDateInfer {
public:
    using NextTradingDayFn = std::function<uint32_t(uint32_t)>;

    void init(int session_offset_minutes, NextTradingDayFn next_td_fn) {
        assert(next_td_fn && "XtTradingDateInfer: next_trading_day callback is required "
                              "(inject XtBacktestTradingCalendar::next_trading_day or equivalent)");
        next_td_fn_ = std::move(next_td_fn);
        if (session_offset_minutes > 0) {
            const int total_min = 24 * 60 - session_offset_minutes;
            offset_hhmm_ = static_cast<uint32_t>((total_min / 60) * 100 + (total_min % 60));
        } else {
            offset_hhmm_ = 0;
        }
    }

    bool enabled() const { return offset_hhmm_ > 0; }

    uint32_t infer(uint32_t action_date, uint32_t action_time) const {
        if (offset_hhmm_ == 0) return action_date;
        const uint32_t hhmm = xt_time::action_time_to_hhmm(action_time);
        if (hhmm >= offset_hhmm_) {
            assert(next_td_fn_ && "infer() called before init() with valid callback");
            return next_td_fn_(action_date);
        }
        return action_date;
    }

    static uint32_t prev_weekday(uint32_t date_yyyymmdd) {
        uint32_t y = date_yyyymmdd / 10000;
        uint32_t m = (date_yyyymmdd / 100) % 100;
        uint32_t d = date_yyyymmdd % 100;
        static const int t[] = {0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4};
        uint32_t yy = y;
        if (m < 3) yy -= 1;
        int dow = static_cast<int>((yy + yy/4 - yy/100 + yy/400 + t[m-1] + d) % 7);

        int skip = 1;
        if (dow == 1) skip = 3;
        else if (dow == 0) skip = 2;
        static const uint32_t days_in_month[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
        if (d > static_cast<uint32_t>(skip)) {
            d -= static_cast<uint32_t>(skip);
        } else {
            const uint32_t borrow = static_cast<uint32_t>(skip) - d;
            if (m == 1) { m = 12; y -= 1; } else { m -= 1; }
            uint32_t dim = days_in_month[m];
            if (m == 2 && ((y%4==0 && y%100!=0) || y%400==0)) dim = 29;
            d = dim - borrow;
        }
        return y * 10000 + m * 100 + d;
    }

private:
    uint32_t         offset_hhmm_ = 0;
    NextTradingDayFn next_td_fn_;
};
