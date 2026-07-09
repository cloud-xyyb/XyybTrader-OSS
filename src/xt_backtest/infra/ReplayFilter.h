#pragma once

#include <cstdint>

#include "ReplayFilterCalendar.h"
#include "xt_core/market/XtRollEngine.h"
#include "xt_core/session/XtSessionFilter.h"

/// @brief backtest replay 的 calendar/session/roll 组合过滤器。
///
/// `accepts()` 决定一条 tick 是否进入回测 runtime；`should_dispatch()` 只在
/// 换月过滤启用时决定是否派发给策略。过滤配置在 init 阶段注入
struct ReplayFilter {
    /// @brief ReplayFilter 的三类过滤配置快照。
    struct Config {
        XtBacktestTradingCalendar::Config calendar;
        xt_core::Config session;
        XtRollEngine::Config roll;
    };

    XtBacktestTradingCalendar calendar;
    xt_core::Config session;
    XtRollEngine roll;

    void init(const Config& cfg) {
        calendar.set_config(cfg.calendar);
        session = cfg.session;
        xt_core::normalize(session);
        roll.set_config(cfg.roll);
    }

    bool accepts(uint32_t trading_date, uint32_t action_time, const char* code) const {
        if (calendar.enabled() && trading_date != 0 && !calendar.is_trading_day(trading_date)) {
            return false;
        }
        if (roll.active() && !roll.accepts_tick(trading_date, code)) {
            return false;
        }

        if (xt_core::is_enabled(session) &&
            !xt_core::in_session_closed(action_time, session.sections)) {
            return false;
        }
        return true;
    }

    bool should_dispatch(uint32_t trading_date, const char* code) const {
        if (!roll.active()) return true;
        return roll.should_dispatch_tick(trading_date, code);
    }
};

using CtaReplayFilter = ReplayFilter;
