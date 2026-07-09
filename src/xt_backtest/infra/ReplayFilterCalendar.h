#pragma once

#include <cstdint>
#include <string>
#include <unordered_set>

/// @brief 回测交易日历过滤器（backtest trading-day calendar filter）。
///
/// 用于回测交易日集合判断；未启用或集合为空时放行所有日期。
/// `next_trading_day()` 在日历未启用时 fallback 到跳过周末的工作日规则。
class XtBacktestTradingCalendar {
public:

    struct Config {
        bool enabled = false;
        std::unordered_set<uint32_t> days;
    };

    XtBacktestTradingCalendar() = default;
    explicit XtBacktestTradingCalendar(const Config& cfg) { set_config(cfg); }
    void set_config(const Config& cfg);
    bool enabled() const { return enabled_; }
    bool is_trading_day(uint32_t date) const;

    uint32_t next_trading_day(uint32_t date) const;

private:

    static bool parse_date_token(const std::string& token, uint32_t& out);

    bool enabled_ = false;
    std::unordered_set<uint32_t> days_;
};

namespace YAML { class Node; }

XtBacktestTradingCalendar::Config parse_trading_calendar_config(const YAML::Node& node);
