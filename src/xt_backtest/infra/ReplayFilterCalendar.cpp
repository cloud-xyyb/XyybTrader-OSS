#include "ReplayFilterCalendar.h"

#include "xt_config/yaml/ConfigYamlUtil.h"
#include "xt_util/time/XtTimeParse.h"
#include "xt_util/string/XtStringUtil.h"

#include <yaml-cpp/yaml.h>

#include <fstream>

namespace {
bool parse_date_strict(const std::string& s, uint32_t& out) {
    out = XtTimeParse::parse_date_only(s);
    if (out != 0) return true;

    try {
        out = static_cast<uint32_t>(std::stoul(s));
        return out != 0;
    } catch (...) {
        return false;
    }
}

void add_dates_from_yaml_node(const YAML::Node& node, std::unordered_set<uint32_t>& out) {
    if (!node || node.IsNull()) return;
    if (node.IsSequence()) {
        for (const auto& it : node) {
            uint32_t d = XtConfigYamlUtil::parse_date_node(it);
            if (d) out.insert(d);
        }
        return;
    }
    uint32_t d = XtConfigYamlUtil::parse_date_node(node);
    if (d) out.insert(d);
}
}

bool XtBacktestTradingCalendar::parse_date_token(const std::string& token, uint32_t& out) {
    std::string v = XtStringUtil::trim(token);
    if (v.empty()) return false;
    return parse_date_strict(v, out);
}

XtBacktestTradingCalendar::Config parse_trading_calendar_config(const YAML::Node& node) {
    XtBacktestTradingCalendar::Config cfg;
    if (!node || node.IsNull()) return cfg;

    cfg.enabled = node["enabled"] ? node["enabled"].as<bool>() : true;
    if (!cfg.enabled) return cfg;

    add_dates_from_yaml_node(node["days"], cfg.days);
    add_dates_from_yaml_node(node["dates"], cfg.days);
    add_dates_from_yaml_node(node["trading_days"], cfg.days);
    add_dates_from_yaml_node(node["include"], cfg.days);

    if (node["exclude"]) {
        std::unordered_set<uint32_t> ex;
        add_dates_from_yaml_node(node["exclude"], ex);
        for (uint32_t d : ex) cfg.days.erase(d);
    }

    if (node["path"]) {
        try {
            std::string path = node["path"].as<std::string>();
            std::ifstream in(path);
            if (in.good()) {
                std::string line;
                while (std::getline(in, line)) {
                    std::string s = XtStringUtil::trim(line);
                    if (s.empty() || s[0] == '#') continue;
                    size_t pos = s.find_first_of(", \t");
                    std::string token = (pos == std::string::npos) ? s : s.substr(0, pos);
                    uint32_t d = 0;
                    if (parse_date_strict(XtStringUtil::trim(token), d)) cfg.days.insert(d);
                }
            }
        } catch (...) {
        }
    }

    cfg.enabled = cfg.enabled && !cfg.days.empty();
    return cfg;
}

void XtBacktestTradingCalendar::set_config(const Config& cfg) {
    enabled_ = cfg.enabled && !cfg.days.empty();
    days_ = cfg.days;
}

bool XtBacktestTradingCalendar::is_trading_day(uint32_t date) const {

    if (!enabled_ || days_.empty() || date == 0) return true;
    return days_.find(date) != days_.end();
}

uint32_t XtBacktestTradingCalendar::next_trading_day(uint32_t date) const {

    auto advance = [](uint32_t d) -> uint32_t {
        int y = d / 10000, m = (d / 100) % 100, dd = d % 100;
        static const int mdays[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
        const int max_d = (m == 2 && y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 29 : mdays[m];
        if (dd < max_d) return d + 1;
        if (m < 12) return y * 10000 + (m + 1) * 100 + 1;
        return (y + 1) * 10000 + 100 + 1;
    };

    auto day_of_week = [](uint32_t d) -> int {
        static const int t[] = {0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4};
        int y = d / 10000, m = (d / 100) % 100, dd = d % 100;
        if (m < 3) --y;
        return (y + y/4 - y/100 + y/400 + t[m-1] + dd) % 7;
    };

    uint32_t nd = advance(date);
    for (int i = 0; i < 10; ++i) {
        if (enabled_ && !days_.empty()) {
            if (days_.find(nd) != days_.end()) return nd;
        } else {

            const int dow = day_of_week(nd);
            if (dow != 0 && dow != 6) return nd;
        }
        nd = advance(nd);
    }
    return nd;
}
