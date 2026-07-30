#include "xt_core/market/XtRollEngine.h"

#include "xt_util/string/XtStringUtil.h"
#include "xt_util/time/XtTime.h"

#include <algorithm>
#include <cctype>
#include <map>
#include <unordered_set>

namespace {

std::string extract_product_key(const std::string& in) {
    const std::string body = XtStringUtil::strip_dot_prefix(
        XtStringUtil::upper_without_space(in));
    std::string out;
    out.reserve(body.size());
    for (char c : body) {
        if (std::isalpha(static_cast<unsigned char>(c))) {
            out.push_back(c);
        } else if (!out.empty()) {
            break;
        }
    }
    return out;
}

bool parse_main_alias_product(const std::string& in, std::string& product_out) {
    const std::string s = XtStringUtil::upper_without_space(in);
    const size_t p = s.find('@');
    if (p == std::string::npos) return false;
    const std::string rhs = s.substr(p + 1);
    if (rhs != "MAIN") return false;
    const std::string lhs = s.substr(0, p);
    const std::string product = extract_product_key(lhs);
    if (product.empty()) return false;
    product_out = product;
    return true;
}

bool contains_symbol(const std::vector<std::string>& items, const std::string& v) {
    return std::find(items.begin(), items.end(), v) != items.end();
}

}

void XtRollEngine::Config::validate(std::vector<std::string>& errors) const {
    if (!active) return;
    if (switches.empty()) {
        errors.push_back("backtest.roll.active=true 时必须提供可解析的 backtest.roll.path");
        return;
    }
    if (window_before_days < 0 || window_after_days < 0) {
        errors.push_back("backtest.roll.window_before_days/window_after_days 不能为负");
    }
    uint32_t prev_date = 0;
    for (size_t i = 0; i < switches.size(); ++i) {
        const auto& sw = switches[i];
        if (sw.date == 0) {
            errors.push_back("backtest.roll.path 中 date 必须是有效日期");
            continue;
        }
        if (!xt_time::is_valid_yyyymmdd(sw.date)) {
            errors.push_back("backtest.roll.path 中 date 不是合法日历日期: "
                             + std::to_string(sw.date));
            continue;
        }
        if (sw.known_on_date != 0 && !xt_time::is_valid_yyyymmdd(sw.known_on_date)) {
            errors.push_back("backtest.roll.path 中 known_on 不是合法日历日期: "
                             + std::to_string(sw.known_on_date));
            continue;
        }
        if (sw.symbol.empty()) {
            errors.push_back("backtest.roll.path 中 symbol 不能为空");
        }
        if (prev_date != 0 && sw.date <= prev_date) {
            errors.push_back("backtest.roll.path 中 date 必须严格递增");
        }
        prev_date = sw.date;

        if (sw.known_on_date != 0 && sw.known_on_date >= sw.date) {
            errors.push_back("backtest.roll.path 中 known_on 必须早于 date");
        }
        if (window_before_days > 0) {
            if (sw.known_on_date == 0) {
                errors.push_back("window_before_days>0 时，roll.path 中 known_on 必填");
                continue;
            }
            if (sw.known_on_date >
                xt_time::yyyymmdd_add_days(sw.date, -static_cast<int64_t>(window_before_days))) {
                errors.push_back("roll.path 中 known_on 必须早于等于 (date-window_before_days)");
            }
        }

        if (i + 1 < switches.size() && (window_before_days > 0 || window_after_days > 0)) {
            const int64_t gap = xt_time::yyyymmdd_diff_days(sw.date, switches[i + 1].date);
            bool overlap = false;
            if (window_before_days > 0 && window_after_days > 0) {
                overlap = gap <= static_cast<int64_t>(window_before_days + window_after_days);
            } else if (window_before_days > 0) {
                overlap = gap < static_cast<int64_t>(window_before_days);
            } else {
                overlap = gap <= static_cast<int64_t>(window_after_days);
            }
            if (overlap) {
                errors.push_back("相邻换月间距（" + std::to_string(gap)
                    + "自然日）与窗口（before=" + std::to_string(window_before_days)
                    + ", after=" + std::to_string(window_after_days)
                    + "）重叠或截断");
            }
        }
    }
}

void XtRollEngine::set_config(const Config& cfg) {
    active_ = cfg.active && !cfg.switches.empty();
    logical_symbol_ = cfg.logical_symbol;
    logical_product_.clear();
    (void)parse_main_alias_product(logical_symbol_, logical_product_);
    window_before_days_ = std::max(0, cfg.window_before_days);
    window_after_days_ = std::max(0, cfg.window_after_days);
    dispatch_parallel_ticks_ = cfg.dispatch_parallel_ticks;
    switches_ = cfg.switches;
    std::sort(switches_.begin(),
              switches_.end(),
              [](const SwitchPoint& a, const SwitchPoint& b) { return a.date < b.date; });
    all_symbols_.clear();
    std::unordered_set<std::string> seen;
    for (const auto& sw : switches_) {
        if (sw.symbol.empty()) continue;
        if (seen.insert(sw.symbol).second) all_symbols_.push_back(sw.symbol);
    }
    if (switches_.empty()) active_ = false;

    cached_date_ = 0;
    cached_result_ = {};
}

int XtRollEngine::active_index(uint32_t date) const {
    if (!active_ || switches_.empty() || date == 0) return -1;

    int lo = 0;
    int hi = static_cast<int>(switches_.size()) - 1;
    int ans = -1;
    while (lo <= hi) {
        const int mid = lo + (hi - lo) / 2;
        if (switches_[mid].date <= date) {
            ans = mid;
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }
    return ans;
}

bool XtRollEngine::can_pre_roll_to(size_t switch_idx, uint32_t date) const {
    if (switch_idx >= switches_.size()) return false;

    const uint32_t known_on = switches_[switch_idx].known_on_date;
    if (known_on == 0) return window_before_days_ == 0;
    return date >= known_on;
}

XtRollEngine::ResolveResult XtRollEngine::resolve(uint32_t date) const {

    if (date != 0 && date == cached_date_) return cached_result_;

    ResolveResult out;
    if (!active_) return out;
    const int idx = active_index(date);
    if (idx < 0) return out;

    out.active = true;
    out.primary_symbol = switches_[static_cast<size_t>(idx)].symbol;
    if (!out.primary_symbol.empty()) out.accepted_symbols.push_back(out.primary_symbol);

    for (int i = 1; i <= idx; ++i) {
        const auto& sw = switches_[static_cast<size_t>(i)];
        if (sw.old_close != 0.0 || sw.new_close != 0.0) {
            out.price_adj += (sw.new_close - sw.old_close);
        }
    }

    if (window_before_days_ > 0 && static_cast<size_t>(idx + 1) < switches_.size()) {
        const auto& next = switches_[static_cast<size_t>(idx + 1)];
        const uint32_t start =
            xt_time::yyyymmdd_add_days(next.date, -static_cast<int64_t>(window_before_days_));
        if (date >= start && date < next.date && can_pre_roll_to(static_cast<size_t>(idx + 1), date)) {
            if (!next.symbol.empty() && !contains_symbol(out.accepted_symbols, next.symbol)) {
                out.accepted_symbols.push_back(next.symbol);
                out.in_roll_window = true;
            }
        }
    }

    if (window_after_days_ > 0 && idx > 0) {
        const auto& cur = switches_[static_cast<size_t>(idx)];
        const auto& prev = switches_[static_cast<size_t>(idx - 1)];
        const uint32_t end =
            xt_time::yyyymmdd_add_days(cur.date, static_cast<int64_t>(window_after_days_));
        if (date >= cur.date && date <= end) {
            if (!prev.symbol.empty() && !contains_symbol(out.accepted_symbols, prev.symbol)) {
                out.accepted_symbols.push_back(prev.symbol);
                out.in_roll_window = true;
            }
        }
    }

    cached_date_ = date;
    cached_result_ = out;
    return out;
}

bool XtRollEngine::accepts_tick(uint32_t date, const char* code) const {
    if (!code || code[0] == '\0') return false;
    const ResolveResult r = resolve(date);
    if (!r.active) return false;
    for (const auto& s : r.accepted_symbols) {
        if (s == code) return true;
    }
    return false;
}

bool XtRollEngine::should_dispatch_tick(uint32_t date, const char* code) const {
    if (!code || code[0] == '\0') return false;
    const ResolveResult r = resolve(date);
    if (!r.active) return false;
    if (dispatch_parallel_ticks_) {

        for (const auto& s : r.accepted_symbols) {
            if (s == code) return true;
        }
        return false;
    }

    return r.primary_symbol == code;
}

bool XtRollEngine::is_parallel_time_valid(uint32_t date, uint32_t action_time,
                                          const char* code, uint32_t primary_action_time) const {
    const ResolveResult r = resolve(date);
    if (!r.in_roll_window) return true;
    if (r.primary_symbol == code) return true;

    if (primary_action_time > 0 && action_time > primary_action_time) {
        return false;
    }
    return true;
}

bool XtRollEngine::is_logical_symbol(const char* code) const {
    if (!code || code[0] == '\0') return false;
    if (!logical_symbol_.empty() && logical_symbol_ == code) return true;

    std::string alias_product;
    if (!parse_main_alias_product(code, alias_product)) return false;
    if (logical_product_.empty()) return false;
    return alias_product == logical_product_;
}

std::vector<std::string> XtRollEngine::symbols_for_logical(const char* code) const {
    if (!active_ || !is_logical_symbol(code)) return {};
    return all_symbols_;
}

std::vector<std::string> XtRollEngine::resolve_symbols(const char* code) const {
    if (!code || code[0] == '\0') return {};
    if (active_ && is_logical_symbol(code)) {
        const auto legs = symbols_for_logical(code);
        if (!legs.empty()) return legs;
    }
    return {std::string(code)};
}

bool XtRollEngine::to_logical(const char* code, std::string& out) const {
    out.clear();
    if (!code || code[0] == '\0') return false;
    if (!active_ || !has_logical_symbol()) return false;
    for (const auto& s : all_symbols_) {
        if (s == code) {
            out = logical_symbol_;
            return !out.empty();
        }
    }
    return false;
}

bool XtRollEngine::to_physical(uint32_t date, const char* logical_code, std::string& out) const {
    out.clear();
    if (!logical_code || logical_code[0] == '\0') return false;
    if (!active_ || !is_logical_symbol(logical_code)) return false;
    const ResolveResult r = resolve(date);
    if (!r.active || r.primary_symbol.empty()) return false;
    out = r.primary_symbol;
    return true;
}

std::vector<XtRollEngine::SwitchPoint> XtRollEngine::detect_switches(
    const std::vector<DetectRecord>& records, int confirm_days, int lag_days) {

    if (records.empty() || confirm_days < 1) return {};
    if (lag_days < 0) lag_days = 0;

    std::map<uint32_t, std::map<std::string, double>> by_date;
    for (const auto& r : records) {
        if (r.date == 0 || r.code.empty()) continue;
        auto& oi_map = by_date[r.date];
        auto it = oi_map.find(r.code);
        if (it == oi_map.end() || r.open_interest > it->second)
            oi_map[r.code] = r.open_interest;
    }
    if (by_date.empty()) return {};

    std::vector<uint32_t> date_seq;
    date_seq.reserve(by_date.size());
    for (const auto& [d, _] : by_date) { date_seq.push_back(d); }

    auto lag_date = [&](size_t cur_idx) -> uint32_t {
        if (lag_days == 0 || cur_idx == 0) return date_seq[cur_idx];
        const size_t target = (cur_idx >= static_cast<size_t>(lag_days))
                                  ? (cur_idx - static_cast<size_t>(lag_days))
                                  : 0;
        return date_seq[target];
    };

    std::vector<SwitchPoint> result;
    std::string current_dominant;
    std::string candidate;
    int consecutive = 0;
    size_t date_idx = 0;

    for (const auto& [date, oi_map] : by_date) {
        std::string top_code;
        double top_oi = -1.0;
        for (const auto& [code, oi] : oi_map) {
            if (oi > top_oi) { top_oi = oi; top_code = code; }
        }
        if (top_code.empty()) { ++date_idx; continue; }

        if (current_dominant.empty()) {
            current_dominant = top_code;

            result.push_back({date, top_code, lag_date(date_idx), 0.0, 0.0});
            ++date_idx;
            continue;
        }

        if (top_code == current_dominant) {
            candidate    = "";
            consecutive  = 0;
        } else if (top_code == candidate) {
            if (++consecutive >= confirm_days) {
                result.push_back({date, candidate, lag_date(date_idx), 0.0, 0.0});
                current_dominant = candidate;
                candidate        = "";
                consecutive      = 0;
            }
        } else {
            candidate   = top_code;
            consecutive = 1;
        }
        ++date_idx;
    }

    return result;
}
