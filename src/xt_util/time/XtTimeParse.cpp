#include "xt_util/time/XtTimeParse.h"

#include <cctype>
#include <cstdio>

#include "xt_util/string/XtStringUtil.h"

namespace {

std::vector<std::string> split_tokens(const std::string& s, char sep) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : s) {
        if (c == sep) {
            out.push_back(XtStringUtil::trim(cur));
            cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    out.push_back(XtStringUtil::trim(cur));
    return out;
}

bool all_digits(const std::string& s) {
    if (s.empty()) return false;
    for (char c : s) {
        if (!std::isdigit(static_cast<unsigned char>(c))) return false;
    }
    return true;
}

bool parse_int_token(const std::string& s, int& out) {
    if (!all_digits(s)) return false;
    try {
        size_t pos = 0;
        const int parsed = std::stoi(s, &pos);
        if (pos != s.size()) return false;
        out = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

bool is_leap_year(int y) {
    return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
}

bool valid_date_parts(int y, int m, int d) {
    if (y <= 0 || m < 1 || m > 12) return false;
    static constexpr int kDaysPerMonth[] = {
        0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    };
    int max_day = kDaysPerMonth[m];
    if (m == 2 && is_leap_year(y)) max_day = 29;
    return d >= 1 && d <= max_day;
}

bool valid_time_parts(int hh, int mm, int ss) {
    return hh >= 0 && hh <= 23 &&
           mm >= 0 && mm <= 59 &&
           ss >= 0 && ss <= 59;
}

}

namespace XtTimeParse {

bool parse_yymm(const std::string& s, int& yy, int& mm) {
    std::string v = XtStringUtil::trim(s);
    if (v.empty()) return false;
    int val = 0;
    if (!parse_int_token(v, val)) return false;
    if (v.size() == 6) {

        yy = (val / 100) % 100;
        mm = val % 100;
    } else if (v.size() == 4) {
        yy = val / 100;
        mm = val % 100;
    } else {
        return false;
    }
    if (mm < 1 || mm > 12) return false;
    return true;
}

std::string format_yymm(int yy, int mm) {
    char buf[8];
    std::snprintf(buf, sizeof(buf), "%02d%02d", yy, mm);
    return std::string(buf);
}

std::string yymm_from_date(uint32_t yyyymmdd) {
    if (yyyymmdd == 0) return "";
    int yy = static_cast<int>((yyyymmdd / 10000) % 100);
    int mm = static_cast<int>((yyyymmdd / 100) % 100);
    if (mm < 1 || mm > 12) return "";
    return format_yymm(yy, mm);
}

std::vector<std::string> months_between(uint32_t start_date, uint32_t end_date) {
    std::vector<std::string> out;
    if (!start_date || !end_date) return out;
    int sy = static_cast<int>(start_date / 10000);
    int sm = static_cast<int>((start_date / 100) % 100);
    int ey = static_cast<int>(end_date / 10000);
    int em = static_cast<int>((end_date / 100) % 100);
    int cur = sy * 12 + sm;
    int end = ey * 12 + em;
    for (int v = cur; v <= end; ++v) {
        int yy = v / 12;
        int mm = v % 12;
        if (mm == 0) {
            mm = 12;
            yy -= 1;
        }
        out.push_back(format_yymm(yy % 100, mm));
    }
    return out;
}

bool month_in_list(const std::vector<std::string>& list, const std::string& yymm) {
    if (yymm.empty()) return false;
    for (const auto& m : list) {
        if (m == yymm) return true;
    }
    return false;
}

bool parse_datetime(const std::string& s, uint32_t& date, uint32_t& time_ms) {
    const std::string v = XtStringUtil::trim(s);
    if (v.size() < 8) return false;

    if (v.size() == 19 && v[4] == '-' && v[7] == '-' &&
        v[10] == ' ' && v[13] == ':' && v[16] == ':') {
        int y = 0, m = 0, d = 0, hh = 0, mm = 0, ss = 0;
        if (!parse_int_token(v.substr(0, 4), y) ||
            !parse_int_token(v.substr(5, 2), m) ||
            !parse_int_token(v.substr(8, 2), d) ||
            !parse_int_token(v.substr(11, 2), hh) ||
            !parse_int_token(v.substr(14, 2), mm) ||
            !parse_int_token(v.substr(17, 2), ss)) {
            return false;
        }
        if (!valid_date_parts(y, m, d) || !valid_time_parts(hh, mm, ss)) return false;
        date = static_cast<uint32_t>(y * 10000 + m * 100 + d);
        time_ms = static_cast<uint32_t>((hh * 10000 + mm * 100 + ss) * 1000);
        return true;
    }

    if (v.find('/') != std::string::npos) {
        auto parts = split_tokens(v, '/');
        if (parts.size() == 3) {
            int y = 0, m = 0, d = 0;
            if (!parse_int_token(parts[0], y) ||
                !parse_int_token(parts[1], m) ||
                !parse_int_token(parts[2], d) ||
                !valid_date_parts(y, m, d)) {
                return false;
            }
            date = static_cast<uint32_t>(y * 10000 + m * 100 + d);
            time_ms = 0;
            return true;
        }
    }
    return false;
}

bool parse_time_hms(const std::string& s, uint32_t& time_ms) {
    const std::string v = XtStringUtil::trim(s);
    if (v.size() < 7) return false;

    if (v.find(':') == std::string::npos) return false;
    auto parts = split_tokens(v, ':');
    if (parts.size() < 2 || parts.size() > 3) return false;
    int hh = 0, mm = 0, ss = 0;
    if (!parse_int_token(parts[0], hh) ||
        !parse_int_token(parts[1], mm) ||
        (parts.size() == 3 && !parse_int_token(parts[2], ss))) {
        return false;
    }
    if (!valid_time_parts(hh, mm, ss)) return false;
    time_ms = static_cast<uint32_t>((hh * 10000 + mm * 100 + ss) * 1000);
    return true;
}

uint32_t parse_date_only(const std::string& s) {
    const std::string v = XtStringUtil::trim(s);
    if (v.empty()) return 0;
    if (v.find('-') != std::string::npos) {

        if (v.size() != 10 || v[4] != '-' || v[7] != '-') return 0;
        int y = 0, m = 0, d = 0;
        if (!parse_int_token(v.substr(0, 4), y) ||
            !parse_int_token(v.substr(5, 2), m) ||
            !parse_int_token(v.substr(8, 2), d) ||
            !valid_date_parts(y, m, d)) {
            return 0;
        }
        return static_cast<uint32_t>(y * 10000 + m * 100 + d);
    }

    if (v.size() == 8) {
        int ymd = 0;
        if (!parse_int_token(v, ymd)) return 0;
        const int y = ymd / 10000;
        const int m = (ymd / 100) % 100;
        const int d = ymd % 100;
        if (!valid_date_parts(y, m, d)) return 0;
        return static_cast<uint32_t>(ymd);
    }
    return 0;
}

uint32_t add_ms(uint32_t time_ms, int delta_ms) {
    uint32_t base = time_ms;
    uint32_t hhmmss = base / 1000;
    uint32_t ms = base % 1000;
    int hh = static_cast<int>(hhmmss / 10000);
    int mm = static_cast<int>((hhmmss / 100) % 100);
    int ss = static_cast<int>(hhmmss % 100);
    int total = ((hh * 60 + mm) * 60 + ss) * 1000 + static_cast<int>(ms) + delta_ms;
    if (total < 0) total = 0;
    int nh = (total / 1000) / 3600;
    int nm = ((total / 1000) / 60) % 60;
    int ns = (total / 1000) % 60;
    int nms = total % 1000;
    return static_cast<uint32_t>((nh * 10000 + nm * 100 + ns) * 1000 + nms);
}

}
