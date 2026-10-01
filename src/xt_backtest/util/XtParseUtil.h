#pragma once

/// @file
/// @brief 回测 CSV / 时间字段解析 helper。
///
/// 本文件提供无异常、无 locale 依赖的字符串 trim、CSV split、日期/时间和数值解析。
/// 调用方负责字段语义校验和 domain 对象组装。

#include <charconv>
#include <cctype>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

// parse_time_with_ms 复用 XtTimeParse 的 HH:MM:SS 基础解析。
#include "xt_util/time/XtTimeParse.h"

namespace XtParseUtil {

// ── 内部辅助（internal helpers） ──────────────────────────────────────────────

/// 去除 string_view 首尾 ASCII 控制字符和空格。
inline std::string_view trim_sv(std::string_view s) {
    while (!s.empty() && static_cast<unsigned char>(s.front()) <= ' ') s.remove_prefix(1);
    while (!s.empty() && static_cast<unsigned char>(s.back())  <= ' ') s.remove_suffix(1);
    return s;
}

inline bool valid_date_parts(uint32_t y, uint32_t m, uint32_t d) {
    if (y == 0 || m < 1U || m > 12U || d < 1U) return false;
    constexpr uint32_t days_by_month[] = {
        0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    };
    uint32_t days = days_by_month[m];
    const bool leap = (y % 4U == 0U && y % 100U != 0U) || (y % 400U == 0U);
    if (m == 2U && leap) days = 29U;
    return d <= days;
}

// ── 列名规范化（column name normalization） ──────────────────────────────────────────────

/// 去掉非字母数字字符并转小写，用于 CSV 表头列名识别。
inline std::string normalize_col(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (unsigned char c : s) {
        if (std::isalnum(c)) out.push_back(static_cast<char>(std::tolower(c)));
    }
    return out;
}

// ── CSV 行分割（CSV line split） ──────────────────────────────────────────────

/// 按逗号分割 line，每列 trim 首尾空白后写入 cols（复用已有 buffer）。
inline void split_csv_trim(const std::string& line, std::vector<std::string>& cols) {
    cols.clear();
    const char* p   = line.data();
    const char* end = p + line.size();
    while (p <= end) {
        const char* sep = p;
        while (sep < end && *sep != ',') ++sep;
        const char* l = p;
        const char* r = sep;
        while (l < r && static_cast<unsigned char>(*l) <= ' ') ++l;
        while (r > l && static_cast<unsigned char>(*(r - 1)) <= ' ') --r;
        cols.emplace_back(l, r);
        p = sep + 1;
    }
}

/// 按逗号分割 line，每列 trim 后存为 string_view（zero-copy）。
///
/// `cols` 中的 string_view 直接引用 `line` 的内存；调用方必须保证 line 在
/// cols 使用完毕前保持有效。
inline void split_csv_trim_sv(const std::string& line, std::vector<std::string_view>& cols) {
    cols.clear();
    const char* p   = line.data();
    const char* end = p + line.size();
    while (p <= end) {
        const char* sep = p;
        while (sep < end && *sep != ',') ++sep;
        const char* l = p;
        const char* r = sep;
        while (l < r && static_cast<unsigned char>(*l) <= ' ') ++l;
        while (r > l && static_cast<unsigned char>(*(r - 1)) <= ' ') --r;
        cols.emplace_back(l, static_cast<std::size_t>(r - l));
        p = sep + 1;
    }
}

// ── 日期解析（date parsing） ──────────────────────────────────────────────

/// 解析日期字符串为 YYYYMMDD 编码的 uint32。
/// 支持格式：
///   - "YYYYMMDD"    （8 位纯数字）
///   - "YYYY-MM-DD"  （带连字符）
/// 使用 from_chars：无异常、无 locale 依赖。失败返回 false，out 不修改。
inline bool parse_date(std::string_view s, uint32_t& out) {
    s = trim_sv(s);
    if (s.size() == 8) {
        uint32_t v = 0;
        auto [p, ec] = std::from_chars(s.data(), s.data() + 8, v);
        if (ec != std::errc{} || p != s.data() + 8) return false;
        if (!valid_date_parts(v / 10000U, (v / 100U) % 100U, v % 100U)) return false;
        out = v;
        return true;
    }
    if (s.size() == 10 && s[4] == '-' && s[7] == '-') {
        uint32_t y = 0, m = 0, d = 0;
        auto [p1, ec1] = std::from_chars(s.data(),     s.data() + 4, y);
        auto [p2, ec2] = std::from_chars(s.data() + 5, s.data() + 7, m);
        auto [p3, ec3] = std::from_chars(s.data() + 8, s.data() + 10, d);
        if (ec1 != std::errc{} || p1 != s.data() + 4 ||
            ec2 != std::errc{} || p2 != s.data() + 7 ||
            ec3 != std::errc{} || p3 != s.data() + 10) return false;
        if (!valid_date_parts(y, m, d)) return false;
        out = y * 10000u + m * 100u + d;
        return true;
    }
    return false;
}

// ── 无异常数字解析（exception-free numeric parsers） ──────────────────────────────────────────────

/// 解析 uint32，自动去首尾空白。失败返回 false，out 不修改。
inline bool parse_u32(std::string_view s, uint32_t& out) {
    s = trim_sv(s);
    if (s.empty()) return false;
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc{} && p == s.data() + s.size();
}

/// 解析 uint64，自动去首尾空白。
inline bool parse_u64(std::string_view s, uint64_t& out) {
    s = trim_sv(s);
    if (s.empty()) return false;
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc{} && p == s.data() + s.size();
}

/// 解析 int（带符号），自动去首尾空白。
inline bool parse_i32(std::string_view s, int& out) {
    s = trim_sv(s);
    if (s.empty()) return false;
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc{} && p == s.data() + s.size();
}

/// 解析 uint8，自动去首尾空白。
inline bool parse_u8(std::string_view s, uint8_t& out) {
    uint32_t v = 0;
    if (!parse_u32(s, v)) return false;
    if (v > 255U) return false;
    out = static_cast<uint8_t>(v & 0xFFU);
    return true;
}

// ── 时间解析（time parsing） ──────────────────────────────────────────────

/// 把 D/DD/DDD 位小数秒填充到毫秒（6→600, 60→600, 600→600）。
inline uint32_t pad_frac_to_ms(uint32_t v, std::size_t digit_count) {
    for (std::size_t n = digit_count; n < 3; ++n) v *= 10;
    return v;
}

/// 将时间字符串解析为 HHMMSS*1000+ms 编码的 uint32。
///
/// 支持格式：
///   - "HH:MM:SS"         / "HH:MM:SS.mmm"  → HHMMSS*1000 [+ mmm]
///   - "HHMMSS"     (6)                     → HHMMSS*1000
///   - "HHMMSSD"    (7)                     → HHMMSS*1000 + D*100
///   - "HHMMSSCC"   (8)                     → HHMMSS*1000 + CC*10
///   - "HHMMSSMMM"  (9)                     → HHMMSS*1000 + MMM
inline bool parse_time_with_ms(std::string_view s, uint32_t& time_ms) {
    s = trim_sv(s);
    if (s.empty()) return false;

    // HH:MM:SS[.mmm] 格式
    if (s.find(':') != std::string_view::npos) {
        int extra_ms = 0;
        std::string_view base = s;
        auto dot = s.find('.');
        if (dot != std::string_view::npos) {
            base = s.substr(0, dot);
            std::string_view frac = s.substr(dot + 1);
            if (frac.empty() || frac.size() > 3) return false;
            uint32_t ms = 0;
            if (!parse_u32(frac, ms)) return false;
            extra_ms = static_cast<int>(pad_frac_to_ms(ms, frac.size()));
        }
        std::string base_str(base);
        if (!XtTimeParse::parse_time_hms(base_str, time_ms)) return false;
        if (extra_ms > 0) time_ms = XtTimeParse::add_ms(time_ms, extra_ms);
        return true;
    }

    // ── 纯数字格式：按长度 positional slicing（6=HHMMSS, 7=+D, 8=+CC, 9=+MMM） ──
    //
    // Why not modular decomposition（packed /= 1000/100/100）：那会假设
    // 输入永远带 3 位 ms 后缀；6 位 HHMMSS 会被错位分解（"190000"
    // → sec=90, reject）。positional slicing 不受输入长度影响。
    if (s.size() < 6 || s.size() > 9) return false;

    auto parse_2 = [](const char* p, uint32_t& out) -> bool {
        auto [end, ec] = std::from_chars(p, p + 2, out);
        return ec == std::errc{} && end == p + 2;
    };
    uint32_t hour = 0, min = 0, sec = 0;
    if (!parse_2(s.data(),     hour)) return false;
    if (!parse_2(s.data() + 2, min))  return false;
    if (!parse_2(s.data() + 4, sec))  return false;
    if (hour >= 24U || min >= 60U || sec >= 60U) return false;

    uint32_t ms = 0;
    if (s.size() > 6) {
        std::string_view frac = s.substr(6);
        uint32_t tmp = 0;
        auto [end, ec] = std::from_chars(frac.data(), frac.data() + frac.size(), tmp);
        if (ec != std::errc{} || end != frac.data() + frac.size()) return false;
        ms = pad_frac_to_ms(tmp, frac.size());
    }

    time_ms = (hour * 10000U + min * 100U + sec) * 1000U + ms;
    return true;
}

// ── 浮点解析（floating-point parsing） ──────────────────────────────────────────────

/// 解析 double，自动去首尾空白；无异常、无 locale 依赖、无额外堆分配。
inline bool parse_double(std::string_view s, double& out) {
    s = trim_sv(s);
    if (s.empty()) return false;
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc{} && p == s.data() + s.size();
}

} // namespace XtParseUtil
