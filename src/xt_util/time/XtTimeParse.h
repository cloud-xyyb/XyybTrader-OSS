#pragma once
#include <cstdint>
#include <string>
#include <vector>

/// @file XtTimeParse.h
/// @brief 日期、月份和固定宽度交易所时间字段解析工具。
///
/// 解析失败按调用场景返回 false、0 或空容器；本模块不抛异常给调用方。
namespace XtTimeParse {

bool parse_yymm(const std::string& s, int& yy, int& mm);
std::string format_yymm(int yy, int mm);
std::string yymm_from_date(uint32_t yyyymmdd);
std::vector<std::string> months_between(uint32_t start_date, uint32_t end_date);
bool month_in_list(const std::vector<std::string>& list, const std::string& yymm);

/// @brief action_time（HHMMSSmmm）转当天毫秒数。
///
/// 用于 CTA bar 聚合和 TWAP 时间比较；不校验 HH/MM/SS 字段合法性。
inline uint32_t action_time_to_ms(uint32_t action_time) noexcept {
    const uint32_t h  = action_time / 10000000;
    const uint32_t m  = (action_time / 100000) % 100;
    const uint32_t s  = (action_time / 1000) % 100;
    const uint32_t ms = action_time % 1000;
    return h * 3600000 + m * 60000 + s * 1000 + ms;
}

/// @brief 解析固定宽度时间字段（HH:MM:SS + ms）为 HHMMSSmmm。
/// @note 只读取前 8 字节，适合 SDK 固定 char 时间字段；非法输入返回 0。
/// @note 毫秒字段会 clamp 到 [0, 999]；需要严格拒绝越界时不要使用本接口。
inline uint32_t parse_fixed_time_hms_ms(const char* s, int ms) noexcept {
    if (!s || !s[0]) return 0;
    for (int i = 0; i < 8; ++i) {
        if (s[i] == '\0') return 0;
    }
    auto is_digit = [](char c) { return c >= '0' && c <= '9'; };
    if (!(is_digit(s[0]) && is_digit(s[1]) && s[2] == ':' &&
          is_digit(s[3]) && is_digit(s[4]) && s[5] == ':' &&
          is_digit(s[6]) && is_digit(s[7]))) {
        return 0;
    }

    const int hh = (s[0] - '0') * 10 + (s[1] - '0');
    const int mm = (s[3] - '0') * 10 + (s[4] - '0');
    const int ss = (s[6] - '0') * 10 + (s[7] - '0');
    if (hh > 23 || mm > 59 || ss > 59) return 0;

    if (ms < 0) ms = 0;
    if (ms > 999) ms = 999;
    return static_cast<uint32_t>((hh * 10000 + mm * 100 + ss) * 1000 + ms);
}

/// @brief 解析固定宽度日期字段（YYYYMMDD）为 uint32_t。
/// @note 只读取前 8 字节，适合 SDK 固定 char 日期字段；非法输入返回 0。
inline uint32_t parse_fixed_date_yyyymmdd(const char* s) noexcept {
    if (!s || !s[0]) return 0;
    auto is_digit = [](char c) { return c >= '0' && c <= '9'; };
    for (int i = 0; i < 8; ++i) {
        if (!is_digit(s[i])) return 0;
    }

    const int y = (s[0] - '0') * 1000 + (s[1] - '0') * 100 +
                  (s[2] - '0') * 10 + (s[3] - '0');
    const int m = (s[4] - '0') * 10 + (s[5] - '0');
    const int d = (s[6] - '0') * 10 + (s[7] - '0');
    if (y <= 0 || m < 1 || m > 12) return 0;

    static constexpr int kDaysPerMonth[] = {
        0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    };
    int max_day = kDaysPerMonth[m];
    const bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
    if (m == 2 && leap) max_day = 29;
    if (d < 1 || d > max_day) return 0;
    return static_cast<uint32_t>(y * 10000 + m * 100 + d);
}

bool parse_datetime(const std::string& s, uint32_t& date, uint32_t& time_ms);
bool parse_time_hms(const std::string& s, uint32_t& time_ms);
uint32_t parse_date_only(const std::string& s);
uint32_t add_ms(uint32_t time_ms, int delta_ms);

}  // namespace XtTimeParse
