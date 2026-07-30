#pragma once
#include <cstdint>
#include <ctime>

/// @file XtTime.h
/// @brief 时间转换 hlper
namespace xt_time {

constexpr int64_t kPackedTsDateFactor = 1'000'000'000LL;

inline int64_t packed_ts(uint32_t date, uint32_t time_ms) noexcept {
    return static_cast<int64_t>(date) * kPackedTsDateFactor +
           static_cast<int64_t>(time_ms);
}

inline uint32_t action_time_to_hhmm(uint32_t action_time_ms) noexcept {
    return action_time_ms / 100000u;
}

constexpr int64_t days_from_civil(int y, unsigned m, unsigned d) noexcept {
    y -= (m <= 2);
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153u * (m + (m > 2 ? -3u : 9u)) + 2u) / 5u + d - 1u;
    const unsigned doe = yoe * 365u + yoe / 4u - yoe / 100u + doy;
    return static_cast<int64_t>(era) * 146097 + static_cast<int64_t>(doe) - 719468;
}

constexpr void civil_from_days(int64_t z, int& y, unsigned& m, unsigned& d) noexcept {
    z += 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const unsigned doe = static_cast<unsigned>(z - era * 146097);
    const unsigned yoe = (doe - doe / 1460u + doe / 36524u - doe / 146096u) / 365u;
    y = static_cast<int>(yoe) + static_cast<int>(era) * 400;
    const unsigned doy = doe - (365u * yoe + yoe / 4u - yoe / 100u);
    const unsigned mp  = (5u * doy + 2u) / 153u;
    d = doy - (153u * mp + 2u) / 5u + 1u;
    m = mp + (mp < 10u ? 3u : -9u);
    y += (m <= 2);
}

constexpr int64_t days_from_yyyymmdd(uint32_t yyyymmdd) noexcept {
    return days_from_civil(static_cast<int>(yyyymmdd / 10000u),
                           (yyyymmdd / 100u) % 100u,
                           yyyymmdd % 100u);
}

constexpr uint32_t yyyymmdd_from_days(int64_t z) noexcept {
    int y = 0;
    unsigned m = 0, d = 0;
    civil_from_days(z, y, m, d);
    return static_cast<uint32_t>(y) * 10000u + m * 100u + d;
}

constexpr uint32_t yyyymmdd_add_days(uint32_t yyyymmdd, int64_t days) noexcept {
    return yyyymmdd_from_days(days_from_yyyymmdd(yyyymmdd) + days);
}

constexpr int64_t yyyymmdd_diff_days(uint32_t a, uint32_t b) noexcept {
    return days_from_yyyymmdd(b) - days_from_yyyymmdd(a);
}

constexpr bool is_valid_yyyymmdd(uint32_t yyyymmdd) noexcept {
    const unsigned m = (yyyymmdd / 100u) % 100u;
    const unsigned d = yyyymmdd % 100u;
    if (m < 1u || m > 12u || d < 1u || d > 31u) return false;
    return yyyymmdd_from_days(days_from_yyyymmdd(yyyymmdd)) == yyyymmdd;
}

int64_t to_ns(uint32_t date, uint32_t time_ms) noexcept;

void from_ns(int64_t ns, uint32_t& out_date, uint32_t& out_time_ms) noexcept;

int64_t to_ns_from_key(uint64_t key) noexcept;

/// @brief OS 本地 wall clock 的日期与 HHMM。
///
/// `yyyymmdd == 0` 表示转换失败；`hhmm == 0` 也可能是合法午夜，不能单独当失败哨兵。
struct LocalDateHhmm {
    uint32_t yyyymmdd = 0;
    uint32_t hhmm     = 0;
};

LocalDateHhmm local_date_hhmm_from_time_t(std::time_t ts) noexcept;

LocalDateHhmm local_date_hhmm_now() noexcept;

int local_hhmm_now() noexcept;

uint32_t local_yyyymmdd_now() noexcept;

inline int64_t packed_ts_from_epoch_ms_cst(int64_t epoch_ms) noexcept {
    constexpr int64_t kCstOffsetMs = 8LL * 3600LL * 1000LL;
    uint32_t date = 0, time_ms = 0;
    from_ns((epoch_ms + kCstOffsetMs) * 1'000'000LL, date, time_ms);
    return packed_ts(date, time_ms);
}

inline uint64_t cst_wall_to_epoch_us(uint32_t date, uint32_t time_ms) noexcept {
    if (date == 0) return 0;
    constexpr int64_t kCstOffsetNs = 8LL * 3600LL * 1'000'000'000LL;
    const int64_t utc_ns = to_ns(date, time_ms) - kCstOffsetNs;
    return utc_ns > 0 ? static_cast<uint64_t>(utc_ns / 1000) : 0;
}

}

inline uint64_t xt_yyyymmdd_to_day_start_ns(uint32_t yyyymmdd) {
    const int year  = static_cast<int>(yyyymmdd / 10000);
    const int month = static_cast<int>((yyyymmdd / 100) % 100);
    const int day   = static_cast<int>(yyyymmdd % 100);

    const int64_t unix_days = xt_time::days_from_civil(
        year, static_cast<unsigned>(month), static_cast<unsigned>(day));

    static constexpr int64_t kHours8Ns = 8LL * 3600 * 1000000000LL;
    const uint64_t day_start_ns = static_cast<uint64_t>(unix_days) * 86400ULL * 1000000000ULL;
    return day_start_ns > static_cast<uint64_t>(kHours8Ns)
               ? day_start_ns - static_cast<uint64_t>(kHours8Ns)
               : 0;
}

inline uint64_t xt_ms_to_ns(uint64_t ms) {
    return ms * 1000000ULL;
}
