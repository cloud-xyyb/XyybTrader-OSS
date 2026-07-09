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
