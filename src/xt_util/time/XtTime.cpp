#include "xt_util/time/XtTime.h"

#include <ctime>

namespace {

constexpr int64_t kNsPerSec = 1'000'000'000LL;
constexpr int64_t kSecPerDay = 86400LL;

}

namespace xt_time {

int64_t to_ns(uint32_t date, uint32_t time_ms) noexcept {
    // 按日 memoize：BT replayer / gateway 每 tick 调用而 date 一天只变一次，
    // 跳过重复的 days_from_civil 日历换算。算术与未缓存路径完全一致
    //（bit-exact）；thread_local 无跨线程共享，纯函数语义不变。
    struct DayCache { uint32_t date = 0; int64_t day_secs = 0; bool valid = false; };
    thread_local DayCache cache;
    if (!cache.valid || cache.date != date) {
        const int      year  = static_cast<int>(date / 10000u);
        const unsigned month = (date / 100u) % 100u;
        const unsigned day   = date % 100u;
        cache.day_secs = days_from_civil(year, month, day) * kSecPerDay;
        cache.date  = date;
        cache.valid = true;
    }

    const uint32_t hms    = time_ms / 1000u;
    const int      hour   = static_cast<int>(hms / 10000u);
    const int      minute = static_cast<int>((hms / 100u) % 100u);
    const int      second = static_cast<int>(hms % 100u);
    const int      milli  = static_cast<int>(time_ms % 1000u);

    const int64_t secs = cache.day_secs
                       + static_cast<int64_t>(hour) * 3600
                       + static_cast<int64_t>(minute) * 60
                       + static_cast<int64_t>(second);
    return secs * kNsPerSec + static_cast<int64_t>(milli) * 1'000'000LL;
}

void from_ns(int64_t ns, uint32_t& out_date, uint32_t& out_time_ms) noexcept {

    int64_t total_secs = ns / kNsPerSec;
    int64_t sub_sec_ns = ns - total_secs * kNsPerSec;
    if (sub_sec_ns < 0) { sub_sec_ns += kNsPerSec; --total_secs; }

    int64_t days = total_secs / kSecPerDay;
    int64_t tod  = total_secs - days * kSecPerDay;
    if (tod < 0) { tod += kSecPerDay; --days; }

    // 按日 memoize（同 to_ns）：同一 days 反复转换时跳过 civil_from_days。
    struct DayCache { int64_t days = 0; uint32_t date = 0; bool valid = false; };
    thread_local DayCache cache;
    if (!cache.valid || cache.days != days) {
        int      y;
        unsigned m, d;
        civil_from_days(days, y, m, d);
        cache.date = static_cast<uint32_t>(y) * 10000u
                   + static_cast<uint32_t>(m) * 100u
                   + static_cast<uint32_t>(d);
        cache.days  = days;
        cache.valid = true;
    }
    out_date = cache.date;

    const int hour   = static_cast<int>(tod / 3600);
    const int minute = static_cast<int>((tod / 60) % 60);
    const int second = static_cast<int>(tod % 60);
    const int milli  = static_cast<int>(sub_sec_ns / 1'000'000LL);

    out_time_ms = static_cast<uint32_t>(hour)   * 10'000'000u
                + static_cast<uint32_t>(minute) *    100'000u
                + static_cast<uint32_t>(second) *      1'000u
                + static_cast<uint32_t>(milli);
}

int64_t to_ns_from_key(uint64_t key) noexcept {
    const uint32_t date    = static_cast<uint32_t>(key / 1'000'000'000ULL);
    const uint32_t time_ms = static_cast<uint32_t>(key % 1'000'000'000ULL);
    return to_ns(date, time_ms);
}

LocalDateHhmm local_date_hhmm_from_time_t(std::time_t ts) noexcept {
    std::tm tm_now{};
#if defined(_WIN32)
    if (localtime_s(&tm_now, &ts) != 0) return {};
#else
    if (!localtime_r(&ts, &tm_now)) return {};
#endif
    LocalDateHhmm out;
    out.yyyymmdd = static_cast<uint32_t>((tm_now.tm_year + 1900) * 10000
                                       + (tm_now.tm_mon + 1) * 100
                                       + tm_now.tm_mday);
    out.hhmm = static_cast<uint32_t>(tm_now.tm_hour * 100 + tm_now.tm_min);
    return out;
}

LocalDateHhmm local_date_hhmm_now() noexcept {
    return local_date_hhmm_from_time_t(std::time(nullptr));
}

int local_hhmm_now() noexcept {
    return static_cast<int>(local_date_hhmm_now().hhmm);
}

uint32_t local_yyyymmdd_now() noexcept {
    return local_date_hhmm_now().yyyymmdd;
}

}
