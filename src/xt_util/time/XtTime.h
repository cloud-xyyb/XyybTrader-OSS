#pragma once
#include <cstdint>
#include <ctime>

/// @file XtTime.h
/// @brief 时间转换 hlper
namespace xt_time {

/// @brief packed timestamp 编码因子: `packed_ts = date * kPackedTsDateFactor + time_ms`,
///        其中 date = YYYYMMDD,time_ms = HHMMSSmmm。
/// @note factor 给 time_ms 留 9 位，足够容纳 HHMMSSmmm 最大值；调用方需传入
///       已按同一格式编码的时间字段。
constexpr int64_t kPackedTsDateFactor = 1'000'000'000LL;

/// @brief (date, time_ms) → packed timestamp。
///
/// 只做整数拼接，不校验 date/time_ms 合法性；调用方负责保证 `time_ms` 使用 HHMMSSmmm。
inline int64_t packed_ts(uint32_t date, uint32_t time_ms) noexcept {
    return static_cast<int64_t>(date) * kPackedTsDateFactor +
           static_cast<int64_t>(time_ms);
}

/// @brief action_time(HHMMSSmmm) → HHMM。
///
/// `XtRawTick.action_time`、recorder action_time 和控制面 last_tick_action_time
/// 使用同一 HHMMSSmmm 编码；返回值只截取分钟粒度，不校验字段合法性。
inline uint32_t action_time_to_hhmm(uint32_t action_time_ms) noexcept {
    return action_time_ms / 100000u;
}

/// @brief y/m/d → 距 1970-01-01 的天数（可为负），proleptic Gregorian。
///
/// 使用 Howard Hinnant 纯整型公式；不校验输入合法性。
/// @param m 月份 ∈ [1,12]。
/// @param d 日 ∈ [1,31]。
constexpr int64_t days_from_civil(int y, unsigned m, unsigned d) noexcept {
    y -= (m <= 2);
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);                 // [0, 399]
    const unsigned doy = (153u * (m + (m > 2 ? -3u : 9u)) + 2u) / 5u + d - 1u; // [0, 365]
    const unsigned doe = yoe * 365u + yoe / 4u - yoe / 100u + doy;             // [0, 146096]
    return static_cast<int64_t>(era) * 146097 + static_cast<int64_t>(doe) - 719468;
}

/// @brief 距 1970-01-01 的天数 → y/m/d，`days_from_civil` 的逆变换。
constexpr void civil_from_days(int64_t z, int& y, unsigned& m, unsigned& d) noexcept {
    z += 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const unsigned doe = static_cast<unsigned>(z - era * 146097);               // [0, 146096]
    const unsigned yoe = (doe - doe / 1460u + doe / 36524u - doe / 146096u) / 365u;
    y = static_cast<int>(yoe) + static_cast<int>(era) * 400;
    const unsigned doy = doe - (365u * yoe + yoe / 4u - yoe / 100u);            // [0, 365]
    const unsigned mp  = (5u * doy + 2u) / 153u;                                // [0, 11]
    d = doy - (153u * mp + 2u) / 5u + 1u;                                       // [1, 31]
    m = mp + (mp < 10u ? 3u : -9u);                                             // [1, 12]
    y += (m <= 2);
}

/// @brief YYYYMMDD → 距 1970-01-01 的天数。不校验输入合法性。
constexpr int64_t days_from_yyyymmdd(uint32_t yyyymmdd) noexcept {
    return days_from_civil(static_cast<int>(yyyymmdd / 10000u),
                           (yyyymmdd / 100u) % 100u,
                           yyyymmdd % 100u);
}

/// @brief 距 1970-01-01 的天数 → YYYYMMDD。
constexpr uint32_t yyyymmdd_from_days(int64_t z) noexcept {
    int y = 0;
    unsigned m = 0, d = 0;
    civil_from_days(z, y, m, d);
    return static_cast<uint32_t>(y) * 10000u + m * 100u + d;
}

/// @brief YYYYMMDD 自然日加减（Gregorian calendar days，days 可为负）。
///
/// 日历编码直接做整数加减会在跨月/跨年处产生伪日期（如 20220531+1=20220532）；
/// 所有 YYYYMMDD 窗口算术必须经由本函数/`yyyymmdd_diff_days`。
constexpr uint32_t yyyymmdd_add_days(uint32_t yyyymmdd, int64_t days) noexcept {
    return yyyymmdd_from_days(days_from_yyyymmdd(yyyymmdd) + days);
}

/// @brief 两个 YYYYMMDD 的自然日差（b - a）。
constexpr int64_t yyyymmdd_diff_days(uint32_t a, uint32_t b) noexcept {
    return days_from_yyyymmdd(b) - days_from_yyyymmdd(a);
}

/// @brief YYYYMMDD 是否为合法 Gregorian 日期（roundtrip 校验，含闰年/月长）。
constexpr bool is_valid_yyyymmdd(uint32_t yyyymmdd) noexcept {
    const unsigned m = (yyyymmdd / 100u) % 100u;
    const unsigned d = yyyymmdd % 100u;
    if (m < 1u || m > 12u || d < 1u || d > 31u) return false;
    return yyyymmdd_from_days(days_from_yyyymmdd(yyyymmdd)) == yyyymmdd;
}

/// @brief (date, time_ms) → ns since epoch (本地时间，无时区)
/// @param date    YYYYMMDD，自然日 (natural day)
/// @param time_ms HHMMSSmmm = HHMMSS × 1000 + ms，例如 09:30:00.500 = 93000500
///
/// @note 假设交易所本地时间，不做时区转换。夜盘横跨自然日时调用方传入
///       实际自然日。例如夜盘 21:00:00 应传 date=20260406，而非交易日 20260407。
///       与 from_ns 严格互逆 (round-trip) 。
int64_t to_ns(uint32_t date, uint32_t time_ms) noexcept;

/// @brief ns since epoch → (date, time_ms)
/// @post out_date = YYYYMMDD，out_time_ms = HHMMSSmmm
void from_ns(int64_t ns, uint32_t& out_date, uint32_t& out_time_ms) noexcept;

/// @brief 从 XtReplayEvent::key 提取 ns
///
/// key 编码为 date × 1e9 + HHMMSSmmm；本函数只负责解码并委托 `to_ns()`。
int64_t to_ns_from_key(uint64_t key) noexcept;

/// @brief OS 本地 wall clock 的日期与 HHMM。
///
/// `yyyymmdd == 0` 表示转换失败；`hhmm == 0` 也可能是合法午夜，不能单独当失败哨兵。
struct LocalDateHhmm {
    uint32_t yyyymmdd = 0;
    uint32_t hhmm     = 0;
};

/// @brief time_t → OS 本地 (YYYYMMDD, HHMM)，使用线程安全 localtime_r/localtime_s。
LocalDateHhmm local_date_hhmm_from_time_t(std::time_t ts) noexcept;

/// @brief 当前 OS 本地 (YYYYMMDD, HHMM)。
LocalDateHhmm local_date_hhmm_now() noexcept;

/// @brief 当前 OS 本地 HHMM；转换失败返回 0。
int local_hhmm_now() noexcept;

/// @brief 当前 OS 本地 YYYYMMDD；转换失败返回 0。
uint32_t local_yyyymmdd_now() noexcept;

/// @brief UTC epoch ms → CST (UTC+8) packed timestamp (`YYYYMMDD*1e9 + HHMMSSmmm`)。
///
/// Live 路径 wall clock 转 packed，供仍以 packed key 表达事件时间的接口使用。
/// CST 硬编码为 UTC+8；新代码优先携带明确的 epoch ns/us 字段。
///
/// @param epoch_ms UTC epoch ms (`std::chrono::system_clock::now()` 派生)。
///                 **不能传 steady_clock 值** — `XtSteadyClock::steady_now_ms()` 是
///                 monotonic since boot,不携 wall clock 含义。
/// @return packed ts (`YYYYMMDD*1e9 + HHMMSSmmm`),CST 时区下 wall clock 值。
inline int64_t packed_ts_from_epoch_ms_cst(int64_t epoch_ms) noexcept {
    constexpr int64_t kCstOffsetMs = 8LL * 3600LL * 1000LL;
    uint32_t date = 0, time_ms = 0;
    from_ns((epoch_ms + kCstOffsetMs) * 1'000'000LL, date, time_ms);
    return packed_ts(date, time_ms);
}

/// @brief CST(UTC+8) 墙上时间 (date, time_ms) → 真实 UTC epoch 微秒。
///
/// `to_ns` 不做时区转换；本函数额外减 8h 得到 UTC 绝对瞬时，供 `fill_ts_us`
/// 等跨 venue epoch 字段使用。CTP 只提供 CST 墙上时间和自然日时，可用本函数
/// 还原秒级绝对时刻。
///
/// @param date    YYYYMMDD,**自然日历日** (夜盘传实际自然日,非 trading_day)。
/// @param time_ms HHMMSSmmm (CST 墙上时间;CTP 仅秒级 → ms 段为 0)。
/// @return 真实 UTC epoch 微秒;date==0 (无效/缺失) 返回 0。
inline uint64_t cst_wall_to_epoch_us(uint32_t date, uint32_t time_ms) noexcept {
    if (date == 0) return 0;
    constexpr int64_t kCstOffsetNs = 8LL * 3600LL * 1'000'000'000LL;
    const int64_t utc_ns = to_ns(date, time_ms) - kCstOffsetNs;
    return utc_ns > 0 ? static_cast<uint64_t>(utc_ns / 1000) : 0;
}

}  // namespace xt_time

/// @brief yyyymmdd → Unix 0 点 ns epoch，硬编码 CST(UTC+8)。
///
/// 仅在调用方明确需要 `YYYYMMDD` 日起点时使用；常规 date+ms timestamp
/// 合成应使用 `xt_time::to_ns(date, ms)`，避免额外引入隐式 TZ 假设。
inline uint64_t xt_yyyymmdd_to_day_start_ns(uint32_t yyyymmdd) {
    const int year  = static_cast<int>(yyyymmdd / 10000);
    const int month = static_cast<int>((yyyymmdd / 100) % 100);
    const int day   = static_cast<int>(yyyymmdd % 100);

    const int64_t unix_days = xt_time::days_from_civil(
        year, static_cast<unsigned>(month), static_cast<unsigned>(day));

    // CST = UTC+8 → 减去 8 小时得到 UTC 0 点
    static constexpr int64_t kHours8Ns = 8LL * 3600 * 1000000000LL;
    const uint64_t day_start_ns = static_cast<uint64_t>(unix_days) * 86400ULL * 1000000000ULL;
    return day_start_ns > static_cast<uint64_t>(kHours8Ns)
               ? day_start_ns - static_cast<uint64_t>(kHours8Ns)
               : 0;
}

/// @brief 毫秒转纳秒。
inline uint64_t xt_ms_to_ns(uint64_t ms) {
    return ms * 1000000ULL;
}
