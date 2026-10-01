#include "SelTaskScheduler.h"

#include <algorithm>
#include <ctime>

#include "xt_util/time/XtTime.h"   // cst_wall_to_epoch_us (fireTime_ms)

namespace xt_engine_sel {

namespace {

/// @brief weekday(0=Sun, 1=Mon, ..., 6=Sat) — Zeller 公式。
int day_of_week(uint32_t yyyymmdd) noexcept {
    int y = static_cast<int>(yyyymmdd / 10000);
    int m = static_cast<int>((yyyymmdd / 100) % 100);
    int d = static_cast<int>(yyyymmdd % 100);
    if (m < 3) { m += 12; y -= 1; }
    int K = y % 100;
    int J = y / 100;
    int h = (d + 13 * (m + 1) / 5 + K + K / 4 + J / 4 + 5 * J) % 7;
    // Zeller: h=0 Sat, 1 Sun, 2 Mon, ..., 6 Fri → 调整到 Sun=0
    return (h + 6) % 7;
}

[[nodiscard]] uint32_t ymd_from_tm(const std::tm& tm) noexcept {
    return static_cast<uint32_t>((tm.tm_year + 1900) * 10000
                                  + (tm.tm_mon + 1) * 100
                                  + tm.tm_mday);
}

[[nodiscard]] bool make_ymd(int year, int month, int day, uint32_t& out) noexcept {
    if (month < 1 || month > 12 || day < 1 || day > 31) return false;
    std::tm tm{};
    tm.tm_year = year - 1900;
    tm.tm_mon  = month - 1;
    tm.tm_mday = day;
    tm.tm_isdst = -1;
    if (std::mktime(&tm) == static_cast<std::time_t>(-1)) return false;
    if (tm.tm_year != year - 1900 || tm.tm_mon != month - 1 || tm.tm_mday != day) {
        return false;
    }
    out = ymd_from_tm(tm);
    return true;
}

[[nodiscard]] uint32_t add_days(uint32_t yyyymmdd, int days) noexcept {
    std::tm tm{};
    tm.tm_year = static_cast<int>(yyyymmdd / 10000) - 1900;
    tm.tm_mon  = static_cast<int>((yyyymmdd / 100) % 100) - 1;
    tm.tm_mday = static_cast<int>(yyyymmdd % 100) + days;
    tm.tm_isdst = -1;
    if (std::mktime(&tm) == static_cast<std::time_t>(-1)) return 0;
    return ymd_from_tm(tm);
}

[[nodiscard]] uint32_t scheduled_date_for_task(const SelTaskInfo& task,
                                               uint32_t curDate) noexcept {
    const int year = static_cast<int>(curDate / 10000);
    const int month = static_cast<int>((curDate / 100) % 100);
    const int day = static_cast<int>(curDate % 100);

    switch (task.period) {
        case TaskPeriod::Minute:
        case TaskPeriod::Daily:
            return curDate;
        case TaskPeriod::Weekly: {
            if (task.day < 1 || task.day > 7) return 0;
            const int wd = day_of_week(curDate);
            const uint32_t adjusted_wd = (wd == 0) ? 7 : static_cast<uint32_t>(wd);
            int days_since_due = static_cast<int>(adjusted_wd) - static_cast<int>(task.day);
            if (days_since_due < 0) days_since_due += 7;
            return add_days(curDate, -days_since_due);
        }
        case TaskPeriod::Monthly: {
            if (task.day < 1 || task.day > 31) return 0;
            int scheduled_year = year;
            int scheduled_month = month;
            if (day < static_cast<int>(task.day)) {
                --scheduled_month;
                if (scheduled_month == 0) {
                    scheduled_month = 12;
                    --scheduled_year;
                }
            }
            uint32_t out = 0;
            return make_ymd(scheduled_year, scheduled_month,
                            static_cast<int>(task.day), out) ? out : 0;
        }
        case TaskPeriod::Yearly: {
            const int scheduled_month = static_cast<int>(task.day / 100);
            const int scheduled_day = static_cast<int>(task.day % 100);
            const uint32_t cur_mmdd = curDate % 10000;
            const int scheduled_year =
                (cur_mmdd >= task.day) ? year : (year - 1);
            uint32_t out = 0;
            return make_ymd(scheduled_year, scheduled_month,
                            scheduled_day, out) ? out : 0;
        }
        default:
            return 0;
    }
}

}  // namespace

SelTaskScheduler::SelTaskScheduler(const IHolidayProvider* holiday_provider)
    : holiday_provider_(holiday_provider) {}

uint32_t SelTaskScheduler::add_task(SelTaskInfo info) {
    info.id = next_task_id_++;
    tasks_.push_back(std::move(info));
    return tasks_.back().id;
}

std::vector<SelTaskInfo> SelTaskScheduler::list_tasks() const {
    return tasks_;
}

void SelTaskScheduler::on_minute_end(uint32_t curDate,
                                      uint32_t curTime,
                                      FireCallback cb) {
    // wt WtSelEngine.cpp:226 用 nextTime = curTime + 1 算下一分钟；
    // ct 简化为直接用 curTime 比对（语义对齐 backtest 同步推进）。
    for (auto& task : tasks_) {
        if (!should_fire_(task, curDate, curTime)) continue;

        // 同日重复防护：Daily/Weekly/Monthly/Yearly 每周期只触发一次。
        // Minute 周期不去重（每分钟都触发）。
        if (task.last_exe_date == curDate && task.period != TaskPeriod::Minute) continue;
        task.last_exe_date = curDate;

        // fireTime_ms：调度触发时刻的真 epoch ms（由 curDate/curTime 经 CST 换算，
        // live/BT 同式确定性）。可与 now_ms 做延迟差值（fire-latency 监控口径）。
        const int64_t fire_time_ms = static_cast<int64_t>(
            xt_time::cst_wall_to_epoch_us(curDate, curTime * 100000u) / 1000ULL);

        if (cb) cb(task, curDate, curTime, fire_time_ms);
    }
}

bool SelTaskScheduler::should_fire_(SelTaskInfo& task,
                                     uint32_t curDate,
                                     uint32_t curTime) const {
    if (task.period == TaskPeriod::None) return false;

    // 时间匹配。strict 严格 == ；非 strict ≥ task.time（last_exe_date 防同日重复）。
    const bool time_match = task.strict_time ? (curTime == task.time)
                                              : (curTime >= task.time);
    if (!time_match) return false;

    const bool cur_is_holiday =
        holiday_provider_ &&
        holiday_provider_->is_holiday(task.trdtpl, curDate, /*ifWeekend=*/true);

    const uint32_t scheduled_date = scheduled_date_for_task(task, curDate);
    if (scheduled_date == 0) return false;
    if (scheduled_date == curDate) return !cur_is_holiday;
    if (cur_is_holiday || !holiday_provider_) return false;
    if (!holiday_provider_->is_holiday(task.trdtpl, scheduled_date, /*ifWeekend=*/true)) {
        return false;
    }
    return holiday_provider_->next_trading_date(task.trdtpl, scheduled_date) == curDate;
}

}  // namespace xt_engine_sel
