#include <gtest/gtest.h>
#include <set>
#include <string>
#include <unordered_map>

#include "xt_engine/sel/SelTaskScheduler.h"
#include "xt_util/time/XtTime.h"   // cst_wall_to_epoch_us (fireTime epoch 锚)

namespace xt_engine_sel {
namespace {

class FakeHolidayProvider : public IHolidayProvider {
public:
    bool is_holiday(const std::string&, uint32_t date, bool ifWeekend) const override {
        (void)ifWeekend;
        return holidays_.count(date) > 0;
    }
    uint32_t next_trading_date(const std::string&, uint32_t curDate) const override {
        const auto it = next_trading_dates_.find(curDate);
        if (it != next_trading_dates_.end()) return it->second;
        return curDate + 1;
    }
    void mark_holiday(uint32_t date) { holidays_.insert(date); }
    void set_next_trading_date(uint32_t date, uint32_t next) {
        next_trading_dates_[date] = next;
    }

private:
    std::set<uint32_t> holidays_;
    std::unordered_map<uint32_t, uint32_t> next_trading_dates_;
};

}  // namespace

// SelTaskScheduler 单测

TEST(SelTaskScheduler, Daily_TriggersOncePerDay) {
    FakeHolidayProvider h;
    SelTaskScheduler sched(&h);
    SelTaskInfo task;
    task.name = "daily_15:00";
    task.strategy_id = "test_sid";
    task.period = TaskPeriod::Daily;
    task.time = 1500;
    task.strict_time = true;
    sched.add_task(task);

    int fire_count = 0;
    sched.on_minute_end(20260109, 1500,
        [&fire_count](const SelTaskInfo&, uint32_t, uint32_t, int64_t) { fire_count++; });
    sched.on_minute_end(20260109, 1500,
        [&fire_count](const SelTaskInfo&, uint32_t, uint32_t, int64_t) { fire_count++; });
    EXPECT_EQ(fire_count, 1);  // 同日去重
}

TEST(SelTaskScheduler, Monthly_TriggersOnDayOfMonth) {
    FakeHolidayProvider h;
    SelTaskScheduler sched(&h);
    SelTaskInfo task;
    task.period = TaskPeriod::Monthly;
    task.day = 15;
    task.time = 1500;
    task.strict_time = true;
    sched.add_task(task);

    int fire_count = 0;
    sched.on_minute_end(20260115, 1500,
        [&fire_count](const SelTaskInfo&, uint32_t, uint32_t, int64_t) { fire_count++; });
    EXPECT_EQ(fire_count, 1);

    sched.on_minute_end(20260116, 1500,
        [&fire_count](const SelTaskInfo&, uint32_t, uint32_t, int64_t) { fire_count++; });
    EXPECT_EQ(fire_count, 1);  // 16 号不该触发
}

TEST(SelTaskScheduler, Holiday_SkipsTrigger) {
    FakeHolidayProvider h;
    h.mark_holiday(20260120);
    SelTaskScheduler sched(&h);
    SelTaskInfo task;
    task.period = TaskPeriod::Daily;
    task.time = 1500;
    task.strict_time = true;
    sched.add_task(task);

    int fire_count = 0;
    sched.on_minute_end(20260120, 1500,
        [&fire_count](const SelTaskInfo&, uint32_t, uint32_t, int64_t) { fire_count++; });
    EXPECT_EQ(fire_count, 0);  // 节假日不触发
}

TEST(SelTaskScheduler, MonthlyHolidayRollsToNextTradingDateOnce) {
    FakeHolidayProvider h;
    h.mark_holiday(20260115);
    h.set_next_trading_date(20260115, 20260116);
    SelTaskScheduler sched(&h);
    SelTaskInfo task;
    task.period = TaskPeriod::Monthly;
    task.day = 15;
    task.time = 1500;
    task.strict_time = true;
    sched.add_task(task);

    int fire_count = 0;
    sched.on_minute_end(20260115, 1500,
        [&fire_count](const SelTaskInfo&, uint32_t, uint32_t, int64_t) { fire_count++; });
    EXPECT_EQ(fire_count, 0);

    sched.on_minute_end(20260116, 1500,
        [&fire_count](const SelTaskInfo&, uint32_t, uint32_t, int64_t) { fire_count++; });
    EXPECT_EQ(fire_count, 1);

    sched.on_minute_end(20260116, 1500,
        [&fire_count](const SelTaskInfo&, uint32_t, uint32_t, int64_t) { fire_count++; });
    sched.on_minute_end(20260117, 1500,
        [&fire_count](const SelTaskInfo&, uint32_t, uint32_t, int64_t) { fire_count++; });
    EXPECT_EQ(fire_count, 1);
}

TEST(SelTaskScheduler, WeeklyHolidayRollsAcrossWeekend) {
    FakeHolidayProvider h;
    h.mark_holiday(20260116);  // Friday
    h.set_next_trading_date(20260116, 20260119);
    SelTaskScheduler sched(&h);
    SelTaskInfo task;
    task.period = TaskPeriod::Weekly;
    task.day = 5;  // Friday
    task.time = 1500;
    task.strict_time = true;
    sched.add_task(task);

    int fire_count = 0;
    sched.on_minute_end(20260116, 1500,
        [&fire_count](const SelTaskInfo&, uint32_t, uint32_t, int64_t) { fire_count++; });
    sched.on_minute_end(20260119, 1500,
        [&fire_count](const SelTaskInfo&, uint32_t, uint32_t, int64_t) { fire_count++; });
    sched.on_minute_end(20260120, 1500,
        [&fire_count](const SelTaskInfo&, uint32_t, uint32_t, int64_t) { fire_count++; });

    EXPECT_EQ(fire_count, 1);
}

TEST(SelTaskScheduler, StrictTime_BlocksDelayed) {
    FakeHolidayProvider h;
    SelTaskScheduler sched(&h);
    SelTaskInfo task;
    task.period = TaskPeriod::Daily;
    task.time = 1500;
    task.strict_time = true;
    sched.add_task(task);

    int fire_count = 0;
    sched.on_minute_end(20260120, 1501,
        [&fire_count](const SelTaskInfo&, uint32_t, uint32_t, int64_t) { fire_count++; });
    EXPECT_EQ(fire_count, 0);  // strict 模式 1501 不等于 1500
}

TEST(SelTaskScheduler, NonStrict_FiresAfterTime) {
    FakeHolidayProvider h;
    SelTaskScheduler sched(&h);
    SelTaskInfo task;
    task.period = TaskPeriod::Daily;
    task.time = 1500;
    task.strict_time = false;
    sched.add_task(task);

    int fire_count = 0;
    sched.on_minute_end(20260120, 1502,
        [&fire_count](const SelTaskInfo&, uint32_t, uint32_t, int64_t) { fire_count++; });
    EXPECT_EQ(fire_count, 1);  // non-strict ≥ 1500 即触发
}

TEST(SelTaskScheduler, AddTask_ReturnsIncrementalId) {
    FakeHolidayProvider h;
    SelTaskScheduler sched(&h);
    SelTaskInfo t1;
    t1.period = TaskPeriod::Daily;
    SelTaskInfo t2 = t1;
    uint32_t id1 = sched.add_task(t1);
    uint32_t id2 = sched.add_task(t2);
    EXPECT_GT(id1, 0u);
    EXPECT_GT(id2, id1);
    EXPECT_EQ(sched.list_tasks().size(), 2u);
}

TEST(SelTaskScheduler, FireCallback_PassesFireTime) {
    FakeHolidayProvider h;
    SelTaskScheduler sched(&h);
    SelTaskInfo task;
    task.period = TaskPeriod::Daily;
    task.time = 1500;
    task.strict_time = true;
    sched.add_task(task);

    int64_t captured_fire_time = 0;
    sched.on_minute_end(20260109, 1500,
        [&captured_fire_time](const SelTaskInfo&, uint32_t, uint32_t, int64_t fireTime_ms) {
            captured_fire_time = fireTime_ms;
        });
    EXPECT_GT(captured_fire_time, 0);  // 非零 ms 时间戳
}

// T2.ENG-8：fireTime 是 curDate/curTime 经 CST 换算的真 epoch ms（非墙钟 packed）。
TEST(SelTaskScheduler, FireCallbackPassesEpochFireTime) {
    FakeHolidayProvider h;
    SelTaskScheduler sched(&h);
    SelTaskInfo task;
    task.period = TaskPeriod::Daily;
    task.time = 940;
    task.strict_time = true;
    sched.add_task(task);

    int64_t captured = 0;
    sched.on_minute_end(20260113, 940,
        [&captured](const SelTaskInfo&, uint32_t, uint32_t, int64_t fireTime_ms) {
            captured = fireTime_ms;
        });

    const int64_t expect_ms = static_cast<int64_t>(
        xt_time::cst_wall_to_epoch_us(20260113, 940u * 100000u) / 1000ULL);
    EXPECT_EQ(captured, expect_ms);
    EXPECT_GT(captured, 1'700'000'000'000LL);  // epoch ms 量级 sanity（非 packed/相对值）
}

}  // namespace xt_engine_sel
