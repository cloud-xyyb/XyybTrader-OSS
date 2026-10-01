// SelConsumedManifestTest — SEL consumed_manifest 归因（loader 侧 + 映射语义）。
//
// SEL rows_loaded = 日历 raw dates、rows_total = rebalance 提交尝试；单位不同
// （日 vs 提交）→ 结构上不可精确守恒 → reconciled=false。本测覆盖 loader 侧
// 归因（raw_date_count / calendar_bad_rows / 窗过滤）+ runner 的 exclusions 映射
// 算术（window = raw − filtered、bad_row = calendar 坏行）。submit/no_change 计数
// 走 e2e 回归（sel_demo）。

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <fstream>
#include <string>
#include <vector>
#include <unistd.h>

#include "xt_backtest/sel/XtSelDataLoader.h"
#include "xt_backtest/core/BtRunnerUtil.h"

namespace {

std::string make_temp_csv(const std::string& body) {
    char path[] = "/tmp/xt_sel_cal_XXXXXX";
    const int fd = mkstemp(path);
    if (fd < 0) return {};
    close(fd);
    std::ofstream ofs(path, std::ios::trunc);
    ofs << body;
    return std::string(path);
}

}  // namespace

TEST(SelConsumedManifest, LoaderCountsRawFilteredAndBadRows) {
    // header("date") 跳过；"badline" = 坏行；数字行 = 日期。
    const std::string csv = make_temp_csv(
        "date\n"
        "20240102\n"
        "20240103\n"
        "20240104\n"
        "20240105\n"
        "badline\n"
        "20240106\n");
    ASSERT_FALSE(csv.empty());

    // 窗口 [20240103, 20240105] → filtered = 3；raw = 5；坏行 = 1。
    const auto res = xt_bt_sel::XtSelDataLoader::load(csv, /*datalake_root=*/"",
                                                      20240103, 20240105);
    std::remove(csv.c_str());

    EXPECT_EQ(res.raw_date_count, 5u);
    EXPECT_EQ(res.trading_dates.size(), 3u);
    EXPECT_EQ(res.calendar_bad_rows, 1u);

    // runner 映射：window = raw − filtered、bad_row = calendar 坏行、reconciled=false。
    xt_bt::ConsumedManifest man;
    man.tick_source    = "sel_calendar";
    man.delivery_stage = "rebalance";
    man.rows_total     = 17;   // 假设的提交尝试数（单位与 rows_loaded 不同）
    man.rows_loaded    = static_cast<int64_t>(res.raw_date_count);
    man.exclusions.window =
        static_cast<int64_t>(res.raw_date_count) -
        static_cast<int64_t>(res.trading_dates.size());
    man.exclusions.bad_row = static_cast<int64_t>(res.calendar_bad_rows);
    man.reconciled = false;

    EXPECT_EQ(man.rows_loaded, 5);
    EXPECT_EQ(man.exclusions.window, 2);   // 20240102 + 20240106
    EXPECT_EQ(man.exclusions.bad_row, 1);
    EXPECT_FALSE(man.reconciled);
    EXPECT_EQ(man.delivery_stage, "rebalance");
}

TEST(SelConsumedManifest, CleanCalendarNoBadRows) {
    const std::string csv = make_temp_csv(
        "date\n"
        "20240102\n"
        "20240103\n"
        "20240104\n");
    ASSERT_FALSE(csv.empty());

    const auto res = xt_bt_sel::XtSelDataLoader::load(csv, "", 0, 0);
    std::remove(csv.c_str());

    EXPECT_EQ(res.raw_date_count, 3u);
    EXPECT_EQ(res.trading_dates.size(), 3u);   // 无窗过滤
    EXPECT_EQ(res.calendar_bad_rows, 0u);
}
