#include <gtest/gtest.h>
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <filesystem>
#include <unistd.h>
#include "xt_backtest/sel/XtSelDataLoader.h"

namespace {
class SelCalendarFallbackTest : public ::testing::Test {
protected:
    std::filesystem::path root_;
    void SetUp() override {
        std::string pattern = (std::filesystem::temp_directory_path() / "sel-calendar-XXXXXX").string();
        auto* path = mkdtemp(pattern.data());
        ASSERT_NE(path, nullptr);
        root_ = path;
        std::filesystem::create_directory(root_ / "ashare");
    }
    void TearDown() override {
        if (!root_.empty()) std::filesystem::remove_all(root_);
    }
};

// CSV 参数为空时必须真正读取 parquet；市场过滤和日期闭区间同时生效。
TEST_F(SelCalendarFallbackTest, EmptyCsvLoadsParquetCalendar) {
    arrow::Int32Builder dates;
    arrow::BooleanBuilder flags;
    arrow::StringBuilder exchanges;
    for (int date : {20260105, 20260106, 20260107, 20260108}) {
        ASSERT_TRUE(dates.Append(date).ok());
        ASSERT_TRUE(flags.Append(date != 20260106).ok());
        ASSERT_TRUE(exchanges.Append(date == 20260108 ? "SZSE" : "SSE").ok());
    }
    std::shared_ptr<arrow::Array> d, f, e;
    ASSERT_TRUE(dates.Finish(&d).ok());
    ASSERT_TRUE(flags.Finish(&f).ok());
    ASSERT_TRUE(exchanges.Finish(&e).ok());
    auto schema = arrow::schema({arrow::field("date", arrow::int32()),
                                 arrow::field("is_trading_day", arrow::boolean()),
                                 arrow::field("exchange", arrow::utf8())});
    auto table = arrow::Table::Make(schema, {d, f, e});
    auto stream = arrow::io::FileOutputStream::Open((root_ / "ashare/calendar.parquet").string());
    ASSERT_TRUE(stream.ok());
    ASSERT_TRUE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *stream, 1024).ok());
    ASSERT_TRUE((*stream)->Close().ok());
    auto result = xt_bt_sel::XtSelDataLoader::load("", root_.string(), 20260106, 20260108);
    EXPECT_EQ(result.source, "parquet:" + root_.string());
    EXPECT_EQ(result.trading_dates, (std::vector<uint32_t>{20260107}));
    ASSERT_NE(result.holiday_provider, nullptr);
    EXPECT_TRUE(result.holiday_provider->is_holiday("CHINA", 20260106, false));
    EXPECT_FALSE(result.holiday_provider->is_holiday("CHINA", 20260107, false));
}
}  // namespace
