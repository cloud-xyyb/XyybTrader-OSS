// ============================================================================
// XtDatalakeReaderTest — datalake calendar parquet reader 回归。
// ============================================================================
//
// 用 arrow builder 写真 parquet fixture 到 `<root>/ashare/...`，再经 reader 读回
// 断言。覆盖宽类型 coercion（date Int32/Int64、flag Bool/Int8）、exchange 过滤、
// 二分边界、required 列与文件缺失。

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>

#include "xt_data/datalake/XtCalendarReader.h"
#include "xt_data/datalake/XtDatalakeRoot.h"

namespace xt_data::datalake {
namespace {

namespace fs = std::filesystem;

// ── arrow array builders（Append 循环，避开各类型 AppendValues 重载差异）────────
std::shared_ptr<arrow::Array> i32_array(const std::vector<int32_t>& v) {
    arrow::Int32Builder b;
    for (auto x : v) EXPECT_TRUE(b.Append(x).ok());
    std::shared_ptr<arrow::Array> out;
    EXPECT_TRUE(b.Finish(&out).ok());
    return out;
}
std::shared_ptr<arrow::Array> i64_array(const std::vector<int64_t>& v) {
    arrow::Int64Builder b;
    for (auto x : v) EXPECT_TRUE(b.Append(x).ok());
    std::shared_ptr<arrow::Array> out;
    EXPECT_TRUE(b.Finish(&out).ok());
    return out;
}
std::shared_ptr<arrow::Array> i8_array(const std::vector<int8_t>& v) {
    arrow::Int8Builder b;
    for (auto x : v) EXPECT_TRUE(b.Append(x).ok());
    std::shared_ptr<arrow::Array> out;
    EXPECT_TRUE(b.Finish(&out).ok());
    return out;
}
std::shared_ptr<arrow::Array> bool_array(const std::vector<bool>& v) {
    arrow::BooleanBuilder b;
    for (bool x : v) EXPECT_TRUE(b.Append(x).ok());
    std::shared_ptr<arrow::Array> out;
    EXPECT_TRUE(b.Finish(&out).ok());
    return out;
}
std::shared_ptr<arrow::Array> f64_array(const std::vector<double>& v) {
    arrow::DoubleBuilder b;
    for (double x : v) EXPECT_TRUE(b.Append(x).ok());
    std::shared_ptr<arrow::Array> out;
    EXPECT_TRUE(b.Finish(&out).ok());
    return out;
}
std::shared_ptr<arrow::Array> str_array(const std::vector<std::string>& v) {
    arrow::StringBuilder b;
    for (const auto& x : v) EXPECT_TRUE(b.Append(x).ok());
    std::shared_ptr<arrow::Array> out;
    EXPECT_TRUE(b.Finish(&out).ok());
    return out;
}

void write_parquet(const fs::path& path,
                   const std::shared_ptr<arrow::Schema>& schema,
                   const std::vector<std::shared_ptr<arrow::Array>>& cols) {
    fs::create_directories(path.parent_path());
    auto table = arrow::Table::Make(schema, cols);
    auto maybe_out = arrow::io::FileOutputStream::Open(path.string());
    ASSERT_TRUE(maybe_out.ok());
    std::shared_ptr<arrow::io::FileOutputStream> out = *maybe_out;
    ASSERT_TRUE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1024).ok());
}

class XtDatalakeReaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        root_ = fs::temp_directory_path() /
                ("xt_datalake_reader_" + std::to_string(getpid()) + "_" +
                 info->test_suite_name() + "_" + info->name());
        fs::create_directories(root_);
        dl_.set_root(root_.string());
    }
    void TearDown() override {
        std::error_code ec;
        fs::remove_all(root_, ec);
    }

    fs::path calendar_path() const { return root_ / "ashare" / "calendar.parquet"; }
    fs::path pricebar_path(const std::string& ts_code) const {
        return root_ / "ashare" / "pro_bar_hfq" / (ts_code + ".parquet");
    }

    fs::path root_;
    XtDatalakeRoot dl_;
};

// ── XtCalendarReader ────────────────────────────────────────────────────────

// Int32 date + Bool flag：非交易日过滤 + is_trading_day O(1) 查询。
TEST_F(XtDatalakeReaderTest, Calendar_Int32Date_BoolFlag) {
    write_parquet(calendar_path(),
        arrow::schema({arrow::field("date", arrow::int32()),
                       arrow::field("is_trading_day", arrow::boolean()),
                       arrow::field("exchange", arrow::utf8())}),
        {i32_array({20260105, 20260106, 20260107}),
         bool_array({true, false, true}),
         str_array({"SSE", "SSE", "SSE"})});

    XtCalendarReader r(dl_);
    ASSERT_TRUE(r.load("SSE"));
    EXPECT_EQ(r.trading_dates(), (std::vector<uint32_t>{20260105, 20260107}));
    EXPECT_TRUE(r.is_trading_day(20260105));
    EXPECT_FALSE(r.is_trading_day(20260106));  // 非交易日被过滤
    EXPECT_TRUE(r.is_trading_day(20260107));
    EXPECT_FALSE(r.is_trading_day(20260108));
}

// Int64 date + Int8 flag：宽类型 coercion（历史 ingest 批次）读对，Int8 0 须过滤。
TEST_F(XtDatalakeReaderTest, Calendar_Int64Date_Int8Flag_WideTypeCoercion) {
    write_parquet(calendar_path(),
        arrow::schema({arrow::field("date", arrow::int64()),
                       arrow::field("is_trading_day", arrow::int8()),
                       arrow::field("exchange", arrow::utf8())}),
        {i64_array({20260105, 20260106, 20260107}),
         i8_array({1, 0, 1}),  // Int8 flag：0 = 非交易日，验证 Value(i)!=0 过滤分支
         str_array({"SSE", "SSE", "SSE"})});

    XtCalendarReader r(dl_);
    ASSERT_TRUE(r.load("SSE"));
    EXPECT_EQ(r.trading_dates(), (std::vector<uint32_t>{20260105, 20260107}));
    EXPECT_FALSE(r.is_trading_day(20260106));  // Int8 0 → 过滤
}

// exchange 列过滤：只取目标市场；空 exchange = 不过滤。
TEST_F(XtDatalakeReaderTest, Calendar_ExchangeFilter) {
    write_parquet(calendar_path(),
        arrow::schema({arrow::field("date", arrow::int32()),
                       arrow::field("is_trading_day", arrow::boolean()),
                       arrow::field("exchange", arrow::utf8())}),
        {i32_array({20260105, 20260106, 20260107}),
         bool_array({true, true, true}),
         str_array({"SSE", "SZSE", "SSE"})});

    XtCalendarReader sse(dl_);
    ASSERT_TRUE(sse.load("SSE"));
    EXPECT_EQ(sse.trading_dates(), (std::vector<uint32_t>{20260105, 20260107}));

    XtCalendarReader szse(dl_);
    ASSERT_TRUE(szse.load("SZSE"));
    EXPECT_EQ(szse.trading_dates(), (std::vector<uint32_t>{20260106}));

    XtCalendarReader all(dl_);
    ASSERT_TRUE(all.load(""));  // 空 → 不过滤
    EXPECT_EQ(all.trading_dates(), (std::vector<uint32_t>{20260105, 20260106, 20260107}));
}

// next/prev/range 二分边界（首/末/越界）。
TEST_F(XtDatalakeReaderTest, Calendar_BinarySearchBoundaries) {
    write_parquet(calendar_path(),
        arrow::schema({arrow::field("date", arrow::int32()),
                       arrow::field("is_trading_day", arrow::boolean()),
                       arrow::field("exchange", arrow::utf8())}),
        {i32_array({20260105, 20260107, 20260109}),
         bool_array({true, true, true}),
         str_array({"SSE", "SSE", "SSE"})});

    XtCalendarReader r(dl_);
    ASSERT_TRUE(r.load("SSE"));

    // next = 首个 >= cur
    EXPECT_EQ(r.next_trading_date(20260104), 20260105u);
    EXPECT_EQ(r.next_trading_date(20260105), 20260105u);
    EXPECT_EQ(r.next_trading_date(20260106), 20260107u);
    EXPECT_EQ(r.next_trading_date(20260110), 0u);  // 越界

    // prev = 严格 < cur 的上一个
    EXPECT_EQ(r.prev_trading_date(20260105), 0u);  // 无更早
    EXPECT_EQ(r.prev_trading_date(20260106), 20260105u);
    EXPECT_EQ(r.prev_trading_date(20260109), 20260107u);
    EXPECT_EQ(r.prev_trading_date(20260110), 20260109u);

    // range = 闭区间 [from, to]
    EXPECT_EQ(r.trading_dates_in_range(20260105, 20260107),
              (std::vector<uint32_t>{20260105, 20260107}));
    EXPECT_EQ(r.trading_dates_in_range(20260106, 20260108),
              (std::vector<uint32_t>{20260107}));
    EXPECT_EQ(r.trading_dates_in_range(20260105, 20260109),
              (std::vector<uint32_t>{20260105, 20260107, 20260109}));
    EXPECT_TRUE(r.trading_dates_in_range(20260110, 20260120).empty());
}

// 缺 required 列（is_trading_day）→ load 返 false。
TEST_F(XtDatalakeReaderTest, Calendar_MissingRequiredColumnFails) {
    write_parquet(calendar_path(),
        arrow::schema({arrow::field("date", arrow::int32())}),
        {i32_array({20260105, 20260106})});

    XtCalendarReader r(dl_);
    EXPECT_FALSE(r.load("SSE"));
    EXPECT_TRUE(r.trading_dates().empty());
}

// 文件不存在 → load 返 false。
TEST_F(XtDatalakeReaderTest, Calendar_MissingFileFails) {
    XtCalendarReader r(dl_);
    EXPECT_FALSE(r.load("SSE"));
}

}  // namespace
}  // namespace xt_data::datalake
