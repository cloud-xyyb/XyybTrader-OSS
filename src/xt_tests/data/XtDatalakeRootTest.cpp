#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <unistd.h>

#include "xt_data/datalake/XtDatalakeRoot.h"
#include "xt_data/datalake/schema_v1.h"

namespace xt_data::datalake {
namespace {

namespace fs = std::filesystem;

class XtDatalakeRootTest : public ::testing::Test {
protected:
    void SetUp() override {
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        tmp_root_ = fs::temp_directory_path() /
                    ("xt_datalake_test_" + std::to_string(getpid()) + "_" +
                     info->test_suite_name() + "_" + info->name());
        fs::create_directories(tmp_root_);
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(tmp_root_, ec);
    }

    void write_schema_json(const std::string& content) {
        const fs::path meta_dir = tmp_root_ / "_meta";
        fs::create_directories(meta_dir);
        std::ofstream ofs(meta_dir / "schema_version.json");
        ofs << content;
    }

    fs::path tmp_root_;
};

TEST_F(XtDatalakeRootTest, ResolvePath_PriceBarHfq) {
    XtDatalakeRoot dl;
    dl.set_root(tmp_root_.string());
    const auto path = dl.resolve_path(TableKind::PriceBarHfq, {"600000.SH"});
    EXPECT_NE(path.find("ashare"), std::string::npos);
    EXPECT_NE(path.find("pro_bar_hfq"), std::string::npos);
    EXPECT_NE(path.find("600000.SH.parquet"), std::string::npos);
}

TEST_F(XtDatalakeRootTest, ResolvePath_DailyBasic_PartitionByMonth) {
    XtDatalakeRoot dl;
    dl.set_root(tmp_root_.string());
    const auto path = dl.resolve_path(TableKind::DailyBasic, {"2026", "2026-01"});
    EXPECT_NE(path.find("daily_basic"), std::string::npos);
    EXPECT_NE(path.find("2026"), std::string::npos);
    EXPECT_NE(path.find("2026-01.parquet"), std::string::npos);
}

TEST_F(XtDatalakeRootTest, ResolvePath_Calendar_SingleFile) {
    XtDatalakeRoot dl;
    dl.set_root(tmp_root_.string());
    const auto path = dl.resolve_path(TableKind::Calendar, {});
    EXPECT_NE(path.find("calendar.parquet"), std::string::npos);
}

TEST_F(XtDatalakeRootTest, ValidateSchema_StrictThrows_OnMissingFile) {
    XtDatalakeRoot dl;
    dl.set_root(tmp_root_.string());
    // 不写 schema_version.json，期望 strict 抛异常
    EXPECT_THROW(dl.validate_schema({{"ashare.calendar", 1, {"date"}}}),
                 std::runtime_error);
}

TEST_F(XtDatalakeRootTest, ValidateSchema_AllowSkew_NoThrow) {
    XtDatalakeRoot dl;
    dl.set_root(tmp_root_.string());
    dl.set_compat_policy(SchemaCompatPolicy::AllowSkew);
    // 不写 schema_version.json，但 AllowSkew 允许返 false 不 throw
    EXPECT_NO_THROW({
        const bool ok = dl.validate_schema({{"ashare.calendar", 1, {"date"}}});
        EXPECT_FALSE(ok);
    });
}

TEST_F(XtDatalakeRootTest, ValidateSchema_StrictPasses_WhenMatch) {
    XtDatalakeRoot dl;
    dl.set_root(tmp_root_.string());
    write_schema_json(R"({
      "version": "1.0.0",
      "tables": {
        "ashare.calendar": {
          "schema_version": 1,
          "columns": ["date", "is_trading_day", "exchange"]
        }
      },
      "last_updated": "2026-05-09T00:00:00Z"
    })");
    EXPECT_TRUE(dl.validate_schema({{"ashare.calendar", 1, {"date", "is_trading_day"}}}));
}

TEST_F(XtDatalakeRootTest, ValidateSchema_StrictFails_OnVersionMismatch) {
    XtDatalakeRoot dl;
    dl.set_root(tmp_root_.string());
    write_schema_json(R"({
      "tables": {
        "ashare.calendar": {
          "schema_version": 2,
          "columns": ["date"]
        }
      }
    })");
    EXPECT_THROW(dl.validate_schema({{"ashare.calendar", 1, {"date"}}}),
                 std::runtime_error);
}

TEST_F(XtDatalakeRootTest, ValidateSchema_StrictFails_OnMissingColumn) {
    XtDatalakeRoot dl;
    dl.set_root(tmp_root_.string());
    write_schema_json(R"({
      "tables": {
        "ashare.calendar": {
          "schema_version": 1,
          "columns": ["date"]
        }
      }
    })");
    // expects "is_trading_day" 但 actual columns 只有 "date"
    EXPECT_THROW(dl.validate_schema({{"ashare.calendar", 1, {"date", "is_trading_day"}}}),
                 std::runtime_error);
}

TEST(SchemaV1, ConstantsMatchProtocol) {
    // SSOT verification: cra (ingest_ashare.py) 写入端 + ct (本端) 字段名共享。
    EXPECT_STREQ(schema_v1::calendar::kColDate, "date");
    EXPECT_STREQ(schema_v1::calendar::kColIsTradingDay, "is_trading_day");
    EXPECT_STREQ(schema_v1::pro_bar_hfq::kColTsCode, "ts_code");
    EXPECT_STREQ(schema_v1::pro_bar_hfq::kColTradeDate, "trade_date");
    EXPECT_STREQ(schema_v1::pro_bar_hfq::kColClose, "close");
    EXPECT_EQ(schema_v1::kSchemaVersion, 1);
}

}  // namespace
}  // namespace xt_data::datalake
