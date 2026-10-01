/// @file
/// @brief SEL tradability 单测:check_tradability 纯函数矩阵 + CSV reader 解析/fail-fast 契约。

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <string>
#include <unistd.h>

#include "xt_backtest/sel/XtSelTradability.h"

using xt_bt_sel::TradabilityCalendar;
using xt_bt_sel::TradabilityFlags;
using xt_bt_sel::TradabilityVerdict;
using xt_bt_sel::check_tradability;

namespace {

/// @brief 写临时 CSV 并返回路径;RAII 清理(测试临时文件规范)。
class TempCsv {
public:
    explicit TempCsv(const std::string& body) {
        char path[] = "/tmp/xt_sel_trad_XXXXXX";
        const int fd = mkstemp(path);
        if (fd < 0) return;
        close(fd);
        path_ = path;
        std::ofstream ofs(path_, std::ios::out | std::ios::trunc);
        ofs << body;
    }
    ~TempCsv() {
        if (!path_.empty()) std::remove(path_.c_str());
    }
    [[nodiscard]] const std::string& path() const { return path_; }

private:
    std::string path_;
};

}  // namespace

// ── check_tradability 全矩阵 ──────────────────────────────────────────

TEST(SelTradabilityCheck, NullFlagsIsFullyTradable) {
    EXPECT_EQ(check_tradability(nullptr, /*is_buy=*/true), TradabilityVerdict::Ok);
    EXPECT_EQ(check_tradability(nullptr, /*is_buy=*/false), TradabilityVerdict::Ok);
}

TEST(SelTradabilityCheck, DefaultFlagsIsFullyTradable) {
    const TradabilityFlags fl;  // is_open=1, limit flags=0
    EXPECT_EQ(check_tradability(&fl, true), TradabilityVerdict::Ok);
    EXPECT_EQ(check_tradability(&fl, false), TradabilityVerdict::Ok);
}

TEST(SelTradabilityCheck, SuspendedRejectsBothSides) {
    TradabilityFlags fl;
    fl.is_open = false;
    EXPECT_EQ(check_tradability(&fl, true), TradabilityVerdict::Suspended);
    EXPECT_EQ(check_tradability(&fl, false), TradabilityVerdict::Suspended);
}

TEST(SelTradabilityCheck, LimitUpRejectsBuyOnly) {
    TradabilityFlags fl;
    fl.is_limit_up = true;
    EXPECT_EQ(check_tradability(&fl, true), TradabilityVerdict::LimitUpBuy);
    EXPECT_EQ(check_tradability(&fl, false), TradabilityVerdict::Ok);
}

TEST(SelTradabilityCheck, LimitDownRejectsSellOnly) {
    TradabilityFlags fl;
    fl.is_limit_down = true;
    EXPECT_EQ(check_tradability(&fl, false), TradabilityVerdict::LimitDownSell);
    EXPECT_EQ(check_tradability(&fl, true), TradabilityVerdict::Ok);
}

TEST(SelTradabilityCheck, SuspendedTakesPriorityOverLimitFlags) {
    TradabilityFlags fl;
    fl.is_open       = false;
    fl.is_limit_up   = true;
    fl.is_limit_down = true;
    EXPECT_EQ(check_tradability(&fl, true), TradabilityVerdict::Suspended);
    EXPECT_EQ(check_tradability(&fl, false), TradabilityVerdict::Suspended);
}

TEST(SelTradabilityCheck, BothLimitFlagsRejectPerSide) {
    TradabilityFlags fl;
    fl.is_limit_up   = true;
    fl.is_limit_down = true;
    EXPECT_EQ(check_tradability(&fl, true), TradabilityVerdict::LimitUpBuy);
    EXPECT_EQ(check_tradability(&fl, false), TradabilityVerdict::LimitDownSell);
}

// ── TradabilityCalendar::load 解析契约 ────────────────────────────────

TEST(SelTradabilityCalendar, LoadsWithHeaderAndFindsFlags) {
    const TempCsv csv(
        "date,code,is_open,is_limit_up,is_limit_down\n"
        "20260112,600000.SH,1,0,0\n"
        "20260113,600000.SH,0,0,0\n"
        "20260113,000001.SZ,1,1,0\n"
        "20260114,600000.SH,1,0,1\n");
    TradabilityCalendar cal;
    std::string err;
    ASSERT_TRUE(cal.load(csv.path(), err)) << err;
    EXPECT_EQ(cal.size(), 4u);
    EXPECT_EQ(cal.date_count(), 3u);

    const auto* normal = cal.find(20260112, "600000.SH");
    ASSERT_NE(normal, nullptr);
    EXPECT_TRUE(normal->is_open);
    EXPECT_FALSE(normal->is_limit_up);
    EXPECT_FALSE(normal->is_limit_down);

    const auto* suspended = cal.find(20260113, "600000.SH");
    ASSERT_NE(suspended, nullptr);
    EXPECT_FALSE(suspended->is_open);

    const auto* limit_up = cal.find(20260113, "000001.SZ");
    ASSERT_NE(limit_up, nullptr);
    EXPECT_TRUE(limit_up->is_open);
    EXPECT_TRUE(limit_up->is_limit_up);

    const auto* limit_down = cal.find(20260114, "600000.SH");
    ASSERT_NE(limit_down, nullptr);
    EXPECT_TRUE(limit_down->is_limit_down);
}

TEST(SelTradabilityCalendar, LoadsWithoutHeaderFirstDataRowKept) {
    const TempCsv csv("20260112,600000.SH,0,0,0\n");
    TradabilityCalendar cal;
    std::string err;
    ASSERT_TRUE(cal.load(csv.path(), err)) << err;
    EXPECT_EQ(cal.size(), 1u);
    const auto* fl = cal.find(20260112, "600000.SH");
    ASSERT_NE(fl, nullptr);
    EXPECT_FALSE(fl->is_open);
}

TEST(SelTradabilityCalendar, ToleratesCrlfLineEndings) {
    const TempCsv csv(
        "date,code,is_open,is_limit_up,is_limit_down\r\n"
        "20260112,600000.SH,1,1,0\r\n");
    TradabilityCalendar cal;
    std::string err;
    ASSERT_TRUE(cal.load(csv.path(), err)) << err;
    const auto* fl = cal.find(20260112, "600000.SH");
    ASSERT_NE(fl, nullptr);
    EXPECT_TRUE(fl->is_limit_up);
}

TEST(SelTradabilityCalendar, SkipsRowWithMissingColumns) {
    const TempCsv csv(
        "date,code,is_open,is_limit_up,is_limit_down\n"
        "20260112,600000.SH,1\n"          // 缺列 → skip
        "20260113,600000.SH,1,0,0\n");
    TradabilityCalendar cal;
    std::string err;
    ASSERT_TRUE(cal.load(csv.path(), err)) << err;
    EXPECT_EQ(cal.size(), 1u);
    EXPECT_EQ(cal.find(20260112, "600000.SH"), nullptr);
    EXPECT_NE(cal.find(20260113, "600000.SH"), nullptr);
}

TEST(SelTradabilityCalendar, SkipsRowWithExtraColumns) {
    // 真严格 5 列:>5 列(整行 >4 个逗号)与缺列同路径 skip + warn,不入库。
    // 旧实现在收满 5 字段后于下一逗号截断 → 6 列行"...,1,0,0,extra"被静默接受;本 case 钉修复。
    const TempCsv csv(
        "date,code,is_open,is_limit_up,is_limit_down\n"
        "20260112,600000.SH,1,0,0,extra\n"  // 6 列(flags 本身合法) → skip
        "20260113,600000.SH,1,0,0\n");
    TradabilityCalendar cal;
    std::string err;
    ASSERT_TRUE(cal.load(csv.path(), err)) << err;
    EXPECT_EQ(cal.size(), 1u);
    EXPECT_EQ(cal.find(20260112, "600000.SH"), nullptr);
    EXPECT_NE(cal.find(20260113, "600000.SH"), nullptr);
}

TEST(SelTradabilityCalendar, DuplicateDateCodeLastRowWins) {
    // 同 (date,code) 两行相反 flags → 后行覆盖前行(load 内 last-wins 契约锚)。
    const TempCsv csv(
        "date,code,is_open,is_limit_up,is_limit_down\n"
        "20260112,600000.SH,1,0,0\n"
        "20260112,600000.SH,0,1,1\n");
    TradabilityCalendar cal;
    std::string err;
    ASSERT_TRUE(cal.load(csv.path(), err)) << err;
    EXPECT_EQ(cal.size(), 2u);  // total_rows_ 计有效行数,非去重后 key 数
    const auto* fl = cal.find(20260112, "600000.SH");
    ASSERT_NE(fl, nullptr);
    EXPECT_FALSE(fl->is_open);
    EXPECT_TRUE(fl->is_limit_up);
    EXPECT_TRUE(fl->is_limit_down);
}

TEST(SelTradabilityCalendar, SkipsRowWithNonNumericDate) {
    // date 全数字先验:尾垃圾("2026O113")不走 stoul 宽松解析入库错误键。
    const TempCsv csv(
        "date,code,is_open,is_limit_up,is_limit_down\n"
        "2026O113,600000.SH,1,0,0\n"      // 字母 O 混入 → skip
        "20260114,600000.SH,1,0,0\n");
    TradabilityCalendar cal;
    std::string err;
    ASSERT_TRUE(cal.load(csv.path(), err)) << err;
    EXPECT_EQ(cal.size(), 1u);
    EXPECT_EQ(cal.find(2026, "600000.SH"), nullptr);  // 不得按 stoul 前缀 2026 入库
    EXPECT_NE(cal.find(20260114, "600000.SH"), nullptr);
}

TEST(SelTradabilityCalendar, SkipsRowWithOverlongDate) {
    // date ≤8 位(YYYYMMDD 域):9+ 位全数字源值(含 >2^32 截断伪造合法键情形)不得入库。
    const TempCsv csv(
        "date,code,is_open,is_limit_up,is_limit_down\n"
        "202601011,600000.SH,1,0,0\n"     // 9 位 → skip
        "4315227397,600000.SH,0,0,0\n"    // 20260101+2^32,截断会伪造 20260101 → skip
        "20260101,600000.SH,1,0,0\n");
    TradabilityCalendar cal;
    std::string err;
    ASSERT_TRUE(cal.load(csv.path(), err)) << err;
    EXPECT_EQ(cal.size(), 1u);
    const auto* fl = cal.find(20260101, "600000.SH");
    ASSERT_NE(fl, nullptr);
    EXPECT_TRUE(fl->is_open);  // 截断行若入库会把 is_open 盖成 0
}

TEST(SelTradabilityCalendar, SkipsRowWithNonBinaryFlagValue) {
    // 严格 0/1:非法值该行 skip(正确性输入宁缺勿错),不做"非 0 即 1"。
    const TempCsv csv(
        "date,code,is_open,is_limit_up,is_limit_down\n"
        "20260112,600000.SH,2,0,0\n"       // is_open=2 → skip
        "20260112,000001.SZ,1,x,0\n"       // is_limit_up=x → skip
        "20260112,600519.SH,1,0,true\n"    // 非 0/1 字面量 → skip
        "20260113,600000.SH,1,0,0\n");
    TradabilityCalendar cal;
    std::string err;
    ASSERT_TRUE(cal.load(csv.path(), err)) << err;
    EXPECT_EQ(cal.size(), 1u);
    EXPECT_EQ(cal.find(20260112, "600000.SH"), nullptr);
    EXPECT_EQ(cal.find(20260112, "000001.SZ"), nullptr);
    EXPECT_EQ(cal.find(20260112, "600519.SH"), nullptr);
    EXPECT_NE(cal.find(20260113, "600000.SH"), nullptr);
}

TEST(SelTradabilityCalendar, FindMissingDateOrCodeReturnsNull) {
    const TempCsv csv("20260112,600000.SH,1,0,0\n");
    TradabilityCalendar cal;
    std::string err;
    ASSERT_TRUE(cal.load(csv.path(), err)) << err;
    EXPECT_EQ(cal.find(20260113, "600000.SH"), nullptr);       // 缺日
    EXPECT_EQ(cal.find(20260112, "000001.SZ"), nullptr);       // 缺 code
    EXPECT_EQ(cal.find_date(20260113), nullptr);
    ASSERT_NE(cal.find_date(20260112), nullptr);
    EXPECT_EQ(cal.find_date(20260112)->size(), 1u);
}

TEST(SelTradabilityCalendar, CodeMatchIsExactNoNormalization) {
    // code 逐字符精确匹配(D7):不做大小写/后缀归一化。
    const TempCsv csv("20260112,600000.SH,0,0,0\n");
    TradabilityCalendar cal;
    std::string err;
    ASSERT_TRUE(cal.load(csv.path(), err)) << err;
    EXPECT_NE(cal.find(20260112, "600000.SH"), nullptr);
    EXPECT_EQ(cal.find(20260112, "600000.sh"), nullptr);
    EXPECT_EQ(cal.find(20260112, "600000"), nullptr);
}

// ── fail-fast 契约 ────────────────────────────────────────────────────

TEST(SelTradabilityCalendar, LoadFailsOnMissingFile) {
    TradabilityCalendar cal;
    std::string err;
    EXPECT_FALSE(cal.load("/nonexistent/path/tradability.csv", err));
    EXPECT_NE(err.find("cannot open"), std::string::npos) << err;
}

TEST(SelTradabilityCalendar, LoadFailsOnEmptyFile) {
    const TempCsv csv("");
    TradabilityCalendar cal;
    std::string err;
    EXPECT_FALSE(cal.load(csv.path(), err));
    EXPECT_NE(err.find("0 valid data rows"), std::string::npos) << err;
}

TEST(SelTradabilityCalendar, LoadFailsOnHeaderOnlyFile) {
    const TempCsv csv("date,code,is_open,is_limit_up,is_limit_down\n");
    TradabilityCalendar cal;
    std::string err;
    EXPECT_FALSE(cal.load(csv.path(), err));
    EXPECT_NE(err.find("0 valid data rows"), std::string::npos) << err;
}

TEST(SelTradabilityCalendar, LoadFailsWhenAllDataRowsInvalid) {
    const TempCsv csv(
        "date,code,is_open,is_limit_up,is_limit_down\n"
        "20260112,600000.SH,yes,no,no\n");
    TradabilityCalendar cal;
    std::string err;
    EXPECT_FALSE(cal.load(csv.path(), err));
    EXPECT_NE(err.find("0 valid data rows"), std::string::npos) << err;
    EXPECT_NE(err.find("skipped 1"), std::string::npos) << err;
}
