// SelWeightsCsvStrategy normalize 幅度告警单测。
//
// 背景：SelWeightsCsvStrategy 默认 normalize=true 时把 weights 除以
// gross（sum(|w|)）缩放到 1.0。当来源数据本身 gross 已显著偏离 1.0（如
// 0.95 类刻意保留的现金 buffer）时，这一缩放会被静默放大到满仓——
// 隐性加杠杆。本测试钉：normalize 生效且 |gross-1| > 0.5% 时触发一次性
// warn + 计数；gross==1.0（no-op 缩放）时不触发；normalize:false 时即使
// gross 偏离也不触发（未生效不应误报）。
//
// 插件本身无既有单测基建（.cpp 只作为独立 .so 编译，未接入任何 gtest
// 目标）；本文件把 SelWeightsCsvStrategy.cpp 加入 xt_strategy_tests 编译，
// 直接驱动 init/on_init/on_schedule 三段真实生产路径（写临时 weights.csv
// 触发 load_weights_csv_，非绕过）。log 断言走 FakeSelCtx 捕获 log_warn
// 文本 + 计数器双重观测——计数器 accessor 只是 Plan 里提到的 fallback，
// 本文件实测证明日志断言可行，故两者都测。

#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include "xt_strategy_plugin/sel_weights_csv/SelWeightsCsvStrategy.h"
#include "api/IStrategyContext.h"
#include "xt_strategy_engine/XtStrategyInitConfig.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtOrderTypes.h"

namespace {

namespace fs = std::filesystem;

// ── Fake IStrategyContext（模板取自 XtSelBacktestE2ETest.cpp 的 MockSelCtx / ──
//    HftTickRevertRejectBackoffTest.cpp 的 FakeRevertCtx）：29 pure virtual
//    stub no-op；本测试只关心 log_warn/log_info 的捕获。
class FakeSelCtx : public IStrategyContext {
public:
    uint64_t buy_fp(const char*, int64_t, int64_t, XtTimeInForce, const char*, XtOffset) override { return 0; }
    uint64_t sell_fp(const char*, int64_t, int64_t, XtTimeInForce, const char*, XtOffset) override { return 0; }
    void cancel(uint64_t) override {}
    const XtTick* get_last_tick(const char*) override { return nullptr; }
    int64_t get_last_price_fp(const char*) override { return 0; }
    size_t get_ticks(const char*, XtTick*, size_t) override { return 0; }
    size_t get_bars(const char*, const char*, XtBar*, size_t) override { return 0; }
    size_t get_open_orders(const char*, XtOrderExecUpdate*, size_t) override { return 0; }
    int64_t get_position_fp(const char*) override { return 0; }
    int64_t projected_inflight_fp(const char*) override { return 0; }
    int64_t get_position_avg_price_fp(const char*) override { return 0; }
    int64_t get_position_unrealized_fp(const char*) override { return 0; }
    size_t get_position_detail(const char*,
                                xt_runtime::XtStraPositionDetail*,
                                size_t) override { return 0; }
    int64_t get_cash_fp() override { return 0; }
    int64_t get_equity_fp() override { return 0; }
    const xt_runtime::XtStraContractInfo* get_contract_info(const char*) override { return nullptr; }
    uint32_t get_date() override { return 0; }
    uint32_t get_trading_date() override { return 0; }
    uint32_t get_time_ms() override { return 0; }
    std::string get_param_str(const char*, const char* def) override { return def ? def : ""; }
    int64_t get_param_int(const char*, int64_t def) override { return def; }
    double get_param_double(const char*, double def) override { return def; }
    bool get_param_bool(const char*, bool def) override { return def; }
    void sub_tick(const char*) override {}
    void sub_bar(const char*, const char*, size_t) override {}
    void log_debug(const char*) override {}
    void log_info(const char*) override { ++info_count; }
    void log_warn(const char* msg) override {
        ++warn_count;
        if (msg) last_warn = msg;
    }
    void report_metric(const char*, double) override {}
    void save_userdata(const char*, const char*) override {}
    const char* load_userdata(const char*, const char* def) override { return def; }
    // 槽选择断言的观测口：策略真正推给引擎的那一组权重。
    void set_target_weights(const std::unordered_map<std::string, double>& w) override {
        ++weights_calls;
        last_weights = w;
    }

    // observable test counters
    int warn_count = 0;
    int info_count = 0;
    int weights_calls = 0;
    std::string last_warn;
    std::unordered_map<std::string, double> last_weights;
};

// ── 临时 weights.csv RAII helper（模板取自 xt_backtest 姊妹测试的 TempPath）──
class TempCsvPath {
public:
    TempCsvPath() {
        path_ = fs::temp_directory_path() /
                ("sel_weights_csv_test_" + std::to_string(::getpid()) + "_" +
                 std::to_string(reinterpret_cast<uintptr_t>(this)) + ".csv");
    }
    ~TempCsvPath() {
        std::error_code ec;
        fs::remove(path_, ec);
    }
    std::string str() const { return path_.string(); }
    void write(const std::string& body) const {
        std::ofstream f(path_);
        f << body;
    }

private:
    fs::path path_;
};

/// @brief 装配一个已 init+on_init 完成、csv 已加载的 strategy 实例。
SelWeightsCsvStrategy make_loaded_strategy(FakeSelCtx& ctx,
                                            const std::string& csv_path,
                                            bool normalize,
                                            const std::string& task_uid = "") {
    SelWeightsCsvStrategy strat;
    XtStrategyInitConfig cfg;
    cfg.values["weights_path"] = csv_path;
    cfg.values["normalize"] = normalize ? "true" : "false";
    if (!task_uid.empty()) cfg.values["task_uid"] = task_uid;
    strat.init(cfg);
    strat.on_init(&ctx);
    return strat;
}

}  // namespace

// gross=0.95（sub-unity，超 0.5% 阈值）+ normalize 生效 → 触发一次性 warn + 计数。
TEST(SelWeightsCsvStrategy, NormalizeDriftWarnTriggersOnSubUnityGross) {
    TempCsvPath csv;
    csv.write(
        "date,symbol,weight\n"
        "20260109,600000.SH,0.30\n"
        "20260109,000001.SZ,0.35\n"
        "20260109,600519.SH,0.30\n");  // sum(|w|) = 0.95

    FakeSelCtx ctx;
    auto strat = make_loaded_strategy(ctx, csv.str(), /*normalize=*/true);

    strat.on_schedule(&ctx, 20260109, 1500, 0);

    EXPECT_EQ(strat.normalize_drift_warn_count(), 1u);
    EXPECT_EQ(ctx.warn_count, 1);
    EXPECT_NE(ctx.last_warn.find("normalize"), std::string::npos);
}

// gross=1.0（恰好归一，no-op 缩放）+ normalize 生效 → 不触发。
TEST(SelWeightsCsvStrategy, NormalizeNoWarnWhenGrossIsOne) {
    TempCsvPath csv;
    csv.write(
        "date,symbol,weight\n"
        "20260109,600000.SH,0.30\n"
        "20260109,000001.SZ,0.40\n"
        "20260109,600519.SH,0.30\n");  // sum(|w|) = 1.00

    FakeSelCtx ctx;
    auto strat = make_loaded_strategy(ctx, csv.str(), /*normalize=*/true);

    strat.on_schedule(&ctx, 20260109, 1500, 0);

    EXPECT_EQ(strat.normalize_drift_warn_count(), 0u);
    EXPECT_EQ(ctx.warn_count, 0);
}

// gross=0.95 但 normalize:false（未生效）→ 即使 gross 偏离 1 也不触发
// （护栏只在 normalize 真正执行缩放时才有意义；normalize:false 时权重
// 本就按原样透传，不构成"隐性加杠杆"）。
TEST(SelWeightsCsvStrategy, NoWarnWhenNormalizeDisabled) {
    TempCsvPath csv;
    csv.write(
        "date,symbol,weight\n"
        "20260109,600000.SH,0.30\n"
        "20260109,000001.SZ,0.35\n"
        "20260109,600519.SH,0.30\n");  // sum(|w|) = 0.95

    FakeSelCtx ctx;
    auto strat = make_loaded_strategy(ctx, csv.str(), /*normalize=*/false);

    strat.on_schedule(&ctx, 20260109, 1500, 0);

    EXPECT_EQ(strat.normalize_drift_warn_count(), 0u);
    EXPECT_EQ(ctx.warn_count, 0);
}

// ── fail-closed 三洞（B0 fold；本 target 直接编译生产 .cpp，不依赖插件 .so）────

namespace {

/// @brief on_init 必须抛（fail-closed）；返回异常消息供断言。
std::string expect_load_throws(const std::string& body) {
    TempCsvPath csv;
    csv.write(body);
    FakeSelCtx ctx;
    SelWeightsCsvStrategy strat;
    XtStrategyInitConfig cfg;
    cfg.values["weights_path"] = csv.str();
    strat.init(cfg);
    try {
        strat.on_init(&ctx);
    } catch (const std::exception& e) {
        return e.what();
    }
    ADD_FAILURE() << "on_init 未抛（坏 weights 被静默接受）";
    return {};
}

}  // namespace

// 坏首行不得当 header 静默丢弃：首行只能是恰好的表头或数据行。
TEST(SelWeightsCsvStrategy, BadFirstLineIsNotSwallowedAsHeader) {
    const std::string msg = expect_load_throws(
        "BROKEN,foo,bar\n"
        "20260112,600000.SH,0.5\n");
    EXPECT_NE(msg.find("first line"), std::string::npos) << msg;
}

// 表头名不对（列数对但字面不符）同样拒——避免「看起来像 header」的坏行溜过。
TEST(SelWeightsCsvStrategy, WrongHeaderNamesRejected) {
    const std::string msg = expect_load_throws(
        "dt,sym,w\n"
        "20260112,600000.SH,0.5\n");
    EXPECT_NE(msg.find("first line"), std::string::npos) << msg;
}

// 超大日期不得靠 stoul→uint32_t 窄化 wrap 混过范围检查
// （429496729601 wrap 后落在 [19000101, 29991231] 内）。
TEST(SelWeightsCsvStrategy, OversizedDateDoesNotWrapThroughRangeCheck) {
    const std::string msg = expect_load_throws(
        "date,symbol,weight\n"
        "429496729601,600000.SH,0.5\n");
    EXPECT_NE(msg.find("8 digits"), std::string::npos) << msg;
}

// 8 位 + 落在范围内还不够：月日必须是真实日历日（Gregorian，含闰年）。
// `20260231` 永远不是交易日 → 这一行的调仓永不触发，跑出来的是「少一次调仓」的
// 另一个实验，而不是一次错误。判据与 SEL 回测侧 CA CSV 解析一致。
TEST(SelWeightsCsvStrategy, NonCalendarDateRejected) {
    const std::string msg = expect_load_throws(
        "date,symbol,weight\n"
        "20260231,600000.SH,0.5\n");
    EXPECT_NE(msg.find("calendar"), std::string::npos) << msg;
}

// 同一判据的月份侧与闰年侧：13 月拒；2028 是闰年故 0229 放行、2026 不是故拒。
TEST(SelWeightsCsvStrategy, CalendarCheckCoversMonthAndLeapYear) {
    EXPECT_NE(expect_load_throws("date,symbol,weight\n"
                                 "20261301,600000.SH,0.5\n").find("calendar"),
              std::string::npos);
    EXPECT_NE(expect_load_throws("date,symbol,weight\n"
                                 "20260229,600000.SH,0.5\n").find("calendar"),
              std::string::npos);
    TempCsvPath csv;
    csv.write("date,symbol,weight\n20280229,600000.SH,0.5\n");  // 2028 闰年
    FakeSelCtx ctx;
    auto strat = make_loaded_strategy(ctx, csv.str(), /*normalize=*/false);
    strat.on_schedule(&ctx, 20280229, 1500, 0);
    EXPECT_EQ(ctx.info_count, 2) << "闰年 2 月 29 日被误拒（日历校验收得过紧）";
}

// 多余列不再静默丢弃：多一列通常意味着这份文件的 schema 跟本解析器认的不是一回事
// （错位一列后 symbol 读成 weight 也可能恰好过后面的取值校验）。
TEST(SelWeightsCsvStrategy, ExtraColumnRejectedNotSilentlyIgnored) {
    const std::string msg = expect_load_throws(
        "date,symbol,weight\n"
        "20260112,600000.SH,0.5,20260111\n");
    EXPECT_NE(msg.find("exactly 3 columns"), std::string::npos) << msg;
}

// 未知可选列名整份拒：静默忽略等于放行一份没被理解的输入（列位一错，
// effective_time 可能被读成 task_uid）。
TEST(SelWeightsCsvStrategy, UnknownOptionalColumnRejected) {
    const std::string msg = expect_load_throws(
        "date,symbol,weight,whatever\n"
        "20260112,600000.SH,0.5,x\n");
    EXPECT_NE(msg.find("unknown optional column"), std::string::npos) << msg;
}

// canonical 数据契约的第二种合法表头形态：`date,symbol,weight,signal_asof_date`
// （as-of = 这行权重用到哪天为止的信息，是研究 provenance）。引擎不消费该列的值
// ——未来函数由 validator / walk-forward gate 判——但必须能加载，否则「validator
// 判过的权重文件引擎读不了」，as-of 自证形同虚设。
TEST(SelWeightsCsvStrategy, AsOfHeaderFormAcceptedAndValueIgnored) {
    TempCsvPath csv;
    csv.write("date,symbol,weight,signal_asof_date\n"
              "20260112,600000.SH,0.6,20260109\n"
              "20260112,000001.SZ,0.4,20260109\n");
    FakeSelCtx ctx;
    auto strat = make_loaded_strategy(ctx, csv.str(), /*normalize=*/false);
    strat.on_schedule(&ctx, 20260112, 1500, 0);
    EXPECT_EQ(ctx.info_count, 2) << "4 列表头形态被拒（与 canonical 契约/validator 脱节）";
    EXPECT_EQ(ctx.warn_count, 0);

    // as-of 只校验不消费：坏的 as-of 仍整份拒（provenance 不可信 = 下游未来函数
    // 防线建立在没解析过的字段上）。
    const std::string msg = expect_load_throws(
        "date,symbol,weight,signal_asof_date\n"
        "20260112,600000.SH,0.6,20260231\n");
    EXPECT_NE(msg.find("signal_asof_date"), std::string::npos) << msg;
    EXPECT_NE(msg.find("calendar"), std::string::npos) << msg;
}

// 列数由首行形态定死：4 列表头下的 3 列数据行是错位（第 4 列缺失还是第 1 列多余
// 无从判断），必须拒而不是按 3 列凑合读。
TEST(SelWeightsCsvStrategy, AsOfHeaderRequiresFourColumnDataRows) {
    const std::string msg = expect_load_throws(
        "date,symbol,weight,signal_asof_date\n"
        "20260112,600000.SH,0.6\n");
    EXPECT_NE(msg.find("exactly 4 columns"), std::string::npos) << msg;
}

// ── F10 槽维（effective_time / task_uid）───────────────────────────────────

// 同日两次 fire 各取其槽：10:30 与 15:00 两个 effective_time 的权重不得互相覆盖，
// 也不得合并成一份从未被任何一行表达过的组合。
TEST(SelWeightsCsvStrategy, SameDayTwoFiresPickTheirOwnSlot) {
    TempCsvPath csv;
    csv.write("date,symbol,weight,effective_time\n"
              "20260112,600000.SH,0.30,1030\n"
              "20260112,000001.SZ,0.70,1030\n"
              "20260112,600519.SH,1.00,1500\n");
    FakeSelCtx ctx;
    auto strat = make_loaded_strategy(ctx, csv.str(), /*normalize=*/false);

    strat.on_schedule(&ctx, 20260112, 1030, 0);
    ASSERT_EQ(ctx.weights_calls, 1);
    EXPECT_EQ(ctx.last_weights.size(), 2u);
    EXPECT_DOUBLE_EQ(ctx.last_weights.at("600000.SH"), 0.30);
    EXPECT_DOUBLE_EQ(ctx.last_weights.at("000001.SZ"), 0.70);
    EXPECT_EQ(ctx.last_weights.count("600519.SH"), 0u) << "两槽被合并成一份权重";

    strat.on_schedule(&ctx, 20260112, 1500, 0);
    ASSERT_EQ(ctx.weights_calls, 2);
    EXPECT_EQ(ctx.last_weights.size(), 1u);
    EXPECT_DOUBLE_EQ(ctx.last_weights.at("600519.SH"), 1.00);

    // 无对应槽的 fire 时刻 → no-op（保留上次 state），不回退到别的时刻。
    strat.on_schedule(&ctx, 20260112, 1400, 0);
    EXPECT_EQ(ctx.weights_calls, 2);
}

// task_uid 维：同一时刻两个 task 各取自己的槽；本实例的 uid 由 config 声明。
TEST(SelWeightsCsvStrategy, TaskUidSelectsItsOwnSlot) {
    TempCsvPath csv;
    csv.write("date,symbol,weight,task_uid\n"
              "20260112,600000.SH,1.00,alpha\n"
              "20260112,000001.SZ,1.00,beta\n");
    FakeSelCtx ctx_a;
    auto strat_a = make_loaded_strategy(ctx_a, csv.str(), /*normalize=*/false, "alpha");
    strat_a.on_schedule(&ctx_a, 20260112, 1500, 0);
    ASSERT_EQ(ctx_a.weights_calls, 1);
    EXPECT_EQ(ctx_a.last_weights.size(), 1u);
    EXPECT_DOUBLE_EQ(ctx_a.last_weights.at("600000.SH"), 1.00);

    FakeSelCtx ctx_b;
    auto strat_b = make_loaded_strategy(ctx_b, csv.str(), /*normalize=*/false, "beta");
    strat_b.on_schedule(&ctx_b, 20260112, 1500, 0);
    ASSERT_EQ(ctx_b.weights_calls, 1);
    EXPECT_DOUBLE_EQ(ctx_b.last_weights.at("000001.SZ"), 1.00);

    // 未声明 uid 的实例只吃通配槽——本文件没有通配槽 → no-op（不静默拿别人的槽）。
    FakeSelCtx ctx_none;
    auto strat_none = make_loaded_strategy(ctx_none, csv.str(), /*normalize=*/false);
    strat_none.on_schedule(&ctx_none, 20260112, 1500, 0);
    EXPECT_EQ(ctx_none.weights_calls, 0);
}

// 「精确 → 通配」优先级：本 task 的定时槽 > 通配槽；缺精确槽时退通配。
TEST(SelWeightsCsvStrategy, ExactSlotWinsOverWildcardFallback) {
    TempCsvPath csv;
    csv.write("date,symbol,weight,effective_time,task_uid\n"
              "20260112,600000.SH,1.00,,\n"            // 全通配
              "20260112,000001.SZ,1.00,1030,alpha\n"); // 精确槽
    FakeSelCtx ctx;
    auto strat = make_loaded_strategy(ctx, csv.str(), /*normalize=*/false, "alpha");

    strat.on_schedule(&ctx, 20260112, 1030, 0);
    ASSERT_EQ(ctx.weights_calls, 1);
    EXPECT_DOUBLE_EQ(ctx.last_weights.at("000001.SZ"), 1.00);
    EXPECT_EQ(ctx.last_weights.count("600000.SH"), 0u);

    strat.on_schedule(&ctx, 20260112, 1500, 0);   // 无精确槽 → 退通配
    ASSERT_EQ(ctx.weights_calls, 2);
    EXPECT_DOUBLE_EQ(ctx.last_weights.at("600000.SH"), 1.00);
}

// 同 symbol 在不同槽是合法形态（同日两次调仓）；同槽内重复才是坏行。
TEST(SelWeightsCsvStrategy, SameSymbolAllowedAcrossSlotsButNotWithinOne) {
    TempCsvPath csv;
    csv.write("date,symbol,weight,effective_time\n"
              "20260112,600000.SH,0.30,1030\n"
              "20260112,600000.SH,0.80,1500\n");
    FakeSelCtx ctx;
    auto strat = make_loaded_strategy(ctx, csv.str(), /*normalize=*/false);
    strat.on_schedule(&ctx, 20260112, 1500, 0);
    ASSERT_EQ(ctx.weights_calls, 1);
    EXPECT_DOUBLE_EQ(ctx.last_weights.at("600000.SH"), 0.80);

    const std::string msg = expect_load_throws(
        "date,symbol,weight,effective_time\n"
        "20260112,600000.SH,0.30,1030\n"
        "20260112,600000.SH,0.80,1030\n");
    EXPECT_NE(msg.find("duplicate"), std::string::npos) << msg;
}

// effective_time 须是真实钟面：`1075` 永不等于任何 fire 时刻，该槽会永不命中
// ——跑出来是「少一次调仓」而不是错误，故加载期就拒（与 date 同判据）。
TEST(SelWeightsCsvStrategy, NonClockEffectiveTimeRejected) {
    const std::string msg = expect_load_throws(
        "date,symbol,weight,effective_time\n"
        "20260112,600000.SH,0.5,1075\n");
    EXPECT_NE(msg.find("effective_time"), std::string::npos) << msg;
    EXPECT_NE(msg.find("clock"), std::string::npos) << msg;
}

// 对照：正常 8 位日期照常加载（前一钉不是把合法输入也拒了）。
TEST(SelWeightsCsvStrategy, EightDigitDateAccepted) {
    TempCsvPath csv;
    csv.write("date,symbol,weight\n20260112,600000.SH,0.5\n");
    FakeSelCtx ctx;
    auto strat = make_loaded_strategy(ctx, csv.str(), /*normalize=*/false);
    // info: on_init 的加载摘要 + on_schedule 的推送摘要（真的推了一次权重）。
    strat.on_schedule(&ctx, 20260112, 1500, 0);
    EXPECT_EQ(ctx.info_count, 2);
}

// 纯空白行按空行跳过（与 validator 空行规则对齐），不算坏行。
TEST(SelWeightsCsvStrategy, WhitespaceOnlyLineSkipped) {
    TempCsvPath csv;
    csv.write("date,symbol,weight\n20260112,600000.SH,0.5\n   \n\t\n");
    FakeSelCtx ctx;
    auto strat = make_loaded_strategy(ctx, csv.str(), /*normalize=*/false);
    strat.on_schedule(&ctx, 20260112, 1500, 0);
    EXPECT_EQ(ctx.info_count, 2);
}

// 读取期 I/O 错误不得被当成正常 EOF：目录当文件读会在 getline 上置 badbit。
TEST(SelWeightsCsvStrategy, ReadIoErrorIsNotTreatedAsEof) {
    const auto dir = fs::temp_directory_path() /
                     ("sel_weights_dir_" + std::to_string(::getpid()));
    std::error_code ec;
    fs::create_directories(dir, ec);
    FakeSelCtx ctx;
    SelWeightsCsvStrategy strat;
    XtStrategyInitConfig cfg;
    cfg.values["weights_path"] = dir.string();
    strat.init(cfg);
    EXPECT_THROW(strat.on_init(&ctx), std::runtime_error);
    fs::remove_all(dir, ec);
}
