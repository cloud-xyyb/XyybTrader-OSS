// -----------------------------------------------------------------------------
// XtSelBtArtifactsTest — SEL 回测新产物与 money-path 不变量
// -----------------------------------------------------------------------------
// 覆盖 B0 冻结 schema 的产物面 + 深挖 fold 的行为修正：
//   ③ SubmitResult 落列（本地未受理必须在 target_weights.csv 上可见）
//   ④ 费用拆分守恒（fill_costs.total_fee == trades.csv fee 逐笔）
//   ⑤ 会计恒等（Σ daily_positions.market_value 含 CASH == daily_equity）
//   ⑥ T+1 真启用（买入日今仓可卖为 0、次日滚动释放）
//   ⑧ 资金可负担截断（旧实现按 T close 强制成交把现金扣成负数）
//   ⑨ weights csv 坏行 fail-fast（旧实现静默跳过）
//   ⑩ risk.allow_short=true 配置期拒（SEL v1 long-only）
//   ⑪ 同一 config 从不同 cwd 跑结果一致（路径不再依赖 cwd）
//   + 写盘协议：fail-fast 跑不留半截产物
// -----------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include <unistd.h>

#include "api/IStrategyContext.h"
#include "xt_backtest/sel/XtSelBacktestRunner.h"
#include "xt_backtest/sel/XtSelRunnerConfig.h"
#include "xt_strategy_engine/XtStrategyBase.h"
#include "xt_strategy_engine/loader/XtStrategyFactory.h"

namespace sel_b0_artifacts {
namespace {

// ── CSV 读取 helper（按 header 取列，schema 变更时断言点名列名）────────────────

std::vector<std::string> split_csv(const std::string& line) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : line) {
        if (c == ',') { out.push_back(cur); cur.clear(); }
        else if (c != '\r') cur.push_back(c);
    }
    out.push_back(cur);
    return out;
}

using Row = std::map<std::string, std::string>;

std::vector<Row> read_csv(const std::filesystem::path& p) {
    std::vector<Row> rows;
    std::ifstream ifs(p);
    if (!ifs) return rows;
    std::string line;
    if (!std::getline(ifs, line)) return rows;
    const auto header = split_csv(line);
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        const auto f = split_csv(line);
        Row r;
        for (size_t i = 0; i < header.size() && i < f.size(); ++i) r[header[i]] = f[i];
        rows.push_back(std::move(r));
    }
    return rows;
}

std::string read_text(const std::filesystem::path& p) {
    std::ifstream ifs(p);
    std::stringstream ss;
    ss << ifs.rdbuf();
    return ss.str();
}

/// @brief 从 summary.json 取数值字段（缺失返 NaN）。
double summary_num(const std::string& body, const std::string& key) {
    const std::string k = "\"" + key + "\": ";
    const auto pos = body.find(k);
    if (pos == std::string::npos) return std::nan("");
    return std::strtod(body.c_str() + pos + k.size(), nullptr);
}

/// @brief external 注入 stub：按日期查表设目标权重。
class MapWeightsStrategy final : public XtStrategyBase {
public:
    explicit MapWeightsStrategy(
        std::map<uint32_t, std::unordered_map<std::string, double>> by_date)
        : by_date_(std::move(by_date)) {}
    void on_schedule(IStrategyContext* ctx, uint32_t curDate, uint32_t,
                     int64_t) override {
        const auto it = by_date_.find(curDate);
        if (it != by_date_.end()) ctx->set_target_weights(it->second);
    }

private:
    std::map<uint32_t, std::unordered_map<std::string, double>> by_date_;
};

/// @brief 临时目录 + config 生成 + runner 调用的共享 fixture。
class SelArtifactsFixture : public ::testing::Test {
protected:
    void SetUp() override {
        char tmpl[] = "/tmp/xt_sel_b0_XXXXXX";
        ASSERT_NE(mkdtemp(tmpl), nullptr);
        dir_ = tmpl;
    }

    void TearDown() override {
        if (!dir_.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(dir_, ec);  // 测试临时文件规范：结束即清理
        }
    }

    void write_file(const std::string& name, const std::string& body) const {
        std::ofstream ofs(dir_ / name);
        ofs << body;
    }

    /// @brief 生成 config；`extra` 追加顶层节（fees / risk / commodities 等）。
    /// @param relative_inputs true 时输入路径写成裸文件名（cwd 无关性钉用）。
    /// @param top_extra 追加到文档顶层的块（如 `backtest:\n  trace_mode: full\n`）。
    /// @param selector_extra 追加到 `selector:` 节内的行（2 空格缩进，如
    ///        `  corporate_actions_csv: <path>\n`）。
    std::string make_cfg(const std::string& sub, double initial_cash,
                         const std::string& extra, bool relative_inputs = false,
                         const std::string& top_extra = "",
                         const std::string& selector_extra = "") const {
        const std::string cfg_path = (dir_ / (sub + "_cfg.yaml")).string();
        const std::string dates = relative_inputs
            ? "trading_dates.csv" : (dir_ / "trading_dates.csv").string();
        const std::string closes = relative_inputs
            ? "close_prices.csv" : (dir_ / "close_prices.csv").string();
        std::ostringstream y;
        y << "selector:\n"
          << "  trading_dates_csv: " << dates << "\n"
          << "  close_prices_csv: " << closes << "\n"
          << "  task:\n"
          << "    strategy_id: sel_b0\n"
          << "    period: daily\n"
          << "    time: 1500\n"
          << "    strict_time: true\n"
          << "  strategy:\n"
          << "    id: sel_b0\n"
          << selector_extra
          << "account:\n"
          << "  initial_cash: " << initial_cash << "\n"
          << extra
          << top_extra
          << "report:\n"
          << "  output_dir: " << (dir_ / sub).string() << "\n";
        std::ofstream ofs(cfg_path);
        ofs << y.str();
        return cfg_path;
    }

    static bool load_cfg(const std::string& cfg_path,
                         xt_bt_sel::XtSelRunnerConfig& out, std::string& err) {
        std::string a0 = "test";
        std::string a1 = cfg_path;
        char* argv[] = {a0.data(), a1.data()};
        return xt_bt_sel::load_sel_config(2, argv, out, err);
    }

    int run(const std::string& cfg_path, XtStrategyBase* stub) const {
        xt_bt_sel::XtSelRunnerConfig cfg;
        std::string err;
        if (!load_cfg(cfg_path, cfg, err)) {
            ADD_FAILURE() << "load_sel_config failed: " << err;
            return -1;
        }
        return xt_bt_sel::XtSelBacktestRunner::run(cfg, stub);
    }

    std::filesystem::path dir_;
};

/// @brief A 股常规摩擦（印花税 sell-only / 过户费双向 / 5 元最低佣金 / 100 股一手）。
const char* kAShareFrictions =
    "commodities:\n"
    "  '600000.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0, min_lot: 100 }\n"
    "  '000001.SZ': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0, min_lot: 100 }\n"
    "  '600519.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0, min_lot: 100 }\n"
    "fees:\n"
    "  '600000.SH': { open: 0.00025, close: 0.00025, stamp_duty_rate: 0.0005,"
    " transfer_fee_rate: 0.00001, min_commission: 5.0 }\n"
    "  '000001.SZ': { open: 0.00025, close: 0.00025, stamp_duty_rate: 0.0005,"
    " transfer_fee_rate: 0.0, min_commission: 5.0 }\n"
    "  '600519.SH': { open: 0.00025, close: 0.00025, stamp_duty_rate: 0.0005,"
    " transfer_fee_rate: 0.00001, min_commission: 5.0 }\n"
    "risk:\n"
    "  max_order_qty: 1000000.0\n"
    "  enforce_long_t_plus_1: true\n"
    "  allow_short: false\n";

}  // namespace

// ── ⑥ T+1 真启用：买入日今仓不可卖，次日 session_begin 滚动释放 ──────────────
//
// 旧实现只有 risk.enforce_long_t_plus_1（账本 t1_enabled=false → long_today 恒 0
// → yesterday 口径 = 全仓 → T+1 检查恒放行）。本断言在旧实现下：D1 行
// long_today=0/available_sell=25000（两列压根不存在）→ FAIL。
// 反向半边（漏 session_begin）会让 D2 的 long_today 停在 25000 → 同样 FAIL。
TEST_F(SelArtifactsFixture, T1TodayQuantityRollsAtSessionBegin) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.25}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kAShareFrictions);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto rows = read_csv(dir_ / "rep" / "daily_positions.csv");
    ASSERT_FALSE(rows.empty());
    // 买入日：全量为今仓 → 可卖 0（A 股 T+1）。
    bool saw_d1 = false, saw_d2 = false;
    for (const auto& r : rows) {
        if (r.at("code") != "600000.SH") continue;
        if (r.at("date") == "20260112") {
            saw_d1 = true;
            EXPECT_EQ(r.at("qty"), "25000.0000");
            EXPECT_EQ(r.at("long_today"), "25000.0000");
            EXPECT_EQ(r.at("available_sell"), "0.0000")
                << "T+1 未真启用：当日买入的仓位被算成可卖";
        } else if (r.at("date") == "20260113") {
            saw_d2 = true;
            EXPECT_EQ(r.at("long_today"), "0.0000")
                << "session_begin 未应用到账本：今仓跨日未归零";
            EXPECT_EQ(r.at("available_sell"), "25000.0000");
        }
    }
    EXPECT_TRUE(saw_d1);
    EXPECT_TRUE(saw_d2);
    // 无误拒：T+1 打开后不应把正常的隔日卖出拦掉。
    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "risk_rejects"), 0.0);
}

// ── ⑤ 会计恒等：Σ market_value（含 CASH 行）== 同日 daily_equity ─────────────
TEST_F(SelArtifactsFixture, DailyPositionsSumMatchesDailyEquity) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260109,000001.SZ,20.0\n"
               "20260112,600000.SH,10.0\n20260112,000001.SZ,20.0\n"
               "20260113,600000.SH,12.0\n20260113,000001.SZ,19.0\n"
               "20260114,600000.SH,11.0\n20260114,000001.SZ,21.0\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.4}, {"000001.SZ", 0.3}}},
        {20260114u, {{"600000.SH", 0.2}, {"000001.SZ", 0.5}}},
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, kAShareFrictions);
    ASSERT_EQ(run(cfg, &stub), 0);

    std::map<std::string, double> mv_by_date;
    for (const auto& r : read_csv(dir_ / "rep" / "daily_positions.csv")) {
        mv_by_date[r.at("date")] += std::stod(r.at("market_value"));
    }
    const auto eq_rows = read_csv(dir_ / "rep" / "daily_equity.csv");
    ASSERT_EQ(eq_rows.size(), 3u);
    for (const auto& r : eq_rows) {
        const auto it = mv_by_date.find(r.at("date"));
        ASSERT_NE(it, mv_by_date.end()) << r.at("date");
        // 两侧都按 %.2f 落盘 → 容差只留一次打印舍入。
        EXPECT_NEAR(it->second, std::stod(r.at("equity")), 0.02)
            << "date=" << r.at("date");
    }
    // CASH 行必在（缺了就凑不齐 equity，而不是恒等失败）。
    bool has_cash = false;
    for (const auto& r : read_csv(dir_ / "rep" / "daily_positions.csv")) {
        if (r.at("code") == "CASH") { has_cash = true; break; }
    }
    EXPECT_TRUE(has_cash);
}

// ── ④ 费用拆分守恒：total_fee 与 trades.csv 逐笔相等，分项和落在一次舍入内 ────
TEST_F(SelArtifactsFixture, FillCostsBreakdownMatchesTradesFee) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    // 600519.SH 走 8 元价 + 极小权重：notional 9600 → 佣金 2.4 被 5 元下限抬到 5.0、
    // 过户费 0.096 → 原始合计 5.096，只有走同一 2dp 舍入才等于 trades.csv 的 5.10。
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260109,000001.SZ,20.0\n20260109,600519.SH,8.0\n"
               "20260112,600000.SH,10.0\n20260112,000001.SZ,20.0\n20260112,600519.SH,8.0\n"
               "20260113,600000.SH,10.0\n20260113,000001.SZ,20.0\n20260113,600519.SH,8.0\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.4}, {"000001.SZ", 0.3}, {"600519.SH", 0.0096}}},
        {20260113u, {{"600000.SH", 0.2}, {"000001.SZ", 0.5}, {"600519.SH", 0.0096}}},
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, kAShareFrictions);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto trades = read_csv(dir_ / "rep" / "trades.csv");
    const auto costs  = read_csv(dir_ / "rep" / "fill_costs.csv");
    ASSERT_FALSE(trades.empty());
    ASSERT_EQ(trades.size(), costs.size());

    std::map<std::string, const Row*> trade_by_oid;
    for (const auto& t : trades) trade_by_oid[t.at("order_id")] = &t;

    bool saw_sell_stamp = false;
    bool saw_nonzero_fee = false;
    for (const auto& c : costs) {
        const auto it = trade_by_oid.find(c.at("order_id"));
        ASSERT_NE(it, trade_by_oid.end()) << "order_id=" << c.at("order_id");
        const double total = std::stod(c.at("total_fee"));
        const double fee   = std::stod(it->second->at("fee"));
        EXPECT_DOUBLE_EQ(total, fee) << "order_id=" << c.at("order_id");
        if (fee != 0.0) saw_nonzero_fee = true;
        const double parts = std::stod(c.at("commission")) +
                             std::stod(c.at("stamp_duty")) +
                             std::stod(c.at("transfer_fee"));
        EXPECT_NEAR(parts, total, 0.005) << "order_id=" << c.at("order_id");
        // 印花税只在卖向；买向必须为 0（分项不是把 total 平摊出来的）。
        if (c.at("direction") == "SELL") {
            if (std::stod(c.at("stamp_duty")) > 0.0) saw_sell_stamp = true;
        } else {
            EXPECT_DOUBLE_EQ(std::stod(c.at("stamp_duty")), 0.0);
        }
    }
    EXPECT_TRUE(saw_nonzero_fee) << "fixture 前提被破坏：全零费用下守恒断言恒真";
    EXPECT_TRUE(saw_sell_stamp) << "fixture 前提被破坏：无卖出腿则印花税分项未被覆盖";
    // 对照前提：至少一笔的原始分项和不是整分（否则「同一舍入」无从被检出）。
    bool saw_sub_cent = false;
    for (const auto& c : costs) {
        const double parts = std::stod(c.at("commission")) +
                             std::stod(c.at("stamp_duty")) +
                             std::stod(c.at("transfer_fee"));
        if (std::fabs(parts - std::stod(c.at("total_fee"))) > 1e-9) saw_sub_cent = true;
    }
    EXPECT_TRUE(saw_sub_cent)
        << "fixture 前提被破坏：所有费用恰为整分，2dp 舍入口径不可观测";
}

// ── ③ SubmitResult 落列：本地未受理必须在产物上可见 ──────────────────────────
//
// max_order_qty 收紧 → 风控在 submit 内拒 → SubmitResult.order_id==0。
// 旧实现 `(void)gw_p->submit(...)` 丢弃返回值，产物上看不出「发了但没受理」。
TEST_F(SelArtifactsFixture, TargetWeightsRecordsUnacceptedSubmit) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    // target = 0.5×1e6/10 = 50000 股，max_order_qty=1000 → 整单被风控拒。
    const std::string extra =
        "commodities:\n"
        "  '600000.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0, min_lot: 100 }\n"
        "risk:\n"
        "  max_order_qty: 1000.0\n"
        "  allow_short: false\n";
    const auto cfg = make_cfg("rep", 1'000'000.0, extra);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto rows = read_csv(dir_ / "rep" / "target_weights.csv");
    ASSERT_EQ(rows.size(), 1u);
    const auto& r = rows[0];
    EXPECT_EQ(r.at("planning_status"), "ok");
    EXPECT_EQ(r.at("gate"), "ok");
    EXPECT_EQ(r.at("phase"), "buy");
    EXPECT_EQ(r.at("submitted_qty"), "50000.0000");
    EXPECT_EQ(r.at("accepted"), "0") << "本地未受理未落列（SubmitResult 被丢弃）";
    EXPECT_FALSE(r.at("reject_code").empty());
    EXPECT_EQ(r.at("filled_qty"), "0.0000");
    EXPECT_EQ(r.at("post_qty"), "0.0000");

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_GT(summary_num(body, "risk_rejects"), 0.0);  // 对照：确有拒单发生
    EXPECT_EQ(read_csv(dir_ / "rep" / "trades.csv").size(), 0u);
}

// ── ⑧ 负现金钉：T close 强制成交下，买入成本超现金必须截断而不是透支 ──────────
//
// fixture 无费用/滑点：T-1 close=10 → target 10000 股；T 日 close=20 →
// 名义 200000 > 现金 100000。旧实现整单成交 → cash = −100000（免费融资）。
// 新实现截到 5000 股（50 手 × 100）→ cash 恰好 0。
TEST_F(SelArtifactsFixture, AffordabilityCapPreventsNegativeCash) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,20.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 1.0}}}});
    const std::string extra =
        "commodities:\n"
        "  '600000.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0, min_lot: 100 }\n"
        "risk:\n"
        "  max_order_qty: 1000000.0\n"
        "  allow_short: false\n";
    const auto cfg = make_cfg("rep", 100'000.0, extra);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto trades = read_csv(dir_ / "rep" / "trades.csv");
    ASSERT_EQ(trades.size(), 1u);
    EXPECT_EQ(trades[0].at("qty"), "5000.0000")
        << "买入量未按可负担现金截断（旧实现整单 10000 股 → 透支）";

    // 现金不为负：CASH 行即账户现金。
    bool saw_cash = false;
    for (const auto& r : read_csv(dir_ / "rep" / "daily_positions.csv")) {
        if (r.at("code") != "CASH") continue;
        saw_cash = true;
        EXPECT_GE(std::stod(r.at("market_value")), 0.0) << "现金落负";
        EXPECT_NEAR(std::stod(r.at("market_value")), 0.0, 0.005);
    }
    EXPECT_TRUE(saw_cash);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "affordability_caps"), 1.0);
    EXPECT_EQ(summary_num(body, "negative_cash_events"), 0.0);
    EXPECT_NEAR(summary_num(body, "affordability_shortfall"), 100'000.0, 0.01);
    // 执行列如实反映「发了 10000、只落账 5000」——目标与提交量都不被改写，
    // 落差只出现在 filled_qty 上（截断发生在落账口）。
    const auto rows = read_csv(dir_ / "rep" / "target_weights.csv");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].at("target_qty"), "10000.0000");
    EXPECT_EQ(rows[0].at("submitted_qty"), "10000.0000");
    EXPECT_EQ(rows[0].at("filled_qty"), "5000.0000");
    EXPECT_EQ(rows[0].at("post_qty"), "5000.0000");
}

// 对照钉：现金充裕时不截断（截断逻辑不误伤正常路径）。
TEST_F(SelArtifactsFixture, NoAffordabilityCapWhenCashSuffices) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const std::string extra =
        "commodities:\n"
        "  '600000.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0, min_lot: 100 }\n"
        "risk:\n"
        "  max_order_qty: 1000000.0\n"
        "  allow_short: false\n";
    const auto cfg = make_cfg("rep", 1'000'000.0, extra);
    ASSERT_EQ(run(cfg, &stub), 0);
    const auto trades = read_csv(dir_ / "rep" / "trades.csv");
    ASSERT_EQ(trades.size(), 1u);
    EXPECT_EQ(trades[0].at("qty"), "50000.0000");
    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "affordability_caps"), 0.0);
}

// ── 负权重防御闸（F05 引擎侧）────────────────────────────────────────────────
TEST_F(SelArtifactsFixture, NegativeTargetWeightRejected) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260109,000001.SZ,20.0\n"
               "20260112,600000.SH,10.0\n20260112,000001.SZ,20.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}, {"000001.SZ", -0.2}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kAShareFrictions);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto rows = read_csv(dir_ / "rep" / "target_weights.csv");
    ASSERT_EQ(rows.size(), 2u);
    for (const auto& r : rows) {
        if (r.at("code") == "000001.SZ") {
            EXPECT_EQ(r.at("planning_status"), "negative_weight");
            EXPECT_TRUE(r.at("target_qty").empty());
            EXPECT_TRUE(r.at("gate").empty());  // 未进执行
        } else {
            EXPECT_EQ(r.at("planning_status"), "ok");
        }
    }
    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "negative_weight_rejects"), 1.0);
    // 只有 long 腿成交（负腿没有变成做空单）。
    EXPECT_EQ(read_csv(dir_ / "rep" / "trades.csv").size(), 1u);
}

// ── min_lot 截零可见 + residual cash 观测（F09）───────────────────────────────
TEST_F(SelArtifactsFixture, MinLotZeroTargetIsVisibleInSummary) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260109,000001.SZ,20.0\n"
               "20260112,600000.SH,10.0\n20260112,000001.SZ,20.0\n");
    // 000001.SZ 权重极小：0.0001×1e6/20 = 5 股 < min_lot 100 → 截零。
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}, {"000001.SZ", 0.0001}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kAShareFrictions);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "min_lot_zero_targets"), 1.0);
    EXPECT_GT(summary_num(body, "residual_cash"), 0.0);
    bool saw = false;
    for (const auto& r : read_csv(dir_ / "rep" / "target_weights.csv")) {
        if (r.at("code") == "000001.SZ") {
            saw = true;
            EXPECT_EQ(r.at("planning_status"), "min_lot_zero");
            EXPECT_EQ(r.at("target_qty"), "0.0000");
            EXPECT_TRUE(r.at("gate").empty());  // diff=0 → 未进执行
        }
    }
    EXPECT_TRUE(saw);
}

// ── schedule_fires：fired 与 skip_reason 分列可读 ─────────────────────────────
TEST_F(SelArtifactsFixture, ScheduleFiresSeparatesFiredFromNoWeights) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kAShareFrictions);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto rows = read_csv(dir_ / "rep" / "schedule_fires.csv");
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0].at("exec_date"), "20260112");
    EXPECT_EQ(rows[0].at("fired"), "1");
    EXPECT_TRUE(rows[0].at("skip_reason").empty());
    EXPECT_EQ(rows[0].at("n_targets"), "1");
    EXPECT_EQ(rows[0].at("n_buys"), "1");
    EXPECT_EQ(rows[0].at("fire_seq"), "1");
    // task_uid 用 scheduler 真 task id（同 sid 多 task 可区分）。
    EXPECT_EQ(rows[0].at("task_uid"), "task:1");
    EXPECT_EQ(rows[0].at("revision"), "1");
    EXPECT_EQ(rows[0].at("planned_time"), "1500");
    // 第二天 scheduler 照常触发，但策略没设权重 → fired=1 + no_weights。
    EXPECT_EQ(rows[1].at("fired"), "1");
    EXPECT_EQ(rows[1].at("skip_reason"), "no_weights");
    EXPECT_EQ(rows[1].at("fire_seq"), "0");

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "n_fires"), 1.0);
    EXPECT_EQ(summary_num(body, "n_skips"), 1.0);
}

// ── 写盘协议：fail-fast 跑（rc=8）不留任何半截新产物 ─────────────────────────
TEST_F(SelArtifactsFixture, FailFastRunLeavesNoPartialArtifacts) {
    // 首日无 T-1 close 且策略当日 sizing → rc=8。
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv", "date,code,close\n20260112,600000.SH,10.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kAShareFrictions);
    EXPECT_EQ(run(cfg, &stub), 8);

    for (const char* f : {"schedule_fires.csv", "target_weights.csv",
                          "daily_positions.csv", "fill_costs.csv",
                          "trades.csv", "summary.json"}) {
        EXPECT_FALSE(std::filesystem::exists(dir_ / "rep" / f)) << f;
    }
    // .tmp 也不得残留（AtomicOfstream 析构清理）。
    if (std::filesystem::exists(dir_ / "rep")) {
        for (const auto& e : std::filesystem::directory_iterator(dir_ / "rep")) {
            ADD_FAILURE() << "残留文件: " << e.path().string();
        }
    }
}

// ── ⑩ allow_short=true 配置期拒 ──────────────────────────────────────────────
TEST_F(SelArtifactsFixture, AllowShortTrueRejectedAtParse) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv", "date,code,close\n20260112,600000.SH,10.0\n");
    const auto cfg = make_cfg("rep", 1'000'000.0,
                              "risk:\n  allow_short: true\n");
    xt_bt_sel::XtSelRunnerConfig out;
    std::string err;
    EXPECT_FALSE(load_cfg(cfg, out, err));
    EXPECT_NE(err.find("allow_short"), std::string::npos) << err;
    EXPECT_NE(err.find("long-only"), std::string::npos) << err;
}

// 对照钉：allow_short=false 正常放行。
TEST_F(SelArtifactsFixture, AllowShortFalseAccepted) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv", "date,code,close\n20260112,600000.SH,10.0\n");
    const auto cfg = make_cfg("rep", 1'000'000.0,
                              "risk:\n  allow_short: false\n");
    xt_bt_sel::XtSelRunnerConfig out;
    std::string err;
    EXPECT_TRUE(load_cfg(cfg, out, err)) << err;
}

// ── ⑪ cwd 无关性：同一 config（裸相对路径）从不同 cwd 跑结果一致 ──────────────
//
// 旧实现按 cwd 解析 → 从任意非数据目录跑必失败（rc=1）。
TEST_F(SelArtifactsFixture, SameConfigRunsFromAnyCwd) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,11.0\n");
    const auto cfg = make_cfg("rep_cwd", 1'000'000.0, kAShareFrictions,
                              /*relative_inputs=*/true);

    char cwd_buf[4096];
    ASSERT_NE(::getcwd(cwd_buf, sizeof(cwd_buf)), nullptr);
    const std::string orig_cwd = cwd_buf;

    // 从 "/" 跑：相对路径只能靠 config 目录解析。
    ASSERT_EQ(::chdir("/"), 0);
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const int rc = run(cfg, &stub);
    const bool restored = (::chdir(orig_cwd.c_str()) == 0);
    ASSERT_EQ(rc, 0) << "config 目录相对路径在非数据 cwd 下解析失败";
    ASSERT_TRUE(restored);

    const auto trades = read_csv(dir_ / "rep_cwd" / "trades.csv");
    ASSERT_EQ(trades.size(), 1u);
    EXPECT_EQ(trades[0].at("qty"), "50000.0000");
}

// ── ⑨ weights csv fail-closed（需 plugin .so；缺失则 SKIP）────────────────────

namespace {

/// @brief repo root 解析（与 XtSelBacktestRunnerTest 同式）。
std::filesystem::path resolve_repo_root() {
    if (const char* env = ::getenv("XT_REPO_ROOT"); env && *env) return env;
    std::filesystem::path self(__FILE__);
    if (!self.is_absolute()) return {};
    return self.parent_path().parent_path().parent_path().parent_path();
}

class ScopedEnvVar {
public:
    ScopedEnvVar(const char* name, const char* value) : name_(name) {
        if (const char* old = ::getenv(name)) { had_ = true; old_ = old; }
        ::setenv(name, value, 1);
    }
    ~ScopedEnvVar() {
        if (had_) ::setenv(name_.c_str(), old_.c_str(), 1);
        else      ::unsetenv(name_.c_str());
    }
    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;

private:
    std::string name_;
    std::string old_;
    bool had_ = false;
};

}  // namespace

class SelWeightsFailClosedFixture : public SelArtifactsFixture {
protected:
    void SetUp() override {
        SelArtifactsFixture::SetUp();
        const auto root = resolve_repo_root();
        plugin_so_ = root / "var/plugins/strategies/libsel_weights_csv_strategy.so";
        if (root.empty() || !std::filesystem::exists(plugin_so_)) {
            GTEST_SKIP() << "libsel_weights_csv_strategy.so 不在位（未 git-track 资产）: "
                         << plugin_so_;
        }
        env_.push_back(std::make_unique<ScopedEnvVar>(
            "CLOUDTRADER_PLUGIN_ALLOWLIST", "libsel_weights_csv_strategy.so"));
        env_.push_back(std::make_unique<ScopedEnvVar>(
            "CLOUDTRADER_PLUGIN_ALLOWLIST_ENFORCE", "1"));
        env_.push_back(std::make_unique<ScopedEnvVar>(
            "CLOUDTRADER_PLUGIN_STRICT_SIGNING", "1"));
        {
            XtStrategyFactory probe;
            ASSERT_TRUE(probe.register_plugin("preflight", plugin_so_.string()))
                << "plugin preflight load failed: " << probe.last_error();
        }
        write_file("trading_dates.csv", "date\n20260112\n20260113\n");
        write_file("close_prices.csv",
                   "date,code,close\n"
                   "20260109,600000.SH,10.0\n"
                   "20260112,600000.SH,10.0\n"
                   "20260113,600000.SH,10.0\n");
    }

    void TearDown() override {
        env_.clear();
        SelArtifactsFixture::TearDown();
    }

    /// @brief 用 plugin 路径跑（weights_path 指向 fixture 内 weights.csv）。
    /// @param task_time config 的 task 触发时刻（HHMM）；weights 的 effective_time
    ///                  槽按它匹配。
    /// @param task_uid  策略实例声明的 task 身份；空 = 只走通配槽。
    int run_with_plugin(const std::string& sub, uint32_t task_time = 1500,
                        const std::string& task_uid = "") const {
        const std::string cfg_path = (dir_ / (sub + "_cfg.yaml")).string();
        std::ostringstream y;
        y << "selector:\n"
          << "  trading_dates_csv: " << (dir_ / "trading_dates.csv").string() << "\n"
          << "  close_prices_csv: " << (dir_ / "close_prices.csv").string() << "\n"
          << "  task:\n"
          << "    strategy_id: sel_b0\n"
          << "    period: daily\n"
          << "    time: " << task_time << "\n"
          << "    strict_time: true\n"
          << "  strategy:\n"
          << "    id: sel_b0\n"
          << "    so: " << plugin_so_.string() << "\n"
          << "    params:\n"
          << "      weights_path: " << (dir_ / "weights.csv").string() << "\n"
          << (task_uid.empty() ? "" : "      task_uid: " + task_uid + "\n")
          << "      normalize: false\n"
          << "account:\n"
          << "  initial_cash: 1000000.0\n"
          << kAShareFrictions
          << "report:\n"
          << "  output_dir: " << (dir_ / sub).string() << "\n";
        { std::ofstream ofs(cfg_path); ofs << y.str(); }
        xt_bt_sel::XtSelRunnerConfig cfg;
        std::string err;
        if (!load_cfg(cfg_path, cfg, err)) {
            ADD_FAILURE() << "load_sel_config failed: " << err;
            return -1;
        }
        return xt_bt_sel::XtSelBacktestRunner::run(cfg);
    }

    std::filesystem::path plugin_so_;
    std::vector<std::unique_ptr<ScopedEnvVar>> env_;
};

// 干净 weights → 正常跑（对照钉，防 fail-closed 变成 fail-always）。
TEST_F(SelWeightsFailClosedFixture, CleanWeightsRunsToCompletion) {
    write_file("weights.csv", "date,symbol,weight\n20260112,600000.SH,0.5\n");
    ASSERT_EQ(run_with_plugin("ok"), 0);
    EXPECT_EQ(read_csv(dir_ / "ok" / "trades.csv").size(), 1u);
}

// 坏行（缺列）→ init 抛错 → runner rc=5，不产报告（旧实现静默跳过该行 rc=0）。
TEST_F(SelWeightsFailClosedFixture, ShortRowFailsFast) {
    write_file("weights.csv",
               "date,symbol,weight\n"
               "20260112,600000.SH,0.5\n"
               "20260113,600000.SH\n");   // 缺 weight 列
    EXPECT_EQ(run_with_plugin("bad_short"), 5);
    EXPECT_FALSE(std::filesystem::exists(dir_ / "bad_short" / "trades.csv"));
}

TEST_F(SelWeightsFailClosedFixture, BadNumberFailsFast) {
    write_file("weights.csv",
               "date,symbol,weight\n"
               "20260112,600000.SH,zero-point-five\n");
    EXPECT_EQ(run_with_plugin("bad_num"), 5);
}

TEST_F(SelWeightsFailClosedFixture, BadDateFailsFast) {
    write_file("weights.csv",
               "date,symbol,weight\n"
               "2026-01-12,600000.SH,0.5\n");
    EXPECT_EQ(run_with_plugin("bad_date"), 5);
}

TEST_F(SelWeightsFailClosedFixture, EmptySymbolFailsFast) {
    write_file("weights.csv",
               "date,symbol,weight\n"
               "20260112,,0.5\n");
    EXPECT_EQ(run_with_plugin("bad_sym"), 5);
}

// 重复 (date,symbol)：旧实现 last-wins 静默丢掉一条真实意图。
TEST_F(SelWeightsFailClosedFixture, DuplicateKeyFailsFast) {
    write_file("weights.csv",
               "date,symbol,weight\n"
               "20260112,600000.SH,0.5\n"
               "20260112,600000.SH,0.3\n");
    EXPECT_EQ(run_with_plugin("dup"), 5);
}

// 零有效行（只有 header）→ 配置了 weights 却读不到任何目标，必须报错。
TEST_F(SelWeightsFailClosedFixture, HeaderOnlyFailsFast) {
    write_file("weights.csv", "date,symbol,weight\n");
    EXPECT_EQ(run_with_plugin("empty"), 5);
}

// 文件不存在 → 同样 fail-fast（旧实现只 warn 后照跑）。
TEST_F(SelWeightsFailClosedFixture, MissingFileFailsFast) {
    EXPECT_EQ(run_with_plugin("missing"), 5);
}

// ── F10 槽维（effective_time / task_uid）的 runner 级接线 ─────────────────────
//
// 策略侧的槽解析有自己的单测（SelWeightsCsvStrategyTest），但「runner 把 config 的
// task time 真喂到 on_schedule 的 curTime、策略据此选中对应槽」这一段只有端到端能
// 证。同日三槽刻意给互斥的权重，选错哪个都会在成交量上直接分叉：
//   (1030, 通配)   → 0.9 → 90,000 股
//   (1500, task:1) → 0.5 → 50,000 股   ← config 是 time=1500 + task_uid=task:1
//   (通配, 通配)   → 0.1 → 10,000 股
TEST_F(SelWeightsFailClosedFixture, RunnerPicksWeightSlotByConfiguredTaskTime) {
    write_file("weights.csv",
               "date,symbol,weight,effective_time,task_uid\n"
               "20260112,600000.SH,0.9,1030,\n"
               "20260112,600000.SH,0.5,1500,task:1\n"
               "20260112,600000.SH,0.1,,\n");
    ASSERT_EQ(run_with_plugin("slot", /*task_time=*/1500, /*task_uid=*/"task:1"), 0);

    const auto trades = read_csv(dir_ / "slot" / "trades.csv");
    ASSERT_EQ(trades.size(), 1u);
    EXPECT_EQ(trades[0].at("qty"), "50000.0000")
        << "runner 没把 config 的 task time / task_uid 接到槽解析上";

    // 对照：换一个 task time，同一份 weights 必须选中另一个槽（判据不是「恒取首行」）。
    ASSERT_EQ(run_with_plugin("slot1030", /*task_time=*/1030, /*task_uid=*/"task:1"), 0);
    const auto t2 = read_csv(dir_ / "slot1030" / "trades.csv");
    ASSERT_EQ(t2.size(), 1u);
    EXPECT_EQ(t2[0].at("qty"), "90000.0000");

    // 对照：不声明 task_uid 时 1500 槽的 uid 不匹配 → 落到通配槽。
    ASSERT_EQ(run_with_plugin("slotany", /*task_time=*/1500, /*task_uid=*/""), 0);
    const auto t3 = read_csv(dir_ / "slotany" / "trades.csv");
    ASSERT_EQ(t3.size(), 1u);
    EXPECT_EQ(t3[0].at("qty"), "10000.0000");
}

// ── B6-5 trace：SEL 原生门禁链事件流（backtest.trace_mode: full）──────────────

namespace {

/// @brief events_full.csv 一行（外层 4 列；payload 内含逗号 → 引擎侧 CSV 引号转义）。
struct TraceRow {
    long long   ts = 0;
    unsigned long long seq = 0;
    std::string type;
    std::map<std::string, std::string> payload;
};

/// @brief 读 events_full.csv（含 CSV 引号反解 + payload 的 `k=v,` 拆分）。
std::vector<TraceRow> read_trace(const std::filesystem::path& p) {
    std::vector<TraceRow> out;
    std::ifstream ifs(p);
    if (!ifs) return out;
    std::string line;
    if (!std::getline(ifs, line)) return out;   // header
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        // 前三列无引号（ts / seq / event_type），第四列是可能带引号的 payload。
        const auto c1 = line.find(',');
        const auto c2 = line.find(',', c1 + 1);
        const auto c3 = line.find(',', c2 + 1);
        if (c1 == std::string::npos || c2 == std::string::npos ||
            c3 == std::string::npos) {
            continue;
        }
        TraceRow r;
        r.ts   = std::stoll(line.substr(0, c1));
        r.seq  = std::stoull(line.substr(c1 + 1, c2 - c1 - 1));
        r.type = line.substr(c2 + 1, c3 - c2 - 1);
        std::string payload = line.substr(c3 + 1);
        if (payload.size() >= 2 && payload.front() == '"' && payload.back() == '"') {
            payload = payload.substr(1, payload.size() - 2);
            std::string un;
            for (size_t i = 0; i < payload.size(); ++i) {
                un.push_back(payload[i]);
                if (payload[i] == '"' && i + 1 < payload.size() && payload[i + 1] == '"') ++i;
            }
            payload = un;
        }
        size_t start = 0;
        while (start <= payload.size()) {
            const auto comma = payload.find(',', start);
            const std::string tok = payload.substr(
                start, comma == std::string::npos ? std::string::npos : comma - start);
            const auto eq = tok.find('=');
            if (eq != std::string::npos) r.payload[tok.substr(0, eq)] = tok.substr(eq + 1);
            if (comma == std::string::npos) break;
            start = comma + 1;
        }
        out.push_back(std::move(r));
    }
    return out;
}

}  // namespace

// trace_mode: full → 六段门禁链 `sel_fire → sel_target → sel_gate → order_submit
// → trade → sel_settle` 逐段发射，且按 fire_seq / order_id 首尾相连。
// 列形态与 HFT XtEventStreamSink 一致（producer events_ingest 零改动可消费）。
TEST_F(SelArtifactsFixture, TraceModeFullEmitsCompleteSelChain) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.25}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kAShareFrictions,
                              /*relative_inputs=*/false,
                              /*top_extra=*/"backtest:\n  trace_mode: full\n");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto path = dir_ / "rep" / "events_full.csv";
    ASSERT_TRUE(std::filesystem::exists(path)) << "trace_mode: full 未产出 events_full.csv";
    // 表头逐字对齐 HFT sink（producer 按此列序解析）。
    std::ifstream hdr(path);
    std::string header;
    std::getline(hdr, header);
    EXPECT_EQ(header, "ts,seq,event_type,payload");

    const auto rows = read_trace(path);
    ASSERT_FALSE(rows.empty());

    std::map<std::string, int> by_type;
    for (const auto& r : rows) ++by_type[r.type];
    for (const char* t : {"sel_fire", "sel_target", "sel_gate", "order_submit",
                          "trade", "sel_settle"}) {
        EXPECT_GT(by_type[t], 0) << "门禁链缺段：" << t;
    }
    EXPECT_EQ(by_type["sel_settle"], 2) << "sel_settle 应逐交易日各一条";

    // seq 全局单调、ts 日内单调不减（合成时间轴的排序性）。
    for (size_t i = 1; i < rows.size(); ++i) {
        EXPECT_EQ(rows[i].seq, rows[i - 1].seq + 1) << "seq 不连续";
        EXPECT_GE(rows[i].ts, rows[i - 1].ts) << "ts 非单调";
    }
    // ts = exec_date × 1e9 + HHMMSSmmm（基准 = fire 时刻 15:00）。
    EXPECT_GE(rows.front().ts, 20260112LL * 1'000'000'000LL + 150'000'000LL);
    EXPECT_LT(rows.front().ts, 20260112LL * 1'000'000'000LL + 160'000'000LL);

    // 链条相连：fire_seq 从 sel_fire 贯到 sel_gate；order_id 从 order_submit 贯到 trade。
    std::string fire_seq;
    for (const auto& r : rows) {
        if (r.type == "sel_fire") { fire_seq = r.payload.at("fire_seq"); break; }
    }
    ASSERT_FALSE(fire_seq.empty());
    for (const auto& r : rows) {
        if (r.type == "sel_target" || r.type == "sel_gate") {
            EXPECT_EQ(r.payload.at("fire_seq"), fire_seq) << r.type << " 未挂到本次 fire";
        }
    }
    std::string order_id;
    for (const auto& r : rows) {
        if (r.type == "order_submit") {
            order_id = r.payload.at("order_id");
            EXPECT_EQ(r.payload.at("status"), "Submitted");
            EXPECT_EQ(r.payload.at("side"), "BUY");
            break;
        }
    }
    ASSERT_FALSE(order_id.empty());
    bool linked_fill = false;
    for (const auto& r : rows) {
        if (r.type == "trade" && r.payload.at("order_id") == order_id) linked_fill = true;
    }
    EXPECT_TRUE(linked_fill) << "成交行未挂回提交它的 order_id";

    // 门禁放行段带 decision=pass（拒因段带 reason）。
    bool saw_pass = false;
    for (const auto& r : rows) {
        if (r.type == "sel_gate" && r.payload.at("decision") == "pass") saw_pass = true;
    }
    EXPECT_TRUE(saw_pass);
}

// trace_mode 缺省（off）→ 不产 events_full.csv，产物集合逐位回到基线。
TEST_F(SelArtifactsFixture, TraceModeOffProducesNoEventStream) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.25}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kAShareFrictions);
    ASSERT_EQ(run(cfg, &stub), 0);
    EXPECT_FALSE(std::filesystem::exists(dir_ / "rep" / "events_full.csv"));
}

// 非法 trace_mode 取值在解析期 fail-fast：静默退 off 会让「开了 trace 却没产物」
// 看起来像引擎坏了。
TEST_F(SelArtifactsFixture, InvalidTraceModeRejectedAtParse) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv", "date,code,close\n20260112,600000.SH,10.0\n");
    const auto cfg_path = make_cfg("rep", 1'000'000.0, kAShareFrictions,
                                   /*relative_inputs=*/false,
                                   /*top_extra=*/"backtest:\n  trace_mode: verbose\n");
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    EXPECT_FALSE(load_cfg(cfg_path, cfg, err));
    EXPECT_NE(err.find("trace_mode"), std::string::npos) << err;
}

// ── B7e 执行真实度：整数投影（F09）+ 参与率上限与跨日 carry（F15）─────────────
//
// 全部用**零费用**装置（不写 `fees:` 节 → 无 fee rule → 逐笔费用恒 0），让现金约束
// 的边界是可手算的整数：投影的判据是「差一手就是差一手」，掺进两位小数的佣金后
// 断言只能写成近似，边界钉就钉不住了。

namespace {

/// @brief 零费用 A 股摩擦（只留整手与 vol_scale；`fees:` 刻意缺席）。
const char* kZeroFeeFrictions =
    "commodities:\n"
    "  '600000.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0, min_lot: 100 }\n"
    "  '600519.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0, min_lot: 100 }\n"
    "risk:\n"
    "  max_order_qty: 10000000.0\n"
    "  allow_short: false\n";

/// @brief daily_positions.csv 里某 (date, code) 的 qty；缺行返 -1。
double position_qty(const std::vector<Row>& rows, const std::string& date,
                    const std::string& code) {
    for (const auto& r : rows) {
        if (r.at("date") == date && r.at("code") == code) {
            return std::strtod(r.at("qty").c_str(), nullptr);
        }
    }
    return -1.0;
}

/// @brief summary.json 的 execution 块里某数值字段（块缺席 → NaN）。
double exec_num(const std::string& body, const std::string& key) {
    const auto pos = body.find("\"execution\": {");
    if (pos == std::string::npos) return std::nan("");
    return summary_num(body.substr(pos), key);
}

}  // namespace

// ── F09 边界钉：余数最大者拿到那一手，第二名恰好差一手 ───────────────────────
//
// 装置（初始现金 1,000,000，零费用，整手 100）：
//   600000.SH 价 10，权重 0.4897 → 原始目标 48,970 股 → 整手基线 48,900，余数 70 股
//   600519.SH 价 20，权重 0.5103 → 原始目标 25,515 股 → 整手基线 25,500，余数 15 股
//   基线占款 489,000 + 510,000 = 999,000 → 残余现金恰 1,000。
//   补一手的代价：600000 是 1,000（恰好花光），600519 是 2,000（差 1,000）。
// 故正解唯一：余数大的 600000 补到 49,000，600519 停在 25,500，收尾现金恰 0。
//
// 旧实现（无投影，逐 code 向零截断）下 600000 停在 48,900 → 本断言 FAIL。
// 若现金约束写成严格小于（`<` 而非 `<=`），600000 那一手会被误拒 → 同样 FAIL。
// 若投影不看现金，600519 也会被补一手 → 现金落负、run 直接 rc=10。
TEST_F(SelArtifactsFixture, LargestRemainderGivesLotToBiggestRemainderWithinCash) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260109,600519.SH,20.0\n"
               "20260112,600000.SH,10.0\n"
               "20260112,600519.SH,20.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.4897},
                                          {"600519.SH", 0.5103}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kZeroFeeFrictions,
                              /*relative_inputs=*/false,
                              "execution:\n  integer_projection: largest_remainder\n");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260112", "600000.SH"), 49000.0);
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260112", "600519.SH"), 25500.0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_DOUBLE_EQ(summary_num(body, "residual_cash"), 0.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "projection_lots_added"), 1.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "projection_lots_removed"), 0.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "projection_rejected_entries"), 0.0);
    // 落账口的可负担截断不该被触发：投影已把目标压在可用现金内。
    EXPECT_DOUBLE_EQ(summary_num(body, "affordability_caps"), 0.0);
}

// 同一装置的对照半边：缺省 floor 投影下两个 code 都停在整手基线，1,000 元残余
// 现金投不出去。这一条与上一条**必须同时成立**——否则「largest_remainder 生效了」
// 的证据可能只是装置本身算出了 49,000。
TEST_F(SelArtifactsFixture, FloorProjectionLeavesResidualCashUninvested) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260109,600519.SH,20.0\n"
               "20260112,600000.SH,10.0\n"
               "20260112,600519.SH,20.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.4897},
                                          {"600519.SH", 0.5103}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kZeroFeeFrictions,
                              /*relative_inputs=*/false,
                              "execution:\n  integer_projection: floor\n");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260112", "600000.SH"), 48900.0);
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260112", "600519.SH"), 25500.0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_DOUBLE_EQ(summary_num(body, "residual_cash"), 1000.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "projection_fires"), 0.0);
}

// min_lot == 1（不做整手截断）下投影契约仍成立：基线本身不是「整手的整数倍」，
// 契约若按写回值的绝对值判整手，会把一条都没改过的基线全判违约、整批空转。
// 判据是「与基线之差是整手」，故 rejected 恒 0，目标停在基线上。
TEST_F(SelArtifactsFixture, ProjectionContractHoldsWhenMinLotIsOne) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,3.0\n"
               "20260112,600000.SH,3.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 1.0}}}});
    const char* kUnitLot =
        "commodities:\n"
        "  '600000.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0, min_lot: 1 }\n"
        "risk:\n"
        "  max_order_qty: 10000000.0\n"
        "  allow_short: false\n";
    const auto cfg = make_cfg("rep", 1'000'000.0, kUnitLot,
                              /*relative_inputs=*/false,
                              "execution:\n  integer_projection: largest_remainder\n");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_DOUBLE_EQ(exec_num(body, "projection_fires"), 1.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "projection_rejected_entries"), 0.0);
    // min_lot=1 时 up-round 整体惰性：`round_to_min_lot_fp` 对 min_lot ≤ 1 是直通，
    // 基线 ≡ 原始目标 ⇒ 余数恒 0 ⇒ 没有可补的手。这是量纲规则的推论，不是现金
    // 约束把补手拒掉了（现金还剩一整股都不到，但那不是本条的判据）。
    EXPECT_DOUBLE_EQ(exec_num(body, "projection_adjusted_targets"), 0.0);
}

// 非法 integer_projection 取值解析期 fail-fast（同 trace_mode 先例）。
TEST_F(SelArtifactsFixture, InvalidIntegerProjectionRejectedAtParse) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv", "date,code,close\n20260112,600000.SH,10.0\n");
    const auto cfg_path = make_cfg("rep", 1'000'000.0, kZeroFeeFrictions,
                                   /*relative_inputs=*/false,
                                   "execution:\n  integer_projection: hungarian\n");
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    EXPECT_FALSE(load_cfg(cfg_path, cfg, err));
    EXPECT_NE(err.find("integer_projection"), std::string::npos) << err;
}

// 参与率越界（0 或 > 1）解析期 fail-fast。
TEST_F(SelArtifactsFixture, OutOfRangeParticipationRejectedAtParse) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv", "date,code,close\n20260112,600000.SH,10.0\n");
    for (const char* bad : {"0.0", "1.5"}) {
        const auto cfg_path = make_cfg(
            "rep", 1'000'000.0, kZeroFeeFrictions, /*relative_inputs=*/false,
            std::string("execution:\n  max_participation: ") + bad + "\n");
        xt_bt_sel::XtSelRunnerConfig cfg;
        std::string err;
        EXPECT_FALSE(load_cfg(cfg_path, cfg, err)) << bad;
        EXPECT_NE(err.find("max_participation"), std::string::npos) << err;
    }
}

// ── F15 边界钉：当日成交量的 10% 恰好卡住成交量 ─────────────────────────────
//
// 装置：目标 100,000 股（1,000,000 现金 / 10 元，全仓），当日成交量 100,000 股，
// 参与率缺省 0.1 → 当日最多成 10,000 股。旧实现（T 日 close 强制整笔成交）会一次
// 吃满 100,000 股 → 本断言 FAIL。
TEST_F(SelArtifactsFixture, ParticipationCapLimitsDailyFillToVolumeShare) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close,volume\n"
               "20260109,600000.SH,10.0,100000\n"
               "20260112,600000.SH,10.0,100000\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 1.0}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kZeroFeeFrictions);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260112", "600000.SH"), 10000.0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_DOUBLE_EQ(exec_num(body, "capped_fills"), 1.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "capped_shortfall_qty"), 90000.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_created"), 1.0);
    // volume 列在场即启用，无需写 `execution:` 节。
    EXPECT_NE(body.find("\"volume_available\": true"), std::string::npos);
}

// 未成量跨日复投：次日份额刷新后继续成交，且**不重复挂第二条 carry 腿**
// （复投单的未成量记回原腿；漏了这一步会让同一份缺口被追两遍）。
TEST_F(SelArtifactsFixture, CarryResubmitsNextDayWithoutDoubleCounting) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close,volume\n"
               "20260109,600000.SH,10.0,100000\n"
               "20260112,600000.SH,10.0,100000\n"
               "20260113,600000.SH,10.0,100000\n");
    // 权重只在首日给：次日无 fire → 当日成交全部来自 carry 复投。
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 1.0}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kZeroFeeFrictions);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260112", "600000.SH"), 10000.0);
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260113", "600000.SH"), 20000.0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_created"), 1.0);   // 恰一条，不是两条
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_resubmits"), 1.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_outstanding"), 1.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_outstanding_qty"), 80000.0);
}

// carry 到限丢弃，语义 = 「最多跨 N 个交易日复投」。judge-before-replay：
//   0112 生成（days=0，当日不计龄）→ 0113 日界闸 days(0) < 1 放行、复投一次，
//   日末 days→1 → 0114 日界闸 days(1) >= 1 → 丢弃，0114/0115 无腿可投。
// 判龄若放在复投之后（先投后计龄），`carry_max_days: 1` 会复投**两天**、`0` 还能
// 复投一次 —— 配置写下的天数与实际执行天数差一。
// 到限不丢会让 carry 无限期挂着，回测里「几个月后终于买到了」的成交在现实中根本
// 不属于这次调仓决策。
TEST_F(SelArtifactsFixture, CarryDroppedAfterMaxDays) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n20260115\n");
    write_file("close_prices.csv",
               "date,code,close,volume\n"
               "20260109,600000.SH,10.0,1000\n"
               "20260112,600000.SH,10.0,1000\n"
               "20260113,600000.SH,10.0,1000\n"
               "20260114,600000.SH,10.0,1000\n"
               "20260115,600000.SH,10.0,1000\n");
    // 日成交量 1,000 股 × 10% = 100 股/日 = 恰一手：目标 100,000 股永远吃不完。
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 1.0}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kZeroFeeFrictions,
                              /*relative_inputs=*/false,
                              "execution:\n  carry_max_days: 1\n");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_created"), 1.0);
    // 恰一次跨日复投（0113）；0114 的日界闸把它丢掉。
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_resubmits"), 1.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_dropped"), 1.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_outstanding"), 0.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_noop_removed"), 0.0);
    // 100 股/日 × 2 日（0112/0113）= 200 股成交，余 99,800 股被丢弃。
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_dropped_qty"), 99800.0);
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260115", "600000.SH"), 200.0);
}

// `carry_max_days: 0` = 一天都不跨：生成日之后的第一个日界闸就丢弃，零复投。
// 旧实现（先复投后计龄）下这一条会复投一次、position 变 200 → FAIL。
TEST_F(SelArtifactsFixture, CarryMaxDaysZeroDoesNotCrossDay) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close,volume\n"
               "20260109,600000.SH,10.0,1000\n"
               "20260112,600000.SH,10.0,1000\n"
               "20260113,600000.SH,10.0,1000\n"
               "20260114,600000.SH,10.0,1000\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 1.0}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kZeroFeeFrictions,
                              /*relative_inputs=*/false,
                              "execution:\n  carry_max_days: 0\n");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_created"), 1.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_resubmits"), 0.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_dropped"), 1.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_dropped_qty"), 99900.0);
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260114", "600000.SH"), 100.0);
}

// 新 fire 的目标取代同 code 未成的 carry：diff = target − actual 已含未成量，
// 两条通道各提交一次就买成两份。取代计数必须可见（否则「carry 去哪了」无从解释）。
TEST_F(SelArtifactsFixture, NewFireSupersedesOutstandingCarry) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close,volume\n"
               "20260109,600000.SH,10.0,100000\n"
               "20260112,600000.SH,10.0,100000\n"
               "20260113,600000.SH,10.0,100000\n");
    // 两日都 fire：次日的 target 重算过，首日的 carry 应被取代而非叠加。
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 1.0}}},
                             {20260113u, {{"600000.SH", 1.0}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kZeroFeeFrictions);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_superseded"), 1.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_resubmits"), 0.0);
    // 次日 fire 自己又被截一次 → 累计 2 条 carry 生成、末日仍挂 1 条。
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_created"), 2.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_outstanding"), 1.0);
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260113", "600000.SH"), 20000.0);
}

// ── 未配置 = 零漂移 ────────────────────────────────────────────────────────
//
// volume 列缺席且无 `execution:` 节 → summary 无 execution 块，产物集合与本特性
// 落地前逐位一致（regression 的两个 SEL golden 是同一判据的全量版本）。
TEST_F(SelArtifactsFixture, NoVolumeNoExecutionSectionInSummary) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.25}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kAShareFrictions);
    ASSERT_EQ(run(cfg, &stub), 0);
    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(body.find("\"execution\""), std::string::npos);
}

// volume 列本身不扰动定价：上限设到 1.0（不 bind）时 trades.csv 与同装置的无量跑
// 逐字节相同。这一条钉住「多一列会不会把 close 解析歪」——第四列在场时 close 字段
// 的切法变了，切错的话价格会带上成交量的尾巴。
TEST_F(SelArtifactsFixture, VolumeColumnDoesNotPerturbPricing) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n");
    MapWeightsStrategy plain({{20260112u, {{"600000.SH", 0.25}}}});
    ASSERT_EQ(run(make_cfg("plain", 1'000'000.0, kAShareFrictions), &plain), 0);
    const auto plain_trades = read_text(dir_ / "plain" / "trades.csv");

    write_file("close_prices.csv",
               "date,code,close,volume\n"
               "20260109,600000.SH,10.0,999999999\n"
               "20260112,600000.SH,10.0,999999999\n");
    MapWeightsStrategy withvol({{20260112u, {{"600000.SH", 0.25}}}});
    const auto cfg = make_cfg("withvol", 1'000'000.0, kAShareFrictions,
                              /*relative_inputs=*/false,
                              "execution:\n  max_participation: 1.0\n");
    ASSERT_EQ(run(cfg, &withvol), 0);

    EXPECT_EQ(read_text(dir_ / "withvol" / "trades.csv"), plain_trades);
    const auto body = read_text(dir_ / "withvol" / "summary.json");
    EXPECT_DOUBLE_EQ(exec_num(body, "capped_fills"), 0.0);
}

// ── 盲审 fold 钉：carry 对账 / SELL 费用 / 投影现金判据 / 行序 / 解析 ───────────

// ① carry 存**绝对目标**而非未成增量：复投前用 `target − 实时持仓` 重算差额。
//
// 装置：0112 目标 30,000 股，当日参与率只放行 20,000 → carry 记「目标 30,000」。
// 0113 十送十（record 0112 收盘仓 20,000 → 送 20,000 股，0114 到账）：sizing 口径的
// 持仓（含在途）变成 40,000，已经越过 30,000 → 差额反向 → 按 noop 摘除。
// 存增量的旧实现会盲投「还差 10,000 股」，在一只刚送完股的票上再买一笔：
// 0114 持仓 50,000 而不是 40,000（本断言在旧实现下 FAIL）。
TEST_F(SelArtifactsFixture, CarryRecomputesAgainstLivePositionAcrossCorporateAction) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close,volume\n"
               "20260109,600000.SH,10.0,200000\n"
               "20260112,600000.SH,10.0,200000\n"
               "20260113,600000.SH,5.0,200000\n"
               "20260114,600000.SH,5.0,200000\n");
    // 十送十：ex 20260113、record 20260112、无现金、红股 20260114 到账。
    write_file("ca.csv",
               "code,record_date,ex_date,pay_date,cash_per_share,"
               "bonus_transfer_per_share,rights_per_share,rights_price,is_delist,"
               "shares_list_date\n"
               "600000.SH,20260112,20260113,,0,1.0,0,0,0,20260114\n");
    // 只在 0112 fire：0113 的成交（若有）只可能来自 carry 复投。
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.3}}}});
    const auto cfg = make_cfg(
        "rep", 1'000'000.0, kZeroFeeFrictions, /*relative_inputs=*/false,
        /*top_extra=*/"",
        "  corporate_actions_csv: " + (dir_ / "ca.csv").string() + "\n");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_created"), 1.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_noop_removed"), 1.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_resubmits"), 0.0)
        << "carry 在送转后仍盲投 —— 未按目标对账";
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_outstanding"), 0.0);

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260112", "600000.SH"), 20000.0);
    // 20,000 自有 + 20,000 送转到账；多出的 10,000 = 盲投的那一笔。
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260114", "600000.SH"), 40000.0);
}

// ② SELL 被参与率缩量后「所得 < 费用」→ 整腿顺延，不落成负现金。
//
// 装置：600519.SH 价 1、佣金下限 500（比例费率极低，靠下限抬升）。0112 满仓后现金
// 恰为 0；0113 清仓该票，当日成交量只有 1,000 股 → 参与率放行 100 股 = 所得 100 元，
// 而佣金下限仍要 500 元。旧实现（缩量后跳过 SELL 侧资金校验）直接落账 → 现金
// −400.05 → run 撞 rc=10（本 ASSERT_EQ(run, 0) 在旧实现下 FAIL）。
TEST_F(SelArtifactsFixture, SellDeferredWhenParticipationCapLeavesFeeUnaffordable) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close,volume\n"
               "20260109,600000.SH,10.0,1000000000\n"
               "20260109,600519.SH,1.0,1000000000\n"
               "20260112,600000.SH,10.0,1000000000\n"
               "20260112,600519.SH,1.0,1000000000\n"
               "20260113,600000.SH,10.0,1000000000\n"
               "20260113,600519.SH,1.0,1000\n");
    // 600000.SH 无 fee rule（零费用）；600519.SH 佣金下限 500 元。
    const char* kMinCommissionOnPenny =
        "commodities:\n"
        "  '600000.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0, min_lot: 100 }\n"
        "  '600519.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0, min_lot: 100 }\n"
        "fees:\n"
        "  '600519.SH': { open: 0.00025, close: 0.00025, stamp_duty_rate: 0.0005,"
        " transfer_fee_rate: 0.0, min_commission: 500.0 }\n"
        "risk:\n"
        "  max_order_qty: 10000000.0\n"
        "  allow_short: false\n";
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.6}, {"600519.SH", 0.4}}},
        {20260113u, {{"600000.SH", 1.0}, {"600519.SH", 0.0}}},
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, kMinCommissionOnPenny);
    ASSERT_EQ(run(cfg, &stub), 0) << "SELL 缩量后未复检费用 → 现金落负 (rc=10)";

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_DOUBLE_EQ(exec_num(body, "sell_fee_deferrals"), 1.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "capped_fills"), 1.0);
    EXPECT_DOUBLE_EQ(summary_num(body, "negative_cash_events"), 0.0);
    // 整腿顺延 → 持仓一股没动。
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260113", "600519.SH"),
                     position_qty(pos, "20260112", "600519.SH"));
}

// ③ F09 现金判据按「今天真能卖到手的钱」算：停牌的 SELL 腿回款计 0。
//
// 装置：0112 满仓 600000.SH（现金归零）；0113 该票停牌，权重切到
// {600000.SH: 0, 600519.SH: 0.5}。停牌腿今天一股卖不出 → 可用现金仍是 0，
// BUY 腿的整手基线本身就买不起 → 投影按 down-round 削到 0，落账口无事可做。
// 旧实现把停牌腿的名义额当现金 → 反而给 BUY 腿**补**了一手，落账口再被可负担
// 截断打回来（补出来的那一手在报告上表现为「投影加了仓、实际没买到」）。
TEST_F(SelArtifactsFixture, ProjectionCountsOnlySellableProceedsAsCash) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260109,600519.SH,7.0\n"
               "20260112,600000.SH,10.0\n"
               "20260112,600519.SH,7.0\n"
               "20260113,600000.SH,10.0\n"
               "20260113,600519.SH,7.0\n");
    // 0113 停牌 600000.SH（is_open=0）。
    write_file("trad.csv",
               "date,code,is_open,is_limit_up,is_limit_down\n"
               "20260113,600000.SH,0,0,0\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 1.0}}},
        {20260113u, {{"600000.SH", 0.0}, {"600519.SH", 0.5}}},
    });
    const auto cfg = make_cfg(
        "rep", 500'000.0, kZeroFeeFrictions, /*relative_inputs=*/false,
        "execution:\n  integer_projection: largest_remainder\n",
        "  tradability_csv: " + (dir_ / "trad.csv").string() + "\n");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_DOUBLE_EQ(exec_num(body, "projection_lots_added"), 0.0)
        << "投影拿卖不掉的钱补了手数";
    EXPECT_GT(exec_num(body, "projection_lots_removed"), 0.0);
    // 投影已把目标压回可用现金内 → 落账口无需再截。
    EXPECT_DOUBLE_EQ(summary_num(body, "affordability_caps"), 0.0);
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260113", "600519.SH"), -1.0)
        << "现金为 0 时不该建出新仓";
}

// ④ 决策行序 = code 字典序，早退条目不因分批而前移。
//
// 装置：三个 code，字典序中间那个当日无 T-1 价（kNoPrice 早退）。行序是
// target_weights.csv / events_full.csv 的消费方契约（producer 的 normalize 依赖
// 落地序）。旧实现在 Pass 1 就地发早退行 → 无价行跑到最前 → 本断言 FAIL。
TEST_F(SelArtifactsFixture, DecisionRowsKeepCodeOrderAcrossEarlyExits) {
    write_file("trading_dates.csv", "date\n20260112\n");
    // 600519.SH 只有 T 日行、没有 T-1 行 → fire 时 price_resolver 落空。
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260109,600999.SH,20.0\n"
               "20260112,600000.SH,10.0\n"
               "20260112,600519.SH,30.0\n"
               "20260112,600999.SH,20.0\n");
    const char* kThreeCodes =
        "commodities:\n"
        "  '600000.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0, min_lot: 100 }\n"
        "  '600519.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0, min_lot: 100 }\n"
        "  '600999.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0, min_lot: 100 }\n"
        "risk:\n"
        "  max_order_qty: 10000000.0\n"
        "  allow_short: false\n";
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.3},
                                          {"600519.SH", 0.3},
                                          {"600999.SH", 0.3}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kThreeCodes);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto rows = read_csv(dir_ / "rep" / "target_weights.csv");
    ASSERT_EQ(rows.size(), 3u);
    EXPECT_EQ(rows[0].at("code"), "600000.SH");
    EXPECT_EQ(rows[1].at("code"), "600519.SH") << "无价行被提到批首 —— 行序漂移";
    EXPECT_EQ(rows[2].at("code"), "600999.SH");
    EXPECT_EQ(rows[1].at("planning_status"), "no_price");
}

// ⑤a 无价早退不蒸发 carry：kNoPrice 既没算 target 也不会有替代订单，把 carry 记成
// 「被取代」等于让意图凭空消失还谎报了去向。
//
// 装置：0112 fire 建 carry；0113 无 fire（carry 复投）；0113 该票**没有 close 行**
// → 0114 的 fire 拿不到 T-1 价 → kNoPrice。旧实现在该决策上照样 supersede，
// carry_superseded 变 1、腿没了（本断言 FAIL）。
TEST_F(SelArtifactsFixture, NoPriceDecisionDoesNotSupersedeCarry) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close,volume\n"
               "20260109,600000.SH,10.0,100000\n"
               "20260112,600000.SH,10.0,100000\n"
               "20260114,600000.SH,10.0,100000\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 1.0}}},
                             {20260114u, {{"600000.SH", 1.0}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kZeroFeeFrictions);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_superseded"), 0.0)
        << "kNoPrice 早退把 carry 记成了「被取代」";
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_dropped_stale"), 0.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_outstanding"), 1.0);
}

// ⑤b 新 fire 缺席的 code：carry 显式丢弃并计数。
//
// 一次 fire 是对整个组合的重估 —— 这一轮没给某个 code 出决策就是不再要它了，
// 让零成交的旧意图继续跨日执行等于让一份已作废的判断几天后才落地。
// 旧实现只在「本轮出了决策的 code」上摘 carry，缺席的那条会一直挂着复投。
TEST_F(SelArtifactsFixture, NewFireDropsCarryForCodesAbsentFromDecisionSet) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close,volume\n"
               "20260109,600000.SH,10.0,100000\n"
               "20260109,600519.SH,10.0,100000\n"
               "20260112,600000.SH,10.0,100000\n"
               "20260112,600519.SH,10.0,100000\n"
               "20260113,600000.SH,10.0,100000\n"
               "20260113,600519.SH,10.0,100000\n");
    // 首日两个 code 都被参与率截断 → 各挂一条 carry；次日只给 600000.SH 出决策。
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}, {"600519.SH", 0.5}}},
                             {20260113u, {{"600000.SH", 1.0}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kZeroFeeFrictions);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_created"), 3.0);   // 首日 2 条 + 次日 1 条
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_superseded"), 1.0);  // 600000.SH：出了决策
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_dropped_stale"), 1.0)  // 600519.SH：缺席
        << "缺席 code 的 carry 仍在跨日执行一份已作废的意图";
    EXPECT_GT(exec_num(body, "carry_dropped_stale_qty"), 0.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_resubmits"), 0.0);
}

// ⑥ ADV20 求和用 __int128：canonical 上界 volume（1e12 股 = 1e18 fp）满窗 20 个
// 就越过 int64，int64 累加会绕回一个**小得多**的分母，把 ratio 放大一个量级。
//
// 装置：20 个 warmup 交易日各 1e12 股 → ADV20 恰 1e12 股；第 21 日提交 100,000 股
// → ratio 应为 1e-7。int64 溢出下分母塌成 ~7.77e10 股 → ratio ≈ 1.29e-6（本断言 FAIL）。
TEST_F(SelArtifactsFixture, Adv20SumDoesNotOverflowAtCanonicalVolumeCeiling) {
    std::string dates = "date\n";
    std::string closes = "date,code,close,volume\n20260109,600000.SH,10.0,1000000000000\n";
    // 20 个只推进 ADV 窗口的交易日 + 第 21 日交易。
    for (int d = 0; d < 21; ++d) {
        const int day = 20260112 + d;   // 连续自然日即可（trading_dates 是权威日历）
        dates += std::to_string(day) + "\n";
        closes += std::to_string(day) + ",600000.SH,10.0,1000000000000\n";
    }
    write_file("trading_dates.csv", dates);
    write_file("close_prices.csv", closes);
    MapWeightsStrategy stub({{20260132u, {{"600000.SH", 1.0}}}});  // 第 21 日
    const auto cfg = make_cfg("rep", 1'000'000.0, kZeroFeeFrictions);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    ASSERT_DOUBLE_EQ(exec_num(body, "adv20_samples"), 1.0);
    // 100,000 股 / 1e12 股 = 1e-7。
    EXPECT_NEAR(exec_num(body, "adv20_ratio_max"), 1e-7, 1e-9);
}

// ⑦a volume 列名精确匹配：`volume_usd` 是另一口径（单位是钱不是股），前缀匹配会
// 把它当股数吃进参与率分母。精确匹配下该列被忽略 → 无 execution 块。
TEST_F(SelArtifactsFixture, VolumeUsdHeaderIsNotTreatedAsShareVolume) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close,volume_usd\n"
               "20260109,600000.SH,10.0,1000\n"
               "20260112,600000.SH,10.0,1000\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 1.0}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kZeroFeeFrictions);
    ASSERT_EQ(run(cfg, &stub), 0);
    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(body.find("\"execution\""), std::string::npos)
        << "volume_usd 被当成了股数成交量";
    // 未被 cap → 全额成交。
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260112", "600000.SH"), 100000.0);
}

// ⑦b volume 单元格必须是纯整数股数且全串消费：`1e6` 被 stoll 读成 1、`123.5` 读成
// 123、`-5` 读成 -5 —— 都把「数据坏了」静默变成「流动性极低」，方向恰与参与率上限
// 一致、肉眼不可辨。坏行 fail-fast（与 CA 坏行同惯例，rc=1）。
TEST_F(SelArtifactsFixture, MalformedVolumeCellFailsFast) {
    write_file("trading_dates.csv", "date\n20260112\n");
    for (const char* bad : {"1e6", "123.5", "-5", "abc", ""}) {
        write_file("close_prices.csv",
                   std::string("date,code,close,volume\n"
                               "20260109,600000.SH,10.0,100000\n"
                               "20260112,600000.SH,10.0,") + bad + "\n");
        MapWeightsStrategy stub({{20260112u, {{"600000.SH", 1.0}}}});
        const auto cfg = make_cfg("rep", 1'000'000.0, kZeroFeeFrictions);
        EXPECT_EQ(run(cfg, &stub), 1) << "volume 单元格 '" << bad << "' 未被拒";
    }
}

// ⑧ 参与率量化后为 0 的取值解析期 fail-fast：1e-9 在 1e6 定点下塌成 0，
// 那不是「上限极小」而是「一股都不许成交」，与写下的意图相反。
TEST_F(SelArtifactsFixture, ParticipationQuantizingToZeroRejectedAtParse) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv", "date,code,close\n20260112,600000.SH,10.0\n");
    const auto cfg_path = make_cfg("rep", 1'000'000.0, kZeroFeeFrictions,
                                   /*relative_inputs=*/false,
                                   "execution:\n  max_participation: 0.000000001\n");
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    EXPECT_FALSE(load_cfg(cfg_path, cfg, err));
    EXPECT_NE(err.find("max_participation"), std::string::npos) << err;
}

// ⑨ SELL 侧参与率截断：卖出腿同样受当日成交量上限，未成量转 carry。
// 「只 cap 买入」的实现会让清仓在一只没量的票上瞬间完成 —— 回测最容易高估的
// 恰是退出侧的可执行性。
TEST_F(SelArtifactsFixture, ParticipationCapAppliesToSellLegs) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close,volume\n"
               "20260109,600000.SH,10.0,1000000000\n"
               "20260112,600000.SH,10.0,1000000000\n"
               "20260113,600000.SH,10.0,50000\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 1.0}}},
                             {20260113u, {{"600000.SH", 0.0}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kZeroFeeFrictions);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    // 100,000 股持仓，0113 只能卖 50,000 × 10% = 5,000 股 → 余 95,000。
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260112", "600000.SH"), 100000.0);
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260113", "600000.SH"), 95000.0);
    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_DOUBLE_EQ(exec_num(body, "capped_fills"), 1.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_created"), 1.0);
}

// ⑩ 投影按 T-1 价、落账按 T 价：价差窗口由落账口的可负担截断兜底，而不是把
// 目标截成 0。投影的前提只是「投影认为买得起 ⇒ 落账口买得起」，T 日涨价不在
// 该前提的保护范围内。
TEST_F(SelArtifactsFixture, ProjectionPriceGapFallsBackToAffordabilityCap) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,11.0\n");   // T 日比 T-1 涨 10%
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 1.0}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kZeroFeeFrictions,
                              /*relative_inputs=*/false,
                              "execution:\n  integer_projection: largest_remainder\n");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    // 投影按 10 元算得起 100,000 股；落账按 11 元只买得起 90,900 股（整手）。
    EXPECT_DOUBLE_EQ(summary_num(body, "affordability_caps"), 1.0);
    EXPECT_DOUBLE_EQ(summary_num(body, "negative_cash_events"), 0.0);
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260112", "600000.SH"), 90900.0);
}

// ── B7e 窄审 fold 钉：同槽双 fire 的 carry 归属 / 投影卖出额度整手 / carry 守恒 ──

namespace {

/// @brief 同一次 `on_schedule` 内连发两次目标的 external 注入 stub（同槽双 fire）。
///
/// engine 对每次 `set_target_weights` 开一个独立 fire_seq 组，两组在同一天的
/// SELL/BUY 分段里按 FIFO 依次派发 —— 这是「同槽后发 fire 重定同一 code 目标」
/// 这一形态在 runner 级别唯一的驱动方式。
class TwoFireStrategy final : public XtStrategyBase {
public:
    TwoFireStrategy(uint32_t date,
                    std::unordered_map<std::string, double> first,
                    std::unordered_map<std::string, double> second)
        : date_(date), first_(std::move(first)), second_(std::move(second)) {}

    void on_schedule(IStrategyContext* ctx, uint32_t curDate, uint32_t,
                     int64_t) override {
        if (curDate != date_) return;
        ctx->set_target_weights(first_);
        ctx->set_target_weights(second_);
    }

private:
    uint32_t                                 date_;
    std::unordered_map<std::string, double>  first_;
    std::unordered_map<std::string, double>  second_;
};

/// @brief carry 生命周期的六个计数 + created，供守恒钉逐项取值。
struct CarryCounters {
    double created      = 0.0;
    double completed    = 0.0;
    double superseded   = 0.0;
    double noop_removed = 0.0;
    double dropped_stale = 0.0;
    double dropped      = 0.0;
    double outstanding  = 0.0;

    /// @brief 终局侧合计（`created` 之外的七项去掉自身）。
    [[nodiscard]] double terminals() const {
        return completed + superseded + noop_removed + dropped_stale + dropped +
               outstanding;
    }
};

CarryCounters read_carry_counters(const std::string& body) {
    CarryCounters c;
    c.created       = exec_num(body, "carry_created");
    c.completed     = exec_num(body, "carry_completed");
    c.superseded    = exec_num(body, "carry_superseded");
    c.noop_removed  = exec_num(body, "carry_noop_removed");
    c.dropped_stale = exec_num(body, "carry_dropped_stale");
    c.dropped       = exec_num(body, "carry_dropped");
    c.outstanding   = exec_num(body, "carry_outstanding");
    return c;
}

}  // namespace

// ① 同槽后发 fire 已重定目标 → 先发 fire 的未成量不建 carry 腿。
//
// 装置（现金 1,000,000，价 10，日成交量 100,000 → 参与率放行 10,000 股/日）：
// 0112 一次 on_schedule 内连发两个目标 —— fire#1 权重 1.0（目标 100,000 股）、
// fire#2 权重 0.5（目标 50,000 股）。
//
// 引擎的幂等替换在 planning 期就收敛：fire#2 就地覆盖 fire#1 的未 dispatch
// pending（target 100,000 → 50,000，same_slot_target_replacements=1），派发只有
// 一条 50,000 腿 → 当日成 10,000，未成 40,000 建**唯一**一条 carry；0113-0116
// 逐日复投至持仓 = 最新目标 50,000 并 completed。0117 持仓停在 50,000。
//
// 旧实现（只跳过 diff==0、不覆盖）会让 fire#1 的旧目标 100,000 照常派发：
// 旧目标的未成量跨日推仓，终态越过最新目标 —— latest-target 语义失效。
// 建腿点的 latest_decided_fire 过滤（carry_stale_before_create 计数）保留为
// defense-in-depth：常规 BT 时序下覆盖先于一切派发，该分支不可达；它罩的是
// 未来 engine 语义漂移（若覆盖失效，旧腿在这里被拦下并计数披露）。
TEST_F(SelArtifactsFixture, StaleCarryFromEarlierSameDayFireIsNotCreated) {
    write_file("trading_dates.csv",
               "date\n20260112\n20260113\n20260114\n20260115\n20260116\n20260117\n");
    std::string closes = "date,code,close,volume\n20260109,600000.SH,10.0,100000\n";
    for (const char* d : {"20260112", "20260113", "20260114", "20260115",
                          "20260116", "20260117"}) {
        closes += std::string(d) + ",600000.SH,10.0,100000\n";
    }
    write_file("close_prices.csv", closes);
    TwoFireStrategy stub(20260112u, {{"600000.SH", 1.0}}, {{"600000.SH", 0.5}});
    const auto cfg = make_cfg("rep", 1'000'000.0, kZeroFeeFrictions);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    // 幂等替换在 planning 期收敛：只建一条腿（最新目标），两个 stale 计数均 0。
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_created"), 1.0)
        << "同一 code 挂了两条腿（旧目标 + 新目标），同一份意图被追两遍";
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_dropped_stale"), 0.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_stale_before_create"), 0.0)
        << "常规时序下覆盖先于派发，建腿点过滤不该触发";
    // carry 生命周期只推进最新目标：一条腿补齐即完结，收尾无残留。
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_completed"), 1.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "carry_outstanding"), 0.0);

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    // 终态持仓 = 最新目标（min_lot 100 下 50,000 是整手，无截断余量）。
    EXPECT_LE(position_qty(pos, "20260117", "600000.SH"), 50000.0)
        << "旧目标的执行把持仓推过了最新目标";
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260117", "600000.SH"), 50000.0);

    // 守恒：无建腿点 stale → 素式成立（created == 六终局之和）。
    const auto c = read_carry_counters(body);
    EXPECT_DOUBLE_EQ(c.created, c.terminals())
        << "carry 生命周期计数不守恒";
}

// ② F09 投影的卖出额度必须过整手截断：不足一手的参与率额度 = 今天一股卖不出，
//    回款按 0 计。
//
// 装置（零费用，min_lot 100）：0112 用 1,000 元满仓 600000.SH 100 股，现金归零；
// 0113 权重切到 {600000.SH: 0, 600519.SH: 1.0}，当日 600000.SH 成交量只有 999 股
// → 参与率额度 99.9 股 < 一手 → 落账口一股不成（整腿转 carry）。
//   投影若同样按 0 计回款：可用现金仍是 0 → 600519.SH 的整手基线（100 股 × 7 元）
//   买不起 → down-round 削到 0，落账口无事可做（affordability_caps 恒 0）。
//   投影若按未截整手的 99.9 股计回款（+999 元）：基线判定为「买得起」，目标留在
//   100 股，落账口再被可负担截断打回来 —— 报告上表现为「投影没削、实际没买到」。
TEST_F(SelArtifactsFixture, ProjectionSellCapBelowOneLotYieldsZeroProceeds) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close,volume\n"
               "20260109,600000.SH,10.0,1000000\n"
               "20260112,600000.SH,10.0,1000000\n"
               "20260112,600519.SH,7.0,1000000\n"
               "20260113,600000.SH,10.0,999\n"      // 额度 99.9 股 —— 差一手
               "20260113,600519.SH,7.0,1000000\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 1.0}}},
        {20260113u, {{"600000.SH", 0.0}, {"600519.SH", 1.0}}},
    });
    const auto cfg = make_cfg("rep", 1'000.0, kZeroFeeFrictions,
                              /*relative_inputs=*/false,
                              "execution:\n  integer_projection: largest_remainder\n");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_DOUBLE_EQ(exec_num(body, "projection_lots_added"), 0.0);
    EXPECT_DOUBLE_EQ(exec_num(body, "projection_lots_removed"), 1.0)
        << "投影把卖不出的零股回款当成了现金 —— 基线没被削";
    EXPECT_DOUBLE_EQ(summary_num(body, "affordability_caps"), 0.0)
        << "投影分了收不到的钱，缺口被推给落账口的可负担截断";
    // 装置自证：卖腿确实被参与率截到 0（持仓一股没动），且未成量转了 carry。
    EXPECT_DOUBLE_EQ(exec_num(body, "capped_fills"), 1.0);
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260113", "600000.SH"), 100.0);
    EXPECT_DOUBLE_EQ(position_qty(pos, "20260113", "600519.SH"), -1.0)
        << "回款按 0 计时不该建出新仓";
}

// ③ carry 守恒：每条建出来的腿恰好走一个终局，且终局计数互不吞并。
//
// 恒等式（**建腿点 stale 不计 created**，故它不进这条式子）：
//   carry_created ==
//       carry_completed + carry_superseded + carry_noop_removed
//     + carry_dropped_stale + carry_dropped + carry_outstanding
// 其中 `carry_dropped_stale` 有两个来源：
//   (a) fire 收口（`drop_stale_carry_for_sid`）—— 丢的是**已建**的腿，计 created；
//   (b) carry 建腿点（同槽后发 fire 已重定目标）—— 腿没建，**不**计 created。
// 本钉的三个场景都不含 (b)（每日至多一次 fire），故上式逐字成立；(b) 形态的守恒
// 由 `StaleCarryFromEarlierSameDayFireIsNotCreated` 单独钉住。
//
// 三个场景合起来让七个计数各自至少非零一次 —— 任一计数被抹掉（或被别的终局吞并）
// 都会让其中一个场景的恒等式破裂。
TEST_F(SelArtifactsFixture, CarryLifecycleCountersAreConserved) {
    // 每个场景跑完立刻核恒等式并回传计数，供末尾的覆盖度断言汇总。
    const auto check = [this](const std::string& sub) {
        const auto body = read_text(dir_ / sub / "summary.json");
        const auto c = read_carry_counters(body);
        EXPECT_DOUBLE_EQ(c.created, c.terminals()) << "scenario=" << sub;
        EXPECT_GT(c.created, 0.0) << "scenario=" << sub << " 装置没建出 carry 腿";
        return c;
    };

    // 场景 A：新 fire 取代 / 缺席丢弃 / 复投补齐三个终局同场。
    //   0112 {A:0.5, B:0.5} → 两腿都被参与率截断（各成 10,000 股，各挂一腿）
    //   0113 {A:0.25}       → A 腿被新目标取代（superseded），B 腿因缺席被丢弃
    //                         （dropped_stale，fire 收口来源）；A 的新腿再挂一条
    //   0114 无 fire        → A 新腿复投 5,000 股补齐 → completed
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    {
        std::string closes = "date,code,close,volume\n";
        for (const char* d : {"20260109", "20260112", "20260113", "20260114"}) {
            closes += std::string(d) + ",600000.SH,10.0,100000\n";
            closes += std::string(d) + ",600519.SH,10.0,100000\n";
        }
        write_file("close_prices.csv", closes);
    }
    {
        MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}, {"600519.SH", 0.5}}},
                                 {20260113u, {{"600000.SH", 0.25}}}});
        ASSERT_EQ(run(make_cfg("sa", 1'000'000.0, kZeroFeeFrictions), &stub), 0);
    }
    const auto a = check("sa");
    EXPECT_DOUBLE_EQ(a.superseded, 1.0);
    EXPECT_DOUBLE_EQ(a.dropped_stale, 1.0);
    EXPECT_DOUBLE_EQ(a.completed, 1.0);

    // 场景 B：到限丢弃 + 收尾残留。`carry_max_days: 1` 下 0114 的日界闸丢掉首腿
    //   （当天无 fire，故不会被 supersede 抢走终局）；0115 的 fire 再挂一条到收尾。
    write_file("trading_dates.csv",
               "date\n20260112\n20260113\n20260114\n20260115\n");
    {
        std::string closes = "date,code,close,volume\n20260109,600000.SH,10.0,1000\n";
        for (const char* d : {"20260112", "20260113", "20260114", "20260115"}) {
            closes += std::string(d) + ",600000.SH,10.0,1000\n";
        }
        write_file("close_prices.csv", closes);
    }
    {
        MapWeightsStrategy stub({{20260112u, {{"600000.SH", 1.0}}},
                                 {20260115u, {{"600000.SH", 1.0}}}});
        ASSERT_EQ(run(make_cfg("sb", 1'000'000.0, kZeroFeeFrictions,
                               /*relative_inputs=*/false,
                               "execution:\n  carry_max_days: 1\n"), &stub), 0);
    }
    const auto b = check("sb");
    EXPECT_DOUBLE_EQ(b.dropped, 1.0);
    EXPECT_DOUBLE_EQ(b.outstanding, 1.0);

    // 场景 C：送转把持仓推过目标 → 复投前重算差额反向 → noop 摘除。
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close,volume\n"
               "20260109,600000.SH,10.0,200000\n"
               "20260112,600000.SH,10.0,200000\n"
               "20260113,600000.SH,5.0,200000\n"
               "20260114,600000.SH,5.0,200000\n");
    write_file("ca.csv",
               "code,record_date,ex_date,pay_date,cash_per_share,"
               "bonus_transfer_per_share,rights_per_share,rights_price,is_delist,"
               "shares_list_date\n"
               "600000.SH,20260112,20260113,,0,1.0,0,0,0,20260114\n");
    {
        MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.3}}}});
        ASSERT_EQ(run(make_cfg("sc", 1'000'000.0, kZeroFeeFrictions,
                               /*relative_inputs=*/false, /*top_extra=*/"",
                               "  corporate_actions_csv: " +
                                   (dir_ / "ca.csv").string() + "\n"), &stub), 0);
    }
    const auto c = check("sc");
    EXPECT_DOUBLE_EQ(c.noop_removed, 1.0);

    // 覆盖度：七个计数各自至少在一个场景里非零 —— 否则恒等式对某个计数是空转，
    // 抹掉它也不会红。
    EXPECT_GT(a.created + b.created + c.created, 0.0);
    EXPECT_GT(a.completed + b.completed + c.completed, 0.0);
    EXPECT_GT(a.superseded + b.superseded + c.superseded, 0.0);
    EXPECT_GT(a.noop_removed + b.noop_removed + c.noop_removed, 0.0);
    EXPECT_GT(a.dropped_stale + b.dropped_stale + c.dropped_stale, 0.0);
    EXPECT_GT(a.dropped + b.dropped + c.dropped, 0.0);
    EXPECT_GT(a.outstanding + b.outstanding + c.outstanding, 0.0);
}

}  // namespace sel_b0_artifacts
