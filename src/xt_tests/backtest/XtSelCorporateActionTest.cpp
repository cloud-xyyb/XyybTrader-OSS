// -----------------------------------------------------------------------------
// XtSelCorporateActionTest — SEL 回测 corporate action（除权除息 / 退市）不变量
// -----------------------------------------------------------------------------
// 覆盖 B0b 冻结设计 + 编排方 R2 设计修正：
//   ① 除权日 portfolio 权益连续（税外无跳变，含 receivable 口径）
//   ② 同日 CA + rebalance 复合序（先 CA 后 sizing：diff 基于调整后 qty/参考价）
//   ③ 退市清算守恒 + 窗内非交易日事件归到前一交易日
//   ④ ex→pay 窗口买入不得动用应收（现金不足场景按可用现金截断）
//   ⑤ 未配置 CA 文件时产物集合与数值回到 B0 基线
//   ⑥ 送转后卖出 realized PnL 归因正确（avg ÷(1+b) 真生效）
//   ⑦ target_weights.reject_code 落真风控拒因（不再是 Unknown）
//   + 窗外退市事件整条忽略（禁止归日 → 防 future leak）
//   + 清算费用超清算所得时压到所得（不产生负现金）
//   + 分红股权数按 record_date 收盘锁定（不读 ex 日仓位）
//   + 三产物（daily_equity / trades.equity_after / summary.final_equity）同日一致
//   + 新股上市日滞后：ex→list 窗口计在途新股市值，list 日转真实持仓；CSV 未给
//     shares_list_date 时缺省取 ex 的下一交易日（B6-3）
//   + 配股全量（B6-1）：subscribe 缴款认购（现金守恒 / 现金不足按可缴整股截断）、
//     forfeit 弃权（除权损失如实进权益）、allow_skip_rights 整行跳过
//   + 换股（B6-2 / F12）：按比例换新 code、成本基准迁移、零股现金替代、旧 code 闩锁
//   + CA CSV 坏行 fail-fast（列数 / 日期序 / 事件型混填 / 配股单边列 / 重复键）
// -----------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include <unistd.h>

#include "api/IStrategyContext.h"
#include "xt_backtest/sel/XtSelBacktestRunner.h"
#include "xt_backtest/sel/XtSelRunnerConfig.h"
#include "xt_strategy_engine/XtStrategyBase.h"

namespace sel_b0b_ca {
namespace {

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

/// @brief events_full.csv 的一行（payload 已解引号）。
struct EventRow {
    int64_t     ts   = 0;
    uint64_t    seq  = 0;
    std::string type;
    std::string payload;
};

/// @brief 读 events_full.csv。
///
/// 不能复用 `read_csv`：payload 内的逗号是 k=v 分隔符，落盘时整段走 CSV 引号转义
/// （`csv_escape_field`），按逗号硬切会把它劈成碎片。payload 恒是最后一列，故前三个
/// 逗号定位列边界、其余原样取回并解引号（`""` → `"`）。
std::vector<EventRow> read_events_full(const std::filesystem::path& p) {
    std::vector<EventRow> rows;
    std::ifstream ifs(p);
    if (!ifs) return rows;
    std::string line;
    if (!std::getline(ifs, line)) return rows;  // header
    while (std::getline(ifs, line)) {
        while (!line.empty() && (line.back() == '\r' || line.back() == '\n')) line.pop_back();
        if (line.empty()) continue;
        const size_t c1 = line.find(',');
        const size_t c2 = c1 == std::string::npos ? c1 : line.find(',', c1 + 1);
        const size_t c3 = c2 == std::string::npos ? c2 : line.find(',', c2 + 1);
        if (c3 == std::string::npos) continue;
        EventRow r;
        r.ts   = std::strtoll(line.substr(0, c1).c_str(), nullptr, 10);
        r.seq  = std::strtoull(line.substr(c1 + 1, c2 - c1 - 1).c_str(), nullptr, 10);
        r.type = line.substr(c2 + 1, c3 - c2 - 1);
        std::string payload = line.substr(c3 + 1);
        if (payload.size() >= 2 && payload.front() == '"' && payload.back() == '"') {
            payload = payload.substr(1, payload.size() - 2);
            std::string un;
            for (size_t i = 0; i < payload.size(); ++i) {
                un.push_back(payload[i]);
                if (payload[i] == '"' && i + 1 < payload.size() && payload[i + 1] == '"') ++i;
            }
            payload.swap(un);
        }
        r.payload = std::move(payload);
        rows.push_back(std::move(r));
    }
    return rows;
}

/// @brief 取 payload 里某个 k= 的值（到下一个逗号止）；缺失返空串。
std::string payload_value(const std::string& payload, const std::string& key) {
    const std::string k = key + '=';
    size_t pos = payload.find(k);
    // 只认字段起点（行首或紧跟逗号），避免 `order_id=` 匹配到 `xxx_order_id=`。
    while (pos != std::string::npos && pos != 0 && payload[pos - 1] != ',') {
        pos = payload.find(k, pos + 1);
    }
    if (pos == std::string::npos) return {};
    const size_t start = pos + k.size();
    const size_t end   = payload.find(',', start);
    return payload.substr(start, end == std::string::npos ? end : end - start);
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

/// @brief 取某日 daily_equity（缺失返 NaN）。
double equity_on(const std::vector<Row>& rows, const std::string& date) {
    for (const auto& r : rows) {
        if (r.at("date") == date) return std::stod(r.at("equity"));
    }
    return std::nan("");
}

/// @brief 取某日某 code 的 daily_positions 行（缺失返 nullptr）。
const Row* position_row(const std::vector<Row>& rows, const std::string& date,
                        const std::string& code) {
    for (const auto& r : rows) {
        if (r.at("date") == date && r.at("code") == code) return &r;
    }
    return nullptr;
}

/// @brief 取某 type 的第一条 CA 事件行（缺失返 nullptr）。
const Row* ca_event(const std::vector<Row>& rows, const std::string& type,
                    const std::string& code = "") {
    for (const auto& r : rows) {
        if (r.at("type") != type) continue;
        if (!code.empty() && r.at("code") != code) continue;
        return &r;
    }
    return nullptr;
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
class SelCaFixture : public ::testing::Test {
protected:
    void SetUp() override {
        char tmpl[] = "/tmp/xt_sel_b0b_XXXXXX";
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

    /// @brief A 股常规摩擦 + CA 相关的两个可调项。
    /// @param enforce_t1 false 时不写 `enforce_long_t_plus_1`（走 config 默认关闭），
    ///                   供 daily_positions 单一 schema 钉在 T+1 关闭下复用同一份摩擦。
    /// @note `fees.dividend_tax_rate` 恒写出（含 0）——顺带守 fees 下标量兄弟节点
    ///       与 per-symbol 子 map 共存时的解析路径。
    static std::string frictions(double dividend_tax = 0.0, double delist_haircut = 0.0,
                                double min_commission = 5.0, bool enforce_t1 = true) {
        std::ostringstream y;
        y << "commodities:\n"
          << "  '600000.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0,"
             " min_lot: 100 }\n"
          << "  '000001.SZ': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0,"
             " min_lot: 100 }\n"
          << "fees:\n"
          << "  '600000.SH': { open: 0.00025, close: 0.00025, stamp_duty_rate: 0.0005,"
             " transfer_fee_rate: 0.00001, min_commission: " << min_commission << " }\n"
          << "  '000001.SZ': { open: 0.00025, close: 0.00025, stamp_duty_rate: 0.0005,"
             " transfer_fee_rate: 0.0, min_commission: " << min_commission << " }\n"
          << "  dividend_tax_rate: " << dividend_tax << "\n"
          << "risk:\n"
          << "  max_order_qty: 1000000.0\n";
        if (enforce_t1) y << "  enforce_long_t_plus_1: true\n";
        y << "  allow_short: false\n"
          << "  delist_haircut_pct: " << delist_haircut << "\n";
        return y.str();
    }

    /// @brief 零费用摩擦：让成本/权益断言落在整数上（费用不掩盖成本迁移的分叉）。
    static std::string zero_fee_frictions() {
        return "commodities:\n"
               "  '600000.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0,"
               " min_lot: 100 }\n"
               "  '000001.SZ': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0,"
               " min_lot: 100 }\n"
               "fees:\n"
               "  '600000.SH': { open: 0.0, close: 0.0, stamp_duty_rate: 0.0,"
               " transfer_fee_rate: 0.0, min_commission: 0.0 }\n"
               "  '000001.SZ': { open: 0.0, close: 0.0, stamp_duty_rate: 0.0,"
               " transfer_fee_rate: 0.0, min_commission: 0.0 }\n"
               "  dividend_tax_rate: 0.0\n"
               "risk:\n"
               "  max_order_qty: 1000000.0\n"
               "  allow_short: false\n";
    }

    /// @param ca_file  非空时写 `selector.corporate_actions_csv`（相对 dir_）。
    /// @param selector_extra 追加到 `selector:` 块的行（如 `  rights_policy: forfeit\n`）。
    /// @param top_extra 追加到文档顶层的块（如 `backtest:\n  trace_mode: full\n`）。
    std::string make_cfg(const std::string& sub, double initial_cash,
                         const std::string& extra, const std::string& ca_file = "",
                         bool allow_skip_rights = false,
                         const std::string& selector_extra = "",
                         const std::string& top_extra = "") const {
        const std::string cfg_path = (dir_ / (sub + "_cfg.yaml")).string();
        std::ostringstream y;
        y << "selector:\n"
          << "  trading_dates_csv: " << (dir_ / "trading_dates.csv").string() << "\n"
          << "  close_prices_csv: " << (dir_ / "close_prices.csv").string() << "\n";
        if (!ca_file.empty()) {
            y << "  corporate_actions_csv: " << (dir_ / ca_file).string() << "\n";
        }
        if (allow_skip_rights) y << "  allow_skip_rights: true\n";
        y << selector_extra;
        y << "  task:\n"
          << "    strategy_id: sel_ca\n"
          << "    period: daily\n"
          << "    time: 1500\n"
          << "    strict_time: true\n"
          << "  strategy:\n"
          << "    id: sel_ca\n"
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

    int run(const std::string& cfg_path, MapWeightsStrategy* stub) const {
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

/// @brief CA CSV header（三日期 schema；可选第 10 列 shares_list_date 由 caller 决定）。
constexpr const char* kCaHeader =
    "code,record_date,ex_date,pay_date,cash_per_share,bonus_transfer_per_share,"
    "rights_per_share,rights_price,is_delist\n";
constexpr const char* kCaHeader10 =
    "code,record_date,ex_date,pay_date,cash_per_share,bonus_transfer_per_share,"
    "rights_per_share,rights_price,is_delist,shares_list_date\n";
/// @brief 全 13 列 header（含 F12 换股三列）。
constexpr const char* kCaHeaderFull =
    "code,record_date,ex_date,pay_date,cash_per_share,bonus_transfer_per_share,"
    "rights_per_share,rights_price,is_delist,shares_list_date,"
    "conversion_to,conversion_ratio,cash_in_lieu\n";

}  // namespace

// ── ① 除权日权益连续：ex→pay 窗口靠应收补齐，税外无跳变 ─────────────────────
//
// 20260112 买 50000 股 @10（费 130）→ cash 499,870 / equity 999,870。
// 20260113 除息 0.5/股（record 20260112、pay 20260114）：参考价 10→9.5，当日真实
// close 也是 9.5 → 持仓市值 475,000 + 应收 25,000 + 现金 499,870 = 999,870。
// 旧行为（权益不含应收）当日会凹 25,000 再在 pay 日跳回 —— 假回撤。
TEST_F(SelCaFixture, ExDayEquityContinuousAcrossReceivableWindow) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,9.5\n"
               "20260114,600000.SH,9.5\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,20260114,0.5,0,0,0,0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto eq = read_csv(dir_ / "rep" / "daily_equity.csv");
    ASSERT_EQ(eq.size(), 3u);
    EXPECT_NEAR(equity_on(eq, "20260112"), 999'870.0, 0.01);
    EXPECT_NEAR(equity_on(eq, "20260113"), 999'870.0, 0.01)
        << "除权日权益不连续：应收未计入 portfolio 口径";
    EXPECT_NEAR(equity_on(eq, "20260114"), 999'870.0, 0.01)
        << "pay 日权益跳变：应收转现金时口径不一致";

    // 应收在 ex 日出现、pay 日消失（RECEIVABLE 行仅非零时写）。
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    const Row* recv13 = position_row(pos, "20260113", "RECEIVABLE");
    ASSERT_NE(recv13, nullptr) << "ex 日缺 RECEIVABLE 行";
    EXPECT_NEAR(std::stod(recv13->at("market_value")), 25'000.0, 0.01);
    EXPECT_EQ(position_row(pos, "20260114", "RECEIVABLE"), nullptr)
        << "pay 日应收未清";
    const Row* cash14 = position_row(pos, "20260114", "CASH");
    ASSERT_NE(cash14, nullptr);
    EXPECT_NEAR(std::stod(cash14->at("market_value")), 524'870.0, 0.01);

    // events：ex 日 receivable_created + pay 日 pay_cash，各自解释一次跳变。
    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    const Row* created = ca_event(evs, "receivable_created");
    ASSERT_NE(created, nullptr);
    EXPECT_EQ(created->at("ex_date"), "20260113");
    EXPECT_NEAR(std::stod(created->at("cash_delta")), 25'000.0, 0.01);
    EXPECT_NEAR(std::stod(created->at("ref_price_before")), 10.0, 0.0001);
    EXPECT_NEAR(std::stod(created->at("ref_price_after")), 9.5, 0.0001);
    const Row* paid = ca_event(evs, "pay_cash");
    ASSERT_NE(paid, nullptr);
    EXPECT_EQ(paid->at("ex_date"), "20260114");
    EXPECT_NEAR(std::stod(paid->at("cash_delta")), 25'000.0, 0.01);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_NEAR(summary_num(body, "cash_dividends_total"), 25'000.0, 0.01);
    EXPECT_EQ(summary_num(body, "applied"), 1.0);
    EXPECT_EQ(summary_num(body, "equity_identity_violations"), 0.0);
    EXPECT_NEAR(summary_num(body, "receivable_outstanding_cash"), 0.0, 0.01);
}

// 分红税：唯一的权益跌幅来源就是税额（其余仍连续）。
TEST_F(SelCaFixture, DividendTaxIsTheOnlyEquityDrop) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,9.5\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,20260115,0.5,0,0,0,0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(/*dividend_tax=*/0.2),
                              "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto eq = read_csv(dir_ / "rep" / "daily_equity.csv");
    // 税后应收 20,000（毛 25,000 × 0.8）→ 权益比昨日低 5,000，恰为税额。
    EXPECT_NEAR(equity_on(eq, "20260113"), 999'870.0 - 5'000.0, 0.01);
    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_NEAR(summary_num(body, "cash_dividends_total"), 20'000.0, 0.01);
    // pay_date 在窗外 → 收尾仍挂应收（权益已含，summary 披露）。
    EXPECT_NEAR(summary_num(body, "receivable_outstanding_cash"), 20'000.0, 0.01);
}

// ── ② 同日 CA + rebalance：先 CA 后 sizing ────────────────────────────────────
//
// 20260113 十送十：敞口 50000→100000（新股在途，到账日 = ex+1 = 20260114）、参考价
// 10→5。当日 weight=1.0 的 target 必须按 (调整后权益 / 调整后参考价) = 199,900 股算，
// pre_qty 也必须是 100,000 —— 权益分子含在途股市值，pre_qty 就必须含其股数。
// 若 CA 落在 sizing 之后（或不施加），target 会算成 99,900、pre_qty 50,000；
// 若 pre_qty 漏了在途股，diff 变 149,900 → 现金不够、affordability_caps 非 0（超买）。
// 窗末必须留出 ex+1（20260114），否则新股走的是「窗外到账」分支而非在途路径。
TEST_F(SelCaFixture, SameDayCorporateActionPrecedesSizing) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,5.0\n"
               "20260114,600000.SH,5.0\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,,0,1.0,0,0,0\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.5}}},
        {20260113u, {{"600000.SH", 1.0}}},
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto targets = read_csv(dir_ / "rep" / "target_weights.csv");
    const Row* d2 = nullptr;
    for (const auto& r : targets) {
        if (r.at("exec_date") == "20260113") { d2 = &r; break; }
    }
    ASSERT_NE(d2, nullptr);
    EXPECT_EQ(d2->at("pre_qty"), "100000.0000")
        << "sizing 读到的是未送转的持仓（CA 施加点晚于 sizing）";
    EXPECT_EQ(d2->at("target_qty"), "199900.0000")
        << "sizing 用的是未除权的参考价";
    EXPECT_EQ(d2->at("diff_qty"), "99900.0000");
    EXPECT_EQ(d2->at("filled_qty"), "99900.0000");

    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    const Row* bonus = ca_event(evs, "bonus_transfer");
    ASSERT_NE(bonus, nullptr);
    // ex 日新股走在途账 → 该行的 qty 前后都是真实持仓（未变）。
    EXPECT_EQ(bonus->at("qty_before"), "50000.0000");
    EXPECT_EQ(bonus->at("qty_after"), "50000.0000");
    EXPECT_NEAR(std::stod(bonus->at("ref_price_before")), 10.0, 0.0001);
    EXPECT_NEAR(std::stod(bonus->at("ref_price_after")), 5.0, 0.0001);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "affordability_caps"), 0.0)
        << "sizing 的 pre_qty 漏了在途新股 → diff 超买，现金被打穿后截断";
    EXPECT_EQ(summary_num(body, "equity_identity_violations"), 0.0);
}

// ── ⑥ 送转后卖出 realized PnL 归因正确 ───────────────────────────────────────
//
// 十送十后 avg 必须 10→5；20260114 以 6.0 清仓 100000 股 → realized = +100,000。
// 若 avg 未调整 → −400,000；若 avg 被清零 → +600,000。三态互斥，钉可判。
TEST_F(SelCaFixture, BonusTransferPreservesRealizedPnlAttribution) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,5.0\n"
               "20260114,600000.SH,6.0\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,,0,1.0,0,0,0\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.5}}},
        {20260114u, {{"600000.SH", 0.0}}},
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto trades = read_csv(dir_ / "rep" / "trades.csv");
    ASSERT_EQ(trades.size(), 2u);
    EXPECT_EQ(trades[1].at("direction"), "SELL");
    EXPECT_EQ(trades[1].at("qty"), "100000.0000");

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_NEAR(summary_num(body, "realized_pnl"), 100'000.0, 0.01)
        << "送转后均价未按 (1+b) 调低 → 平仓 PnL 归因错";
    // 现金守恒：499,870 + 600,000 − 456 = 1,099,414（fee = 150 + 300 + 6）。
    EXPECT_NEAR(std::stod(trades[1].at("fee")), 456.0, 0.01);
    EXPECT_NEAR(summary_num(body, "final_equity"), 1'099'414.0, 0.01);
}

// ── ③ 退市清算守恒 + 窗内非交易日归日 ────────────────────────────────────────
//
// ex_date 20260114 不是交易日（窗内）→ 归到 20260113。清算价 = 最后可得 close 12.0，
// 费用按正常卖出（佣金 150 + 印花税 300 + 过户费 6 = 456）。
TEST_F(SelCaFixture, DelistLiquidationConservesCashAndRebasesToTradingDate) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260116\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,12.0\n"
               "20260116,600000.SH,12.0\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,,20260114,,0,0,0,0,1\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto trades = read_csv(dir_ / "rep" / "trades.csv");
    ASSERT_EQ(trades.size(), 2u) << "退市清算未合成 SELL 进 trades.csv";
    EXPECT_EQ(trades[1].at("direction"), "SELL");
    EXPECT_EQ(trades[1].at("date"), "20260113") << "非交易日事件未归到前一交易日";
    EXPECT_EQ(trades[1].at("qty"), "50000.0000");
    EXPECT_NEAR(std::stod(trades[1].at("exec_price")), 12.0, 0.0001);
    EXPECT_NEAR(std::stod(trades[1].at("fee")), 456.0, 0.01);

    // 清算后持仓行消失，现金 = 499,870 + 600,000 − 456。
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_EQ(position_row(pos, "20260113", "600000.SH"), nullptr)
        << "清算后仍留持仓行（stale carry 未根治）";
    const Row* cash = position_row(pos, "20260113", "CASH");
    ASSERT_NE(cash, nullptr);
    EXPECT_NEAR(std::stod(cash->at("market_value")), 1'099'414.0, 0.01);
    const auto eq = read_csv(dir_ / "rep" / "daily_equity.csv");
    EXPECT_NEAR(equity_on(eq, "20260113"), 1'099'414.0, 0.01);
    EXPECT_NEAR(equity_on(eq, "20260116"), 1'099'414.0, 0.01);

    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    const Row* liq = ca_event(evs, "delist_liquidation");
    ASSERT_NE(liq, nullptr);
    EXPECT_EQ(liq->at("ex_date"), "20260113");
    EXPECT_EQ(liq->at("qty_before"), "50000.0000");
    EXPECT_EQ(liq->at("qty_after"), "0.0000");
    EXPECT_NEAR(std::stod(liq->at("cash_delta")), 600'000.0 - 456.0, 0.01);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "delist_liquidations"), 1.0);
    EXPECT_EQ(summary_num(body, "rebased_to_trading_date"), 1.0);
    EXPECT_EQ(summary_num(body, "out_of_window_ignored"), 0.0);
    EXPECT_EQ(summary_num(body, "delist_fee_capped"), 0.0);
    EXPECT_EQ(summary_num(body, "equity_identity_violations"), 0.0);
}

// 窗外退市事件整条忽略：归日到窗内会把未来事实搬进回测（future leak）。
TEST_F(SelCaFixture, DelistEventAfterWindowIsIgnored) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,,20260120,,0,0,0,0,1\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    EXPECT_EQ(read_csv(dir_ / "rep" / "trades.csv").size(), 1u)
        << "窗外退市被归日到窗内执行（future leak）";
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    const Row* held = position_row(pos, "20260113", "600000.SH");
    ASSERT_NE(held, nullptr);
    EXPECT_EQ(held->at("qty"), "50000.0000");

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "delist_liquidations"), 0.0);
    EXPECT_EQ(summary_num(body, "out_of_window_ignored"), 1.0);
    // 事件被忽略 → events 文件仍产出（只是 0 行），gated 面不变。
    EXPECT_TRUE(std::filesystem::exists(dir_ / "rep" / "corporate_action_events.csv"));
    EXPECT_EQ(read_csv(dir_ / "rep" / "corporate_action_events.csv").size(), 0u);
}

// 清算所得不得资助当日早先的决策：清算在当日全部提交与 drain 之后执行。
//
// 20260113 组合里 000001.SZ 的目标要 ~298,000 现金，而清算前只有 ~100,760 →
// 该腿必须被挡（本地未受理）。若清算提前到开盘前，现金会有 ~998,000，该腿就
// 成交了 —— 未来资金 leak。
TEST_F(SelCaFixture, DelistProceedsDoNotFundSameDayDecisions) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260109,000001.SZ,20.0\n"
               "20260112,600000.SH,10.0\n20260112,000001.SZ,20.0\n"
               "20260113,600000.SH,10.0\n20260113,000001.SZ,20.0\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,,20260113,,0,0,0,0,1\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.9}}},
        {20260113u, {{"600000.SH", 0.9}, {"000001.SZ", 0.3}}},
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto targets = read_csv(dir_ / "rep" / "target_weights.csv");
    const Row* buy_leg = nullptr;
    for (const auto& r : targets) {
        if (r.at("exec_date") == "20260113" && r.at("code") == "000001.SZ") {
            buy_leg = &r;
            break;
        }
    }
    ASSERT_NE(buy_leg, nullptr);
    EXPECT_EQ(buy_leg->at("target_qty"), "14900.0000");
    EXPECT_EQ(buy_leg->at("submitted_qty"), "14900.0000");
    // 清算前现金 100,760.49 → 20.0 成交价下只买得起 5,000 股（50 手）。
    EXPECT_EQ(buy_leg->at("filled_qty"), "5000.0000")
        << "清算所得提前入账，当日买入用上了未来资金";

    // 成交序：D1 BUY 90,000 → D2 rebalance SELL 100 → D2 BUY 5,000 →
    // D2 EOD 清算 SELL 89,900（清算恒在当日最后）。
    const auto trades = read_csv(dir_ / "rep" / "trades.csv");
    ASSERT_EQ(trades.size(), 4u);
    EXPECT_EQ(trades[3].at("direction"), "SELL");
    EXPECT_EQ(trades[3].at("code"), "600000.SH");
    EXPECT_EQ(trades[3].at("qty"), "89900.0000");

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "delist_liquidations"), 1.0);
    EXPECT_EQ(summary_num(body, "affordability_caps"), 1.0);
    EXPECT_EQ(summary_num(body, "negative_cash_events"), 0.0);
}

// 清算费用超清算所得 → 压到所得（强制清算不产生负现金）。
// haircut 0.99 → 清算价 0.1；100 股清算所得 10 元，而最低佣金 50 元。
TEST_F(SelCaFixture, DelistFeeCappedAtProceeds) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,,20260113,,0,0,0,0,1\n");
    // weight 0.001 → 1,000/10 = 100 股（恰一手）。
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.001}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0,
                              frictions(/*dividend_tax=*/0.0,
                                        /*delist_haircut=*/0.99,
                                        /*min_commission=*/50.0),
                              "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto trades = read_csv(dir_ / "rep" / "trades.csv");
    ASSERT_EQ(trades.size(), 2u);
    EXPECT_NEAR(std::stod(trades[1].at("exec_price")), 0.1, 0.0001);
    EXPECT_NEAR(std::stod(trades[1].at("fee")), 10.0, 0.01)
        << "清算费用未压到清算所得";

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "delist_fee_capped"), 1.0);
    EXPECT_EQ(summary_num(body, "negative_cash_events"), 0.0);
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    const Row* cash = position_row(pos, "20260113", "CASH");
    ASSERT_NE(cash, nullptr);
    EXPECT_GE(std::stod(cash->at("market_value")), 0.0) << "强制清算把现金扣成负数";

    // fill_costs 不得与 trades/账本分叉：cap 后 total_fee 取落账真值，分项等比缩。
    const auto costs = read_csv(dir_ / "rep" / "fill_costs.csv");
    ASSERT_EQ(costs.size(), 2u);
    const Row& liq_cost = costs[1];
    EXPECT_EQ(liq_cost.at("direction"), "SELL");
    EXPECT_NEAR(std::stod(liq_cost.at("total_fee")), std::stod(trades[1].at("fee")), 0.01)
        << "fill_costs.total_fee 按公式重算（未取 capped 落账值）→ 与 trades 分叉";
    const double parts = std::stod(liq_cost.at("commission")) +
                         std::stod(liq_cost.at("stamp_duty")) +
                         std::stod(liq_cost.at("transfer_fee"));
    EXPECT_NEAR(parts, std::stod(liq_cost.at("total_fee")), 0.01)
        << "分项未随 cap 等比缩（分项和 ≫ total）";
}

// ── ④ ex→pay 窗口买入不得动用应收 ────────────────────────────────────────────
//
// 20260113 除息 1.0/股（pay 20260116）→ 应收 50,000、现金仍 499,870。当日 weight
// 0.9 的加仓按调整后权益算出 target 99,900（提交 49,900 股，报价 9.0 通过风控），
// 但按 T 日真实 close 11.0 成交需 548,900 → 只能落账 45,400 股（可负担现金口径）。
// 若应收被当成可用资金（例如 ex 日直接入现金），落账会是 49,900 股。
TEST_F(SelCaFixture, ReceivableNotSpendableInExToPayWindow) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n20260116\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,11.0\n"
               "20260114,600000.SH,11.0\n"
               "20260116,600000.SH,11.0\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,20260116,1.0,0,0,0,0\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.5}}},
        {20260113u, {{"600000.SH", 0.9}}},
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto targets = read_csv(dir_ / "rep" / "target_weights.csv");
    const Row* d2 = nullptr;
    for (const auto& r : targets) {
        if (r.at("exec_date") == "20260113") { d2 = &r; break; }
    }
    ASSERT_NE(d2, nullptr);
    EXPECT_EQ(d2->at("target_qty"), "99900.0000");
    EXPECT_EQ(d2->at("submitted_qty"), "49900.0000");
    EXPECT_EQ(d2->at("filled_qty"), "45400.0000")
        << "买入用了应收资金（可负担截断口径不是 account 可用现金）";

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "affordability_caps"), 1.0);
    EXPECT_EQ(summary_num(body, "negative_cash_events"), 0.0);
    EXPECT_EQ(summary_num(body, "equity_identity_violations"), 0.0);
    EXPECT_NEAR(summary_num(body, "receivable_outstanding_cash"), 0.0, 0.01);

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    for (const char* d : {"20260113", "20260114"}) {
        const Row* recv = position_row(pos, d, "RECEIVABLE");
        ASSERT_NE(recv, nullptr) << d;
        EXPECT_NEAR(std::stod(recv->at("market_value")), 50'000.0, 0.01) << d;
    }
    EXPECT_EQ(position_row(pos, "20260116", "RECEIVABLE"), nullptr);
    const Row* cash16 = position_row(pos, "20260116", "CASH");
    ASSERT_NE(cash16, nullptr);
    EXPECT_NEAR(std::stod(cash16->at("market_value")), 50'340.16, 0.01);

    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    const Row* created = ca_event(evs, "receivable_created");
    ASSERT_NE(created, nullptr);
    EXPECT_NEAR(std::stod(created->at("cash_delta")), 50'000.0, 0.01);
    const Row* paid = ca_event(evs, "pay_cash");
    ASSERT_NE(paid, nullptr);
    EXPECT_EQ(paid->at("ex_date"), "20260116");
}

// 三产物同日一致：daily_equity / trades.equity_after / summary.final_equity 同口径。
TEST_F(SelCaFixture, PortfolioEquityConsistentAcrossArtifacts) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n20260116\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,11.0\n"
               "20260114,600000.SH,11.0\n"
               "20260116,600000.SH,11.0\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,20260116,1.0,0,0,0,0\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.5}}},
        {20260113u, {{"600000.SH", 0.9}}},
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto eq     = read_csv(dir_ / "rep" / "daily_equity.csv");
    const auto trades = read_csv(dir_ / "rep" / "trades.csv");
    const auto pos    = read_csv(dir_ / "rep" / "daily_positions.csv");
    const auto body   = read_text(dir_ / "rep" / "summary.json");
    ASSERT_EQ(trades.size(), 2u);

    // 20260113 那笔是当日最后一笔成交 → equity_after 必等于当日收盘权益。
    const double eq_13 = equity_on(eq, "20260113");
    EXPECT_NEAR(std::stod(trades[1].at("equity_after")), eq_13, 0.02)
        << "trades.equity_after 与 daily_equity 口径分裂（应收未同步）";
    // 末日权益 == summary.final_equity。
    EXPECT_NEAR(equity_on(eq, "20260116"), summary_num(body, "final_equity"), 0.02);
    // E3 恒等（CSV 2dp 口径）：Σ(持仓行) + CASH + RECEIVABLE == 当日权益。
    double sum13 = 0.0;
    for (const auto& r : pos) {
        if (r.at("date") == "20260113") sum13 += std::stod(r.at("market_value"));
    }
    EXPECT_NEAR(sum13, eq_13, 0.02);
}

// 分红股权数按 record_date 收盘锁定：20260113 加仓到 79,900 后，20260114 的除息
// 仍按 record 日（20260112）收盘的 50,000 股计。读 ex 日仓位会算成 79,900。
TEST_F(SelCaFixture, DividendQtyLockedAtRecordDate) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n"
               "20260114,600000.SH,9.0\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260114,,1.0,0,0,0,0\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.5}}},
        {20260113u, {{"600000.SH", 0.8}}},
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    // 前置：ex 日开盘仓位确实 ≠ record 日仓位（钉不空转）。
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    const Row* held13 = position_row(pos, "20260113", "600000.SH");
    ASSERT_NE(held13, nullptr);
    EXPECT_EQ(held13->at("qty"), "79900.0000");

    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    const Row* div = ca_event(evs, "cash_div");
    ASSERT_NE(div, nullptr) << "pay_date 缺省应即时入现金（cash_div 行）";
    EXPECT_EQ(div->at("qty_before"), "50000.0000")
        << "分红股权数读了 ex 日仓位而非 record 日收盘仓位";
    EXPECT_NEAR(std::stod(div->at("cash_delta")), 50'000.0, 0.01);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "record_qty_fallbacks"), 0.0);
    EXPECT_NEAR(summary_num(body, "receivable_outstanding_cash"), 0.0, 0.01);
}

// 红股上市日滞后：ex→list 窗口 qty 不动、在途红股按修正后 ref 计市值，list 日
// （非交易日则顺延到下一交易日）转真实持仓，全程权益连续。
TEST_F(SelCaFixture, BonusSharesListedLaterStayInReceivable) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n20260116\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,5.0\n"
               "20260114,600000.SH,5.0\n"
               "20260116,600000.SH,5.0\n");
    write_file("ca.csv", std::string(kCaHeader10) +
               "600000.SH,20260112,20260113,,0,1.0,0,0,0,20260115\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    // ex→list 窗口：持仓 qty 不变，在途红股 50,000 股 × 5.0 = 250,000 计应收。
    const Row* held13 = position_row(pos, "20260113", "600000.SH");
    ASSERT_NE(held13, nullptr);
    EXPECT_EQ(held13->at("qty"), "50000.0000");
    const Row* recv13 = position_row(pos, "20260113", "RECEIVABLE");
    ASSERT_NE(recv13, nullptr) << "在途红股未计入应收 → 权益先凹后跳";
    EXPECT_NEAR(std::stod(recv13->at("market_value")), 250'000.0, 0.01);
    // list_date 20260115 非交易日 → 顺延到 20260116 到账。
    const Row* held16 = position_row(pos, "20260116", "600000.SH");
    ASSERT_NE(held16, nullptr);
    EXPECT_EQ(held16->at("qty"), "100000.0000");
    EXPECT_EQ(position_row(pos, "20260116", "RECEIVABLE"), nullptr);

    const auto eq = read_csv(dir_ / "rep" / "daily_equity.csv");
    for (const char* d : {"20260112", "20260113", "20260114", "20260116"}) {
        EXPECT_NEAR(equity_on(eq, d), 999'870.0, 0.01) << d;
    }
    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    const Row* listed = ca_event(evs, "shares_listed");
    ASSERT_NE(listed, nullptr);
    EXPECT_EQ(listed->at("ex_date"), "20260116");
    EXPECT_EQ(listed->at("qty_before"), "50000.0000");
    EXPECT_EQ(listed->at("qty_after"), "100000.0000");

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_NEAR(summary_num(body, "receivable_outstanding_bonus"), 0.0, 0.01);
    EXPECT_EQ(summary_num(body, "equity_identity_violations"), 0.0);
}

// ── ⑤ 未配置 CA：产物集合与数值回到 B0 基线 ─────────────────────────────────
TEST_F(SelCaFixture, NoCorporateActionConfigKeepsBaselineOutputs) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,9.5\n"
               "20260114,600000.SH,9.5\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions());  // 无 ca.csv
    ASSERT_EQ(run(cfg, &stub), 0);

    // CA 产物与 summary 块严格 gated。
    EXPECT_FALSE(std::filesystem::exists(dir_ / "rep" / "corporate_action_events.csv"));
    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(body.find("corporate_actions"), std::string::npos)
        << "未配置 CA 仍写出 corporate_actions 块";
    // RECEIVABLE 行不出现（daily_positions 与 B0 逐位一致）。
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    for (const auto& r : pos) EXPECT_NE(r.at("code"), "RECEIVABLE");

    // 权益仍是纯 account 口径：50,000 股 × 9.5 + 499,870。
    const auto eq = read_csv(dir_ / "rep" / "daily_equity.csv");
    EXPECT_NEAR(equity_on(eq, "20260112"), 999'870.0, 0.01);
    EXPECT_NEAR(equity_on(eq, "20260113"), 974'870.0, 0.01);
    EXPECT_NEAR(equity_on(eq, "20260114"), 974'870.0, 0.01);
    EXPECT_NEAR(summary_num(body, "final_equity"), 974'870.0, 0.01);
    const auto trades = read_csv(dir_ / "rep" / "trades.csv");
    ASSERT_EQ(trades.size(), 1u);
    EXPECT_NEAR(std::stod(trades[0].at("equity_after")), 999'870.0, 0.02);
    EXPECT_EQ(summary_num(body, "equity_identity_violations"), 0.0);
    // data_source 写 config 原串（此处是绝对路径写法 → 原样）。
    EXPECT_NE(body.find((dir_ / "trading_dates.csv").string()), std::string::npos);
}

// ── ⑦ reject_code 落真风控拒因 ──────────────────────────────────────────────
//
// max_order_qty 收紧 → 风控在 submit 内拒。gateway 的 SubmitResult 只给 Unknown，
// 真拒因只经 IRejectSink 出现一次；产物必须落到具体拒因。
TEST_F(SelCaFixture, RejectCodeCarriesRealRiskReason) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const std::string extra =
        "commodities:\n"
        "  '600000.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0,"
        " min_lot: 100 }\n"
        "risk:\n"
        "  max_order_qty: 1000.0\n"
        "  allow_short: false\n";
    const auto cfg = make_cfg("rep", 1'000'000.0, extra);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto rows = read_csv(dir_ / "rep" / "target_weights.csv");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].at("accepted"), "0");
    EXPECT_EQ(rows[0].at("reject_code"), "MaxOrderQty")
        << "reject_code 落的是 SubmitResult 的 Unknown，不是真风控拒因";
}

// 对照钉：正常受理时 reject_code 保持 NoError（sink 不污染成功路径）。
TEST_F(SelCaFixture, AcceptedSubmitKeepsNoErrorRejectCode) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions());
    ASSERT_EQ(run(cfg, &stub), 0);
    const auto rows = read_csv(dir_ / "rep" / "target_weights.csv");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].at("accepted"), "1");
    EXPECT_EQ(rows[0].at("reject_code"), "NoError");
}

// ── 配股：subscribe 缴款认购 / forfeit 弃权 / allow_skip_rights 整行跳过 ──────
//
// 共用场景：20260112 买 50,000 股 @10（费 130）→ cash 499,870 / equity 999,870。
// 20260113 十配十（rights_per_share=1.0）配股价 5.0，record 20260112。
// 参考价全式 (10 − 0 + 1.0×5.0) / (1 + 0 + 1.0) = 7.5；T 日 close 亦 7.5。
constexpr const char* kRightsCloses =
    "date,code,close\n"
    "20260109,600000.SH,10.0\n"
    "20260112,600000.SH,10.0\n"
    "20260113,600000.SH,7.5\n"
    "20260114,600000.SH,7.5\n";

// ① 认购现金守恒：缴款额 = 配股数 × 配股价，逐分从现金划走；除权日 portfolio 权益
//    （持仓 + 在途新股 + 现金）与除权前逐分相等——认购是等值交换，不产生盈亏。
//    新股按缺省到账日（ex 的下一交易日）落 20260114，ex 日只在应收侧计市值。
TEST_F(SelCaFixture, RightsSubscriptionConservesCashAndEquity) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv", kRightsCloses);
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,,0,0,1.0,5.0,0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    // 缴款：50,000 × 1.0 = 50,000 股 × 5.0 = 250,000 → cash 499,870 → 249,870。
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    const Row* cash_ex = position_row(pos, "20260113", "CASH");
    ASSERT_NE(cash_ex, nullptr);
    EXPECT_NEAR(std::stod(cash_ex->at("market_value")), 249'870.0, 0.01)
        << "认购缴款未按 配股数×配股价 从现金划走";

    // 权益连续：375,000（持仓 50,000×7.5）+ 375,000（在途 50,000×7.5）+ 249,870。
    const auto eq = read_csv(dir_ / "rep" / "daily_equity.csv");
    EXPECT_NEAR(equity_on(eq, "20260112"), 999'870.0, 0.01);
    EXPECT_NEAR(equity_on(eq, "20260113"), 999'870.0, 0.01)
        << "认购在除权日制造了盈亏（现金腿与新股腿未配平）";
    EXPECT_NEAR(equity_on(eq, "20260114"), 999'870.0, 0.01);

    // ex 日持仓不动（新股在途），到账日转真实持仓。
    const Row* ex_day = position_row(pos, "20260113", "600000.SH");
    ASSERT_NE(ex_day, nullptr);
    EXPECT_EQ(ex_day->at("qty"), "50000.0000");
    ASSERT_NE(position_row(pos, "20260113", "RECEIVABLE"), nullptr);
    const Row* list_day = position_row(pos, "20260114", "600000.SH");
    ASSERT_NE(list_day, nullptr);
    EXPECT_EQ(list_day->at("qty"), "100000.0000");

    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    const Row* sub = ca_event(evs, "rights_subscribed");
    ASSERT_NE(sub, nullptr);
    EXPECT_NEAR(std::stod(sub->at("cash_delta")), -250'000.0, 0.01);
    EXPECT_NEAR(std::stod(sub->at("ref_price_after")), 7.5, 1e-6)
        << "参考价未用全式 (prev − d + r·p_r)/(1 + b + r)";
    ASSERT_NE(ca_event(evs, "shares_listed"), nullptr);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_NE(body.find("\"rights_policy\": \"subscribe\""), std::string::npos);
    EXPECT_EQ(summary_num(body, "rights_subscribed"), 1.0);
    EXPECT_EQ(summary_num(body, "rights_forfeited"), 0.0);
    EXPECT_EQ(summary_num(body, "rights_cash_capped"), 0.0);
    EXPECT_NEAR(summary_num(body, "rights_cash_paid"), 250'000.0, 0.01);
}

// ② 弃权：不缴款、不获配，但参考价照全式摊薄 → 除权损失如实进权益。
//    与 ① 同一份 CA 输入，差异只在 rights_policy，损失 = 50,000 × (10.0 − 7.5)。
//    **ex 日刻意不给 T 日 close**：否则当日估值走的是行情文件里的价，权益断言就跟
//    除权公式脱钩（丢 `+r·p_r` 或丢分母 `+r` 都照样绿）。没有 T 日 close 时估值只能
//    用 CA 修正后的参考价，equity 才真正由全式定。
TEST_F(SelCaFixture, RightsForfeitBooksExRightsLoss) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n");   // 20260113 无行情 → 估值取除权参考价
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,,0,0,1.0,5.0,0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv",
                              /*allow_skip_rights=*/false,
                              /*selector_extra=*/"  rights_policy: forfeit\n");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    const Row* cash_ex = position_row(pos, "20260113", "CASH");
    ASSERT_NE(cash_ex, nullptr);
    EXPECT_NEAR(std::stod(cash_ex->at("market_value")), 499'870.0, 0.01)
        << "弃权仍扣了缴款";
    // 持仓不增（未获配），参考价仍摊薄到 7.5 → 权益少 125,000。
    const Row* ex_day = position_row(pos, "20260113", "600000.SH");
    ASSERT_NE(ex_day, nullptr);
    EXPECT_EQ(ex_day->at("qty"), "50000.0000");
    EXPECT_EQ(position_row(pos, "20260113", "RECEIVABLE"), nullptr)
        << "弃权不该产生在途新股";

    const auto eq = read_csv(dir_ / "rep" / "daily_equity.csv");
    EXPECT_NEAR(equity_on(eq, "20260113"), 874'870.0, 0.01)
        << "弃权的除权损失没进权益（参考价没按全式摊薄）";

    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    const Row* forfeit = ca_event(evs, "rights_forfeited");
    ASSERT_NE(forfeit, nullptr);
    EXPECT_NEAR(std::stod(forfeit->at("cash_delta")), 0.0, 0.01);
    EXPECT_NEAR(std::stod(forfeit->at("ref_price_before")), 10.0, 1e-6);
    EXPECT_NEAR(std::stod(forfeit->at("ref_price_after")), 7.5, 1e-6)
        << "弃权路径没用全式 (prev − d + r·p_r)/(1 + b + r)";

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_NE(body.find("\"rights_policy\": \"forfeit\""), std::string::npos);
    EXPECT_EQ(summary_num(body, "rights_forfeited"), 1.0);
    EXPECT_EQ(summary_num(body, "rights_subscribed"), 0.0);
    EXPECT_NEAR(summary_num(body, "rights_cash_paid"), 0.0, 0.01);
}

// ③ 现金不足：按可缴整股截断（缴不起的部分自然弃权），账户不透支。
//    0.95 权重几乎打光现金 → 应缴 475,000 远超余额。
TEST_F(SelCaFixture, RightsSubscriptionCappedByAvailableCash) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv", kRightsCloses);
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,,0,0,1.0,5.0,0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.95}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    const Row* cash_before = position_row(pos, "20260112", "CASH");
    ASSERT_NE(cash_before, nullptr);
    const double avail = std::stod(cash_before->at("market_value"));
    const double paid_shares = std::floor(avail / 5.0);   // 可缴整股

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "rights_cash_capped"), 1.0)
        << "现金不足未按可缴整股截断（要么透支、要么整笔丢弃）";
    EXPECT_EQ(summary_num(body, "rights_subscribed"), 1.0);
    EXPECT_NEAR(summary_num(body, "rights_cash_paid"), paid_shares * 5.0, 0.01);

    const Row* cash_after = position_row(pos, "20260113", "CASH");
    ASSERT_NE(cash_after, nullptr);
    EXPECT_GE(std::stod(cash_after->at("market_value")), 0.0) << "认购把现金扣成负数";
    EXPECT_NEAR(std::stod(cash_after->at("market_value")), avail - paid_shares * 5.0, 0.01);

    // 截断后的股数在到账日入仓（原持仓 + 可缴整股）。
    const Row* held_before = position_row(pos, "20260112", "600000.SH");
    ASSERT_NE(held_before, nullptr);
    const Row* list_day = position_row(pos, "20260114", "600000.SH");
    ASSERT_NE(list_day, nullptr);
    EXPECT_NEAR(std::stod(list_day->at("qty")),
                std::stod(held_before->at("qty")) + paid_shares, 0.01);
}

TEST_F(SelCaFixture, RightsIssueSkippedWhenAllowed) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,,0,0,0.3,8.0,0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv",
                              /*allow_skip_rights=*/true);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "rights_skipped"), 1.0);
    EXPECT_EQ(summary_num(body, "applied"), 0.0) << "跳过的行不该记为已施加";
    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    const Row* skipped = ca_event(evs, "rights_skipped");
    ASSERT_NE(skipped, nullptr);
    EXPECT_EQ(skipped->at("qty_before"), skipped->at("qty_after"));
    EXPECT_NEAR(std::stod(skipped->at("cash_delta")), 0.0, 0.01);
    // 跳过 = 不改仓不动钱：权益仍是 B0 基线值。
    const auto eq = read_csv(dir_ / "rep" / "daily_equity.csv");
    EXPECT_NEAR(equity_on(eq, "20260113"), 999'870.0, 0.01);
}

TEST_F(SelCaFixture, RightsContentionCountsCashStarvedSecondLeg) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260109,600001.SH,10.0\n"
               "20260112,600000.SH,10.0\n20260112,600001.SH,10.0\n"
               "20260113,600000.SH,10.0\n20260113,600001.SH,10.0\n"
               "20260114,600000.SH,10.0\n20260114,600001.SH,10.0\n");
    // 同 ex 日两笔配股（施加序 = 行序）：首腿的应缴款把现金吃到 <1 股价，
    // 第二腿应配量 >0 却被截成 0——它恰是结果最依赖施加序的腿，必须计入
    // contention 披露（漏计时交换 A/B 行序会改变实际获配证券而盘面零提示）。
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,,0,0,2.0,5.0,0\n"
               "600001.SH,20260112,20260113,,0,0,2.0,5.0,0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}, {"600001.SH", 0.45}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "rights_same_day_cash_contention"), 1.0)
        << "被首腿耗尽现金、截为零的第二腿必须计入竞争披露";
    EXPECT_EQ(summary_num(body, "rights_cash_capped"), 2.0);
    EXPECT_EQ(summary_num(body, "rights_subscribed"), 1.0);
}

TEST_F(SelCaFixture, RightsBelowOneShareEntitlementIsANoOp) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n");
    // 应配 = record_qty×0.000001 < 1 股且现金充足：零动作。发 rights_cash_capped
    // 事件行会与未 ++ 的计数矛盾（事件面与 summary 面得出相反结论）。
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,,0,0,0.000001,5.0,0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "rights_subscribed"), 0.0);
    EXPECT_EQ(summary_num(body, "rights_cash_capped"), 0.0);
    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    EXPECT_EQ(ca_event(evs, "rights_cash_capped"), nullptr)
        << "entitlement<1 股不是现金截断——事件类型说谎";
    EXPECT_EQ(ca_event(evs, "rights_subscribed"), nullptr);
}

// ── CA CSV 坏行 fail-fast（rc=1）：宁缺勿错，不静默跳过 ──────────────────────
class SelCaBadRowFixture : public SelCaFixture {
protected:
    /// @return runner rc；坏行一律 1（load 阶段 fail-fast）。
    int run_with_ca_body(const std::string& ca_body) {
        write_file("trading_dates.csv", "date\n20260112\n20260113\n");
        write_file("close_prices.csv",
                   "date,code,close\n"
                   "20260109,600000.SH,10.0\n20260112,600000.SH,10.0\n"
                   "20260113,600000.SH,10.0\n");
        write_file("ca.csv", ca_body);
        MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
        const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
        return run(cfg, &stub);
    }
};

TEST_F(SelCaBadRowFixture, ShortRowFailsFast) {
    EXPECT_EQ(run_with_ca_body(std::string(kCaHeader) +
                               "600000.SH,20260112,20260113,,0,0,0\n"), 1);
}

TEST_F(SelCaBadRowFixture, RecordAfterExDateFailsFast) {
    EXPECT_EQ(run_with_ca_body(std::string(kCaHeader) +
                               "600000.SH,20260114,20260113,,0.5,0,0,0,0\n"), 1);
}

TEST_F(SelCaBadRowFixture, PayBeforeExDateFailsFast) {
    EXPECT_EQ(run_with_ca_body(std::string(kCaHeader) +
                               "600000.SH,20260112,20260113,20260112,0.5,0,0,0,0\n"), 1);
}

TEST_F(SelCaBadRowFixture, DelistRowWithValuesFailsFast) {
    EXPECT_EQ(run_with_ca_body(std::string(kCaHeader) +
                               "600000.SH,,20260113,,0.5,0,0,0,1\n"), 1);
}

TEST_F(SelCaBadRowFixture, DuplicateCodeExDateFailsFast) {
    EXPECT_EQ(run_with_ca_body(std::string(kCaHeader) +
                               "600000.SH,20260112,20260113,,0.5,0,0,0,0\n"
                               "600000.SH,20260112,20260113,,0.5,0,0,0,0\n"), 1);
}

TEST_F(SelCaBadRowFixture, NonNumericRateFailsFast) {
    EXPECT_EQ(run_with_ca_body(std::string(kCaHeader) +
                               "600000.SH,20260112,20260113,,0.5x,0,0,0,0\n"), 1);
}

TEST_F(SelCaBadRowFixture, HeaderOnlyFailsFast) {
    EXPECT_EQ(run_with_ca_body(kCaHeader), 1);
}

TEST_F(SelCaBadRowFixture, MissingFileFailsFast) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv", "date,code,close\n20260112,600000.SH,10.0\n");
    MapWeightsStrategy stub({});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "missing_ca.csv");
    EXPECT_EQ(run(cfg, &stub), 1);
}

// ── B0 fold 钉 ───────────────────────────────────────────────────────────────

// 试算 fee 与落账 fee 必须同一 2dp 舍入路径，否则现金被透支 0.004。
//
// 反例（codex）：价 6 元 / 100 股一手 / 最低佣金 5 / 过户费率 0.00001 →
// raw fee 5.006、落账 fee 5.01；现金 605.006 时用 raw 试算会放行 100 股，
// 落账后 cash = −0.004。修好后该手买不起（cost 605.01 > 605.006）→ 截到 0。
TEST_F(SelCaFixture, SubCentFeeRoundingDoesNotOverdrawCash) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,6.0\n"
               "20260112,600000.SH,6.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 1.0}}}});
    const auto cfg = make_cfg("rep", 605.006, frictions());
    ASSERT_EQ(run(cfg, &stub), 0) << "现金落负应在修好后结构性不可达（rc=10 = 已透支）";

    EXPECT_EQ(read_csv(dir_ / "rep" / "trades.csv").size(), 0u)
        << "raw（未 2dp）试算 fee 放行了买不起的一手";
    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "negative_cash_events"), 0.0);
    EXPECT_EQ(summary_num(body, "affordability_caps"), 1.0);
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    const Row* cash = position_row(pos, "20260112", "CASH");
    ASSERT_NE(cash, nullptr);
    EXPECT_GE(std::stod(cash->at("market_value")), 0.0);
}

/// @brief 逐行断言 daily_positions.csv 的列数恒等于 header 列数。
/// @note read_csv 按 `min(header, fields)` 配对，缺列只表现为 key 缺失（.at 抛），
///       多列则被静默丢弃 —— 都不是「列数」断言。schema 破裂必须在原始行上判。
void expect_uniform_column_count(const std::filesystem::path& csv,
                                 size_t expect_cols) {
    std::ifstream ifs(csv);
    ASSERT_TRUE(ifs.good()) << csv;
    std::string line;
    size_t lineno = 0;
    while (std::getline(ifs, line)) {
        ++lineno;
        if (line.empty()) continue;
        EXPECT_EQ(split_csv(line).size(), expect_cols)
            << csv.filename().string() << " line " << lineno << ": [" << line << "]";
    }
    EXPECT_GT(lineno, 1u) << "文件只有 header，钉未真正覆盖数据行";
}

// E3 单一 schema：T+1 未启用时 long_today/available_sell 两列仍在，值留空。
// 三种行形态都必须守同一 9 列 schema —— 持仓行 / CASH 行 / RECEIVABLE 行。
// RECEIVABLE 只在配了 CA 且应收非零时出现，是最后一个漏补两列的分支（7 列 vs
// header 9 列：按列位读的消费方会把 weight 读成 available_sell，整行错位）。
TEST_F(SelCaFixture, DailyPositionsKeepsSingleSchemaWithoutT1) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260112,600000.SH,10.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    // T+1 关闭（不写 enforce_long_t_plus_1）。
    const std::string extra =
        "commodities:\n"
        "  '600000.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0,"
        " min_lot: 100 }\n"
        "risk:\n"
        "  max_order_qty: 1000000.0\n"
        "  allow_short: false\n";
    const auto cfg = make_cfg("rep", 1'000'000.0, extra);
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "daily_positions.csv");
    EXPECT_NE(body.find("long_today,available_sell"), std::string::npos)
        << "T+1 未启用时列集缩水（daily_positions 变成双 schema）";
    const auto rows = read_csv(dir_ / "rep" / "daily_positions.csv");
    ASSERT_FALSE(rows.empty());
    for (const auto& r : rows) {
        EXPECT_TRUE(r.at("long_today").empty()) << r.at("code");
        EXPECT_TRUE(r.at("available_sell").empty()) << r.at("code");
    }
    expect_uniform_column_count(dir_ / "rep" / "daily_positions.csv", 9);

    // ② CA 场景（同一 T+1 关闭配置 + ex→pay 应收窗口）：RECEIVABLE 行也在其中。
    //    20260112 买 50000 股 @10；20260113 除息 0.5/股（record 20260112、pay
    //    20260114）→ 当日 RECEIVABLE 行市值 25,000。
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,9.5\n"
               "20260114,600000.SH,9.5\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,20260114,0.5,0,0,0,0\n");
    MapWeightsStrategy ca_stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto ca_cfg = make_cfg("rep_ca", 1'000'000.0,
                                 frictions(/*dividend_tax=*/0.0, /*delist_haircut=*/0.0,
                                           /*min_commission=*/5.0, /*enforce_t1=*/false),
                                 "ca.csv");
    ASSERT_EQ(run(ca_cfg, &ca_stub), 0);

    // 列数先判：read_csv 会把缺列吞成「key 不存在」，先在原始行上把 schema 钉死。
    expect_uniform_column_count(dir_ / "rep_ca" / "daily_positions.csv", 9);
    const auto ca_rows = read_csv(dir_ / "rep_ca" / "daily_positions.csv");
    const Row* recv = position_row(ca_rows, "20260113", "RECEIVABLE");
    ASSERT_NE(recv, nullptr) << "钉的前提没了：T+1 关闭 + CA 场景下没写出 RECEIVABLE 行";
    EXPECT_NEAR(std::stod(recv->at("market_value")), 25'000.0, 0.01);
    ASSERT_EQ(recv->count("long_today"), 1u)
        << "RECEIVABLE 行列数不足（T+1 关闭时漏补空字段）";
    for (const auto& r : ca_rows) {
        EXPECT_TRUE(r.at("long_today").empty()) << r.at("code");
        EXPECT_TRUE(r.at("available_sell").empty()) << r.at("code");
    }
    expect_uniform_column_count(dir_ / "rep_ca" / "daily_positions.csv", 9);
}

// CA 块披露三项此前只存在于输入/配置、产物里看不见的量：
//   ① delist_haircut_pct —— 清算所得直接由它定，换了折价重跑结果差异无从归因；
//   ② pay_date 缺省 → 现金分红当日直入现金（ex→pay 资金占用被按零处理）；
//   ③ shares_list_date 显式给 ex 当日 → 红股当日直入可卖仓（不可卖窗口按零处理）。
// ②③ 都是合法写法但结果偏乐观，必须在 summary 上可见而不是靠翻 CSV 输入反推。
TEST_F(SelCaFixture, SummaryDisclosesHaircutAndAbsentLagDateCounts) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260109,000001.SZ,10.0\n"
               "20260112,600000.SH,10.0\n20260112,000001.SZ,10.0\n"
               "20260113,600000.SH,9.5\n20260113,000001.SZ,6.6667\n");
    //   600000.SH 只派现 0.5/股、pay_date 缺省 → 当日直入现金；
    //   000001.SZ 只送转 0.5、shares_list_date 显式写 ex 当日 → 当日直入可卖仓
    //   （缺省写法在窗末已无 ex+1 时走的是「窗外到账」分支，不是即时）。
    write_file("ca.csv", std::string(kCaHeader10) +
               "600000.SH,20260112,20260113,,0.5,0,0,0,0,\n"
               "000001.SZ,20260112,20260113,,0,0.5,0,0,0,20260113\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.4}, {"000001.SZ", 0.4}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0,
                              frictions(/*dividend_tax=*/0.0, /*delist_haircut=*/0.1),
                              "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "applied"), 2.0);
    EXPECT_NEAR(summary_num(body, "delist_haircut_pct"), 0.1, 1e-9)
        << "生效的清算折价没进 summary（配置值在产物里无可见面）";
    EXPECT_EQ(summary_num(body, "pay_date_absent_immediate_cash"), 1.0)
        << "pay_date 缺省 → 当日直入现金的笔数未披露";
    EXPECT_EQ(summary_num(body, "shares_list_date_absent_immediate"), 1.0)
        << "shares_list_date 显式给 ex 当日 → 红股当日入仓的笔数未披露";
    EXPECT_EQ(summary_num(body, "shares_list_date_beyond_window"), 0.0);
    // 对照：两笔都没走应收/在途路径，收尾应收恒 0（计数确实来自「无窗口」分支）。
    EXPECT_EQ(summary_num(body, "receivable_outstanding_cash"), 0.0);
    EXPECT_EQ(summary_num(body, "receivable_outstanding_bonus"), 0.0);
}

// 对照钉：pay_date / shares_list_date 真给了滞后日期时，两个「缺省即时」计数为 0
// （计数器不是「有 CA 就 +1」的常数）。
TEST_F(SelCaFixture, SummaryAbsentLagCountsStayZeroWhenLagDatesGiven) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260109,000001.SZ,10.0\n"
               "20260112,600000.SH,10.0\n20260112,000001.SZ,10.0\n"
               "20260113,600000.SH,9.5\n20260113,000001.SZ,6.6667\n");
    // pay_date=20260115 / shares_list_date=20260115 均晚于 ex 日 → 走应收与在途。
    write_file("ca.csv", std::string(kCaHeader10) +
               "600000.SH,20260112,20260113,20260115,0.5,0,0,0,0,\n"
               "000001.SZ,20260112,20260113,,0,0.5,0,0,0,20260115\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.4}, {"000001.SZ", 0.4}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "pay_date_absent_immediate_cash"), 0.0);
    EXPECT_EQ(summary_num(body, "shares_list_date_absent_immediate"), 0.0);
    EXPECT_GT(summary_num(body, "receivable_outstanding_cash"), 0.0)
        << "对照场景没真走应收路径，钉失去对照意义";
    EXPECT_GT(summary_num(body, "receivable_outstanding_bonus"), 0.0)
        << "对照场景没真走在途红股路径，钉失去对照意义";
}

// F06 补齐：datalake.root 也按 config 目录绝对化（相对 root 不再依赖 cwd）。
TEST_F(SelCaFixture, RelativeDatalakeRootResolvedFromConfigDir) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv", "date,code,close\n20260112,600000.SH,10.0\n");
    std::error_code ec;
    std::filesystem::create_directories(dir_ / "lake", ec);
    const std::string cfg_path = (dir_ / "dl_cfg.yaml").string();
    {
        std::ofstream ofs(cfg_path);
        ofs << "datalake:\n"
            << "  root: lake\n"
            << "selector:\n"
            << "  trading_dates_csv: trading_dates.csv\n"
            << "  close_prices_csv: close_prices.csv\n"
            << "  task:\n    strategy_id: sel_ca\n    period: daily\n    time: 1500\n"
            << "  strategy:\n    id: sel_ca\n"
            << "report:\n  output_dir: " << (dir_ / "rep").string() << "\n";
    }
    xt_bt_sel::XtSelRunnerConfig out;
    std::string err;
    ASSERT_TRUE(load_cfg(cfg_path, out, err)) << err;
    EXPECT_TRUE(std::filesystem::path(out.datalake_root).is_absolute())
        << "datalake.root 仍是相对路径（cwd 依赖未消）: " << out.datalake_root;
    EXPECT_NE(out.datalake_root.find("lake"), std::string::npos) << out.datalake_root;
}

// allow_short 闸不依赖 `risk:` 节存在：缺省无条件 long-only。
TEST_F(SelCaFixture, AllowShortDefaultsToFalseWithoutRiskSection) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv", "date,code,close\n20260112,600000.SH,10.0\n");
    const auto cfg = make_cfg("rep", 1'000'000.0, /*extra=*/"");  // 无 risk 节
    xt_bt_sel::XtSelRunnerConfig out;
    std::string err;
    ASSERT_TRUE(load_cfg(cfg, out, err)) << err;
    EXPECT_FALSE(out.risk_cfg.allow_short)
        << "缺 risk 节时 XtRiskCfg 默认 allow_short=true 被原样带下去";
}

// 产物 writer 打不开 → rc=9 且不留半截文件（report_dir 被一个同名文件占位）。
TEST_F(SelCaFixture, ReportWriterOpenFailureYieldsRc9) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260112,600000.SH,10.0\n");
    write_file("rep", "occupied");  // report_dir 位置已被普通文件占用
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions());
    EXPECT_EQ(run(cfg, &stub), 9);
    // 占位文件内容未被破坏，且没有旁落 .tmp。
    EXPECT_EQ(read_text(dir_ / "rep"), "occupied");
    for (const auto& e : std::filesystem::directory_iterator(dir_)) {
        EXPECT_EQ(e.path().extension(), std::filesystem::path(e.path()).extension());
        EXPECT_NE(e.path().string().find(".tmp"), 0u);
    }
}

// report_dir 文件集白名单 + 两次同输入跑逐字节一致（无 stray 产物、无非确定性）。
TEST_F(SelCaFixture, ReportDirFileSetIsWhitelistedAndDeterministic) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260109,000001.SZ,20.0\n"
               "20260112,600000.SH,10.0\n20260112,000001.SZ,20.0\n"
               "20260113,600000.SH,11.0\n20260113,000001.SZ,19.0\n");
    const std::map<uint32_t, std::unordered_map<std::string, double>> plan = {
        {20260112u, {{"600000.SH", 0.4}, {"000001.SZ", 0.3}}},
        {20260113u, {{"600000.SH", 0.2}, {"000001.SZ", 0.5}}},
    };
    MapWeightsStrategy s1(plan), s2(plan);
    ASSERT_EQ(run(make_cfg("r1", 1'000'000.0, frictions()), &s1), 0);
    ASSERT_EQ(run(make_cfg("r2", 1'000'000.0, frictions()), &s2), 0);

    const std::set<std::string> expected = {
        "trades.csv", "daily_equity.csv", "summary.json", "consumed_manifest.json",
        "schedule_fires.csv", "target_weights.csv", "daily_positions.csv",
        "fill_costs.csv",
    };
    auto file_set = [](const std::filesystem::path& d) {
        std::set<std::string> out;
        for (const auto& e : std::filesystem::directory_iterator(d)) {
            out.insert(e.path().filename().string());
        }
        return out;
    };
    const auto s1_files = file_set(dir_ / "r1");
    EXPECT_EQ(s1_files, expected) << "report_dir 出现白名单外文件（含 .tmp 残留）";
    EXPECT_EQ(file_set(dir_ / "r2"), expected);
    for (const auto& name : expected) {
        // summary/manifest 含 report_dir 路径 → 只对不含路径的产物做 byte 比对。
        if (name == "summary.json" || name == "consumed_manifest.json") continue;
        EXPECT_EQ(read_text(dir_ / "r1" / name), read_text(dir_ / "r2" / name))
            << name << " 两次同输入跑不逐字节一致";
    }
}

// 配置面：税率/折价越界 fail-fast（[0,1) 域）。
TEST_F(SelCaFixture, OutOfRangeRatesRejectedAtParse) {
    write_file("trading_dates.csv", "date\n20260112\n");
    write_file("close_prices.csv", "date,code,close\n20260112,600000.SH,10.0\n");
    {
        const auto cfg = make_cfg("r1", 1'000'000.0, frictions(/*dividend_tax=*/1.0));
        xt_bt_sel::XtSelRunnerConfig out;
        std::string err;
        EXPECT_FALSE(load_cfg(cfg, out, err));
        EXPECT_NE(err.find("dividend_tax_rate"), std::string::npos) << err;
    }
    {
        const auto cfg = make_cfg("r2", 1'000'000.0,
                                  frictions(0.0, /*delist_haircut=*/1.0));
        xt_bt_sel::XtSelRunnerConfig out;
        std::string err;
        EXPECT_FALSE(load_cfg(cfg, out, err));
        EXPECT_NE(err.find("delist_haircut_pct"), std::string::npos) << err;
    }
}

// ── 引擎总批 fold 钉 ─────────────────────────────────────────────────────────

// record_date == ex_date：确权基数 = ex 日**初**持仓（快照 EOD 才生成，施加点在日初），
// 并按 ex_day 拆因计数披露。ex 当日清仓 → 若读 EOD 快照，分红会变 0。
TEST_F(SelCaFixture, RecordEqualsExDateAccruesOnExOpenQtyWithCount) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n"
               "20260114,600000.SH,9.0\n");
    // record == ex == 20260114；当日策略清仓（EOD 仓位 = 0）。
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260114,20260114,,1.0,0,0,0,0\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.5}}},
        {20260114u, {{"600000.SH", 0.0}}},
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    const Row* div = ca_event(evs, "cash_div");
    ASSERT_NE(div, nullptr);
    EXPECT_EQ(div->at("qty_before"), "50000.0000")
        << "record==ex 用了 EOD（清仓后）快照，整笔分红消失";
    EXPECT_NEAR(std::stod(div->at("cash_delta")), 50'000.0, 0.01);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "record_qty_fallback_ex_day"), 1.0);
    EXPECT_EQ(summary_num(body, "record_qty_fallback_absent"), 0.0);
    EXPECT_EQ(summary_num(body, "record_qty_fallback_before_window"), 0.0);
    EXPECT_EQ(summary_num(body, "record_qty_fallbacks"), 1.0);
}

// record_date 早于窗首：确权退化为 ex 日初仓 = **真过度计提**（窗首后才建的仓拿到了
// 分红），必须与「未提供 record_date」分开计数，否则披露面看不出风险。
TEST_F(SelCaFixture, RecordBeforeWindowCountsSeparatelyFromAbsent) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,9.0\n");
    // record 20260105 早于窗首 20260112；仓位是窗首当天才建的。
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260105,20260113,,1.0,0,0,0,0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "record_qty_fallback_before_window"), 1.0)
        << "窗前 record 与「未提供」混成同一个计数（过度计提不可见）";
    EXPECT_EQ(summary_num(body, "record_qty_fallback_absent"), 0.0);
    EXPECT_EQ(summary_num(body, "record_qty_fallback_ex_day"), 0.0);
    // 披露的是「按 ex 日初仓计提了 50,000」这一事实本身。
    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    const Row* div = ca_event(evs, "cash_div");
    ASSERT_NE(div, nullptr);
    EXPECT_NEAR(std::stod(div->at("cash_delta")), 50'000.0, 0.01);
}

// 在途红股窗口内卖出：ex 日**不摊** avg（qty 没变），到账日按总成本不变重算。
//
// 50,000 股 @10（成本 500,000）+ 1:1 红股延迟到 20260116。窗口内 @4 清仓 →
// realized = (4−10)×50,000 = −300,000；红股以 avg 0 到账，@4 卖出 → +200,000；
// 累计 −100,000 = 真成本基础（5/股）下的正确值。若 ex 日摊了 avg（旧行为），第一腿
// 只实现 −50,000 而红股仍按 avg 0 入仓 → 累计 +150,000（凭空多赚 250,000）。
TEST_F(SelCaFixture, DeferredBonusWindowSellRealizesOnUnamortizedAvg) {
    write_file("trading_dates.csv",
               "date\n20260112\n20260113\n20260114\n20260116\n20260119\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,5.0\n"
               "20260114,600000.SH,4.0\n"
               "20260116,600000.SH,4.0\n"
               "20260119,600000.SH,4.0\n");
    write_file("ca.csv", std::string(kCaHeader10) +
               "600000.SH,20260112,20260113,,0,1.0,0,0,0,20260116\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.5}}},
        {20260114u, {{"600000.SH", 0.0}}},   // 窗口内清仓（红股尚未到账）
        {20260119u, {{"600000.SH", 0.0}}},   // 红股到账后清仓
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto trades = read_csv(dir_ / "rep" / "trades.csv");
    ASSERT_EQ(trades.size(), 3u);
    EXPECT_EQ(trades[1].at("qty"), "50000.0000");   // 窗口内清真实仓
    EXPECT_EQ(trades[2].at("qty"), "50000.0000");   // 到账后清红股

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_NEAR(summary_num(body, "realized_pnl"), -100'000.0, 0.01)
        << "ex 日摊了在途红股的 avg（总成本凭空缩水）→ realized 归因错";
    EXPECT_EQ(summary_num(body, "equity_identity_violations"), 0.0);
    EXPECT_NEAR(summary_num(body, "receivable_outstanding_bonus"), 0.0, 0.01);
}

// ex→list 窗口内退市：在途红股必须随清算一并了结（否则市值一直挂在权益里、list 日
// 又加回已退市 symbol）。清算合成单的 strategy_id 落 `ca_delist` sentinel。
TEST_F(SelCaFixture, DelistLiquidatesPendingBonusSharesToo) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,5.0\n"
               "20260114,600000.SH,5.0\n");
    write_file("ca.csv", std::string(kCaHeader10) +
               "600000.SH,20260112,20260113,,0,1.0,0,0,0,20260120\n"  // 红股 list 在窗外
               "600000.SH,,20260114,,0,0,0,0,1,\n");                  // 20260114 退市
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto trades = read_csv(dir_ / "rep" / "trades.csv");
    ASSERT_EQ(trades.size(), 2u);
    EXPECT_EQ(trades[1].at("qty"), "100000.0000")
        << "清算只卖了真实仓，在途红股被留下（退市后复活仓）";
    EXPECT_EQ(trades[1].at("strategy_id"), "ca_delist")
        << "清算合成单没有归因 sentinel";
    EXPECT_NEAR(std::stod(trades[1].at("exec_price")), 5.0, 0.0001);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "delist_pending_bonus_liquidated"), 1.0);
    EXPECT_NEAR(summary_num(body, "receivable_outstanding_bonus"), 0.0, 0.01);
    // 成本基础 = 500,000/100,000 = 5 → 以 5.0 清算 realized 恰 0。
    EXPECT_NEAR(summary_num(body, "realized_pnl"), 0.0, 0.01);
    EXPECT_EQ(summary_num(body, "equity_identity_violations"), 0.0);

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_EQ(position_row(pos, "20260114", "600000.SH"), nullptr);
    EXPECT_EQ(position_row(pos, "20260114", "RECEIVABLE"), nullptr);
    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    ASSERT_NE(ca_event(evs, "pending_bonus_liquidated"), nullptr);
}

// 每股现金分红 ≥ T-1 参考价 → 除息后参考价非正 = 输入数据错误，整跑 fail-fast(rc=1)。
// clamp 到 0 会让 mark 停在除息前价（权益虚增），同 fire 其他 symbol 按虚增权益放大目标。
TEST_F(SelCaFixture, CashDividendAtOrAbovePrevCloseFailsFast) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,,10.0,0,0,0,0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    EXPECT_EQ(run(cfg, &stub), 1) << "参考价被 clamp 到 0 后继续跑（权益虚增）";
    EXPECT_FALSE(std::filesystem::exists(dir_ / "rep" / "summary.json"));
}

// SELL 侧费用超「可用现金 + 卖出所得」→ 提交口拒该腿（不 cap 真实费用、不 abort）。
//
// 反例（codex P1-5）：现金 605.01 买 100@6（fee 5.01）→ 现金恰 0；随后价跌到 0.01，
// 清仓所得 1.00 而佣金下限 5.00 → 提交即透支。旧行为让 rc=10 在普通 long-only
// 路径上可达（产物不写出）。
TEST_F(SelCaFixture, SellRejectedWhenFeeExceedsCashPlusProceeds) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,6.0\n"
               "20260112,600000.SH,6.0\n"
               "20260113,600000.SH,0.01\n"
               "20260114,600000.SH,0.01\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 1.0}}},   // 买 100 股 @6，现金落到 0.00
        {20260114u, {{"600000.SH", 0.0}}},   // 提交价 = 20260113 close 0.01
    });
    const auto cfg = make_cfg("rep", 605.01, frictions());
    ASSERT_EQ(run(cfg, &stub), 0) << "现金被扣负 → rc=10（费用闸没拦住）";

    const auto trades = read_csv(dir_ / "rep" / "trades.csv");
    ASSERT_EQ(trades.size(), 1u) << "亏本卖单成交了（现金必然落负）";
    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "sell_fee_unaffordable_rejects"), 1.0);
    EXPECT_EQ(summary_num(body, "negative_cash_events"), 0.0);

    const auto targets = read_csv(dir_ / "rep" / "target_weights.csv");
    const Row* leg = nullptr;
    for (const auto& r : targets) {
        if (r.at("exec_date") == "20260114") { leg = &r; break; }
    }
    ASSERT_NE(leg, nullptr);
    EXPECT_EQ(leg->at("gate"), "sell_fee_unaffordable");
    EXPECT_TRUE(leg->at("submitted_qty").empty()) << "拒腿仍进了 gateway";
    // 仓位未动（缺口由下期 rebalance 顺延）。
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    const Row* held = position_row(pos, "20260114", "600000.SH");
    ASSERT_NE(held, nullptr);
    EXPECT_EQ(held->at("qty"), "100.0000");
}

// 合成 T-1 价禁止：参考价取自 carry 的 mark（该日无 close 行）时，修正值不得回写进
// 价源——否则 sizing 会拿到一个当日不存在的价格，把 kNoPrice 跳过变成按合成价下单。
TEST_F(SelCaFixture, ExDayRefPriceNotWrittenBackWhenCloseRowMissing) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260109,000001.SZ,20.0\n"
               "20260112,600000.SH,10.0\n20260112,000001.SZ,20.0\n"
               "20260113,600000.SH,10.0\n"            // 000001.SZ 当日缺行
               "20260114,600000.SH,10.0\n20260114,000001.SZ,20.0\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "000001.SZ,20260112,20260114,,1.0,0,0,0,0\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.3}, {"000001.SZ", 0.3}}},
        {20260114u, {{"600000.SH", 0.3}, {"000001.SZ", 0.5}}},
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto targets = read_csv(dir_ / "rep" / "target_weights.csv");
    const Row* leg = nullptr;
    for (const auto& r : targets) {
        if (r.at("exec_date") == "20260114" && r.at("code") == "000001.SZ") {
            leg = &r;
            break;
        }
    }
    ASSERT_NE(leg, nullptr);
    EXPECT_EQ(leg->at("planning_status"), "no_price")
        << "ex 日把修正后的参考价补成了 T-1 价行（合成价 sizing）";
    EXPECT_TRUE(leg->at("target_qty").empty());
    EXPECT_TRUE(leg->at("submitted_qty").empty());

    // 估值修正仍生效（走 mark 分支，不进价源）：分红按 record 日快照确权。
    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    const Row* div = ca_event(evs, "cash_div", "000001.SZ");
    ASSERT_NE(div, nullptr);
    EXPECT_EQ(div->at("qty_before"), "15000.0000");
    EXPECT_NEAR(std::stod(div->at("cash_delta")), 15'000.0, 0.01);
    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "record_qty_fallbacks"), 0.0);
}

// 归日撞键：同 code 的两个非交易日事件并到同一交易日 → 按**原 ex_date 升序**逐条
// 施加（复合公式不可交换）。CSV 故意逆序写，行序不得决定结果。
//
// 升序（送转 20260115 → 分红 20260116）：ref 10→5→4。逆序则是 ref 10→9→4.5。
// 送转股按缺省到账日（ex 的下一交易日 20260119）在途，故分红的确权基数是送转前的
// 50,000 股；ref 链才是本钉的判据（逆序会得到 4.5）。
TEST_F(SelCaFixture, SameCodeRebasedToSameDateAppliesInRawExDateOrder) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260119\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,5.0\n"
               "20260119,600000.SH,5.0\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,,20260116,,1.0,0,0,0,0\n"   // 后发生（现金分红）先写
               "600000.SH,,20260115,,0,1.0,0,0,0\n"); // 先发生（十送十）后写
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    ASSERT_EQ(evs.size(), 3u);
    EXPECT_EQ(evs[0].at("type"), "bonus_transfer") << "施加序按 CSV 行序而非原 ex_date";
    EXPECT_EQ(evs[1].at("type"), "cash_div");
    EXPECT_EQ(evs[2].at("type"), "shares_listed") << "送转股未按缺省滞后到下一交易日";
    // ref 链 10 →(÷2) 5 →(−1) 4：升序施加的唯一结果（逆序为 10→9→4.5）。
    EXPECT_NEAR(std::stod(evs[0].at("ref_price_after")), 5.0, 1e-6);
    EXPECT_NEAR(std::stod(evs[1].at("ref_price_before")), 5.0, 1e-6);
    EXPECT_NEAR(std::stod(evs[1].at("ref_price_after")), 4.0, 1e-6);
    EXPECT_NEAR(std::stod(evs[1].at("cash_delta")), 50'000.0, 0.01)
        << "分红确权基数不是送转股到账前的持仓";
    EXPECT_EQ(evs[2].at("qty_after"), "100000.0000");

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "rebased_to_trading_date"), 2.0);
    EXPECT_EQ(summary_num(body, "equity_identity_violations"), 0.0);
}

// ── 新股到账滞后：CSV 未给 shares_list_date 时取 **ex 的下一交易日**（非 ex 当日）
//    ——A 股送转股份的上市至少滞后一个交易日，ex 日即可卖是把不可卖窗口当零。
TEST_F(SelCaFixture, BonusSharesDefaultToNextTradingDayListing) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,5.0\n"
               "20260114,600000.SH,5.0\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,,0,1.0,0,0,0\n");   // 十送十，无 list 日
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    const Row* ex_day = position_row(pos, "20260113", "600000.SH");
    ASSERT_NE(ex_day, nullptr);
    EXPECT_EQ(ex_day->at("qty"), "50000.0000") << "送转股在 ex 日就进了可卖仓";
    ASSERT_NE(position_row(pos, "20260113", "RECEIVABLE"), nullptr)
        << "ex→list 窗口的在途新股没进应收口径";
    const Row* list_day = position_row(pos, "20260114", "600000.SH");
    ASSERT_NE(list_day, nullptr);
    EXPECT_EQ(list_day->at("qty"), "100000.0000");

    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    const Row* listed = ca_event(evs, "shares_listed");
    ASSERT_NE(listed, nullptr);
    EXPECT_EQ(listed->at("ex_date"), "20260114");

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "shares_list_date_defaulted"), 1.0);
    EXPECT_EQ(summary_num(body, "shares_list_date_absent_immediate"), 0.0)
        << "缺省到账日仍被当成「ex 日即时到账」";
    // 权益连续：ex 日 250,000（持仓）+ 250,000（在途）+ 499,870（现金）。
    const auto eq = read_csv(dir_ / "rep" / "daily_equity.csv");
    EXPECT_NEAR(equity_on(eq, "20260113"), 999'870.0, 0.01);
    EXPECT_EQ(summary_num(body, "equity_identity_violations"), 0.0);
}

// ── F12 换股（吸收合并 / 证券变更）：旧仓按比例换新 code，成本基准整体迁移 ─────
//
// 零费用摩擦让成本迁移在 realized_pnl 上精确可读：
// 20260112 买 50,000 股 600000.SH @10（总成本 500,000）→ 20260113 按 2:1 换成
// 100,000 股 000001.SZ（avg 应为 5.0）→ 20260114 全部 @6 卖出 → realized 恰
// (6 − 5) × 100,000 = 100,000。avg 若未迁移（比如留 10 或归 0），realized 立刻分叉。
TEST_F(SelCaFixture, SecurityConversionMigratesCostBasis) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n20260112,000001.SZ,5.0\n"
               "20260113,600000.SH,10.0\n20260113,000001.SZ,5.0\n"
               "20260114,000001.SZ,6.0\n");
    write_file("ca.csv", std::string(kCaHeaderFull) +
               "600000.SH,,20260113,,0,0,0,0,0,,000001.SZ,2.0,0\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.5}}},
        {20260114u, {{"000001.SZ", 0.0}}},   // 全部卖出，验 realized 归因
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, zero_fee_frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    const Row* out = ca_event(evs, "conversion_out", "600000.SH");
    ASSERT_NE(out, nullptr);
    EXPECT_EQ(out->at("qty_before"), "50000.0000");
    EXPECT_EQ(out->at("qty_after"), "0.0000");
    const Row* in = ca_event(evs, "conversion_in", "000001.SZ");
    ASSERT_NE(in, nullptr);
    EXPECT_EQ(in->at("qty_after"), "100000.0000");

    // 换股当日权益不变（等值交换）：100,000 × 5.0 + 现金 500,000。
    const auto eq = read_csv(dir_ / "rep" / "daily_equity.csv");
    EXPECT_NEAR(equity_on(eq, "20260113"), 1'000'000.0, 0.01);

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_EQ(position_row(pos, "20260113", "600000.SH"), nullptr) << "旧 code 未清零";
    ASSERT_NE(position_row(pos, "20260113", "000001.SZ"), nullptr);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "conversions"), 1.0);
    EXPECT_EQ(summary_num(body, "equity_identity_violations"), 0.0);
    EXPECT_NEAR(summary_num(body, "realized_pnl"), 100'000.0, 0.01)
        << "换股未迁移成本基准（avg 没落到 总成本 ÷ 新股数）";
    EXPECT_NEAR(summary_num(body, "final_equity"), 1'100'000.0, 0.01);
}

// 换股零股：不足整股的余数按 cash_in_lieu 折现进现金（等值，不产生盈亏）；
// 换掉的旧 code 进闩锁，后续买入被拒。
TEST_F(SelCaFixture, SecurityConversionCashesOutOddSharesAndLatchesOldCode) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n20260112,000001.SZ,10.0\n"
               "20260113,600000.SH,10.0\n20260113,000001.SZ,10.0\n"
               "20260114,600000.SH,10.0\n20260114,000001.SZ,10.0\n");
    // 30,000 × 1.00005 = 30,001.5 → 30,001 股 + 0.5 股 × 10.0 = 5.0 现金。
    write_file("ca.csv", std::string(kCaHeaderFull) +
               "600000.SH,,20260113,,0,0,0,0,0,,000001.SZ,1.00005,10.0\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.3}}},
        {20260114u, {{"600000.SH", 0.3}}},   // 换股后再买旧 code → 闩锁拒
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, zero_fee_frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    const Row* newpos = position_row(pos, "20260113", "000001.SZ");
    ASSERT_NE(newpos, nullptr);
    EXPECT_EQ(newpos->at("qty"), "30001.0000");
    const Row* cash = position_row(pos, "20260113", "CASH");
    ASSERT_NE(cash, nullptr);
    EXPECT_NEAR(std::stod(cash->at("market_value")), 700'005.0, 0.01)
        << "零股现金替代没进现金";

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_NEAR(summary_num(body, "conversion_cash_in_lieu"), 5.0, 0.01);
    EXPECT_EQ(summary_num(body, "conversion_buy_rejects"), 1.0)
        << "换掉的旧 code 仍可买入（闩锁缺失）";
    EXPECT_EQ(summary_num(body, "equity_identity_violations"), 0.0);

    const auto targets = read_csv(dir_ / "rep" / "target_weights.csv");
    const Row* rejected = nullptr;
    for (const auto& r : targets) {
        if (r.at("exec_date") == "20260114" && r.at("code") == "600000.SH") {
            rejected = &r;
            break;
        }
    }
    ASSERT_NE(rejected, nullptr);
    EXPECT_EQ(rejected->at("gate"), "converted");
}

// 换股行的形态校验：目标 code 不得等于本 code、比例必须为正、不得与分红/送转混填。
TEST_F(SelCaFixture, ConversionRowShapeIsRejected) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});

    struct Case { const char* row; const char* why; };
    const Case cases[] = {
        {"600000.SH,,20260113,,0,0,0,0,0,,600000.SH,2.0,0\n", "自换股"},
        {"600000.SH,,20260113,,0,0,0,0,0,,000001.SZ,0,0\n",   "比例非正"},
        {"600000.SH,,20260113,,1.0,0,0,0,0,,000001.SZ,2.0,0\n", "与分红混填"},
        {"600000.SH,,20260113,,0,0,0,0,0,,,2.0,0\n",          "无目标 code 却有比例"},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        write_file("ca.csv", std::string(kCaHeaderFull) + cases[i].row);
        const auto cfg = make_cfg("rep" + std::to_string(i), 1'000'000.0,
                                  frictions(), "ca.csv");
        EXPECT_EQ(run(cfg, &stub), 1) << "坏换股行被放行：" << cases[i].why;
    }
}

// 配股行形态：比例与配股价必须同为正或同为 0（缺一即缴款额或价格错位）。
TEST_F(SelCaFixture, RightsRowRequiresRatioAndPriceTogether) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    for (const char* row : {"600000.SH,,20260113,,0,0,1.0,0,0\n",
                            "600000.SH,,20260113,,0,0,0,5.0,0\n"}) {
        write_file("ca.csv", std::string(kCaHeader) + row);
        const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
        EXPECT_EQ(run(cfg, &stub), 1) << "配股比例/价格单边填写被放行：" << row;
    }
}

// 退市闩锁：清算后该 code 拒新买入；数据源若还留着 close 行则计数披露。
TEST_F(SelCaFixture, DelistedCodeRejectsLaterBuyAndCountsPriceReappearance) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n"
               "20260114,600000.SH,10.0\n");   // 退市后仍有 close 行
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,,20260113,,0,0,0,0,1\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.5}}},
        {20260114u, {{"600000.SH", 0.5}}},   // 退市后再买 → 必须被闩锁拒
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto trades = read_csv(dir_ / "rep" / "trades.csv");
    ASSERT_EQ(trades.size(), 2u) << "退市后又在该 code 上建了仓";
    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "delist_buy_rejects"), 1.0);
    EXPECT_EQ(summary_num(body, "delisted_price_reappearances"), 1.0);

    const auto targets = read_csv(dir_ / "rep" / "target_weights.csv");
    const Row* leg = nullptr;
    for (const auto& r : targets) {
        if (r.at("exec_date") == "20260114") { leg = &r; break; }
    }
    ASSERT_NE(leg, nullptr);
    EXPECT_EQ(leg->at("gate"), "delisted");
    EXPECT_TRUE(leg->at("submitted_qty").empty());
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_EQ(position_row(pos, "20260114", "600000.SH"), nullptr);
}

// ── CA 日期严格域 fail-fast（rc=1）────────────────────────────────────────────

// 短位数日期：`2026011` 旧解析当成 2026011 → 窗前事件静默忽略，整笔分红消失。
TEST_F(SelCaBadRowFixture, ShortDateFailsFast) {
    EXPECT_EQ(run_with_ca_body(std::string(kCaHeader) +
                               "600000.SH,20260112,2026011,,0.5,0,0,0,0\n"), 1);
}

// 非法月/日：pay_date 20261399 旧解析放行 → 应收永不到期。
TEST_F(SelCaBadRowFixture, ImpossibleMonthDayInPayDateFailsFast) {
    EXPECT_EQ(run_with_ca_body(std::string(kCaHeader) +
                               "600000.SH,20260112,20260113,20261399,0.5,0,0,0,0\n"), 1);
}

// 非闰年 2 月 31 日：整数域合法但日历不存在。
TEST_F(SelCaBadRowFixture, NonExistentCalendarDayFailsFast) {
    EXPECT_EQ(run_with_ca_body(std::string(kCaHeader) +
                               "600000.SH,20260231,20260113,,0.5,0,0,0,0\n"), 1);
}

// 对照：全 0 的可选日期列仍是「未提供」（严格 8 位不能把这个写法一起拒掉）。
TEST_F(SelCaFixture, ZeroOptionalDateStillMeansAbsent) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,5.0\n");
    write_file("ca.csv", std::string(kCaHeader10) +
               "600000.SH,20260112,20260113,0,0,1.0,0,0,0,0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0) << "pay_date/shares_list_date 写 0 被当成坏行拒了";

    // shares_list_date=0 = 未提供 → 到账日取 ex+1 交易日；ex 恰在窗末、已无次日 →
    // 到账落在窗外，新股整跑留在在途账（不进可卖仓）。
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    const Row* held = position_row(pos, "20260113", "600000.SH");
    ASSERT_NE(held, nullptr);
    EXPECT_EQ(held->at("qty"), "50000.0000")
        << "窗末无 ex+1 时退化成「ex 日即时到账」——凭空多出一批当日可卖股";
    ASSERT_NE(position_row(pos, "20260113", "RECEIVABLE"), nullptr)
        << "窗外到账的新股没进应收口径";
}

// 窗末 ex 日已无 ex+1 交易日：新股到账落在回测窗之外 —— 留在在途账整跑不到账，
// 并按 `shares_list_date_beyond_window` 披露。旧行为退化成「ex 日即时到账」，方向
// 乐观（不可卖窗口被当零 + 当日凭空多出可卖仓），是本钉要守的那条线。
TEST_F(SelCaFixture, WindowEndExDateKeepsSharesPendingBeyondWindow) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,5.0\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,,0,1.0,0,0,0\n");   // 十送十，无 list 日
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "shares_list_date_defaulted"), 1.0);
    EXPECT_EQ(summary_num(body, "shares_list_date_beyond_window"), 1.0)
        << "窗外到账的笔数未披露";
    EXPECT_EQ(summary_num(body, "shares_list_date_absent_immediate"), 0.0)
        << "窗末无 ex+1 被当成「ex 日即时到账」";
    // 在途新股整跑不到账：收尾在途市值 = 50,000 股 × 5.0。
    EXPECT_NEAR(summary_num(body, "receivable_outstanding_bonus"), 250'000.0, 0.01);
    EXPECT_EQ(summary_num(body, "equity_identity_violations"), 0.0);

    // 权益仍连续（在途股按 mark 计在应收里）：499,870 现金 + 250,000 仓 + 250,000 在途。
    const auto eq = read_csv(dir_ / "rep" / "daily_equity.csv");
    EXPECT_NEAR(equity_on(eq, "20260113"), 999'870.0, 0.01);

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    const Row* held = position_row(pos, "20260113", "600000.SH");
    ASSERT_NE(held, nullptr);
    EXPECT_EQ(held->at("qty"), "50000.0000");
    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    EXPECT_EQ(ca_event(evs, "shares_listed"), nullptr) << "窗外到账的新股仍入了仓";
}

// ── B6 fold 钉 ───────────────────────────────────────────────────────────────

// 在途新股必须同时进 sizing 的分子（权益）与分母侧的 pre_qty：ex 日调仓时目标按
// 「含在途股的权益」算，pre_qty 却只数已到账仓 → diff 把已经持有的那部分再买一遍。
//
// 20260112 买 50,000 股 @10（cash 499,870）→ 20260113 十送十（新股在途至 20260114）、
// 参考价 10→5，当日 weight=1.0：portfolio equity 999,870 → target 199,900 股，
// pre_qty 必须是 100,000（50,000 到账 + 50,000 在途）→ diff 99,900，成本 499,500
// 恰在现金内。pre_qty 漏在途股时 diff = 149,900、成本 749,500 > 现金 → 被可负担
// 截断（超买的直接证据）。
TEST_F(SelCaFixture, InFlightSharesCountInSizingPreQty) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,5.0\n"
               "20260114,600000.SH,5.0\n");
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,,0,1.0,0,0,0\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.5}}},
        {20260113u, {{"600000.SH", 1.0}}},
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto targets = read_csv(dir_ / "rep" / "target_weights.csv");
    const Row* d2 = nullptr;
    for (const auto& r : targets) {
        if (r.at("exec_date") == "20260113") { d2 = &r; break; }
    }
    ASSERT_NE(d2, nullptr);
    EXPECT_EQ(d2->at("pre_qty"), "100000.0000")
        << "pre_qty 漏了 ex→list 窗口内的在途新股（与权益口径分裂）";
    EXPECT_EQ(d2->at("target_qty"), "199900.0000");
    EXPECT_EQ(d2->at("diff_qty"), "99900.0000") << "diff 把已持有的在途股又买了一遍";
    EXPECT_EQ(d2->at("filled_qty"), "99900.0000");

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "affordability_caps"), 0.0)
        << "超买导致现金不足 → 落账口截断";
    EXPECT_EQ(summary_num(body, "equity_identity_violations"), 0.0);
    // 目标达成度：终仓（含次日到账的在途股）= 199,900，不多不少。
    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    const Row* d3 = position_row(pos, "20260114", "600000.SH");
    ASSERT_NE(d3, nullptr);
    EXPECT_EQ(d3->at("qty"), "199900.0000");
}

// 对称面：在途新股计进 pre_qty 后，SELL 腿的 diff 会超过可交割量 —— 必须截到账上
// 实际持仓（缺口下期顺延），而不是整腿提交被风控拒（那样连能卖的部分也卖不掉）。
//
// 50,000 股 + 1:1 在途红股（到账 20260116），20260114 清仓：pre_qty=100,000 →
// diff −100,000，可交割只有 50,000 → 截断 + 计数；剩下的在 20260119 到账后卖出。
TEST_F(SelCaFixture, SellLegClampedToDeliverableWhenSharesInFlight) {
    write_file("trading_dates.csv",
               "date\n20260112\n20260113\n20260114\n20260116\n20260119\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,5.0\n"
               "20260114,600000.SH,4.0\n"
               "20260116,600000.SH,4.0\n"
               "20260119,600000.SH,4.0\n");
    write_file("ca.csv", std::string(kCaHeader10) +
               "600000.SH,20260112,20260113,,0,1.0,0,0,0,20260116\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.5}}},
        {20260114u, {{"600000.SH", 0.0}}},   // 在途窗口内清仓
        {20260119u, {{"600000.SH", 0.0}}},   // 到账后清余下的
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto trades = read_csv(dir_ / "rep" / "trades.csv");
    ASSERT_EQ(trades.size(), 3u) << "超量 SELL 被整腿拒（退出意图被在途股卡死）";
    EXPECT_EQ(trades[1].at("qty"), "50000.0000") << "SELL 未截到可交割量";
    EXPECT_EQ(trades[2].at("qty"), "50000.0000");

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "pending_shares_sell_deferrals"), 1.0)
        << "SELL 截断未计数披露";
    EXPECT_EQ(summary_num(body, "risk_rejects"), 0.0)
        << "截断口径与风控可卖量脱节（整腿被拒）";
}

// 换股零股的成本必须按比例留下：零股腿已换成现金（cash_in_lieu），其成本再进新仓
// = 双计。极端形态 —— ratio 小到一股都换不出来时，全额迁移会把整笔旧成本灌进新
// code 已有仓的 avg（B 的 avg 20 → 30），后续卖出结出一笔凭空的 realized。
//
// A(100 股 @10，成本 1,000) + B(已有 100 股 @20) ，ratio 0.005 → 应换 0.5 股 →
// 整股 0 股、零股 0.5 股按 20 折现。B 的 avg 必须恒为 20。
TEST_F(SelCaFixture, ConversionOddLotCostDoesNotPolluteExistingPosition) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260109,000001.SZ,20.0\n"
               "20260112,600000.SH,10.0\n20260112,000001.SZ,20.0\n"
               "20260113,600000.SH,10.0\n20260113,000001.SZ,20.0\n"
               "20260114,600000.SH,10.0\n20260114,000001.SZ,20.0\n");
    write_file("ca.csv", std::string(kCaHeaderFull) +
               "600000.SH,,20260113,,0,0,0,0,0,,000001.SZ,0.005,20.0\n");
    MapWeightsStrategy stub({
        // 100 股 600000.SH（1,000 元）+ 100 股 000001.SZ（2,000 元）。
        {20260112u, {{"600000.SH", 0.001}, {"000001.SZ", 0.002}}},
        {20260114u, {{"000001.SZ", 0.0}}},   // 清仓 B，读 realized 判 avg
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, zero_fee_frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    const Row* b = position_row(pos, "20260113", "000001.SZ");
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(b->at("qty"), "100.0000") << "零股换出了整股";

    const auto body = read_text(dir_ / "rep" / "summary.json");
    // B 以 avg 20 清仓 @20 → realized 恰 0。avg 被灌成 30 时 realized = −1,000。
    EXPECT_NEAR(summary_num(body, "realized_pnl"), 0.0, 0.01)
        << "换股零股的成本进了新 code 的 avg（与已收的 cash_in_lieu 双计）";
    // 0.5 股 × 20 = 10 元现金替代。
    EXPECT_NEAR(summary_num(body, "conversion_cash_in_lieu"), 10.0, 0.01);
    EXPECT_EQ(summary_num(body, "equity_identity_violations"), 0.0);
}

// 旧仓为 0 的换股：conversion_out / conversion_in 必须成对发 —— 目标 code 只在 in
// 行的 code 列上，单发 out 时消费方不知道这只票换成了什么。
TEST_F(SelCaFixture, ZeroPositionConversionStillEmitsTargetCode) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260109,000001.SZ,10.0\n"
               "20260112,600000.SH,10.0\n20260112,000001.SZ,10.0\n"
               "20260113,600000.SH,10.0\n20260113,000001.SZ,10.0\n");
    write_file("ca.csv", std::string(kCaHeaderFull) +
               "600000.SH,,20260113,,0,0,0,0,0,,000001.SZ,2.0,0\n");
    // 只买 000001.SZ —— 换股的旧 code 600000.SH 全程零仓。
    MapWeightsStrategy stub({{20260112u, {{"000001.SZ", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, zero_fee_frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    const Row* out = ca_event(evs, "conversion_out", "600000.SH");
    ASSERT_NE(out, nullptr);
    EXPECT_EQ(out->at("qty_before"), "0.0000");
    const Row* in = ca_event(evs, "conversion_in", "000001.SZ");
    ASSERT_NE(in, nullptr) << "零仓换股只发了 out 行 —— 目标 code 无处可读";
    EXPECT_EQ(in->at("qty_before"), in->at("qty_after")) << "零仓换股改动了新 code 持仓";
}

// 换股闩锁后，同交易日更晚（raw ex 序）的送转行必须被整条拒：它按 record 日快照
// 给一只已换掉的 code 挂新股，到账日会把这只死 code 凭空复活。
//
// 20260113 换股（raw ex 20260113）+ 送转（raw ex 20260114，非交易日 → 归到 20260113，
// 施加序在换股之后）；送转的 record 日 20260112 有 50,000 股快照 → 未修复时会在
// 20260115 复活 50,000 股 600000.SH。
TEST_F(SelCaFixture, ConversionLatchRejectsLaterSameDayBonusLeg) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260115\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n20260109,000001.SZ,10.0\n"
               "20260112,600000.SH,10.0\n20260112,000001.SZ,10.0\n"
               "20260113,600000.SH,10.0\n20260113,000001.SZ,10.0\n"
               "20260115,600000.SH,10.0\n20260115,000001.SZ,10.0\n");
    write_file("ca.csv", std::string(kCaHeaderFull) +
               "600000.SH,,20260113,,0,0,0,0,0,,000001.SZ,1.0,0\n"
               "600000.SH,20260112,20260114,,0,1.0,0,0,0,,,0,0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    const auto cfg = make_cfg("rep", 1'000'000.0, zero_fee_frictions(), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    EXPECT_EQ(summary_num(body, "post_conversion_events_rejected"), 1.0)
        << "换股闩锁后的同 code 事件被放行";
    EXPECT_EQ(summary_num(body, "conversions"), 1.0);
    EXPECT_EQ(summary_num(body, "receivable_outstanding_bonus"), 0.0)
        << "已换股 code 上仍挂了在途新股";

    const auto pos = read_csv(dir_ / "rep" / "daily_positions.csv");
    EXPECT_EQ(position_row(pos, "20260115", "600000.SH"), nullptr)
        << "换掉的 code 在新股到账日复活了";
    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    EXPECT_EQ(ca_event(evs, "shares_listed", "600000.SH"), nullptr);
    EXPECT_EQ(ca_event(evs, "bonus_transfer", "600000.SH"), nullptr);
}

// CA 表头列序错位必须 fail-fast：只认首列 `code` 时数据行仍按下标取值，错位的列会
// 被读成另一个字段 —— 全程无报错、账本静默失真。
//
// 两个形态都刻意选**过得了全部逐值校验**的行（否则拒的是取值而不是表头，钉就空了）：
//   ① 现金分红 / 送转两列互换：0.5 元派现被读成 0.5 送转比例（除权因子全错）；
//   ② 换股比例 / 零股现金替代两列互换：2.0 的换股比例被读成 0.5（整仓静默缩水）。
TEST_F(SelCaBadRowFixture, ShuffledHeaderFailsFast) {
    const std::string cash_bonus_swapped =
        "code,record_date,ex_date,pay_date,bonus_transfer_per_share,cash_per_share,"
        "rights_per_share,rights_price,is_delist\n";
    EXPECT_EQ(run_with_ca_body(cash_bonus_swapped +
                               "600000.SH,20260112,20260113,,0.5,0.2,0,0,0\n"), 1)
        << "cash_per_share / bonus_transfer_per_share 互换的表头被放行";

    const std::string conv_swapped =
        "code,record_date,ex_date,pay_date,cash_per_share,bonus_transfer_per_share,"
        "rights_per_share,rights_price,is_delist,shares_list_date,"
        "conversion_to,cash_in_lieu,conversion_ratio\n";
    EXPECT_EQ(run_with_ca_body(conv_swapped +
                               "600000.SH,,20260113,,0,0,0,0,0,,000001.SZ,0.5,2.0\n"), 1)
        << "conversion_ratio / cash_in_lieu 互换的表头被放行（比例静默错读）";

    // 对照：正确列序的同形行必须放行（判据是列序，不是「有这些列就拒」）。
    EXPECT_EQ(run_with_ca_body(std::string(kCaHeader) +
                               "600000.SH,20260112,20260113,,0.2,0.5,0,0,0\n"), 0);
    EXPECT_EQ(run_with_ca_body(std::string(kCaHeaderFull) +
                               "600000.SH,,20260113,,0,0,0,0,0,,000001.SZ,2.0,0.5\n"), 0);
}

// 配股现金一股都缴不起 = 事实全额弃权：事件行必须发 `rights_cash_capped` 而不是
// `rights_subscribed` 零量行（后者与未自增的 rights_subscribed 计数自相矛盾）。
TEST_F(SelCaFixture, FullyCappedRightsEmitsCashCappedRow) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n");
    // 满仓建仓后剩余现金 ~740 元（99,900 股 @10 + 费）；配股价 5,000 元/股 →
    // 可缴整股数 = floor(740 / 5000) = 0，一股都认购不了。
    write_file("ca.csv", std::string(kCaHeader) +
               "600000.SH,20260112,20260113,,0,0,0.1,5000.0,0\n");
    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 1.0}}}});   // 满仓 → 现金见底
    const auto cfg = make_cfg("rep", 1'000'000.0,
                              frictions(0.0, 0.0, /*min_commission=*/5.0), "ca.csv");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    ASSERT_EQ(summary_num(body, "rights_cash_capped"), 1.0);
    ASSERT_EQ(summary_num(body, "rights_subscribed"), 0.0)
        << "一股都没认购却计了认购成功";

    const auto evs = read_csv(dir_ / "rep" / "corporate_action_events.csv");
    EXPECT_EQ(ca_event(evs, "rights_subscribed"), nullptr)
        << "全额截断仍发 rights_subscribed —— 与 rights_subscribed 计数口径相反";
    const Row* capped = ca_event(evs, "rights_cash_capped");
    ASSERT_NE(capped, nullptr);
    EXPECT_NEAR(std::stod(capped->at("cash_delta")), 0.0, 0.01);
}

// trace 行不得越界读：payload 超过发射点栈缓冲区时按缓冲区容量截断（snprintf 的
// 返回值是「本应写入」长度，直接拿它构造 string 会读到 buffer 之外）。
// 超长 strategy_id 把 sel_fire 的 task_uid 顶出 320 字节缓冲区。
TEST_F(SelCaFixture, OverlongTracePayloadIsTruncatedNotOverread) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n");
    const std::string long_sid(600, 'S');
    // strategy_id 走 task/strategy 两处（trace_task_uid 再拼一遍），远超缓冲区。
    std::ostringstream y;
    y << "selector:\n"
      << "  trading_dates_csv: " << (dir_ / "trading_dates.csv").string() << "\n"
      << "  close_prices_csv: " << (dir_ / "close_prices.csv").string() << "\n"
      << "  task:\n"
      << "    strategy_id: " << long_sid << "\n"
      << "    period: daily\n    time: 1500\n    strict_time: true\n"
      << "  strategy:\n    id: " << long_sid << "\n"
      << "account:\n  initial_cash: 1000000\n"
      << frictions()
      << "backtest:\n  trace_mode: full\n"
      << "report:\n  output_dir: " << (dir_ / "rep").string() << "\n";
    const std::string cfg_path = (dir_ / "long_cfg.yaml").string();
    { std::ofstream ofs(cfg_path); ofs << y.str(); }

    MapWeightsStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    ASSERT_EQ(run(cfg_path, &stub), 0);

    // sel_fire 的发射点缓冲区是 320 字节 → payload 必须被截到 319 以内。未 clamp 时
    // `std::string(buf, n)` 按 snprintf 的「本应写入」长度（这里 >600）复制，
    // 越过缓冲区尾部读到栈上的相邻字节，payload 因此长度超标且内容不可预测。
    const auto rows = read_events_full(dir_ / "rep" / "events_full.csv");
    ASSERT_FALSE(rows.empty());
    bool saw_fire = false;
    for (const auto& r : rows) {
        EXPECT_LT(r.payload.size(), 512u)
            << "payload 超出发射点栈缓冲区容量（越界读）: type=" << r.type;
        if (r.type == "sel_fire") {
            saw_fire = true;
            EXPECT_LT(r.payload.size(), 320u) << "sel_fire payload 越出 320B 缓冲区";
        }
    }
    EXPECT_TRUE(saw_fire) << "超长 payload 把 sel_fire 行整条吞了";
}

// 六段链的 order 侧必须带 fire_seq：同一 code 两次 fire 的 submit/trade 否则无法
// 归回各自的规划行（sel_target/sel_gate 只有 fire_seq，order 域只有 order_id）。
// gateway 未受理的腿走 sel_reject，不发虚构的 order_submit #0。
TEST_F(SelCaFixture, TraceBridgesFireSeqAndSeparatesRejects) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n"
               "20260114,600000.SH,10.0\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.2}}},
        {20260113u, {{"600000.SH", 0.4}}},
    });
    const auto cfg = make_cfg("rep", 1'000'000.0, frictions(), /*ca_file=*/"",
                              /*allow_skip_rights=*/false, /*selector_extra=*/"",
                              "backtest:\n  trace_mode: full\n");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto rows = read_events_full(dir_ / "rep" / "events_full.csv");
    ASSERT_FALSE(rows.empty());
    std::set<std::string> submit_fire_seqs;
    std::set<std::string> trade_fire_seqs;
    int64_t  prev_ts  = -1;
    uint64_t prev_seq = 0;
    for (const auto& r : rows) {
        // (ts, seq) 复合键严格单调（ts 单独只保证非降 —— 日末饱和后靠 seq 定序）。
        EXPECT_TRUE(r.ts > prev_ts || (r.ts == prev_ts && r.seq > prev_seq))
            << "(ts,seq) 复合键非严格单调: ts=" << r.ts << " seq=" << r.seq;
        prev_ts  = r.ts;
        prev_seq = r.seq;
        const std::string fs = payload_value(r.payload, "fire_seq");
        if (r.type == "order_submit") {
            ASSERT_FALSE(fs.empty()) << "order_submit 缺 fire_seq 桥";
            submit_fire_seqs.insert(fs);
            EXPECT_NE(payload_value(r.payload, "order_id"), "0")
                << "未受理腿被发成了 order_submit #0";
        } else if (r.type == "trade") {
            ASSERT_FALSE(fs.empty()) << "trade 缺 fire_seq 桥";
            trade_fire_seqs.insert(fs);
        }
    }
    // 两次 fire 各一腿 → submit / trade 都能按 fire_seq 分成两组。
    EXPECT_EQ(submit_fire_seqs.size(), 2u)
        << "同 code 两次 fire 的 submit 无法按 fire_seq 区分";
    EXPECT_EQ(trade_fire_seqs.size(), 2u)
        << "同 code 两次 fire 的 trade 无法按 fire_seq 区分";
}

// 风控拒的腿：零条 order_submit、每次拒各一条 sel_reject（合并成虚构订单 #0 的
// 反面钉）。max_order_qty 卡死两次 rebalance 的买入腿。
TEST_F(SelCaFixture, RiskRejectedLegsEmitRejectEventsNotOrderSubmit) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n"
               "20260114,600000.SH,10.0\n");
    MapWeightsStrategy stub({
        {20260112u, {{"600000.SH", 0.5}}},
        {20260113u, {{"600000.SH", 0.6}}},
    });
    // max_order_qty=100 → 5,000 / 1,000 股的两腿都被风控整单拒（order_id=0）。
    std::string fr =
        "commodities:\n"
        "  '600000.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0,"
        " min_lot: 100 }\n"
        "fees:\n"
        "  '600000.SH': { open: 0.00025, close: 0.00025, stamp_duty_rate: 0.0005,"
        " transfer_fee_rate: 0.00001, min_commission: 5.0 }\n"
        "  dividend_tax_rate: 0\n"
        "risk:\n"
        "  max_order_qty: 100.0\n"
        "  allow_short: false\n";
    const auto cfg = make_cfg("rep", 1'000'000.0, fr, /*ca_file=*/"",
                              /*allow_skip_rights=*/false, /*selector_extra=*/"",
                              "backtest:\n  trace_mode: full\n");
    ASSERT_EQ(run(cfg, &stub), 0);

    const auto body = read_text(dir_ / "rep" / "summary.json");
    ASSERT_EQ(summary_num(body, "local_submit_rejects"), 2.0)
        << "场景没真产生两次 gateway 拒（钉失去对照意义）";

    const auto rows = read_events_full(dir_ / "rep" / "events_full.csv");
    size_t n_submit = 0, n_reject = 0;
    for (const auto& r : rows) {
        if (r.type == "order_submit") ++n_submit;
        if (r.type == "sel_reject")   ++n_reject;
    }
    EXPECT_EQ(n_submit, 0u) << "未受理腿被发成 order_submit（多次拒合并成虚构订单 #0）";
    EXPECT_EQ(n_reject, 2u) << "拒绝腿没有各自的 sel_reject 行";
}

}  // namespace sel_b0b_ca
