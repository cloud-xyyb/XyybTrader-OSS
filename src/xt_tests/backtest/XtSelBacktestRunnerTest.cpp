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
#include <utility>
#include <vector>
#include <unistd.h>

#include <unordered_map>

#include "api/IStrategyContext.h"
#include "xt_backtest/sel/XtSelBacktestRunner.h"
#include "xt_backtest/sel/XtSelRunnerConfig.h"
#include "xt_strategy_engine/XtStrategyBase.h"
#include "xt_strategy_engine/loader/XtStrategyFactory.h"

namespace {

std::string make_temp_sel_yaml(const std::string& body) {
    char path[] = "/tmp/xt_sel_cfg_XXXXXX";
    const int fd = mkstemp(path);
    if (fd < 0) return {};
    close(fd);

    std::ofstream ofs(path, std::ios::out | std::ios::trunc);
    ofs << body;
    return std::string(path);
}

bool load_sel_from_yaml_args(const std::string& yaml_body,
                             const std::vector<std::string>& cli_args,
                             xt_bt_sel::XtSelRunnerConfig& out,
                             std::string& err) {
    const std::string path = make_temp_sel_yaml(yaml_body);
    if (path.empty()) {
        err = "mkstemp failed";
        return false;
    }

    std::vector<std::string> args;
    args.push_back("test");
    const std::string token = "{config}";
    for (const auto& arg : cli_args) {
        std::string resolved = arg;
        const size_t pos = resolved.find(token);
        if (pos != std::string::npos) {
            resolved.replace(pos, token.size(), path);
        }
        args.push_back(std::move(resolved));
    }

    std::vector<char*> argv;
    argv.reserve(args.size());
    for (auto& arg : args) argv.push_back(arg.data());
    const bool ok = xt_bt_sel::load_sel_config(static_cast<int>(argv.size()), argv.data(), out, err);
    std::remove(path.c_str());
    return ok;
}

std::string minimal_sel_yaml(const std::string& period = "daily") {
    return
        "selector:\n"
        "  trading_dates_csv: data/regression/sel_demo/trading_dates.csv\n"
        "  task:\n"
        "    strategy_id: sel_demo\n"
        "    period: " + period + "\n"
        "  strategy:\n"
        "    id: sel_demo\n"
        "    so: var/plugins/strategies/libsel_weights_csv_strategy.so\n";
}

}  // namespace

TEST(XtSelRunnerConfig, AcceptsLongConfigFlag) {
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    ASSERT_TRUE(load_sel_from_yaml_args(minimal_sel_yaml(), {"--config", "{config}"}, cfg, err)) << err;
    ASSERT_EQ(cfg.tasks.size(), 1u);
    EXPECT_EQ(cfg.tasks[0].period, TaskPeriod::Daily);
}

TEST(XtSelRunnerConfig, AcceptsConfigEqualsFlag) {
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    ASSERT_TRUE(load_sel_from_yaml_args(minimal_sel_yaml(), {"--config={config}"}, cfg, err)) << err;
    EXPECT_EQ(cfg.strategies.size(), 1u);
    EXPECT_EQ(cfg.strategies[0].strategy_id, "sel_demo");
}

TEST(XtSelRunnerConfig, AcceptsShortConfigFlag) {
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    ASSERT_TRUE(load_sel_from_yaml_args(minimal_sel_yaml("weekly"), {"-c", "{config}"}, cfg, err)) << err;
    ASSERT_EQ(cfg.tasks.size(), 1u);
    EXPECT_EQ(cfg.tasks[0].period, TaskPeriod::Weekly);
}

TEST(XtSelRunnerConfig, RejectsUnknownTaskPeriod) {
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    EXPECT_FALSE(load_sel_from_yaml_args(minimal_sel_yaml("fortnightly"), {"{config}"}, cfg, err));
    EXPECT_NE(err.find("selector.task.period"), std::string::npos) << err;
}

TEST(XtSelRunnerConfig, RejectsMissingTaskPeriod) {
    const std::string yaml =
        "selector:\n"
        "  trading_dates_csv: data/regression/sel_demo/trading_dates.csv\n"
        "  task:\n"
        "    strategy_id: sel_demo\n"
        "  strategy:\n"
        "    id: sel_demo\n"
        "    so: var/plugins/strategies/libsel_weights_csv_strategy.so\n";
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    EXPECT_FALSE(load_sel_from_yaml_args(yaml, {"{config}"}, cfg, err));
    EXPECT_NE(err.find("selector.task.period is required"), std::string::npos) << err;
}

TEST(XtSelRunnerConfig, RejectsMisspelledTaskPeriodKey) {
    const std::string yaml =
        "selector:\n"
        "  trading_dates_csv: data/regression/sel_demo/trading_dates.csv\n"
        "  task:\n"
        "    strategy_id: sel_demo\n"
        "    peroid: daily\n"
        "  strategy:\n"
        "    id: sel_demo\n"
        "    so: var/plugins/strategies/libsel_weights_csv_strategy.so\n";
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    EXPECT_FALSE(load_sel_from_yaml_args(yaml, {"{config}"}, cfg, err));
    // schema 校验先于 parse 兜住 typo，错误直接点名坏 key。
    EXPECT_NE(err.find("unknown config key: selector.task.peroid"), std::string::npos) << err;
}

TEST(XtSelRunnerConfig, RejectsMissingTaskSection) {
    const std::string yaml =
        "selector:\n"
        "  trading_dates_csv: data/regression/sel_demo/trading_dates.csv\n"
        "  strategy:\n"
        "    id: sel_demo\n"
        "    so: var/plugins/strategies/libsel_weights_csv_strategy.so\n";
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    EXPECT_FALSE(load_sel_from_yaml_args(yaml, {"{config}"}, cfg, err));
    EXPECT_NE(err.find("selector.task is required"), std::string::npos) << err;
}

// ── D3: so 允许空（external strategy 注入）/ id 仍必填 ──────────────────

TEST(XtSelRunnerConfig, AcceptsEmptyStrategySoForExternalInjection) {
    const std::string yaml =
        "selector:\n"
        "  trading_dates_csv: data/regression/sel_demo/trading_dates.csv\n"
        "  task:\n"
        "    strategy_id: sel_demo\n"
        "    period: daily\n"
        "  strategy:\n"
        "    id: sel_demo\n";
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    ASSERT_TRUE(load_sel_from_yaml_args(yaml, {"{config}"}, cfg, err)) << err;
    ASSERT_EQ(cfg.strategies.size(), 1u);
    EXPECT_TRUE(cfg.strategies[0].lib_path.empty());
    EXPECT_EQ(cfg.strategies[0].strategy_id, "sel_demo");
}

TEST(XtSelRunnerConfig, RejectsEmptyStrategyIdEvenWithSo) {
    const std::string yaml =
        "selector:\n"
        "  trading_dates_csv: data/regression/sel_demo/trading_dates.csv\n"
        "  task:\n"
        "    strategy_id: sel_demo\n"
        "    period: daily\n"
        "  strategy:\n"
        "    so: var/plugins/strategies/libsel_weights_csv_strategy.so\n";
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    EXPECT_FALSE(load_sel_from_yaml_args(yaml, {"{config}"}, cfg, err));
    EXPECT_NE(err.find("selector.strategy.id missing"), std::string::npos) << err;
}

TEST(XtSelRunnerConfig, RejectsUnknownCliFlagBeforePositionalConfig) {
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    EXPECT_FALSE(load_sel_from_yaml_args(minimal_sel_yaml(), {"--dry-run", "{config}"}, cfg, err));
    EXPECT_NE(err.find("usage: XtSelBacktestRunner"), std::string::npos) << err;
}

// ── schema fail-fast regression ────────────────────────────────────────

// wt-parity 配套节（env/replayer/sel）由外部对照工具消费，schema 整子树放行。
TEST(XtSelRunnerConfig, SchemaAcceptsWtParityForeignSections) {
    const std::string yaml = minimal_sel_yaml() +
        "env:\n"
        "  mocker: wtbtcpp\n"
        "replayer:\n"
        "  mode: csv\n"
        "  basefiles:\n"
        "    session: sessions.json\n"
        "sel:\n"
        "  module: wtpy\n";
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    ASSERT_TRUE(load_sel_from_yaml_args(yaml, {"{config}"}, cfg, err)) << err;
    EXPECT_EQ(cfg.strategies.size(), 1u);
}

TEST(XtSelRunnerConfig, SchemaRejectsUnknownTopLevelKey) {
    const std::string yaml = minimal_sel_yaml() +
        "selectorr:\n"
        "  foo: 1\n";
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    EXPECT_FALSE(load_sel_from_yaml_args(yaml, {"{config}"}, cfg, err));
    EXPECT_NE(err.find("unknown config key: selectorr"), std::string::npos) << err;
}

TEST(XtSelRunnerConfig, SchemaRejectsUnknownSelectorKey) {
    const std::string yaml = minimal_sel_yaml() +
        "account:\n"
        "  initial_cashh: 1000000\n";
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    EXPECT_FALSE(load_sel_from_yaml_args(yaml, {"{config}"}, cfg, err));
    EXPECT_NE(err.find("unknown config key: account.initial_cashh"), std::string::npos) << err;
}

// ── tradability + per-symbol slippage e2e（真跑 XtSelBacktestRunner::run）────
//
// fixture: 2 只股票 × 4 交易日，daily rebalance（weights 每日必产生 600000.SH 与
// 000001.SZ 各一腿）。tradability 场景：D2 停牌 600000、D3 涨停 000001（拒 BUY）、
// D4 跌停 600000（拒 SELL）。断言面：
//   - trades.csv 腿的存在/缺失（gate 生效面）
//   - tradability_events.csv 精确行（date,code,side,reason）
//   - summary.json 全文精确断言（对照组 = 无 tradability 的唯一字节面见证）
//   - per-symbol slippage（600000 走 commodities 覆盖 0.05，000001 回退全局 0.02）
//   - trades.csv.equity_after 逐笔 running equity（T1.132 ③，非统一填 final）
//   - daily_equity.csv 逐日收盘权益（T1.131，与 CTA 同 schema）
//
// final_equity/initial_cash 现走 %.2f 全精度（T1.132 ①），token 仍"文件内提取 + 回填"
// 进 summary 期望串（数值正确性另由 kExpected*FinalEquity 手算锚定）；equity_after 由
// constant-marks（10/20）+ 0 fee 的逐笔 replay 复算校验（fixture 全确定），daily_equity
// 每行则等于该日最后一笔 fill 的 equity_after（marks 恒定、无空仓日）。

namespace sel_e2e {

/// @brief 环境变量 RAII：SetUp 设 plugin 安全 env，TearDown 恢复原值。
/// @note 禁 copy/move：析构副作用（unsetenv）在容器扩容 move 后会提前触发，
///       持有方必须用 unique_ptr 间接存储。
class ScopedEnv {
public:
    ScopedEnv(const char* name, const char* value) : name_(name) {
        if (const char* old = ::getenv(name)) {
            had_old_ = true;
            old_ = old;
        }
        ::setenv(name, value, 1);
    }
    ~ScopedEnv() {
        if (had_old_) ::setenv(name_.c_str(), old_.c_str(), 1);
        else          ::unsetenv(name_.c_str());
    }
    ScopedEnv(const ScopedEnv&)            = delete;
    ScopedEnv& operator=(const ScopedEnv&) = delete;

private:
    std::string name_;
    std::string old_;
    bool had_old_ = false;
};

/// @brief 解析后的 trades.csv 行（include_strategy_id=true schema，12 列）。
struct TradeRow {
    std::string date, code, direction, offset, qty, exec_price, fee, slippage, equity_after;
};

std::vector<std::string> split_line(const std::string& line) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : line) {
        if (c == ',') { out.push_back(cur); cur.clear(); }
        else if (c != '\r') cur.push_back(c);
    }
    out.push_back(cur);
    return out;
}

std::string read_text(const std::filesystem::path& p) {
    std::ifstream ifs(p);
    std::stringstream ss;
    ss << ifs.rdbuf();
    return ss.str();
}

std::vector<TradeRow> parse_trades_csv(const std::filesystem::path& p) {
    std::vector<TradeRow> out;
    std::ifstream ifs(p);
    std::string line;
    bool header = true;
    while (std::getline(ifs, line)) {
        if (header) { header = false; continue; }
        if (line.empty()) continue;
        const auto f = split_line(line);
        if (f.size() < 12) continue;
        TradeRow r;
        r.date = f[1]; r.code = f[3]; r.direction = f[4]; r.offset = f[5];
        r.qty = f[6]; r.exec_price = f[7]; r.fee = f[8]; r.slippage = f[9];
        r.equity_after = f[11];
        out.push_back(std::move(r));
    }
    return out;
}

/// @brief 按 (date,code) 取行；断言面用。
std::vector<TradeRow> rows_for(const std::vector<TradeRow>& rows,
                               const std::string& date, const std::string& code) {
    std::vector<TradeRow> out;
    for (const auto& r : rows) {
        if (r.date == date && r.code == code) out.push_back(r);
    }
    return out;
}

/// @brief repo root 解析：env XT_REPO_ROOT 优先，否则由 __FILE__ 推导
/// （本文件 = <root>/src/xt_tests/backtest/…；out-of-tree build 下 __FILE__ 为绝对路径）。
std::filesystem::path resolve_repo_root() {
    if (const char* env = ::getenv("XT_REPO_ROOT"); env && *env) return env;
    std::filesystem::path self(__FILE__);
    if (!self.is_absolute()) return {};
    // backtest/ → xt_tests/ → src/ → <root>
    return self.parent_path().parent_path().parent_path().parent_path();
}

/// @brief e2e fixture：临时目录 + 数据 CSV + plugin 安全 env（TearDown 全清理）。
class SelTradabilityE2E : public ::testing::Test {
protected:
    void SetUp() override {
        const auto root = resolve_repo_root();
        plugin_so_ = root / "var/plugins/strategies/libsel_weights_csv_strategy.so";
        if (root.empty() || !std::filesystem::exists(plugin_so_)) {
            GTEST_SKIP() << "libsel_weights_csv_strategy.so 不在位（未 git-track 资产，"
                            "fresh worktree 需从主仓 copy）: " << plugin_so_;
        }

        char tmpl[] = "/tmp/xt_sel_e2e_XXXXXX";
        ASSERT_NE(mkdtemp(tmpl), nullptr);
        dir_ = tmpl;

        // plugin 安全 env 对齐 regression（loader: sidecar 存在必校验；allowlist 按 env）。
        env_.push_back(std::make_unique<ScopedEnv>(
            "CLOUDTRADER_PLUGIN_ALLOWLIST", "libsel_weights_csv_strategy.so"));
        env_.push_back(std::make_unique<ScopedEnv>("CLOUDTRADER_PLUGIN_ALLOWLIST_ENFORCE", "1"));
        env_.push_back(std::make_unique<ScopedEnv>("CLOUDTRADER_PLUGIN_STRICT_SIGNING", "1"));

        // preflight：test 进程 XtLogger 未初始化，runner 内 loader 错误会被静默吞掉；
        // 这里用独立 factory 预载 plugin，把 last_error 直接暴露进断言消息。
        {
            XtStrategyFactory probe;
            ASSERT_TRUE(probe.register_plugin("preflight", plugin_so_.string()))
                << "plugin preflight load failed: " << probe.last_error();
        }

        write_file("trading_dates.csv",
                   "date\n20260112\n20260113\n20260114\n20260115\n");
        // 20260109 = warmup T-1 行（非 trading date）：首日 20260112 有 fire，
        // 无 warmup 时 runner 现按 rc=8 fail-fast（拒绝 T 日 close lookahead）。
        // 值与 D1 close 相同（常数价）→ 全部手算期望不变。
        write_file("close_prices.csv",
                   "date,code,close\n"
                   "20260109,600000.SH,10.0\n20260109,000001.SZ,20.0\n"
                   "20260112,600000.SH,10.0\n20260112,000001.SZ,20.0\n"
                   "20260113,600000.SH,10.0\n20260113,000001.SZ,20.0\n"
                   "20260114,600000.SH,10.0\n20260114,000001.SZ,20.0\n"
                   "20260115,600000.SH,10.0\n20260115,000001.SZ,20.0\n");
        // 每日两腿：600000 递减（D2 起 SELL 腿）、000001 递增（BUY 腿）。
        write_file("weights.csv",
                   "date,symbol,weight\n"
                   "20260112,600000.SH,0.30\n20260112,000001.SZ,0.30\n"
                   "20260113,600000.SH,0.20\n20260113,000001.SZ,0.40\n"
                   "20260114,600000.SH,0.15\n20260114,000001.SZ,0.50\n"
                   "20260115,600000.SH,0.05\n20260115,000001.SZ,0.55\n");
        write_file("tradability.csv",
                   "date,code,is_open,is_limit_up,is_limit_down\n"
                   "20260112,600000.SH,1,0,0\n20260112,000001.SZ,1,0,0\n"
                   "20260113,600000.SH,0,0,0\n20260113,000001.SZ,1,0,0\n"
                   "20260114,600000.SH,1,0,0\n20260114,000001.SZ,1,1,0\n"
                   "20260115,600000.SH,1,0,1\n20260115,000001.SZ,1,0,0\n");
    }

    void TearDown() override {
        env_.clear();  // 先恢复 env
        if (!dir_.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(dir_, ec);  // 测试临时文件规范：结束即清理
        }
    }

    void write_file(const std::string& name, const std::string& body) const {
        std::ofstream ofs(dir_ / name);
        ofs << body;
    }

    /// @brief 生成 config yaml 并跑 runner；with_tradability 控制 gate 开关。
    /// @param tradability_path_override 非空时替代默认 tradability.csv 路径
    ///        （fail-fast case 用，指向不存在文件）。
    int run_sel(bool with_tradability, const std::string& report_subdir,
                const std::string& tradability_path_override = {}) const {
        const std::string cfg_path = (dir_ / (report_subdir + "_cfg.yaml")).string();
        std::ostringstream y;
        y << "selector:\n"
          << "  trading_dates_csv: " << (dir_ / "trading_dates.csv").string() << "\n"
          << "  close_prices_csv: " << (dir_ / "close_prices.csv").string() << "\n";
        if (with_tradability) {
            const std::string trad_path = tradability_path_override.empty()
                ? (dir_ / "tradability.csv").string()
                : tradability_path_override;
            y << "  tradability_csv: " << trad_path << "\n";
        }
        y << "  task:\n"
          << "    strategy_id: sel_trad_e2e\n"
          << "    period: daily\n"
          << "    time: 1500\n"
          << "    strict_time: true\n"
          << "  strategy:\n"
          << "    so: " << plugin_so_.string() << "\n"
          << "    id: sel_trad_e2e\n"
          << "    params:\n"
          << "      weights_path: " << (dir_ / "weights.csv").string() << "\n"
          << "      normalize: false\n"
          << "account:\n"
          << "  initial_cash: 500000.0\n"
          // per-symbol slippage：600000 覆盖 0.05；000001 未设 → 回退全局 0.02。
          << "commodities:\n"
          << "  '600000.SH': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0, min_lot: 100, slippage: 0.05 }\n"
          << "  '000001.SZ': { price_tick: 0.01, vol_scale: 1.0, margin_rate: 0.0, min_lot: 100 }\n"
          << "trade_cost:\n"
          << "  slippage: 0.02\n"
          << "report:\n"
          << "  output_dir: " << (dir_ / report_subdir).string() << "\n";
        {
            std::ofstream ofs(cfg_path);
            ofs << y.str();
        }

        xt_bt_sel::XtSelRunnerConfig cfg;
        std::string err;
        std::string arg0 = "test";
        std::string arg1 = cfg_path;
        char* argv[] = {arg0.data(), arg1.data()};
        if (!xt_bt_sel::load_sel_config(2, argv, cfg, err)) {
            ADD_FAILURE() << "load_sel_config failed: " << err;
            return -1;
        }
        return xt_bt_sel::XtSelBacktestRunner::run(cfg);
    }

    /// @brief 从 summary.json 文本提取 final_equity 的原始 token（到 ',' 或 '\n' 为止）。
    static std::string extract_final_equity_token(const std::string& text) {
        const std::string key = "\"final_equity\": ";
        const auto pos = text.find(key);
        if (pos == std::string::npos) return {};
        const auto start = pos + key.size();
        const auto end = text.find_first_of(",\n", start);
        return text.substr(start, end - start);
    }

    /// @brief 逐字节构造期望 summary.json（复刻 runner 写出格式；tradability 块可选）。
    std::string build_expected_summary(size_t trade_rows,
                                       const std::string& final_equity_token,
                                       bool with_tradability,
                                       const std::string& residual_cash,
                                       const std::string& realized_pnl,
                                       size_t n_fires = 4,
                                       size_t n_skips = 0) const {
        std::ostringstream out;
        out << "{\n";
        out << "  \"engine\": \"sel\",\n";
        out << "  \"data_source\": \"csv:" << (dir_ / "trading_dates.csv").string() << "\",\n";
        out << "  \"total_trading_dates\": 4,\n";
        out << "  \"trade_rows\": " << trade_rows << ",\n";
        out << "  \"initial_cash\": 500000.00,\n";   // %.2f 全精度（T1.132 ①）
        out << "  \"final_equity\": " << final_equity_token << ",\n";
        // B0b 追加：累计已实现盈亏（除权除息会调均价，归因正确性只在这里可见）。
        out << "  \"realized_pnl\": " << realized_pnl << ",\n";
        out << "  \"risk_rejects\": 0,\n";              // T1.132 ②：fixture 无风控拒单
        out << "  \"sparse_weights_events\": 0,\n";     // T1.135：weights 恒覆盖 2 == held 2
        // BR-03：回报消费完整性 + 收尾残留冻结，正常路径均为 0。
        out << "  \"undrained_reports\": 0,\n";
        out << "  \"frozen_cash_at_exit\": 0.00,\n";
        // B0 追加三字段：mark carry 披露 + 调度统计（fixture 每日均有价，carry=0；
        // daily task 4 日全触发且全产出调仓 → n_fires=4 / n_skips=0）。
        out << "  \"stale_mark_carry_events\": 0,\n";
        out << "  \"n_fires\": " << n_fires << ",\n";
        out << "  \"n_skips\": " << n_skips << ",\n";
        // 深挖 fold 追加观测：fixture 现金充裕、无负权重、无 min_lot 截零 →
        // 前五项恒 0；residual_cash 由手算现金推导（见 kExpected*FinalEquity 注释）。
        out << "  \"affordability_caps\": 0,\n";
        out << "  \"affordability_shortfall\": 0.00,\n";
        out << "  \"negative_cash_events\": 0,\n";
        out << "  \"min_lot_zero_targets\": 0,\n";
        out << "  \"negative_weight_rejects\": 0,\n";
        // B0b 追加：Σ持仓市值 + 现金 + 应收 == portfolio 权益（fp 精确），恒 0。
        out << "  \"equity_identity_violations\": 0,\n";
        // 引擎总批 fold 追加：SELL 费用可负担闸 + pending 语义四计数。fixture 现金
        // 充裕、每日单 task 单 target、无跨日残留 → 五项恒 0。
        out << "  \"sell_fee_unaffordable_rejects\": 0,\n";
        out << "  \"local_submit_rejects\": 0,\n";
        out << "  \"pending_dispatch_noops\": 0,\n";
        out << "  \"pending_dispatch_direction_flips\": 0,\n";
        out << "  \"cross_sid_same_code_pendings\": 0,\n";
        out << "  \"stale_pending_groups_dropped\": 0,\n";
        out << "  \"residual_cash\": " << residual_cash;
        if (with_tradability) {
            out << ",\n";
            out << "  \"tradability\": {\n";
            out << "    \"source\": \"" << (dir_ / "tradability.csv").string() << "\",\n";
            out << "    \"suspended_skips\": 1,\n";
            out << "    \"limit_up_buy_rejects\": 1,\n";
            out << "    \"limit_down_sell_rejects\": 1\n";
            out << "  }\n";
        } else {
            out << "\n";
        }
        out << "}\n";
        return out.str();
    }

    // ── final_equity 手算精确期望 ─────────────────────────────────────────
    //
    // 共同口径（fixture 全确定）：股票模式 equity = cash + Σ(qty × mark)，mark 恒 =
    // close（600000=10、000001=20）；fee=0；exec = close ± slip（BUY +/SELL −；
    // 600000 per-symbol slip 0.05、000001 回退全局 0.02）；target = weight × equity(fire
    // 时) / close，向 0 floor 到整 lot(100)（XtSelEngine round_to_min_lot_fp）。
    //
    // 对照组（无 gate，4 日 × 两腿全成交）：
    //   D1: eq=500000 → tgt 0.30×500000/10=15000、0.30×500000/20=7500
    //       cash = 500000 − 15000×10.05 − 7500×20.02 = 199100；eq = 199100+150000+150000 = 499100
    //   D2: tgt 0.20×499100/10=9982→9900、0.40×499100/20=9982→9900
    //       SELL 5100×9.95=+50745、BUY 2400×20.02=−48048 → cash=201797；eq=201797+99000+198000=498797
    //   D3: tgt 0.15×498797/10=7481.955→7400、0.50×498797/20=12469.925→12400
    //       SELL 2500×9.95=+24875、BUY 2500×20.02=−50050 → cash=176622；eq=176622+74000+248000=498622
    //   D4: tgt 0.05×498622/10=2493.11→2400、0.55×498622/20=13712.105→13700
    //       SELL 5000×9.95=+49750、BUY 1300×20.02=−26026 → cash=200346
    //   final = 200346 + 2400×10 + 13700×20 = 498346.00
    //
    // gate 组（D2 停牌 600000 / D3 涨停 000001 / D4 跌停 600000）：
    //   D1: 同对照组 → cash=199100，pos 15000/7500，eq=499100
    //   D2: tgt 9900/9900 → SELL 5100 gated；BUY 2400×20.02=−48048 → cash=151052
    //       eq = 151052+150000+198000 = 499052
    //   D3: tgt 0.15×499052/10=7485.78→7400、0.50×499052/20=12476.3→12400
    //       BUY 2500 gated；SELL 15000−7400=7600×9.95=+75620 → cash=226672
    //       eq = 226672+74000+198000 = 498672
    //   D4: tgt 0.05×498672/10=2493.36→2400、0.55×498672/20=13713.48→13700
    //       SELL 5000 gated；BUY 13700−9900=3800×20.02=−76076 → cash=150596
    //   final = 150596 + 7400×10 + 13700×20 = 498596.00
    static constexpr double kExpectedCtrlFinalEquity = 498346.0;
    static constexpr double kExpectedGateFinalEquity = 498596.0;

    /// @brief final_equity token 与 trades.csv equity_after 端点对齐（T1.132 ③ 后语义）。
    /// @note equity_after 现为真逐笔 running equity：末行 == final（最后一笔 fill 后即终值），
    ///       首行 != final（证明不再是旧「统一填 final」bug）。
    static void expect_equity_pinned(const std::string& token,
                                     const std::vector<TradeRow>& rows,
                                     double expected) {
        ASSERT_FALSE(token.empty());
        ASSERT_FALSE(rows.empty());
        EXPECT_NEAR(std::stod(token), expected, 0.01) << "token=" << token;
        EXPECT_NEAR(std::stod(rows.back().equity_after), expected, 0.01)
            << "back equity_after=" << rows.back().equity_after;
        if (rows.size() > 1) {
            EXPECT_GT(std::abs(std::stod(rows.front().equity_after) - expected), 0.01)
                << "首行 equity_after 不应等于 final（应为逐笔 running）: "
                << rows.front().equity_after;
        }
    }

    /// @brief 逐笔 running equity 一致性 oracle（T1.132 ③）。constant marks（600000=10 /
    ///        000001=20）+ 0 fee，A 股全额模式 equity = cash + Σ(qty × mark)；exec_price 已含
    ///        滑点。file 行序 == fill 应用序，逐行 replay 与 equity_after 对齐（±0.01）。
    static void expect_running_equity_consistent(const std::vector<TradeRow>& rows) {
        const std::unordered_map<std::string, double> mark = {
            {"600000.SH", 10.0}, {"000001.SZ", 20.0}};
        double cash = 500000.0;   // fixture account.initial_cash
        std::unordered_map<std::string, double> pos;
        for (const auto& r : rows) {
            const double qty        = std::stod(r.qty);
            const double exec       = std::stod(r.exec_price);
            const double signed_qty = (r.direction == "BUY") ? qty : -qty;
            cash -= signed_qty * exec;   // BUY 花钱 / SELL 回款（fixture 无 fee）
            pos[r.code] += signed_qty;
            double eq = cash;
            for (const auto& kv : pos) {
                const auto it = mark.find(kv.first);
                ASSERT_NE(it, mark.end()) << kv.first;
                eq += kv.second * it->second;
            }
            EXPECT_NEAR(std::stod(r.equity_after), eq, 0.01)
                << "running equity mismatch: " << r.date << " " << r.code
                << " " << r.direction;
        }
    }

    /// @brief daily_equity.csv 断言（T1.131）：schema `date,equity` + 4 交易日有序 + 逐日值
    ///        == 该日最后一笔 fill 的 equity_after（marks 恒定、每日均有成交）+ 末日 == final。
    static void expect_daily_equity_consistent(const std::filesystem::path& report,
                                               const std::vector<TradeRow>& trades,
                                               double final_equity) {
        std::ifstream ifs(report / "daily_equity.csv");
        ASSERT_TRUE(ifs.good()) << "daily_equity.csv 缺失";
        std::string line;
        ASSERT_TRUE(static_cast<bool>(std::getline(ifs, line)));
        while (!line.empty() && line.back() == '\r') line.pop_back();
        EXPECT_EQ(line, "date,equity");   // schema
        std::vector<std::pair<std::string, std::string>> daily;
        while (std::getline(ifs, line)) {
            if (line.empty()) continue;
            const auto f = split_line(line);
            ASSERT_EQ(f.size(), 2u) << line;
            daily.emplace_back(f[0], f[1]);
        }
        ASSERT_EQ(daily.size(), 4u);
        const char* dates[4] = {"20260112", "20260113", "20260114", "20260115"};
        std::unordered_map<std::string, std::string> last_eq;  // date → 当日末笔 equity_after
        for (const auto& r : trades) last_eq[r.date] = r.equity_after;
        for (size_t i = 0; i < 4; ++i) {
            EXPECT_EQ(daily[i].first, dates[i]);
            const auto it = last_eq.find(daily[i].first);
            ASSERT_NE(it, last_eq.end()) << daily[i].first;
            EXPECT_NEAR(std::stod(daily[i].second), std::stod(it->second), 0.01)
                << "daily equity != 当日末笔 equity_after: " << daily[i].first;
        }
        EXPECT_NEAR(std::stod(daily.back().second), final_equity, 0.01);
    }

    std::filesystem::path dir_;
    std::filesystem::path plugin_so_;
    std::vector<std::unique_ptr<ScopedEnv>> env_;
};

TEST_F(SelTradabilityE2E, GateBlocksSuspendedAndLimitLegs) {
    ASSERT_EQ(run_sel(/*with_tradability=*/true, "report_gate"), 0);

    const auto report = dir_ / "report_gate";
    const auto rows = parse_trades_csv(report / "trades.csv");
    ASSERT_EQ(rows.size(), 5u);  // D1 两腿 + D2/D3/D4 各一腿（另一腿被 gate）

    // D1：两腿正常成交；equity=initial 精确 → qty 可全值断言
    // （0.30×500000/10=15000、0.30×500000/20=7500，min_lot=100 整倍）。
    {
        const auto d1_600000 = rows_for(rows, "20260112", "600000.SH");
        ASSERT_EQ(d1_600000.size(), 1u);
        EXPECT_EQ(d1_600000[0].direction, "BUY");
        EXPECT_EQ(d1_600000[0].offset, "OPEN");
        EXPECT_EQ(d1_600000[0].qty, "15000.0000");
        const auto d1_000001 = rows_for(rows, "20260112", "000001.SZ");
        ASSERT_EQ(d1_000001.size(), 1u);
        EXPECT_EQ(d1_000001[0].direction, "BUY");
        EXPECT_EQ(d1_000001[0].qty, "7500.0000");
    }
    // D2 停牌 600000：该 code 当日无任何腿；000001 BUY 正常。
    EXPECT_TRUE(rows_for(rows, "20260113", "600000.SH").empty());
    {
        const auto d2 = rows_for(rows, "20260113", "000001.SZ");
        ASSERT_EQ(d2.size(), 1u);
        EXPECT_EQ(d2[0].direction, "BUY");
    }
    // D3 涨停 000001：BUY 被拒；600000 SELL 正常。
    EXPECT_TRUE(rows_for(rows, "20260114", "000001.SZ").empty());
    {
        const auto d3 = rows_for(rows, "20260114", "600000.SH");
        ASSERT_EQ(d3.size(), 1u);
        EXPECT_EQ(d3[0].direction, "SELL");
        EXPECT_EQ(d3[0].offset, "CLOSE");
    }
    // D4 跌停 600000：SELL 被拒；000001 BUY 正常（cash 冗余，不依赖被 gate 的回款）。
    EXPECT_TRUE(rows_for(rows, "20260115", "600000.SH").empty());
    {
        const auto d4 = rows_for(rows, "20260115", "000001.SZ");
        ASSERT_EQ(d4.size(), 1u);
        EXPECT_EQ(d4[0].direction, "BUY");
    }

    // events CSV：3 行整行精确匹配。qty 由 gate 组推导（见 kExpectedGateFinalEquity
    // 推导注释）：D2 SELL 15000−9900=5100；D3 BUY 12400−9900=2500；D4 SELL 7400−2400=5000。
    const auto ev_text = read_text(report / "tradability_events.csv");
    std::vector<std::string> ev_lines;
    {
        std::istringstream iss(ev_text);
        std::string line;
        while (std::getline(iss, line)) {
            if (!line.empty()) ev_lines.push_back(line);
        }
    }
    ASSERT_EQ(ev_lines.size(), 4u) << ev_text;
    EXPECT_EQ(ev_lines[0], "date,code,side,reason,qty");
    const char* expected_lines[3] = {
        "20260113,600000.SH,SELL,suspended,5100.0000",
        "20260114,000001.SZ,BUY,limit_up,2500.0000",
        "20260115,600000.SH,SELL,limit_down,5000.0000",
    };
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(ev_lines[static_cast<size_t>(i) + 1], expected_lines[i]);
    }

    // summary.json 全文精确断言（tradability 块 + 三计数器 = 1/1/1）。
    const auto summary = read_text(report / "summary.json");
    const auto token = extract_final_equity_token(summary);
    expect_equity_pinned(token, rows, kExpectedGateFinalEquity);
    expect_running_equity_consistent(rows);                              // T1.132 ③
    expect_daily_equity_consistent(report, rows, kExpectedGateFinalEquity);  // T1.131
    EXPECT_EQ(summary, build_expected_summary(5, token, /*with_tradability=*/true,
                                              /*residual_cash=*/"150596.00",
                                              /*realized_pnl=*/"-760.00"));
}

// F4 覆盖：tradability_csv 指向不存在路径 → load_data fail-fast，run() 返回非 0
// 且不进主循环/报告段（trades.csv 不产出）。
TEST_F(SelTradabilityE2E, MissingTradabilityCsvFailsFastWithoutReports) {
    const auto missing = (dir_ / "no_such_tradability.csv").string();
    ASSERT_FALSE(std::filesystem::exists(missing));
    EXPECT_NE(run_sel(/*with_tradability=*/true, "report_missing", missing), 0);
    EXPECT_FALSE(std::filesystem::exists(dir_ / "report_missing" / "trades.csv"));
    EXPECT_FALSE(std::filesystem::exists(dir_ / "report_missing" / "summary.json"));
}

TEST_F(SelTradabilityE2E, ControlGroupWithoutTradabilityIsByteStable) {
    ASSERT_EQ(run_sel(/*with_tradability=*/false, "report_ctrl"), 0);

    const auto report = dir_ / "report_ctrl";
    const auto rows = parse_trades_csv(report / "trades.csv");
    ASSERT_EQ(rows.size(), 8u);  // 4 日 × 两腿全成交

    // 每日两腿方向核对：D1 BUY/BUY，D2-D4 600000 SELL + 000001 BUY。
    const struct { const char* date; const char* dir_600000; const char* dir_000001; } days[4] = {
        {"20260112", "BUY", "BUY"},
        {"20260113", "SELL", "BUY"},
        {"20260114", "SELL", "BUY"},
        {"20260115", "SELL", "BUY"},
    };
    for (const auto& d : days) {
        const auto a = rows_for(rows, d.date, "600000.SH");
        ASSERT_EQ(a.size(), 1u) << d.date;
        EXPECT_EQ(a[0].direction, d.dir_600000) << d.date;
        const auto b = rows_for(rows, d.date, "000001.SZ");
        ASSERT_EQ(b.size(), 1u) << d.date;
        EXPECT_EQ(b[0].direction, d.dir_000001) << d.date;
    }

    // 未配置 tradability：events 文件不创建。
    EXPECT_FALSE(std::filesystem::exists(report / "tradability_events.csv"));

    // summary.json 全文精确断言 —— regression 对 SEL 只 diff trades.csv，
    // 本断言是"未配置时 summary 字节不变"的唯一字节面见证（盲审折叠 #5）。
    const auto summary = read_text(report / "summary.json");
    const auto token = extract_final_equity_token(summary);
    expect_equity_pinned(token, rows, kExpectedCtrlFinalEquity);
    expect_running_equity_consistent(rows);                              // T1.132 ③
    expect_daily_equity_consistent(report, rows, kExpectedCtrlFinalEquity);  // T1.131
    EXPECT_EQ(summary, build_expected_summary(8, token, /*with_tradability=*/false,
                                              /*residual_cash=*/"200346.00",
                                              /*realized_pnl=*/"-1260.00"));
}

TEST_F(SelTradabilityE2E, PerSymbolSlippageOverridesGlobalInFullChain) {
    ASSERT_EQ(run_sel(/*with_tradability=*/false, "report_slip"), 0);

    const auto rows = parse_trades_csv(dir_ / "report_slip" / "trades.csv");
    ASSERT_EQ(rows.size(), 8u);

    // BUY：exec = close + slip；600000 用 per-symbol 0.05，000001 回退全局 0.02。
    const auto d1_600000 = rows_for(rows, "20260112", "600000.SH");
    ASSERT_EQ(d1_600000.size(), 1u);
    EXPECT_EQ(d1_600000[0].exec_price, "10.0500");
    EXPECT_EQ(d1_600000[0].slippage, "0.0500");
    EXPECT_EQ(d1_600000[0].fee, "0.0000");  // fixture 无 fees 节 → 0

    const auto d1_000001 = rows_for(rows, "20260112", "000001.SZ");
    ASSERT_EQ(d1_000001.size(), 1u);
    EXPECT_EQ(d1_000001[0].exec_price, "20.0200");
    EXPECT_EQ(d1_000001[0].slippage, "0.0200");

    // SELL：exec = close − slip（600000 per-symbol 0.05 → 9.95）。
    const auto d3_600000 = rows_for(rows, "20260114", "600000.SH");
    ASSERT_EQ(d3_600000.size(), 1u);
    EXPECT_EQ(d3_600000[0].direction, "SELL");
    EXPECT_EQ(d3_600000[0].exec_price, "9.9500");
    EXPECT_EQ(d3_600000[0].slippage, "0.0500");

    // 000001 的 SELL 腿本 fixture 不产生（递增权重）；BUY 侧回退值已覆盖全局路径。
    const auto d2_000001 = rows_for(rows, "20260113", "000001.SZ");
    ASSERT_EQ(d2_000001.size(), 1u);
    EXPECT_EQ(d2_000001[0].exec_price, "20.0200");
    EXPECT_EQ(d2_000001[0].slippage, "0.0200");
}

// ── external strategy 注入 seam（run(cfg, external)）e2e ────────────────────
//
// fixture: 2 只股票 × 2 交易日，无 commodities/费用/滑点 → 数值全精确
// （min_lot 回退 1，不取整）：
//   D1: eq=500000 → tgt 0.30×500000/10=15000、0.30×500000/20=7500（两腿 BUY）
//       cash=500000−150000−150000=200000，eq 不变 500000
//   D2: tgt 0.20×500000/10=10000（SELL 5000）、0.40×500000/20=10000（BUY 2500）
// external stub 与 sel_weights_csv 插件（normalize:false）喂同一权重序列 →
// trades.csv/summary.json 字节一致（BIT-EXACT 前提，P2 Python demo 的 C++ 前哨）。

/// @brief external seam stub：按 curDate 设固定权重（与 weights.csv fixture 同值）。
class FixedWeightsSelStrategy final : public XtStrategyBase {
public:
    void on_schedule(IStrategyContext* ctx, uint32_t curDate,
                     uint32_t /*curTime*/, int64_t /*fireTime_ms*/) override {
        if (curDate == 20260112) {
            ctx->set_target_weights({{"600000.SH", 0.30}, {"000001.SZ", 0.30}});
        } else if (curDate == 20260113) {
            ctx->set_target_weights({{"600000.SH", 0.20}, {"000001.SZ", 0.40}});
        }
    }
};

class SelExternalSeamE2E : public ::testing::Test {
protected:
    void SetUp() override {
        char tmpl[] = "/tmp/xt_sel_ext_XXXXXX";
        ASSERT_NE(mkdtemp(tmpl), nullptr);
        dir_ = tmpl;
        write_file("trading_dates.csv", "date\n20260112\n20260113\n");
        // 20260109 = warmup T-1 行（首日 fire 需真 T-1，见 SelTradabilityE2E 注）。
        write_file("close_prices.csv",
                   "date,code,close\n"
                   "20260109,600000.SH,10.0\n20260109,000001.SZ,20.0\n"
                   "20260112,600000.SH,10.0\n20260112,000001.SZ,20.0\n"
                   "20260113,600000.SH,10.0\n20260113,000001.SZ,20.0\n");
        write_file("weights.csv",
                   "date,symbol,weight\n"
                   "20260112,600000.SH,0.30\n20260112,000001.SZ,0.30\n"
                   "20260113,600000.SH,0.20\n20260113,000001.SZ,0.40\n");
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

    /// @brief 生成 yaml 并 load cfg；so_path 空 = external 注入形态（so 键省略，
    ///        验证 D3 parse 放宽的端到端通路）。
    bool load_cfg(const std::string& report_subdir, const std::string& so_path,
                  xt_bt_sel::XtSelRunnerConfig& out, std::string& err) const {
        const std::string cfg_path = (dir_ / (report_subdir + "_cfg.yaml")).string();
        std::ostringstream y;
        y << "selector:\n"
          << "  trading_dates_csv: " << (dir_ / "trading_dates.csv").string() << "\n"
          << "  close_prices_csv: " << (dir_ / "close_prices.csv").string() << "\n"
          << "  task:\n"
          << "    strategy_id: sel_ext_e2e\n"
          << "    period: daily\n"
          << "    time: 1500\n"
          << "    strict_time: true\n"
          << "  strategy:\n"
          << "    id: sel_ext_e2e\n";
        if (!so_path.empty()) {
            y << "    so: " << so_path << "\n";
        }
        y << "    params:\n"
          << "      weights_path: " << (dir_ / "weights.csv").string() << "\n"
          << "      normalize: false\n"
          << "account:\n"
          << "  initial_cash: 500000.0\n"
          << "report:\n"
          << "  output_dir: " << (dir_ / report_subdir).string() << "\n";
        {
            std::ofstream ofs(cfg_path);
            ofs << y.str();
        }
        std::string arg0 = "test";
        std::string arg1 = cfg_path;
        char* argv[] = {arg0.data(), arg1.data()};
        return xt_bt_sel::load_sel_config(2, argv, out, err);
    }

    std::filesystem::path dir_;
};

TEST_F(SelExternalSeamE2E, ExternalStubRunsWithEmptySo) {
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    ASSERT_TRUE(load_cfg("report_ext", "", cfg, err)) << err;
    ASSERT_EQ(cfg.strategies.size(), 1u);
    EXPECT_TRUE(cfg.strategies[0].lib_path.empty());

    FixedWeightsSelStrategy stub;
    ASSERT_EQ(xt_bt_sel::XtSelBacktestRunner::run(cfg, &stub), 0);

    const auto rows = parse_trades_csv(dir_ / "report_ext" / "trades.csv");
    ASSERT_EQ(rows.size(), 4u);
    const struct { const char* date; const char* code; const char* dir; const char* qty; }
        expected[4] = {
            {"20260112", "600000.SH", "BUY",  "15000.0000"},
            {"20260112", "000001.SZ", "BUY",  "7500.0000"},
            {"20260113", "600000.SH", "SELL", "5000.0000"},
            {"20260113", "000001.SZ", "BUY",  "2500.0000"},
        };
    for (const auto& e : expected) {
        const auto r = rows_for(rows, e.date, e.code);
        ASSERT_EQ(r.size(), 1u) << e.date << " " << e.code;
        EXPECT_EQ(r[0].direction, e.dir) << e.date << " " << e.code;
        EXPECT_EQ(r[0].qty, e.qty) << e.date << " " << e.code;
    }
}

// 折叠 #4：so 空且无 external → runner 侧具名 fail-fast（rc=3，log 点名
// "strategy.so empty and no external strategy"），不产报告。
TEST_F(SelExternalSeamE2E, EmptySoWithoutExternalFailsFast) {
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    ASSERT_TRUE(load_cfg("report_noext", "", cfg, err)) << err;
    EXPECT_EQ(xt_bt_sel::XtSelBacktestRunner::run(cfg), 3);
    EXPECT_FALSE(std::filesystem::exists(dir_ / "report_noext" / "trades.csv"));
    EXPECT_FALSE(std::filesystem::exists(dir_ / "report_noext" / "summary.json"));
}

// fixup F4：so 非空 + external 注入 → external 优先（so 被忽略不加载；bogus
// 路径证明 plugin factory 未被触碰，只 warn log 不 crash，文案不断言）。
TEST_F(SelExternalSeamE2E, ExternalTakesPrecedenceOverNonEmptySo) {
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    ASSERT_TRUE(load_cfg("report_ext_so",
                         (dir_ / "no_such_plugin.so").string(), cfg, err)) << err;
    ASSERT_EQ(cfg.strategies.size(), 1u);
    ASSERT_FALSE(cfg.strategies[0].lib_path.empty());

    FixedWeightsSelStrategy stub;
    ASSERT_EQ(xt_bt_sel::XtSelBacktestRunner::run(cfg, &stub), 0);
    EXPECT_EQ(parse_trades_csv(dir_ / "report_ext_so" / "trades.csv").size(), 4u);
}

// 折叠 #3：run() 直调方可手构 cfg 绕 parse 校验 → assemble 层 empty 守卫兜底。
TEST_F(SelExternalSeamE2E, EmptyStrategiesGuardRejects) {
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    ASSERT_TRUE(load_cfg("report_guard", "", cfg, err)) << err;
    cfg.strategies.clear();
    FixedWeightsSelStrategy stub;
    EXPECT_EQ(xt_bt_sel::XtSelBacktestRunner::run(cfg, &stub), 3);
}

// D2：external 仅支持单 strategy（size>1 报错）。
TEST_F(SelExternalSeamE2E, ExternalWithMultiStrategyRejects) {
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    ASSERT_TRUE(load_cfg("report_multi", "", cfg, err)) << err;
    cfg.strategies.push_back(cfg.strategies[0]);
    FixedWeightsSelStrategy stub;
    EXPECT_EQ(xt_bt_sel::XtSelBacktestRunner::run(cfg, &stub), 3);
}

// BIT-EXACT 前哨：external stub 与 plugin 路径（同权重序列，normalize:false）
// 产出 trades.csv + summary.json 字节一致。
TEST_F(SelExternalSeamE2E, ExternalStubMatchesPluginTradesByteIdentical) {
    const auto root = resolve_repo_root();
    const auto plugin_so = root / "var/plugins/strategies/libsel_weights_csv_strategy.so";
    if (root.empty() || !std::filesystem::exists(plugin_so)) {
        GTEST_SKIP() << "libsel_weights_csv_strategy.so 不在位（未 git-track 资产，"
                        "fresh worktree 需从主仓 copy）: " << plugin_so;
    }
    std::vector<std::unique_ptr<ScopedEnv>> env;
    env.push_back(std::make_unique<ScopedEnv>(
        "CLOUDTRADER_PLUGIN_ALLOWLIST", "libsel_weights_csv_strategy.so"));
    env.push_back(std::make_unique<ScopedEnv>("CLOUDTRADER_PLUGIN_ALLOWLIST_ENFORCE", "1"));
    env.push_back(std::make_unique<ScopedEnv>("CLOUDTRADER_PLUGIN_STRICT_SIGNING", "1"));
    {
        XtStrategyFactory probe;
        ASSERT_TRUE(probe.register_plugin("preflight", plugin_so.string()))
            << "plugin preflight load failed: " << probe.last_error();
    }

    // plugin 路径（.so 读 weights.csv）
    xt_bt_sel::XtSelRunnerConfig cfg_plugin;
    std::string err;
    ASSERT_TRUE(load_cfg("report_plugin", plugin_so.string(), cfg_plugin, err)) << err;
    ASSERT_EQ(xt_bt_sel::XtSelBacktestRunner::run(cfg_plugin), 0);

    // external stub 路径（同权重序列硬编码）
    xt_bt_sel::XtSelRunnerConfig cfg_ext;
    ASSERT_TRUE(load_cfg("report_ext_eq", "", cfg_ext, err)) << err;
    FixedWeightsSelStrategy stub;
    ASSERT_EQ(xt_bt_sel::XtSelBacktestRunner::run(cfg_ext, &stub), 0);

    EXPECT_EQ(read_text(dir_ / "report_plugin" / "trades.csv"),
              read_text(dir_ / "report_ext_eq" / "trades.csv"));
    EXPECT_EQ(read_text(dir_ / "report_plugin" / "summary.json"),
              read_text(dir_ / "report_ext_eq" / "summary.json"));
    EXPECT_EQ(read_text(dir_ / "report_plugin" / "daily_equity.csv"),
              read_text(dir_ / "report_ext_eq" / "daily_equity.csv"));
    // 非空 sanity（防两侧同为空文件的假阳性）。
    EXPECT_EQ(parse_trades_csv(dir_ / "report_plugin" / "trades.csv").size(), 4u);
}

}  // namespace sel_e2e

// =============================================================================
// SEL mark 时点 P0 修复：T close 刷 mark + 首日 lookahead 禁止（变价 fixture）
// =============================================================================
//
// 既有 e2e/regression 数据全为 constant closes（T-1 ≡ T）→ 对「执行价 T close vs
// 估值价 T-1 close 错位一天」结构性失明。本组用变价 fixture（D1 close=10 →
// D2 close=12）钉：
//   ① 持有过夜的 EOD equity 反映 D2 close（旧实现停在 T-1 → FAIL）
//   ② D2 买入当笔 equity_after 无 (T-1−T)×qty 伪盈亏（旧实现 −30000 → FAIL）
//   ③ final_equity 含末日行情（旧实现 470000 vs 550000 → FAIL）
//   ④ 首日 sizing 用 warmup 真 T-1 而非 T 日 close（旧实现 qty 20000 → FAIL）
//   ⑤ 首日无 T-1 且策略当日 sizing → rc=8 fail-fast（旧实现 rc=0 照跑 → FAIL）
//
// 手算锚（无费用/滑点/min_lot 回退 1，cash 500000）：
//   [②③] D1: T-1(warmup)=10 → target 0.5×500000/10=25000，fill@10 →
//         cash 250000，equity_after=500000
//         D2: fire 时 marks=D1 close(10) → eq=500000，target=0.8×500000/10=40000
//         → BUY 15000 fill@12；fill 前 mark 已刷 12 → equity_after =
//         (250000−15000×12) + 40000×12 = 70000+480000 = 550000（买入按 mark 价
//         成交、零费 → equity 不因买入本身变动，Δ= 持仓 25000×(12−10)=+50000
//         纯 MTM）。旧实现 mark 停在 10：70000+40000×10=470000（−30000 伪亏）。

namespace sel_mark_e2e {

using sel_e2e::TradeRow;
using sel_e2e::parse_trades_csv;
using sel_e2e::read_text;
using sel_e2e::split_line;

/// @brief external 注入 stub：按日期查表设权重（无 .so 依赖，不触发 GTEST_SKIP）。
class MapWeightsSelStrategy final : public XtStrategyBase {
public:
    explicit MapWeightsSelStrategy(
        std::map<uint32_t, std::unordered_map<std::string, double>> by_date)
        : by_date_(std::move(by_date)) {}
    void on_schedule(IStrategyContext* ctx, uint32_t curDate,
                     uint32_t /*curTime*/, int64_t /*fireTime_ms*/) override {
        const auto it = by_date_.find(curDate);
        if (it != by_date_.end()) ctx->set_target_weights(it->second);
    }

private:
    std::map<uint32_t, std::unordered_map<std::string, double>> by_date_;
};

class SelMarkTimingE2E : public ::testing::Test {
protected:
    void SetUp() override {
        char tmpl[] = "/tmp/xt_sel_mark_XXXXXX";
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

    /// @brief 生成 yaml（无 commodities/fees/trade_cost → min_lot=1、fee=0、slip=0，
    ///        数值全精确）并以 external stub 跑 runner。
    int run_with(const std::string& report_subdir, MapWeightsSelStrategy* stub) const {
        const std::string cfg_path = (dir_ / (report_subdir + "_cfg.yaml")).string();
        std::ostringstream y;
        y << "selector:\n"
          << "  trading_dates_csv: " << (dir_ / "trading_dates.csv").string() << "\n"
          << "  close_prices_csv: " << (dir_ / "close_prices.csv").string() << "\n"
          << "  task:\n"
          << "    strategy_id: sel_mark_e2e\n"
          << "    period: daily\n"
          << "    time: 1500\n"
          << "    strict_time: true\n"
          << "  strategy:\n"
          << "    id: sel_mark_e2e\n"
          << "account:\n"
          << "  initial_cash: 500000.0\n"
          << "report:\n"
          << "  output_dir: " << (dir_ / report_subdir).string() << "\n";
        {
            std::ofstream ofs(cfg_path);
            ofs << y.str();
        }
        xt_bt_sel::XtSelRunnerConfig cfg;
        std::string err;
        std::string arg0 = "test";
        std::string arg1 = cfg_path;
        char* argv[] = {arg0.data(), arg1.data()};
        if (!xt_bt_sel::load_sel_config(2, argv, cfg, err)) {
            ADD_FAILURE() << "load_sel_config failed: " << err;
            return -1;
        }
        return xt_bt_sel::XtSelBacktestRunner::run(cfg, stub);
    }

    /// @brief daily_equity.csv → (date, equity) 行序列（跳 header）。
    static std::vector<std::pair<std::string, std::string>>
    parse_daily_equity(const std::filesystem::path& report) {
        std::vector<std::pair<std::string, std::string>> out;
        std::ifstream ifs(report / "daily_equity.csv");
        std::string line;
        bool header = true;
        while (std::getline(ifs, line)) {
            if (header) { header = false; continue; }
            if (line.empty()) continue;
            const auto f = split_line(line);
            if (f.size() == 2) out.emplace_back(f[0], f[1]);
        }
        return out;
    }

    /// @brief summary.json → final_equity 数值（缺失时 NaN）。
    static double final_equity_of(const std::filesystem::path& report) {
        const auto text = read_text(report / "summary.json");
        const std::string key = "\"final_equity\": ";
        const auto pos = text.find(key);
        if (pos == std::string::npos) return std::nan("");
        return std::stod(text.substr(pos + key.size()));
    }

    std::filesystem::path dir_;
};

// ①②③：T close 刷 mark 后，成交/逐笔 equity_after/EOD/final 全部落 T close 估值。
TEST_F(SelMarkTimingE2E, TCloseMarkRefreshValuesFillsEodAndFinal) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"   // warmup 真 T-1
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,12.0\n");
    MapWeightsSelStrategy stub({
        {20260112u, {{"600000.SH", 0.5}}},
        {20260113u, {{"600000.SH", 0.8}}},
    });
    ASSERT_EQ(run_with("report_mark", &stub), 0);

    const auto report = dir_ / "report_mark";
    const auto rows = parse_trades_csv(report / "trades.csv");
    ASSERT_EQ(rows.size(), 2u);

    // D1：建仓 25000 @10；买入按 mark 价、零费 → equity 不变。
    EXPECT_EQ(rows[0].date, "20260112");
    EXPECT_EQ(rows[0].qty, "25000.0000");
    EXPECT_EQ(rows[0].exec_price, "10.0000");
    EXPECT_NEAR(std::stod(rows[0].equity_after), 500000.0, 0.01);

    // ② D2 买入当笔无 (T-1−T)×qty 伪盈亏：equity_after = 550000
    //   （旧实现 mark 停在 T-1=10 → 470000，本断言必 FAIL）。
    EXPECT_EQ(rows[1].date, "20260113");
    EXPECT_EQ(rows[1].direction, "BUY");
    EXPECT_EQ(rows[1].qty, "15000.0000");
    EXPECT_EQ(rows[1].exec_price, "12.0000");
    EXPECT_NEAR(std::stod(rows[1].equity_after), 550000.0, 0.01);
    // Δequity = 持仓 25000×(12−10)=+50000 纯 MTM（旧实现 Δ=−30000 伪亏）。
    EXPECT_NEAR(std::stod(rows[1].equity_after) - std::stod(rows[0].equity_after),
                50000.0, 0.01);

    // ① 持有过夜的 EOD equity 反映 D2 close（旧实现 D2 行 = 470000.00 → FAIL）。
    const auto daily = parse_daily_equity(report);
    ASSERT_EQ(daily.size(), 2u);
    EXPECT_EQ(daily[0].first, "20260112");
    EXPECT_NEAR(std::stod(daily[0].second), 500000.0, 0.01);
    EXPECT_EQ(daily[1].first, "20260113");
    EXPECT_NEAR(std::stod(daily[1].second), 550000.0, 0.01);

    // ③ final_equity 含末日行情（旧实现 470000 → FAIL）。
    EXPECT_NEAR(final_equity_of(report), 550000.0, 0.01);
}

// ④ 首日 sizing 用 warmup 真 T-1 close，而非 T 日 close（lookahead 根除）。
TEST_F(SelMarkTimingE2E, FirstDaySizingUsesWarmupT1CloseNotTClose) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"   // warmup T-1 ≠ T 日 close
               "20260112,600000.SH,12.5\n"
               "20260113,600000.SH,12.5\n");
    MapWeightsSelStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    ASSERT_EQ(run_with("report_warmup", &stub), 0);

    const auto rows = parse_trades_csv(dir_ / "report_warmup" / "trades.csv");
    ASSERT_EQ(rows.size(), 1u);
    // target = 0.5×500000/10（T-1）= 25000；旧实现按 T 日 close 12.5 sizing →
    // 20000（本断言旧实现必 FAIL）。fill 仍在 T 日 close 12.5。
    EXPECT_EQ(rows[0].qty, "25000.0000");
    EXPECT_EQ(rows[0].exec_price, "12.5000");
}

// ⑤ 首日无 T-1 且策略当日 sizing → rc=8 fail-fast，报告不写出
//   （旧实现以 T 日 close 兜底照跑 rc=0 → 本断言 FAIL）。
TEST_F(SelMarkTimingE2E, FirstDayWithoutT1FailsFastWhenStrategySizes) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,10.0\n");
    MapWeightsSelStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    EXPECT_EQ(run_with("report_failfast", &stub), 8);
    EXPECT_FALSE(std::filesystem::exists(dir_ / "report_failfast" / "trades.csv"));
    EXPECT_FALSE(std::filesystem::exists(dir_ / "report_failfast" / "summary.json"));
}

// 首日无 T-1 但当日无 sizing 请求 → 放行（sel_demo 形态：首日只是日历起点）；
// 后续 fire 用真 T-1（前一交易日 close）。
TEST_F(SelMarkTimingE2E, FirstDayWithoutT1PassesWhenNoSizingRequested) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260112,600000.SH,10.0\n"
               "20260113,600000.SH,12.0\n");
    MapWeightsSelStrategy stub({{20260113u, {{"600000.SH", 0.5}}}});
    ASSERT_EQ(run_with("report_latefire", &stub), 0);

    const auto rows = parse_trades_csv(dir_ / "report_latefire" / "trades.csv");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].date, "20260113");
    EXPECT_EQ(rows[0].qty, "25000.0000");     // 0.5×500000/10（T-1=D1 close）
    EXPECT_EQ(rows[0].exec_price, "12.0000"); // fill @ T close
    // 买入按 mark 价、零费 → equity 不变。
    EXPECT_NEAR(std::stod(rows[0].equity_after), 500000.0, 0.01);
}

// 持仓 symbol 当日无 close 行 → carry 旧 mark（观测披露路径）；下个有价日恢复 T 估值。
TEST_F(SelMarkTimingE2E, HeldSymbolWithoutTCloseCarriesStaleMark) {
    write_file("trading_dates.csv", "date\n20260112\n20260113\n20260114\n");
    write_file("close_prices.csv",
               "date,code,close\n"
               "20260109,600000.SH,10.0\n"
               "20260112,600000.SH,10.0\n"
               // 20260113 无任何 close 行（held symbol 缺 T 价 → carry）
               "20260114,600000.SH,14.0\n");
    MapWeightsSelStrategy stub({{20260112u, {{"600000.SH", 0.5}}}});
    ASSERT_EQ(run_with("report_carry", &stub), 0);

    const auto report = dir_ / "report_carry";
    const auto daily = parse_daily_equity(report);
    ASSERT_EQ(daily.size(), 3u);
    EXPECT_NEAR(std::stod(daily[0].second), 500000.0, 0.01);  // D1 @10
    EXPECT_NEAR(std::stod(daily[1].second), 500000.0, 0.01);  // D2 carry @10
    EXPECT_NEAR(std::stod(daily[2].second), 600000.0, 0.01);  // D3 @14: 250000+25000×14
    EXPECT_NEAR(final_equity_of(report), 600000.0, 0.01);
}

}  // namespace sel_mark_e2e

// =============================================================================
// BR-03：SEL 必须 drain order update —— 否则撤单的 frozen/book slot 释放不执行
// =============================================================================
//
// 病灶：gateway 只在 drain_order_updates() 里把 update 派发给 observer
//（drain_and_dispatch_order_updates_() 名字带 dispatch，实际只填内部缓冲）。
// SEL runner 原先只调 drain_all_fills()，于是 SelFrozenReleaseObserver 的撤单分支
// 结构性不可达：当日无 close 的挂单在 flush_session_end 被撤后，冻结资金与
// order-book slot 永不归还。
//
// 断言形态说明：该泄漏在 runner 的**输出表面上不可直接构造**——SEL 里唯一能留下
// 挂单的方式是当日无 close 价，而那同样让次日无法为该标的定价（实测日志
// `on_target_weights: price_fp=0 <= 0; skip`），于是永远没有后续订单去撞被占用的
// 可平额度。因此把「回报必须全部被消费」这条不变量本身做成机器可读量
// （summary.json.undrained_reports）并在此断言，而不是绕道去测一个当前不可达的
// 下游后果。

namespace sel_drain_e2e {

using sel_e2e::read_text;

/// D1 建仓；D2 目标清零（当日无 close → 挂单悬空 → session end 撤单）。
class CancelPendingSelStrategy final : public XtStrategyBase {
public:
    void on_schedule(IStrategyContext* ctx, uint32_t curDate,
                     uint32_t /*curTime*/, int64_t /*fireTime_ms*/) override {
        if (curDate == 20260112) {
            ctx->set_target_weights({{"000001.SZ", 0.80}});
        } else if (curDate == 20260113) {
            ctx->set_target_weights({{"000001.SZ", 0.0}});
        }
    }
};

class SelDrainInvariantE2E : public ::testing::Test {
protected:
    void SetUp() override {
        char tmpl[] = "/tmp/xt_sel_drain_XXXXXX";
        ASSERT_NE(mkdtemp(tmpl), nullptr);
        dir_ = tmpl;
        write_file("trading_dates.csv", "date\n20260112\n20260113\n");
        // 20260113 无 000001.SZ close → 当日 SELL 挂单不可成交 → flush 撤单。
        write_file("close_prices.csv",
                   "date,code,close\n"
                   "20260109,000001.SZ,20.0\n"
                   "20260112,000001.SZ,20.0\n");
    }

    void TearDown() override {
        if (!dir_.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(dir_, ec);  // 测试临时文件规范
        }
    }

    void write_file(const std::string& name, const std::string& body) const {
        std::ofstream ofs(dir_ / name);
        ofs << body;
    }

    bool load_cfg(const std::string& sub, xt_bt_sel::XtSelRunnerConfig& out,
                  std::string& err) const {
        const std::string cfg_path = (dir_ / (sub + "_cfg.yaml")).string();
        std::ostringstream y;
        y << "selector:\n"
          << "  trading_dates_csv: " << (dir_ / "trading_dates.csv").string() << "\n"
          << "  close_prices_csv: " << (dir_ / "close_prices.csv").string() << "\n"
          << "  task:\n"
          << "    strategy_id: sel_drain_e2e\n"
          << "    period: daily\n"
          << "    time: 1500\n"
          << "    strict_time: true\n"
          << "  strategy:\n"
          << "    id: sel_drain_e2e\n"
          << "    params:\n"
          << "      normalize: false\n"
          << "account:\n"
          << "  initial_cash: 500000.0\n"
          << "report:\n"
          << "  output_dir: " << (dir_ / sub).string() << "\n";
        {
            std::ofstream ofs(cfg_path);
            ofs << y.str();
        }
        std::string a0 = "test";
        std::string a1 = cfg_path;
        char* argv[] = {a0.data(), a1.data()};
        return xt_bt_sel::load_sel_config(2, argv, out, err);
    }

    std::filesystem::path dir_;
};

TEST_F(SelDrainInvariantE2E, AllGatewayReportsConsumedAtExit) {
    xt_bt_sel::XtSelRunnerConfig cfg;
    std::string err;
    ASSERT_TRUE(load_cfg("run", cfg, err)) << err;

    CancelPendingSelStrategy strat;
    ASSERT_EQ(xt_bt_sel::XtSelBacktestRunner::run(cfg, &strat), 0);

    const std::string body = read_text(dir_ / "run" / "summary.json");
    ASSERT_FALSE(body.empty()) << "summary.json 未产出";

    // 对照前提：确有挂单被撤（D2 目标清零但当日无 close → 不可成交）。
    // 若该前提不成立，本钉会退化为恒真——用成交数反证：只应有 D1 的一笔买入。
    EXPECT_NE(body.find("\"trade_rows\": 1"), std::string::npos)
        << "fixture 前提被破坏（预期只有 D1 一笔成交）: " << body;

    EXPECT_NE(body.find("\"undrained_reports\": 0"), std::string::npos)
        << "存在从未被 drain 的 gateway 回报 —— 撤单的 frozen 释放与 order-book "
           "slot 清理不会执行: " << body;

    // 咬住"释放"本身而非只咬"送达"：被撤订单的冻结必须归还，否则残留 > 0。
    // 单看 undrained_reports 时，把 observer 里的 order_canceled apply 删掉仍会绿。
    EXPECT_NE(body.find("\"frozen_cash_at_exit\": 0.00"), std::string::npos)
        << "收尾仍有残留冻结 —— 撤单的 frozen 未释放: " << body;
}

}  // namespace sel_drain_e2e
