#include "XtSelRunnerConfig.h"

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "api/XtStrategyRuntimeTypes.h"
#include "xt_core/XtFixedPoint.h"                  // 参与率量化后 >0 的解析期校验
#include "xt_config/ConfigSchemaValidator.h"
#include "xt_config/catalog/XtCommodityCatalog.h"  // parse_commodity_catalog_config
#include "xt_config/catalog/XtFeeRuleBook.h"       // parse_fee_rule_book_config
#include "xt_logger/XtLogger.h"

namespace xt_bt_sel {

namespace {

bool parse_period(const std::string& s, TaskPeriod& out) {
    if (s == "minute")  { out = TaskPeriod::Minute;  return true; }
    if (s == "daily")   { out = TaskPeriod::Daily;   return true; }
    if (s == "weekly")  { out = TaskPeriod::Weekly;  return true; }
    if (s == "monthly") { out = TaskPeriod::Monthly; return true; }
    if (s == "yearly")  { out = TaskPeriod::Yearly;  return true; }
    return false;
}

/// @brief SEL 回测 yaml schema 白名单（unknown-key fail-fast）。
/// 与 parse_yaml 实读字段保持一致；commodities/fees 字段集与 xt_config 全量 parser 对齐。
/// wt-parity 配套节（env/replayer/sel）由外部对照工具消费、本 loader 不读，整子树放行。
XtConfigSchemaRules build_sel_backtest_schema_rules() {
    XtConfigSchemaRules rules;
    rules.allowed_paths = {
        "datalake.root", "datalake.allow_schema_skew",
        "selector.trading_dates_csv", "selector.close_prices_csv",
        "selector.tradability_csv",
        "selector.corporate_actions_csv", "selector.allow_skip_rights",
        "selector.rights_policy",
        "selector.date_from", "selector.date_to",
        "selector.task.strategy_id", "selector.task.period", "selector.task.day",
        "selector.task.time", "selector.task.strict_time",
        "selector.task.trdtpl", "selector.task.session",
        "selector.strategy.id", "selector.strategy.so", "selector.strategy.params",
        "account.initial_cash",
        "commodities.*.price_tick", "commodities.*.vol_scale", "commodities.*.allow_short",
        "commodities.*.margin_rate", "commodities.*.slippage",
        "commodities.*.has_open_interest", "commodities.*.has_settle_price",
        "commodities.*.is_t_plus_1", "commodities.*.min_lot",
        "commodities.*.has_broker_frozen_push", "commodities.*.daily_price_limit_pct",
        "commodities.*.offset_policy",
        "fees.*.open", "fees.*.close", "fees.*.close_today", "fees.*.by_volume",
        "fees.*.maker_rebate", "fees.*.stamp_duty_rate",
        "fees.*.transfer_fee_rate", "fees.*.min_commission",
        "fees.dividend_tax_rate",
        "risk.max_order_qty", "risk.max_order_notional", "risk.max_pos_qty",
        "risk.enforce_long_t_plus_1", "risk.allow_short",
        "risk.delist_haircut_pct",
        "trade_cost.slippage",
        "backtest.trace_mode",
        "execution.integer_projection", "execution.max_participation",
        "execution.carry_max_days",
        "report.output_dir",
    };
    rules.open_prefixes = {
        // 策略自定义参数自由布局。
        "selector.strategy.params",
        // wt-parity 对照工具消费的配套节（同一 yaml 双侧共用，本 loader 不读）。
        "env", "replayer", "sel",
    };
    return rules;
}

/// @brief 把配置里的相对输入路径解析为绝对路径（消 cwd 依赖）。
///
/// 解析序：绝对路径原样 → 相对 config 目录命中则用之（项目约定：路径以 config
/// 文件所在目录为基准）→ 否则相对 cwd 绝对化（兼容既有把 config 拷到临时目录、
/// 数据路径仍以仓库根为基准的跑法，见 scripts/regression_test.sh 的 SEL 用例）。
/// 两条都不存在时保留原串，让下游 loader 报出用户写的原始路径。
void resolve_input_path(std::string& p, const std::filesystem::path& cfg_dir) {
    if (p.empty()) return;
    std::filesystem::path fp(p);
    if (fp.is_absolute()) return;
    std::error_code ec;
    const auto by_cfg = cfg_dir / fp;
    if (std::filesystem::exists(by_cfg, ec)) {
        p = std::filesystem::weakly_canonical(by_cfg, ec).string();
        if (ec) p = by_cfg.string();
        return;
    }
    const auto by_cwd = std::filesystem::absolute(fp, ec);
    if (!ec && std::filesystem::exists(by_cwd, ec)) {
        p = std::filesystem::weakly_canonical(by_cwd, ec).string();
        if (ec) p = by_cwd.string();
    }
}

bool parse_yaml(const std::string& path, XtSelRunnerConfig& out, std::string& err) {
    YAML::Node root;
    try {
        root = YAML::LoadFile(path);
    } catch (const std::exception& e) {
        err = std::string("yaml load error: ") + e.what();
        return false;
    }
    if (!root || !root.IsMap()) {
        err = "yaml root is not a map";
        return false;
    }

    // ── Schema 合规校验（拦截未知 key，typo fail-fast，对齐 HFT/CTA BT）──
    {
        std::vector<std::string> schema_errors;
        validate_unknown_config_keys(root, build_sel_backtest_schema_rules(), schema_errors, "sel");
        if (!schema_errors.empty()) {
            err = schema_errors.front();
            if (schema_errors.size() > 1)
                err += " (+" + std::to_string(schema_errors.size() - 1) + " more)";
            return false;
        }
    }

    // datalake
    if (root["datalake"] && root["datalake"].IsMap()) {
        auto dl = root["datalake"];
        if (dl["root"]) out.datalake_root = dl["root"].as<std::string>("");
        if (dl["allow_schema_skew"])
            out.allow_schema_skew = dl["allow_schema_skew"].as<bool>(false);
    }

    // selector
    if (!root["selector"] || !root["selector"].IsMap()) {
        err = "missing 'selector' section";
        return false;
    }
    auto sel = root["selector"];
    out.trading_dates_csv = sel["trading_dates_csv"].as<std::string>("");
    out.close_prices_csv  = sel["close_prices_csv"].as<std::string>("");
    out.tradability_csv   = sel["tradability_csv"].as<std::string>("");
    out.corporate_actions_csv = sel["corporate_actions_csv"].as<std::string>("");
    if (sel["allow_skip_rights"])
        out.allow_skip_rights = sel["allow_skip_rights"].as<bool>(false);
    if (sel["rights_policy"]) {
        const std::string rp = sel["rights_policy"].as<std::string>("");
        if (!parse_rights_policy(rp, out.rights_policy)) {
            err = "selector.rights_policy must be one of subscribe,forfeit; got '" +
                  rp + "'";
            return false;
        }
        // 两者同时配置时 skip 优先（整行跳过 = 连除权都不做），policy 于是没有裁决面。
        // 不是错误配置（skip 是显式的研究口径），但「设了 forfeit 却看到未除权的价」
        // 极易被读成 bug —— 解析期说一次。
        if (out.allow_skip_rights) {
            XtLogger::warn_m("SEL_CFG",
                "selector.allow_skip_rights=true 使 rights_policy='{}' 无效："
                "配股行整条跳过（不除权、不摊薄、不认购），policy 只在未跳过时裁决",
                rights_policy_name(out.rights_policy));
        }
    }
    if (sel["date_from"]) out.date_from = sel["date_from"].as<uint32_t>(0);
    if (sel["date_to"])   out.date_to   = sel["date_to"].as<uint32_t>(0);

    // task — 当前 yaml 单 task；后续可扩列表。
    if (!sel["task"] || !sel["task"].IsMap()) {
        err = "selector.task is required";
        return false;
    }
    auto t = sel["task"];
    SelTaskCfg cfg;
    cfg.strategy_id = t["strategy_id"].as<std::string>("");
    if (!t["period"] || t["period"].IsNull()) {
        err = "selector.task.period is required";
        return false;
    }
    const std::string period = t["period"].as<std::string>();
    if (!parse_period(period, cfg.period)) {
        err = "selector.task.period must be one of minute,daily,weekly,monthly,yearly; got '" +
              period + "'";
        return false;
    }
    cfg.day         = t["day"].as<uint32_t>(1);
    cfg.time        = t["time"].as<uint32_t>(1500);
    cfg.strict_time = t["strict_time"].as<bool>(true);
    cfg.trdtpl      = t["trdtpl"].as<std::string>("CHINA");
    cfg.session     = t["session"].as<std::string>("default");
    if (cfg.strategy_id.empty()) {
        err = "selector.task.strategy_id is empty";
        return false;
    }
    out.tasks.push_back(std::move(cfg));

    // strategy — 当前 yaml 单 strategy；后续可扩列表。
    if (sel["strategy"] && sel["strategy"].IsMap()) {
        auto s = sel["strategy"];
        SelStrategyCfg cfg;
        cfg.strategy_id = s["id"].as<std::string>("");
        cfg.lib_path    = s["so"].as<std::string>("");
        if (s["params"]) cfg.params = s["params"];
        // so 允许空（external strategy 注入场景，run(cfg, strat)）；so 空且无
        // external 的错误下沉到 runner 装配点具名 fail-fast。id 仍必填。
        if (cfg.strategy_id.empty()) {
            err = "selector.strategy.id missing";
            return false;
        }
        out.strategies.push_back(std::move(cfg));
    }
    if (out.strategies.empty()) {
        err = "selector.strategy is required (at least one)";
        return false;
    }

    // account
    if (root["account"] && root["account"].IsMap()) {
        auto acc = root["account"];
        if (acc["initial_cash"]) out.initial_cash = acc["initial_cash"].as<double>(1'000'000.0);
    }

    // fees.dividend_tax_rate 是账户级单值，与 fees 下的 per-symbol 子 map 同级；
    // parse_fee_rule_book_config 把 fees 的每个子节点都当 symbol 规则（标量子节点会
    // 在取 item["open"] 时抛 InvalidNode）→ 先取走再从节点上摘除，之后照常交给
    // 共享 parser。摘除只作用于本次解析的内存文档。
    if (root["fees"] && root["fees"].IsMap() && root["fees"]["dividend_tax_rate"]) {
        const double rate = root["fees"]["dividend_tax_rate"].as<double>(0.0);
        if (!(rate >= 0.0) || rate >= 1.0) {
            err = "fees.dividend_tax_rate must be in [0, 1); got " + std::to_string(rate);
            return false;
        }
        out.dividend_tax_rate = rate;
        root["fees"].remove("dividend_tax_rate");
    }

    // commodities/fees — 复用 xt_config 全量 parser（capability flags / 印花税等）；
    // 非法枚举值/负费率 throw 转 bool+err（对齐 CTA，避免逃逸 terminate）。
    try {
        if (root["commodities"])
            out.commodities_cfg = parse_commodity_catalog_config(root["commodities"]);
        if (root["fees"])
            out.fee_cfg = parse_fee_rule_book_config(root["fees"]);
    } catch (const std::exception& e) {
        err = e.what();
        return false;
    }

    // SEL v1 是 long-only：XtRiskCfg::allow_short 默认 true（期货口径），完全省略
    // `risk:` 节的 config 会带着有效的 true 跑下去。缺省无条件置 false，显式写 true
    // 才 fail-fast——闸不能挂在「risk 节存在」这个偶然条件上。
    out.risk_cfg.allow_short = false;

    // risk — 仅解析当前用到的字段（max_order_qty / max_pos_qty / enforce_long_t_plus_1）
    if (root["risk"] && root["risk"].IsMap()) {
        auto rn = root["risk"];
        if (rn["max_order_qty"])         out.risk_cfg.max_order_qty         = rn["max_order_qty"].as<double>(0.0);
        if (rn["max_order_notional"])    out.risk_cfg.max_order_notional    = rn["max_order_notional"].as<double>(0.0);
        if (rn["max_pos_qty"])           out.risk_cfg.max_pos_qty           = rn["max_pos_qty"].as<double>(0.0);
        if (rn["enforce_long_t_plus_1"]) out.risk_cfg.enforce_long_t_plus_1 = rn["enforce_long_t_plus_1"].as<bool>(false);
        if (rn["allow_short"])           out.risk_cfg.allow_short           = rn["allow_short"].as<bool>(true);
        // BT-only（XtRiskCfg 是 live 共享类型，不为回测清算语义扩字段）。
        if (rn["delist_haircut_pct"]) {
            const double h = rn["delist_haircut_pct"].as<double>(0.0);
            if (!(h >= 0.0) || h >= 1.0) {
                err = "risk.delist_haircut_pct must be in [0, 1); got " + std::to_string(h);
                return false;
            }
            out.delist_haircut_pct = h;
        }
        // SEL v1 是 long-only：target 权重模型没有做空腿，engine 也不产负目标。
        // 放开 allow_short 只会让「跨零换向」的整单撞风控整体拒（而非拆成
        // 平多 + 开空两腿），是静默失真而不是能力扩展 → 显式拒配。
        if (out.risk_cfg.allow_short) {
            err = "risk.allow_short=true is not supported by the SEL backtest "
                  "(v1 is long-only: target weights carry no short leg, and a "
                  "zero-crossing order would be rejected as a whole). Set "
                  "risk.allow_short: false.";
            return false;
        }
    }

    // trade_cost — slippage 默认 0
    if (root["trade_cost"] && root["trade_cost"].IsMap()) {
        auto tn = root["trade_cost"];
        if (tn["slippage"]) out.trade_cost_cfg.slippage = tn["slippage"].as<double>(0.0);
    }

    // backtest.trace_mode —— 非法取值 fail-fast（HFT 侧靠 schema validator 兜，SEL
    // 的 schema 只查 key 存在性，取值必须在此判；静默退 off 会让「开了 trace 却没
    // 产物」看起来像引擎坏了）。
    if (root["backtest"] && root["backtest"].IsMap() && root["backtest"]["trace_mode"]) {
        const std::string tm = root["backtest"]["trace_mode"].as<std::string>("");
        if (tm == "full")      out.trace_full = true;
        else if (tm == "off")  out.trace_full = false;
        else {
            err = "backtest.trace_mode must be one of off,full; got '" + tm + "'";
            return false;
        }
    }

    // execution —— 执行真实度旋钮（BT-only）。取值非法一律 fail-fast：静默退默认会让
    // 「配了参与率上限却照样整笔成交」看起来像引擎没实现，而不是配置写错了。
    if (root["execution"] && root["execution"].IsMap()) {
        auto ex = root["execution"];
        out.execution_configured = true;
        if (ex["integer_projection"]) {
            const std::string ip = ex["integer_projection"].as<std::string>("");
            if (ip == "largest_remainder")   out.projection_largest_remainder = true;
            else if (ip == "floor")          out.projection_largest_remainder = false;
            else {
                err = "execution.integer_projection must be one of floor,"
                      "largest_remainder; got '" + ip + "'";
                return false;
            }
        }
        if (ex["max_participation"]) {
            const double p = ex["max_participation"].as<double>(0.1);
            if (!(p > 0.0) || p > 1.0) {
                err = "execution.max_participation must be in (0, 1]; got " +
                      std::to_string(p);
                return false;
            }
            // 量化后仍须 > 0：参与率在运行期是 1e6 定点，小于半个 tick 的取值会静默
            // 塌成 0 —— 那不是「上限极小」而是「一股都不许成交」，与写下的意图相反。
            if (xt_fp::to_fp(p) <= 0) {
                err = "execution.max_participation quantizes to 0 at 1e-6 precision; "
                      "got " + std::to_string(p);
                return false;
            }
            out.max_participation = p;
        }
        if (ex["carry_max_days"]) {
            const uint32_t d = ex["carry_max_days"].as<uint32_t>(5);
            // 上界 = 一个交易年：更长的「还在追同一笔调仓」在组合语义上已不成立
            // （下一次 rebalance 早就重算过目标了），多半是写错了单位。
            if (d > 250) {
                err = "execution.carry_max_days must be <= 250; got " +
                      std::to_string(d);
                return false;
            }
            out.carry_max_days = d;
        }
    }

    // report
    if (root["report"] && root["report"].IsMap()) {
        auto rep = root["report"];
        if (rep["output_dir"]) out.report_dir = rep["output_dir"].as<std::string>("./reports");
    }

    out.cfg_path = path;

    // 输入路径绝对化（F06）：同一份 config 从任意 cwd 跑必须读到同一批文件。
    // report.output_dir 刻意不动（输出目录由调用方/脚本决定，相对 cwd 是既有契约）。
    // display 副本先于绝对化留存：产物 source 字段写用户原串，绝对路径只服务打开文件
    // （否则同一 config 在不同目录下 summary.json 字节不同）。
    {
        out.display.trading_dates_csv     = out.trading_dates_csv;
        out.display.close_prices_csv      = out.close_prices_csv;
        out.display.tradability_csv       = out.tradability_csv;
        out.display.corporate_actions_csv = out.corporate_actions_csv;

        std::error_code ec;
        const auto cfg_dir = std::filesystem::absolute(path, ec).parent_path();
        resolve_input_path(out.trading_dates_csv, cfg_dir);
        resolve_input_path(out.close_prices_csv, cfg_dir);
        resolve_input_path(out.tradability_csv, cfg_dir);
        resolve_input_path(out.corporate_actions_csv, cfg_dir);
        // datalake root 也是输入路径（parquet 日历源）；漏掉它 = 相对 root 仍依赖 cwd。
        // ENV XT_DATALAKE_ROOT 的兜底在 runner 侧，不经本函数（env 由运维给绝对路径）。
        resolve_input_path(out.datalake_root, cfg_dir);
        for (auto& s : out.strategies) {
            resolve_input_path(s.lib_path, cfg_dir);
            // 策略参数里的文件路径同样绝对化（weights_path 是 SEL 的主输入）。
            if (s.params && s.params.IsMap() && s.params["weights_path"]) {
                std::string wp = s.params["weights_path"].as<std::string>("");
                resolve_input_path(wp, cfg_dir);
                s.params["weights_path"] = wp;
            }
        }
    }
    return true;
}

}  // namespace

bool load_sel_config(int argc, char** argv,
                       XtSelRunnerConfig& out, std::string& err) {
    std::string yaml_path;
    for (int i = 1; i < argc; ++i) {
        const char* arg = argv[i];
        if (!arg || arg[0] == '\0') continue;
        if (std::strcmp(arg, "--config") == 0 || std::strcmp(arg, "-c") == 0) {
            if (i + 1 >= argc) {
                err = "usage: XtSelBacktestRunner [--config|-c] <config.yaml>";
                return false;
            }
            yaml_path = argv[++i];
            break;
        }
        const char* prefix = "--config=";
        if (std::strncmp(arg, prefix, std::strlen(prefix)) == 0) {
            yaml_path = std::string(arg + std::strlen(prefix));
            break;
        }
        if (arg[0] != '-') {
            yaml_path = arg;
            break;
        }
        err = "usage: XtSelBacktestRunner [--config|-c] <config.yaml>";
        return false;
    }

    if (yaml_path.empty()) {
        err = "usage: XtSelBacktestRunner [--config|-c] <config.yaml>";
        return false;
    }
    std::ifstream test(yaml_path);
    if (!test) {
        err = "cannot open yaml: " + yaml_path;
        return false;
    }
    return parse_yaml(yaml_path, out, err);
}

}  // namespace xt_bt_sel
