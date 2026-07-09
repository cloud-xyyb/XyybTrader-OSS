#include "XtHftBacktestConfig.h"

#include <cstdio>
#include "xt_util/string/XtStringUtil.h"
#include "xt_util/time/XtTimeParse.h"
#include "xt_backtest/infra/XtSessionFilterParser.h"
#include "xt_config/yaml/ConfigYamlUtil.h"
#include "ConfigLocator.h"
#include "ConfigSchemaValidator.h"
#include "xt_cost/XtTradeCostConfigParser.h"
#include "xt_config/yaml/YAMLPathUtil.h"
#include "xt_config/yaml/YAMLMergeUtil.h"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cmath>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include <yaml-cpp/yaml.h>

#include "HFTMatchEngine.h"

bool parse_backtest_engine_config(const YAML::Node& backtest_node,
                             const YAML::Node& primary_strategy_node,
                             const std::string& config_path,
                             const std::string& stra_id,
                             XtHftBacktestEngineCfg& out,
                             std::string& err);

namespace {

bool is_defined(const YAML::Node& n) {
    return n && n.IsDefined() && !n.IsNull();
}

XtRiskCfg parse_risk_config(const YAML::Node& node) {
    XtRiskCfg cfg;
    cfg.max_order_qty = node["max_order_qty"] ? node["max_order_qty"].as<double>() : 0.0;
    cfg.max_order_notional = node["max_order_notional"] ? node["max_order_notional"].as<double>() : 0.0;
    cfg.min_order_notional = node["min_order_notional"] ? node["min_order_notional"].as<double>() : 0.0;
    cfg.max_pos_qty = node["max_pos_qty"] ? node["max_pos_qty"].as<double>() : 0.0;
    cfg.allow_short = node["allow_short"] ? node["allow_short"].as<bool>() : true;
    cfg.max_daily_loss = node["max_daily_loss"] ? node["max_daily_loss"].as<double>() : 0.0;
    cfg.max_annual_loss_pct = node["max_annual_loss_pct"] ? node["max_annual_loss_pct"].as<double>() : 0.0;
    return cfg;
}

bool parse_strategies_assembly(const YAML::Node& backtest_node,
                               XtHftBacktestConfig& out,
                               std::string& err) {
    const YAML::Node strategies_node = backtest_node["strategies"];
    const bool has_strategies_array = strategies_node && !strategies_node.IsNull();
    out.strategies.clear();

    if (has_strategies_array) {
        if (!strategies_node.IsSequence()) {
            err = "backtest.strategies 必须是 list（each entry: {id, so_path, params}）";
            return false;
        }
        if (strategies_node.size() == 0) {
            err = "backtest.strategies 不能为空";
            return false;
        }

        std::unordered_set<std::string> seen_ids;
        for (size_t i = 0; i < strategies_node.size(); ++i) {
            const auto& e = strategies_node[i];
            if (!e || !e.IsMap()) {
                err = "backtest.strategies[" + std::to_string(i) + "] 必须是 map";
                return false;
            }
            BtStrategyEntry entry;
            entry.factory_cfg = parse_strategy_factory_config(e);
            if (entry.factory_cfg.id.empty()) {
                err = "backtest.strategies[" + std::to_string(i) + "].id 必填";
                return false;
            }
            if (!seen_ids.insert(entry.factory_cfg.id).second) {
                err = "backtest.strategies[" + std::to_string(i) + "].id='" +
                      entry.factory_cfg.id + "' 重复 (engine routing 用 sid 做 demux key)";
                return false;
            }
            XtConfigYamlUtil::flatten_yaml_node_to_map(e, "", entry.init_cfg.values);
            out.strategies.push_back(std::move(entry));
        }

        if (strategies_node.size() > 1) {
            std::unordered_map<std::string, size_t> seen_symbols;
            for (size_t i = 0; i < strategies_node.size(); ++i) {
                const auto& e = strategies_node[i];
                const std::string sym = e["symbol"] ? e["symbol"].as<std::string>("") : "";
                if (sym.empty()) continue;
                auto [it, inserted] = seen_symbols.emplace(sym, i);
                if (!inserted) {
                    err = "backtest.strategies[" + std::to_string(i) + "].symbol='" + sym +
                          "' 与 strategies[" + std::to_string(it->second) +
                          "] 同 (多策略 SAME symbol 共享 account position pool 串扰风险)。"
                          " 同 symbol 多策略已不再支持,请拆成不同 symbol。";
                    return false;
                }
            }
        }

        out.strategy_cfg      = out.strategies[0].factory_cfg;
        out.strategy_init_cfg = out.strategies[0].init_cfg;
        return true;
    }
    err = "backtest.strategies 必须配置且不能为空";
    return false;
}

bool is_main_alias(const std::string& s) {
    const size_t at = s.find('@');
    if (at == std::string::npos) return false;
    const std::string_view tail(s.data() + at + 1, s.size() - at - 1);
    if (tail.size() != 4) return false;
    return std::toupper((unsigned char)tail[0]) == 'M'
        && std::toupper((unsigned char)tail[1]) == 'A'
        && std::toupper((unsigned char)tail[2]) == 'I'
        && std::toupper((unsigned char)tail[3]) == 'N';
}

std::string to_lower_ascii(const std::string& s) {
    std::string out = s;
    std::transform(out.begin(), out.end(), out.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return out;
}

std::string format_first_error(const std::vector<std::string>& errors) {
    assert(!errors.empty());
    std::string msg = errors.front();
    if (errors.size() > 1)
        msg += " (+" + std::to_string(errors.size() - 1) + " more)";
    return msg;
}

void dedupe_keep_order(std::vector<std::string>& items, bool skip_empty = false) {
    std::unordered_set<std::string> seen;
    std::vector<std::string> out;
    out.reserve(items.size());
    for (const auto& it : items) {
        if (skip_empty && it.empty()) continue;
        if (seen.insert(it).second) out.push_back(it);
    }
    items.swap(out);
}

void dedupe_errors(std::vector<std::string>& errors) {
    dedupe_keep_order(errors);
}

void apply_strategy_defaults(XtStrategyInitConfig& cfg) {
    if (cfg.get_string("throttle_mode", "").empty())
        cfg.values["throttle_mode"] = "tick";
}

void validate_strategy_throttle(const XtStrategyInitConfig& cfg,
                                 std::vector<std::string>& errors) {
    const std::string mode = to_lower_ascii(cfg.get_string("throttle_mode", ""));
    if (mode == "tick" || mode == "off" || mode == "none") return;
    errors.push_back("backtest.strategies[0].throttle_mode 仅支持 tick/off（不支持 wall_clock）");
}

void patch_tick_symbols_for_roll(const XtRollEngine::Config& roll_cfg,
                                 const XtStrategyInitConfig& stra_init_cfg,
                                 XtMarketDataSourceConfig& md_cfg) {
    if (!roll_cfg.active) return;

    const std::string stra_symbol = stra_init_cfg.get_string("symbol", "");
    bool wants_main = is_main_alias(stra_symbol);

    std::vector<std::string> keep;
    for (const auto& s : md_cfg.tick_symbols) {
        if (is_main_alias(s)) { wants_main = true; continue; }
        keep.push_back(s);
    }
    if (!wants_main) return;

    for (const auto& sw : roll_cfg.switches) {
        if (!sw.symbol.empty()) keep.push_back(sw.symbol);
    }
    dedupe_keep_order(keep,  true);
    md_cfg.tick_symbols.swap(keep);
    md_cfg.tick_auto_main = false;
}

void validate_risk_cfg(const XtRiskCfg& cfg, std::vector<std::string>& errors) {
    if (cfg.max_order_qty < 0.0)     errors.push_back("backtest.risk.max_order_qty 不能为负");
    if (cfg.max_order_notional < 0.0) errors.push_back("backtest.risk.max_order_notional 不能为负");
    if (cfg.max_pos_qty < 0.0)        errors.push_back("backtest.risk.max_pos_qty 不能为负");
}

void validate_trade_cost_cfg(const XtTradeCostConfig& cfg, std::vector<std::string>& errors) {
    if (cfg.slippage < 0.0) errors.push_back("backtest.cost.slippage 不能为负");
}

void validate_match_engine_cfg(const XtHftMatchEngineCfg& cfg, std::vector<std::string>& errors) {

    if (cfg.queue_cancel_ratio < 0.0 || cfg.queue_cancel_ratio > 1.0) errors.push_back("backtest.queue_cancel_ratio 必须在 [0,1]");
    if (cfg.error_rate         < 0.0 || cfg.error_rate         > 1.0) errors.push_back("backtest.error_rate 必须在 [0,1]");
    if (cfg.reject_rate        < 0.0 || cfg.reject_rate        > 1.0) errors.push_back("backtest.reject_rate 必须在 [0,1]");
    if (cfg.random_seed   < 0)    errors.push_back("backtest.seed 必须 >= 0");
    if (cfg.split_min_qty <= 0.0) errors.push_back("backtest.fill.min_qty 必须 > 0");

    if (cfg.split_max_qty <= 0.0)
        errors.push_back("backtest.fill.max_qty 必须 > 0");
    else if (cfg.split_max_qty < cfg.split_min_qty)
        errors.push_back("backtest.fill.max_qty 必须 >= min_qty");
    if (cfg.split_qty_tick <= 0.0) errors.push_back("backtest.fill.qty_tick 必须 > 0");
}

bool finalize_and_validate_backtest_config(XtHftBacktestConfig& cfg, std::string& err) {

    if (cfg.roll_cfg.active && cfg.roll_cfg.logical_symbol.empty()) {
        const std::string stra_symbol = cfg.strategy_init_cfg.get_string("symbol", "");
        if (is_main_alias(stra_symbol))
            cfg.roll_cfg.logical_symbol = stra_symbol;
    }

    patch_tick_symbols_for_roll(cfg.roll_cfg, cfg.strategy_init_cfg, cfg.datasource_cfg);
    apply_strategy_defaults(cfg.strategy_init_cfg);

    if (cfg.engine_cfg.match_delay_ticks >= 1) {
        cfg.datasource_cfg.skip_last_tick_per_day = true;
    }

    std::vector<std::string> validation_errors;
    cfg.strategy_cfg.validate(validation_errors);
    cfg.report_cfg.validate(validation_errors);
    cfg.event_stream_cfg.validate(validation_errors);
    cfg.replay_cfg.validate(validation_errors);
    cfg.engine_cfg.validate(validation_errors);
    cfg.datasource_cfg.validate(validation_errors);
    cfg.roll_cfg.validate(validation_errors);
    validate_strategy_throttle(cfg.strategy_init_cfg, validation_errors);
    validate_risk_cfg(cfg.risk_cfg, validation_errors);
    validate_trade_cost_cfg(cfg.cost_cfg, validation_errors);
    validate_match_engine_cfg(cfg.match_engine_cfg, validation_errors);

    if (cfg.match_engine_cfg.cancel_latency_enabled && !cfg.engine_cfg.latency_model.has_value())
        validation_errors.push_back(
            "backtest.cancel_latency_enabled=true 需要同时配置 backtest.latency_model");
    if (cfg.engine_cfg.margin_call_enabled
        && (cfg.engine_cfg.maintenance_ratio <= 0.0 || cfg.engine_cfg.maintenance_ratio > 1.0))
        validation_errors.push_back("backtest.maintenance_ratio 必须在 (0,1]");

    if (is_main_alias(cfg.strategy_init_cfg.get_string("symbol", "")) && !cfg.roll_cfg.active)
        validation_errors.push_back("策略 symbol 使用 @main 时，必须启用 backtest.roll");

    dedupe_errors(validation_errors);
    if (!validation_errors.empty()) {
        err = format_first_error(validation_errors);
        return false;
    }

    return true;
}

uint32_t parse_date_field(const std::string& s) {
    if (uint32_t v = XtTimeParse::parse_date_only(s)) return v;
    try { return static_cast<uint32_t>(std::stoul(s)); } catch (...) { return 0; }
}

bool parse_roll_switch_line(const std::string& line, XtRollEngine::SwitchPoint& out) {
    const std::string s = XtStringUtil::trim(line);
    if (s.empty() || s[0] == '#') return false;

    std::stringstream ss(s);
    std::string d, sym, known_on, old_close_str, new_close_str;
    if (!std::getline(ss, d,       ',')) return false;
    if (!std::getline(ss, sym,     ',')) return false;
    (void)std::getline(ss, known_on,       ',');
    (void)std::getline(ss, old_close_str,  ',');
    (void)std::getline(ss, new_close_str,  ',');

    d              = XtStringUtil::trim(d);
    sym            = XtStringUtil::trim(sym);
    known_on       = XtStringUtil::trim(known_on);
    old_close_str  = XtStringUtil::trim(old_close_str);
    new_close_str  = XtStringUtil::trim(new_close_str);
    if (d.empty() || sym.empty()) return false;

    out.date = parse_date_field(d);
    if (out.date == 0) return false;
    out.symbol = sym;
    if (!known_on.empty())
        out.known_on_date = parse_date_field(known_on);

    if (!old_close_str.empty() && !new_close_str.empty()) {
        try {
            out.old_close = std::stod(old_close_str);
            out.new_close = std::stod(new_close_str);
        } catch (...) {
            out.old_close = 0.0;
            out.new_close = 0.0;
        }
    }
    return true;
}

}

XtRollEngine::Config parse_roll_config(const YAML::Node& node) {
    XtRollEngine::Config cfg;
    if (!node || node.IsNull()) return cfg;

    if (node["enabled"]) {
        throw std::runtime_error("roll.enabled 已退役，请改用 roll.active");
    }
    cfg.active                  = XtConfigYamlUtil::as_or<bool>(node, "active", false);
    cfg.logical_symbol          = XtConfigYamlUtil::as_or<std::string>(node,  "logical_symbol", "");
    cfg.window_before_days      = XtConfigYamlUtil::as_or<int>(node,  "window_before_days", 0);
    cfg.window_after_days       = XtConfigYamlUtil::as_or<int>(node,  "window_after_days", 0);
    cfg.dispatch_parallel_ticks = XtConfigYamlUtil::as_or<bool>(node, "dispatch_parallel_ticks", false);

    if (node["path"]) {
        try {
            const std::string path_str = node["path"].as<std::string>();
            std::ifstream in(path_str);
            if (in.good()) {
                std::string line;
                int line_no = 0;
                while (std::getline(in, line)) {
                    ++line_no;
                    XtRollEngine::SwitchPoint sw;
                    if (parse_roll_switch_line(line, sw))
                        cfg.switches.push_back(std::move(sw));

                    else if (!XtStringUtil::trim(line).empty() && XtStringUtil::trim(line)[0] != '#') {
                        fprintf(stderr, "[roll] %s:%d: 无法解析换月记录，跳过\n",
                                path_str.c_str(), line_no);
                    }
                }
            } else {
                fprintf(stderr, "[roll] 无法打开换月路径文件: %s\n", path_str.c_str());
            }
        } catch (const std::exception& e) {
            fprintf(stderr, "[roll] 加载换月路径异常: %s\n", e.what());
        }
    }

    std::sort(cfg.switches.begin(), cfg.switches.end(),
              [](const auto& a, const auto& b) { return a.date < b.date; });
    std::vector<XtRollEngine::SwitchPoint> compact;
    for (const auto& sw : cfg.switches) {
        if (!compact.empty() && compact.back().date == sw.date)
            compact.back() = sw;
        else
            compact.push_back(sw);
    }
    cfg.switches.swap(compact);
    return cfg;
}

namespace {

XtConfigSchemaRules build_backtest_schema_rules() {
    XtConfigSchemaRules rules;

    auto add = [&rules](std::initializer_list<const char*> paths) {
        for (const char* p : paths)
            rules.allowed_paths.emplace_back(p);
    };

    add({
        "log.level", "log.console", "log.file",
        "log.max_size", "log.max_files", "log.queue_size", "log.threads",
        "log.modules.*", "log.throttle_ms.*",
    });

    add({

        "commodities.*.price_tick", "commodities.*.vol_scale", "commodities.*.allow_short",
        "commodities.*.margin_rate", "commodities.*.slippage",
        "commodities.*.has_open_interest", "commodities.*.has_settle_price",
        "commodities.*.is_t_plus_1", "commodities.*.min_lot",
        "commodities.*.has_broker_frozen_push", "commodities.*.daily_price_limit_pct",
        "commodities.*.offset_policy",
        "fees.*.open", "fees.*.close", "fees.*.close_today", "fees.*.by_volume",
        "fees.*.maker_rebate", "fees.*.stamp_duty_rate",
        "fees.*.transfer_fee_rate", "fees.*.min_commission",
    });

    add({
        "backtest.replay_sleep_ms",
        "backtest.start_date", "backtest.end_date",
    });

    add({
        "backtest.report_dir", "backtest.report_contract_dir",
        "backtest.trace_mode",
        "backtest.full_max_rows", "backtest.full_max_bytes",
        "backtest.export_tick",
        "backtest.sys_stats_sample_n_ticks",
        "backtest.max_latency_map_entries",
    });

    add({

        "backtest.fill_return_latency_ms",
        "backtest.cancel_latency_enabled",
        "backtest.price_limit_enabled",
        "backtest.margin_call_enabled",
        "backtest.maintenance_ratio",
        "backtest.risk.self_match_check",
        "backtest.eof_grace_ms",
        "backtest.seed", "backtest.t1_enabled",
        "backtest.account", "backtest.account.cash",
        "backtest.queue_cancel_ratio",
        "backtest.ack_delay_ticks", "backtest.order_timeout_ticks",
        "backtest.error_rate", "backtest.reject_rate",
        "backtest.volume_limit",
        "backtest.match_delay_ticks",
        "backtest.session_offset_minutes",
        "backtest.clock_resolution_ns",
        "backtest.conditional_orders_enabled",
    });

    add({
        "backtest.fill.split", "backtest.fill.min_qty",
        "backtest.fill.max_qty", "backtest.fill.qty_tick",
    });

    add({
        "backtest.risk.max_order_qty", "backtest.risk.max_order_notional",
        "backtest.risk.max_pos_qty",   "backtest.risk.allow_short",
        "backtest.risk.enforce_long_t_plus_1",

        "backtest.risk.max_daily_loss",
        "backtest.risk.max_annual_loss_pct",
    });

    add({
        "backtest.cost.slippage",
    });

    add({
        "backtest.datasource.mode",      "backtest.datasource.data_mode",
        "backtest.datasource.stream",    "backtest.datasource.replay_mode",
        "backtest.datasource.bar_path",
        "backtest.datasource.bar.interval_sec",
        "backtest.datasource.bar.root",
        "backtest.datasource.bar.symbol",
        "backtest.datasource.bar.suffix",
        "backtest.datasource.bar.code",
        "backtest.datasource.bar.use_hot",         "backtest.datasource.bar.hot_path",
        "backtest.datasource.bar.simtick.enable",  "backtest.datasource.bar.simtick.dir",
        "backtest.datasource.bar.simtick.format",  "backtest.datasource.bar.simtick.spread",
        "backtest.datasource.bar.simtick.qty_base",
        "backtest.datasource.tick.root",
        "backtest.datasource.tick.fbs_root",
        "backtest.datasource.tick.events_parquet_root",
        "backtest.datasource.tick.cache_dir",
        "backtest.datasource.tick.use_bin_cache", "backtest.datasource.tick.volume_mode",
        "backtest.datasource.tick.qty_base",   "backtest.datasource.tick.main_symbol",
        "backtest.datasource.tick.auto_main",
        "backtest.datasource.tick.files[]",    "backtest.datasource.tick.symbols[]",
        "backtest.datasource.orderflow.order_queue_files[]",
        "backtest.datasource.orderflow.order_detail_files[]",
        "backtest.datasource.orderflow.transaction_files[]",
        "backtest.datasource.orderflow.order_queue.files[]",
        "backtest.datasource.orderflow.order_detail.files[]",
        "backtest.datasource.orderflow.transaction.files[]",
        "backtest.datasource.order_queue_files[]",
        "backtest.datasource.order_detail_files[]",
        "backtest.datasource.transaction_files[]",
    });

    add({
        "backtest.session.enabled", "backtest.session.sections[]",
        "backtest.calendar.enabled",
        "backtest.calendar.days[]",         "backtest.calendar.dates[]",
        "backtest.calendar.trading_days[]", "backtest.calendar.include[]",
        "backtest.calendar.exclude[]",      "backtest.calendar.path",
    });

    add({
        "backtest.roll.active",
        "backtest.roll.logical_symbol",
        "backtest.roll.path",              "backtest.roll.window_before_days",
        "backtest.roll.window_after_days", "backtest.roll.dispatch_parallel_ticks",
        "backtest.roll.strict_no_lookahead",
    });

    add({
        "backtest.latency_model.type",
        "backtest.latency_model.entry_us",
        "backtest.latency_model.response_us",
        "backtest.latency_model.source",
        "backtest.queue_model.type",
        "backtest.queue_model.ratio",
        "backtest.queue_model.cancel_resubmit_ratio",
        "backtest.queue_model.k",
    });

    rules.open_prefixes = {

        "backtest.strategies",

        "runtime",
    };
    return rules;
}

void validate_backtest_enum_fields(const YAML::Node& root, std::vector<std::string>& errors) {
    validate_enum_field(root, "backtest.latency_model.type",
                        {"constant", "interpolated", "cdf"}, errors);
    validate_enum_field(root, "backtest.queue_model.type",
                        {"ratio", "prob_power1", "prob_power2"}, errors);
    validate_enum_field(root, "backtest.datasource.tick.volume_mode",
                        {"cumulative", "incremental", "inc"}, errors, nullptr, false);
    validate_enum_field(root, "backtest.datasource.mode",
                        {"tick", "bar"}, errors);
    validate_enum_field(root, "backtest.datasource.data_mode",
                        {"stream", "memory"}, errors);
    validate_enum_field(root, "backtest.datasource.replay_mode",
                        {"tick_only", "event_stream"}, errors);
    validate_enum_field(root, "backtest.datasource.bar.simtick.format",
                        {"bin", "csv"}, errors);
    validate_enum_field(root, "backtest.trace_mode",
                        {"off", "incident", "full"}, errors, nullptr, false);
}

}

bool load_backtest_config(int argc, char** argv, XtHftBacktestConfig& out, std::string& err) {
    out = {};
    err.clear();

    const std::string cfg_path = xt_locate_config(argc, argv, "XT_CONFIG", "backtest.yaml");
    if (cfg_path.empty()) { err = "config not found"; return false; }

    YAML::Node root;
    std::string load_err;
    if (!XtConfigYamlUtil::load_profile_with_common_and_market(
            cfg_path, "common.yaml", "market.yaml", root, &load_err)) {

        if (load_err.rfind("config not found", 0) == 0) err = load_err;
        else if (!load_err.empty())             err = "config parse failed: " + load_err;
        else                                    err = "config parse failed";
        return false;
    }

    std::vector<std::string> schema_errors;
    validate_unknown_config_keys(root, build_backtest_schema_rules(), schema_errors);
    validate_backtest_enum_fields(root, schema_errors);
    if (!schema_errors.empty()) { err = format_first_error(schema_errors); return false; }

    const YAML::Node log_node         = XtConfigYamlUtil::get_path_node(root, "log");
    const YAML::Node backtest_node    = XtConfigYamlUtil::get_path_node(root, "backtest");
    const YAML::Node commodities_node = XtConfigYamlUtil::get_path_node(root, "commodities");
    const YAML::Node fees_node        = XtConfigYamlUtil::get_path_node(root, "fees");

    if (!is_defined(log_node))         { err = "log config missing"; return false; }
    if (!is_defined(backtest_node))    { err = "backtest config missing"; return false; }
    if (!is_defined(commodities_node)) { err = "commodities missing (expected in market.yaml)"; return false; }
    if (!is_defined(fees_node))        { err = "fees missing (expected in market.yaml)"; return false; }

    const YAML::Node risk_node     = backtest_node["risk"];
    const YAML::Node cost_node     = backtest_node["cost"];

    if (!is_defined(risk_node)) { err = "backtest.risk missing"; return false; }
    if (!is_defined(cost_node)) { err = "backtest.cost missing"; return false; }

    try {
        const uint32_t start_date = XtConfigYamlUtil::parse_date_node(backtest_node["start_date"]);
        const uint32_t end_date   = XtConfigYamlUtil::parse_date_node(backtest_node["end_date"]);
        if (start_date == 0 || end_date == 0) {
            err = "backtest.start_date/backtest.end_date 必须配置且有效";
            return false;
        }
        if (start_date > end_date) {
            err = "backtest.start_date 不能晚于 backtest.end_date";
            return false;
        }

        out.cfg_path      = cfg_path;
        out.log_cfg       = parse_logger_config(log_node);

        if (!parse_strategies_assembly(backtest_node, out, err)) {
            return false;
        }
        const YAML::Node primary_strategy_node = backtest_node["strategies"][0];

        XtMarketDataSourceConfig md_defaults;
        out.datasource_cfg            = parse_market_data_source_config(backtest_node, md_defaults);
        out.datasource_cfg.start_date = start_date;
        out.datasource_cfg.end_date   = end_date;

        out.replay_cfg       = parse_replayer_config(backtest_node);

        constexpr int64_t kHftMinClockResolutionNs = 250'000'000;
        if (out.replay_cfg.clock_resolution_ns < kHftMinClockResolutionNs) {
            out.replay_cfg.clock_resolution_ns = kHftMinClockResolutionNs;
        }
        out.report_cfg = xt_report::parse_report_config(backtest_node);
        out.event_stream_cfg = parse_event_stream_sink_config(backtest_node);

        std::string engine_err;
        if (!parse_backtest_engine_config(backtest_node, primary_strategy_node,
                                          cfg_path, out.strategy_cfg.id,
                                          out.engine_cfg, engine_err)) {
            err = engine_err.empty() ? "invalid backtest engine config" : engine_err;
            return false;
        }

        out.risk_cfg         = parse_risk_config(risk_node);
        if (risk_node["enforce_long_t_plus_1"]) {
            out.risk_cfg.enforce_long_t_plus_1 =
                risk_node["enforce_long_t_plus_1"].as<bool>();
        }
        if (risk_node["self_match_check"]) {
            out.self_match_check = risk_node["self_match_check"].as<bool>();
        }
        out.cost_cfg         = engine_config::parse_trade_cost_config(cost_node);
        out.match_engine_cfg = engine_config::parse_match_engine_config(backtest_node);
        out.session_cfg      = xt_core::parse_config(backtest_node["session"]);

        out.datasource_cfg.session = out.session_cfg;
        out.calendar_cfg     = parse_trading_calendar_config(backtest_node["calendar"]);

        const YAML::Node roll_node = backtest_node["roll"];
        out.roll_cfg = parse_roll_config(roll_node);

        if (out.roll_cfg.active && out.roll_cfg.switches.empty()) {
            err = "backtest.roll 已启用但无换月记录"
                  "（roll.path 未配置，或文件不存在/无有效行）";
            return false;
        }

        out.commodities_cfg  = parse_commodity_catalog_config(commodities_node);
        out.fees_cfg         = parse_fee_rule_book_config(fees_node);

        if (out.commodities_cfg.map.empty()) { err = "commodities empty"; return false; }
        if (out.fees_cfg.rules.empty())      { err = "fees empty"; return false; }
    } catch (const std::exception& e) {
        err = std::string("config parse failed: ") + e.what();
        return false;
    } catch (...) {
        err = "config parse failed";
        return false;
    }

    return finalize_and_validate_backtest_config(out, err);
}

namespace {

bool parse_latency_model_node(const YAML::Node& backtest_node, XtHftBacktestEngineCfg& out,
                             std::string& err) {
    const YAML::Node lm = backtest_node["latency_model"];
    if (!lm) return true;
    if (!lm.IsMap()) { err = "backtest.latency_model must be a map"; return false; }
    LatencyModelCfg cfg;
    cfg.type = lm["type"] ? lm["type"].as<std::string>() : "";
    cfg.entry_us = lm["entry_us"] ? lm["entry_us"].as<int64_t>() : 0;
    cfg.response_us = lm["response_us"] ? lm["response_us"].as<int64_t>() : 0;
    cfg.source = lm["source"] ? lm["source"].as<std::string>() : "";
    if (cfg.type.empty()) {
        err = "backtest.latency_model.type 必须为 constant | interpolated | cdf";
        return false;
    }
    out.latency_model = std::move(cfg);
    return true;
}

bool parse_queue_model_node(const YAML::Node& backtest_node, XtHftBacktestEngineCfg& out,
                           std::string& err) {
    const YAML::Node qm = backtest_node["queue_model"];
    if (!qm) return true;
    if (!qm.IsMap()) { err = "backtest.queue_model must be a map"; return false; }
    QueueModelCfg cfg;
    cfg.type = qm["type"] ? qm["type"].as<std::string>() : "";
    cfg.ratio = qm["ratio"] ? qm["ratio"].as<double>() : 0.0;
    cfg.cancel_resubmit_ratio = qm["cancel_resubmit_ratio"]
        ? qm["cancel_resubmit_ratio"].as<double>()
        : 1.0;
    cfg.k = qm["k"] ? qm["k"].as<double>() : 1.0;
    if (cfg.type.empty()) {
        err = "backtest.queue_model.type 必须为 ratio | prob_power1 | prob_power2";
        return false;
    }
    out.queue_model = std::move(cfg);
    return true;
}

}

bool parse_backtest_engine_config(const YAML::Node& backtest_node,
                                  const YAML::Node& primary_strategy_node,
                                  const std::string& config_path,
                                  const std::string& stra_id,
                                  XtHftBacktestEngineCfg& out,
                                  std::string& err) {
    out = XtHftBacktestEngineCfg{};
    if (!backtest_node || backtest_node.IsNull()) {
        err = "backtest config missing";
        return false;
    }
    try {

        out.fill_return_latency_ms = backtest_node["fill_return_latency_ms"]
            ? backtest_node["fill_return_latency_ms"].as<uint32_t>()
            : 0;

        out.eof_grace_ms = backtest_node["eof_grace_ms"]
            ? backtest_node["eof_grace_ms"].as<uint32_t>()
            : 0;

        out.margin_call_enabled = backtest_node["margin_call_enabled"]
            ? backtest_node["margin_call_enabled"].as<bool>()
            : false;
        out.maintenance_ratio = backtest_node["maintenance_ratio"]
            ? backtest_node["maintenance_ratio"].as<double>()
            : 0.9;

        if (!parse_latency_model_node(backtest_node, out, err)) return false;
        if (!parse_queue_model_node(backtest_node, out, err)) return false;
        if (backtest_node["t1_enabled"] && backtest_node["t1_enabled"].IsDefined() && !backtest_node["t1_enabled"].IsNull()) {
            out.t1_mode = backtest_node["t1_enabled"].as<bool>()
                              ? XtHftBacktestEngineCfg::T1Mode::Enabled
                              : XtHftBacktestEngineCfg::T1Mode::Disabled;
        } else {
            out.t1_mode = XtHftBacktestEngineCfg::T1Mode::Unset;
        }
        const YAML::Node account_node = backtest_node["account"];
        out.initial_cash = (account_node && account_node["cash"]) ? account_node["cash"].as<double>() : 1000000.0;

        if (const YAML::Node mdt_node = backtest_node["match_delay_ticks"]; mdt_node) {
            out.match_delay_ticks = mdt_node.as<uint32_t>();
        }
        out.session_offset_minutes = backtest_node["session_offset_minutes"]
            ? backtest_node["session_offset_minutes"].as<int>() : 0;

        out.conditional_orders_enabled =
            XtConfigYamlUtil::as_or<bool>(backtest_node, "conditional_orders_enabled", false);

        const bool stra_node_present = primary_strategy_node && !primary_strategy_node.IsNull();
        out.history_ticks = (stra_node_present && primary_strategy_node["history_ticks"])
                                ? primary_strategy_node["history_ticks"].as<size_t>() : 2048;
        out.history_bars  = (stra_node_present && primary_strategy_node["history_bars"])
                                ? primary_strategy_node["history_bars"].as<size_t>()  : 512;
        out.debug_signal_log_enabled =
            (stra_node_present && primary_strategy_node["debug_signal_log"])
                ? primary_strategy_node["debug_signal_log"].as<bool>() : false;
        if (stra_node_present && primary_strategy_node["debug_signal_log_every_n_ticks"]) {
            out.debug_signal_log_every_n_ticks =
                std::max<uint32_t>(1, primary_strategy_node["debug_signal_log_every_n_ticks"].as<uint32_t>());
        }
    } catch (const std::exception& ex) {
        err = std::string("invalid backtest engine config: ") + ex.what();
        return false;
    } catch (...) {
        err = "invalid backtest engine config";
        return false;
    }

    out.config_path = config_path;
    out.stra_id = stra_id;
    return true;
}

void XtHftBacktestEngineCfg::validate(std::vector<std::string>& errors) const {
    if (!std::isfinite(initial_cash) || initial_cash < 0.0) {
        errors.push_back("backtest.account.cash must be non-negative finite");
    }
    if (history_ticks == 0) {
        errors.push_back("backtest.strategies[0].history_ticks must be > 0");
    }
    if (history_bars == 0) {
        errors.push_back("backtest.strategies[0].history_bars must be > 0");
    }

}
