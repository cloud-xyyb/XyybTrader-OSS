#include "XtHftBacktestConfig.h"

#include <cstdint>
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

// 其余 parse_* 声明由各自 owner header 提供（经 XtHftBacktestConfig.h 传递包含）。
// parse_backtest_engine_config 定义在本文件下方，此处为同 TU 前置声明。
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

// BT-only YAML→XtRiskCfg 基础字段解析；per-strategy 等 live 专属键由 live parser 负责，
// BT 路径不消费（risk 规则引擎不持有 yaml 依赖，解析归 caller 侧）。
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

// load_backtest_config strategies 装配。
// 只接受 backtest.strategies[]；单数 backtest.strategy 已退休并由 schema fail-fast。
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
        // id 唯一性校验 (engine routing 用 sid demux)
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
        // Hard guard: 多策略 SAME symbol 共享 account position pool → fan-out
        // NET 仓位串扰 internal counter (HFT 无 portfolio router)，同 symbol 多策略一律拒绝。
        // symbol 从 yaml entry 直接读 (flatten 后 init_cfg.values 也含 symbol,
        // 这里 schema 检查更直接 — 与 live 路径同口径)。
        if (strategies_node.size() > 1) {
            std::unordered_map<std::string, size_t> seen_symbols;
            for (size_t i = 0; i < strategies_node.size(); ++i) {
                const auto& e = strategies_node[i];
                const std::string sym = e["symbol"] ? e["symbol"].as<std::string>("") : "";
                if (sym.empty()) continue;  // 沿用全局 datasource symbol,不做 per-strategy 区分
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
        // 镜像字段:strategy_cfg/strategy_init_cfg 对齐 strategies[0]
        out.strategy_cfg      = out.strategies[0].factory_cfg;
        out.strategy_init_cfg = out.strategies[0].init_cfg;
        return true;
    }
    err = "backtest.strategies 必须配置且不能为空";
    return false;
}

// 大小写不敏感匹配 "@main" 后缀
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

// 回测强制 tick 节流，避免策略默认回落到 wall_clock（回测无真实时钟）
void apply_strategy_defaults(XtStrategyInitConfig& cfg) {
    if (cfg.get_string("throttle_mode", "").empty())
        cfg.values["throttle_mode"] = "tick";
}

// 只读校验，须在 apply_strategy_defaults 之后调用
void validate_strategy_throttle(const XtStrategyInitConfig& cfg,
                                 std::vector<std::string>& errors) {
    const std::string mode = to_lower_ascii(cfg.get_string("throttle_mode", ""));
    if (mode == "tick" || mode == "off" || mode == "none") return;
    errors.push_back("backtest.strategies[0].throttle_mode 仅支持 tick/off（不支持 wall_clock）");
}

// 换月模式下将 tick_symbols 中的 @main 别名展开为 roll.switches 的具体合约列表。
// 策略 symbol 与 tick_symbols 两处均检查，任一使用 @main 即触发展开。
void patch_tick_symbols_for_roll(const XtRollEngine::Config& roll_cfg,
                                 const XtStrategyInitConfig& stra_init_cfg,
                                 XtMarketDataSourceConfig& md_cfg) {
    if (!roll_cfg.active) return;

    const std::string stra_symbol = stra_init_cfg.get_string("symbol", "");
    bool wants_main = is_main_alias(stra_symbol);

    // (1) 过滤掉 @main 别名，保留真实合约
    std::vector<std::string> keep;
    for (const auto& s : md_cfg.tick_symbols) {
        if (is_main_alias(s)) { wants_main = true; continue; }
        keep.push_back(s);
    }
    if (!wants_main) return;

    // (2) 从换月记录中提取所有真实合约名
    for (const auto& sw : roll_cfg.switches) {
        if (!sw.symbol.empty()) keep.push_back(sw.symbol);
    }
    dedupe_keep_order(keep, /*skip_empty=*/true);
    md_cfg.tick_symbols.swap(keep);
    md_cfg.tick_auto_main = false;
}

// 值域门统一走「有限非负」：裸 `< 0` 漏 NaN（比较恒 false）与 +inf（inf >= 0 为真），
// 穿门后污染下游定点换算与限额比较。谓词与文案均对齐 live 侧同族门
// （LiveConfigValidators.cpp `runtime.risk.*`）。
bool is_negative_or_non_finite(double x) { return !std::isfinite(x) || x < 0.0; }

void validate_risk_cfg(const XtRiskCfg& cfg, std::vector<std::string>& errors) {
    if (is_negative_or_non_finite(cfg.max_order_qty))
        errors.push_back("backtest.risk.max_order_qty 不能为负");
    if (is_negative_or_non_finite(cfg.max_order_notional))
        errors.push_back("backtest.risk.max_order_notional 不能为负");
    if (is_negative_or_non_finite(cfg.min_order_notional))
        errors.push_back("backtest.risk.min_order_notional 不能为负");
    if (is_negative_or_non_finite(cfg.max_pos_qty))
        errors.push_back("backtest.risk.max_pos_qty 不能为负");
}

void validate_trade_cost_cfg(const XtTradeCostConfig& cfg, std::vector<std::string>& errors) {
    if (is_negative_or_non_finite(cfg.slippage))
        errors.push_back("backtest.cost.slippage 不能为负");
}

void validate_match_engine_cfg(const XtHftMatchEngineCfg& cfg, std::vector<std::string>& errors) {
    // queue_ahead_ratio 由 queue_model.ratio 表达，并在 RatioQueueModel 构造期 clamp。
    if (cfg.queue_cancel_ratio < 0.0 || cfg.queue_cancel_ratio > 1.0) errors.push_back("backtest.queue_cancel_ratio 必须在 [0,1]");
    if (cfg.error_rate         < 0.0 || cfg.error_rate         > 1.0) errors.push_back("backtest.error_rate 必须在 [0,1]");
    if (cfg.reject_rate        < 0.0 || cfg.reject_rate        > 1.0) errors.push_back("backtest.reject_rate 必须在 [0,1]");
    if (cfg.random_seed   < 0)    errors.push_back("backtest.seed 必须 >= 0");
    if (cfg.split_min_qty <= 0.0) errors.push_back("backtest.fill.min_qty 必须 > 0");
    // else if 避免同一字段报两条错误（split_max_qty <= 0 与 max < min 属同一根因）
    if (cfg.split_max_qty <= 0.0)
        errors.push_back("backtest.fill.max_qty 必须 > 0");
    else if (cfg.split_max_qty < cfg.split_min_qty)
        errors.push_back("backtest.fill.max_qty 必须 >= min_qty");
    if (cfg.split_qty_tick <= 0.0) errors.push_back("backtest.fill.qty_tick 必须 > 0");
    // TTL 上界：ceil-us→ns（(ttl_us+999)/1000×1e6）不得越 int64——巨大 ttl_us 会让
    // deadline 饱和/退化（TTL 立即到期或永不到期）。纯溢出护栏，正常 TTL 远达不到。
    if (cfg.ttl_us > 0) {
        const __int128 ttl_ns =
            ((static_cast<__int128>(cfg.ttl_us) + 999) / 1000) * 1'000'000;
        if (ttl_ns > static_cast<__int128>(INT64_MAX))
            errors.push_back("backtest.order_timeout ttl_us 过大：ceil-us→ns 越 int64 上界");
    }
}

bool finalize_and_validate_backtest_config(XtHftBacktestConfig& cfg, std::string& err) {
    // ── derive 阶段：补全派生字段 ──────────────────────────────────────────
    // (1) logical_symbol 回填：策略 @main 别名 → 换月逻辑标的
    if (cfg.roll_cfg.active && cfg.roll_cfg.logical_symbol.empty()) {
        const std::string stra_symbol = cfg.strategy_init_cfg.get_string("symbol", "");
        if (is_main_alias(stra_symbol))
            cfg.roll_cfg.logical_symbol = stra_symbol;
    }
    // (2) tick_symbols 展开 + 策略默认值补全
    patch_tick_symbols_for_roll(cfg.roll_cfg, cfg.strategy_init_cfg, cfg.datasource_cfg);
    apply_strategy_defaults(cfg.strategy_init_cfg);
    // (3) match_delay >= 1 时自动启用 skip_last_tick_per_day：
    //     replayer 到流尾不为最后一笔 tick 额外生成 dispatch
    if (cfg.engine_cfg.match_delay_ticks >= 1) {
        cfg.datasource_cfg.skip_last_tick_per_day = true;
    }

    // ── validate 阶段：全部只读，不修改 cfg ────────────────────────────────
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
    // cancel 延迟依赖 latency model 的 entry 采样；无模型时开关是静默 no-op，
    // fail-fast 拒绝此类自相矛盾配置。
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

// 日期双格式解析：优先 "2023-01-05" 字符串，失败回退 YYYYMMDD 整数（换月路径文件常见写法）
uint32_t parse_date_field(const std::string& s) {
    if (uint32_t v = XtTimeParse::parse_date_only(s)) return v;
    try { return static_cast<uint32_t>(std::stoul(s)); } catch (...) { return 0; }
}

// 解析换月路径记录："date,symbol[,known_on_date[,old_close,new_close]]"，# 开头为注释行
// 兼容 3 列（旧格式）和 5 列（新格式含复权价格）
bool parse_roll_switch_line(const std::string& line, XtRollEngine::SwitchPoint& out) {
    const std::string s = XtStringUtil::trim(line);
    if (s.empty() || s[0] == '#') return false;

    std::stringstream ss(s);
    std::string d, sym, known_on, old_close_str, new_close_str;
    if (!std::getline(ss, d,       ',')) return false;
    if (!std::getline(ss, sym,     ',')) return false;
    (void)std::getline(ss, known_on,       ',');  // known_on_date 可选
    (void)std::getline(ss, old_close_str,  ',');  // old_close 可选（复权用）
    (void)std::getline(ss, new_close_str,  ',');  // new_close 可选（复权用）

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
    // 复权价格：两列都非空时解析
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

} // namespace (close anonymous namespace — parse_roll_config 需要外部链接)

// 文件不存在或格式全错时 switches 留空（静默），
// 调用方 load_backtest_config 在 active=true 时对空 switches 做 fast-fail。
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
                    // 非空非注释行解析失败时记录警告（但不阻断加载）
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

    // 按日期排序，同日保留最后一条（后者覆盖前者）
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

// ---------------------------------------------------------------------------
// Schema 白名单构建
//
// 按子系统分组，新增配置项在对应分组末尾追加。
// ---------------------------------------------------------------------------
XtConfigSchemaRules build_backtest_schema_rules() {
    XtConfigSchemaRules rules;

    auto add = [&rules](std::initializer_list<const char*> paths) {
        for (const char* p : paths)
            rules.allowed_paths.emplace_back(p);
    };

    // runtime.* 整子树通过 open_prefixes 放行（见函数末尾）

    add({
        "log.level", "log.console", "log.file",
        "log.max_size", "log.max_files", "log.queue_size", "log.threads",
        "log.modules.*", "log.throttle_ms.*",
    });

    add({
        // 与 parse_commodity_map (XtCommodityCatalog.cpp) / parse_fee_rule_book_config
        // (XtFeeRuleBook.cpp) 实读字段保持一致：parser 支持而白名单缺失 = 配了即被误拒。
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
        // backtest.fill_latency_us / fill_latency_stddev_us /
        // queue_ahead_ratio / cancel_resubmit_queue_ratio 已移除。
        // 用 backtest.latency_model + backtest.queue_model 节替代。
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
        // min_order_notional 由 parse_risk_config 读、XtRiskEngine 消费（下界名义金额，
        // 仅约束增仓）；allowlist 缺此键会让配了它的 BT profile 在加载期 fail-fast，
        // 与 live 同名键（runtime.risk.min_order_notional）不对称。
        "backtest.risk.min_order_notional",
        "backtest.risk.max_pos_qty",   "backtest.risk.allow_short",
        "backtest.risk.enforce_long_t_plus_1",
        // daily / annual loss 风控字段由 schema allowlist 显式放行；未知 key 仍 fail-fast。
        "backtest.risk.max_daily_loss",
        "backtest.risk.max_annual_loss_pct",
    });

    add({
        "backtest.cost.slippage",
    });

    add({
        "backtest.tick_cache.format",
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
        // HFT backtest strategy 数组形式 yaml,strategies[i].* 全开放
        // (id / so_path / 策略 init params 自由布局)。
        "backtest.strategies",
        // 整 runtime 子树放开（不消费 secrets/live 字段）。
        // load_profile_with_common_and_market 自动 merge configs/secrets.yaml，
        // 后者带 runtime.td.binance.api_key 等。backtest 不读取这些字段，
        // 但 schema 见到要 ack。整棵子树 open 而非逐字段 whitelist 减少维护负担。
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
    validate_enum_field(root, "backtest.tick_cache.format",
                        {"v2", "v3"}, errors);
    validate_enum_field(root, "backtest.datasource.mode",
                        {"tick", "bar"}, errors);
    validate_enum_field(root, "backtest.datasource.data_mode",
                        {"stream", "memory"}, errors);
    validate_enum_field(root, "backtest.datasource.replay_mode",
                        {"tick_only", "event_stream"}, errors);
    validate_enum_field(root, "backtest.datasource.bar.simtick.format",
                        {"bin", "csv"}, errors);
    validate_enum_field(root, "backtest.trace_mode",
                        {"off", "full"}, errors, nullptr, false);
}

} // namespace

// ---------------------------------------------------------------------------
// load_backtest_config
//
// 四阶段流水线，任一阶段失败立即返回：
//   (1) 定位并加载 YAML（含 common/market 合并）
//   (2) Schema 合规校验（拦截未知 key）
//   (3) 提取顶层节点并校验存在性（仅节点判断，不调用 .as<T>()，不抛出）
//   (4) 日期范围 + 各子系统配置解析（涉及 .as<T>()，统一捕获异常）
// 最终调用 finalize_and_validate_backtest_config 做跨子系统一致性校验。
// ---------------------------------------------------------------------------
bool load_backtest_config(int argc, char** argv, XtHftBacktestConfig& out, std::string& err) {
    out = {};
    err.clear();

    // ── (1) 定位并加载 YAML ─────────────────────────────────────────────
    const std::string cfg_path = xt_locate_config(argc, argv, "XT_CONFIG", "backtest.yaml");
    if (cfg_path.empty()) { err = "config not found"; return false; }

    YAML::Node root;
    std::string load_err;
    if (!XtConfigYamlUtil::load_profile_with_common_and_market(
            cfg_path, "common.yaml", "market.yaml", root, &load_err)) {
        // 前缀匹配：loader 的 not-found 消息携带具体路径（"config not found: <path>"）。
        if (load_err.rfind("config not found", 0) == 0) err = load_err;
        else if (!load_err.empty())             err = "config parse failed: " + load_err;
        else                                    err = "config parse failed";
        return false;
    }

    // ── (2) Schema 合规校验 ──────────────────────────────────────────────
    std::vector<std::string> schema_errors;
    validate_unknown_config_keys(root, build_backtest_schema_rules(), schema_errors);
    validate_backtest_enum_fields(root, schema_errors);
    if (!schema_errors.empty()) { err = format_first_error(schema_errors); return false; }

    // ── (3) 提取顶层节点（仅 operator[] + IsDefined，不调用 .as<T>()，不抛出）
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

    // ── (4) 日期校验 + 各子系统配置解析（.as<T>() 可能抛出，统一 try-catch）
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

        // ── strategies[] 装配主路径 (parse_strategies_assembly) ─────────────
        if (!parse_strategies_assembly(backtest_node, out, err)) {
            return false;
        }
        const YAML::Node primary_strategy_node = backtest_node["strategies"][0];

        XtMarketDataSourceConfig md_defaults;
        out.datasource_cfg            = parse_market_data_source_config(backtest_node, md_defaults);
        out.datasource_cfg.start_date = start_date;
        out.datasource_cfg.end_date   = end_date;

        out.replay_cfg       = parse_replayer_config(backtest_node);
        // 硬约束：HFT 回测必须启用 clock 网格，
        // 以便 Replayer 在 tick 之间注入 on_clock 事件驱动 drain_fills()——
        // fill 的可见性走"撮合→回程网络→策略 on_fill"的异步模型，没有 clock
        // 网格就无法在 tick 之间触发 drain。默认 250 ms（半个 tick 间隔），
        // 配置未给值或给 0 时强制拉到 250 ms，给值小于 250 ms 时也拉到 250 ms。
        // 该下限保证 tick 间仍可注入 clock drain。
        constexpr int64_t kHftMinClockResolutionNs = 250'000'000;  // 250 ms
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
        // 同一份 session 配置透传到 data source：让它在 load 阶段过滤 out-of-session tick + 标记 pre-open。
        // data source 拿到的 session config 与 engine 完全一致，避免配置漂移。
        out.datasource_cfg.session = out.session_cfg;
        out.calendar_cfg     = parse_trading_calendar_config(backtest_node["calendar"]);

        const YAML::Node roll_node = backtest_node["roll"];
        out.roll_cfg = parse_roll_config(roll_node);
        // roll.active=true 但无换月记录：引擎无法正确滚动合约，静默放行会导致整个回测跑在错误合约上
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

// ---------------------------------------------------------------------------
// parse_backtest_engine_config — 解析 backtest.engine 配置节
// ---------------------------------------------------------------------------
namespace {

/// @brief 可选 backtest.latency_model 节 (constant|interpolated|cdf)。
/// @note .as<> 异常由 caller 的 try 兜 (本 helper 在其 try 内调用)。
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

/// @brief 可选 backtest.queue_model 节 (ratio|prob_power1|prob_power2)。
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

}  // namespace

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
        // 延迟模型只通过 backtest.latency_model 节配置；fill_return_latency_ms
        // 属于 gateway visible_at_ns 口径，不并入 latency_model。
        out.fill_return_latency_ms = backtest_node["fill_return_latency_ms"]
            ? backtest_node["fill_return_latency_ms"].as<uint32_t>()
            : 0;
        // EOF 收尾只走严格因果；`eof_grace_ms` 是唯一可调窗口。
        out.eof_grace_ms = backtest_node["eof_grace_ms"]
            ? backtest_node["eof_grace_ms"].as<uint32_t>()
            : 0;
        // 爆仓模拟（margin call）：默认关；ratio 合法性在 finalize 校验。
        out.margin_call_enabled = backtest_node["margin_call_enabled"]
            ? backtest_node["margin_call_enabled"].as<bool>()
            : false;
        out.maintenance_ratio = backtest_node["maintenance_ratio"]
            ? backtest_node["maintenance_ratio"].as<double>()
            : 0.9;
        // 可选 latency_model / queue_model 节（factory 负责 type/source 后缀 fail-fast）。
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

        // Canonical key only. Retired match-mode aliases fail earlier via schema unknown-key validation.
        if (const YAML::Node mdt_node = backtest_node["match_delay_ticks"]; mdt_node) {
            out.match_delay_ticks = mdt_node.as<uint32_t>();
        }
        out.session_offset_minutes = backtest_node["session_offset_minutes"]
            ? backtest_node["session_offset_minutes"].as<int>() : 0;
        // 本地条件单开关（bool 走 as_or 容错，缺省 false）。
        out.conditional_orders_enabled =
            XtConfigYamlUtil::as_or<bool>(backtest_node, "conditional_orders_enabled", false);
        // 单策略派生字段从 strategies[0] 取；多策略共享 engine buffer/debug 设置。
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
    // match_delay_ticks >= 1 时 fill_latency_us 可以组合（N>=2 场景），不做额外约束。
}
