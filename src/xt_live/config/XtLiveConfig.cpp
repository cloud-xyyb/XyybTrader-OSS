#include "XtLiveConfig.h"
#include "xt_cost/XtTradeCostConfigParser.h"
#include "LiveConfigSchemaRules.h"     // build_live_schema_rules()
#include "LiveConfigParsers.h"         // parse_* leaf functions
#include "xt_config/yaml/ConfigYamlUtil.h"            // as_or / typed-read templates
#include "LiveConfigValidators.h"      // validate_live_config + validate_strategy_or_strategies_cfg
#include "DeclaredUniverse.h"          // collect_declared_universe（declared 收集单源）

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <unordered_map>

#include <yaml-cpp/yaml.h>

#include "ConfigLocator.h"
#include "ConfigSchemaValidator.h"
#include "XtLiveRuntimeStateCfg.h"
#include "xt_live/hft/runtime/XtHftEventLoop.h"
#include "xt_config/yaml/YAMLPathUtil.h"
#include "xt_config/yaml/YAMLMergeUtil.h"

XtLoggerCfg parse_logger_config(const YAML::Node& node);
XtStrategyFactoryCfg parse_strategy_factory_config(const YAML::Node& node);

namespace {

// 本 TU 通过 namespace alias 保持 YAML typed-read 调用点简短。
namespace YamlNodeUtil = ::XtConfigYamlUtil;

// 解析 md.transport / md.shm_ring / md.stale_md_ms，填入 MdProviderCfg。
// transport=shm 时 factory 改返 XtShmMdGateway（从 connector ring 读行情）；direct
// 为默认现状路径。非法 transport 值 / shm 缺 shm_ring 记 validation_error。
// 另：shm（多盘组合盘）还要求显式 runtime.instance_id（见函数尾部），故需 runtime_node。
void parse_md_transport_cfg(const YAML::Node& md_node,
                            const YAML::Node& runtime_node,
                            gateway_factory::MdProviderCfg& out,
                            std::vector<std::string>& validation_errors) {
    if (!md_node || md_node.IsNull()) return;
    const std::string transport =
        YamlNodeUtil::as_or<std::string>(md_node, "transport", std::string("direct"));
    if (transport == "direct") {
        out.transport = gateway_factory::MdTransport::Direct;
    } else if (transport == "shm") {
        out.transport = gateway_factory::MdTransport::Shm;
    } else {
        validation_errors.push_back(
            "runtime.md.transport 仅支持 'direct' 或 'shm'，得到: '" + transport + "'");
        return;
    }
    out.shm_ring = YamlNodeUtil::as_or<std::string>(md_node, "shm_ring", std::string());
    out.stale_md_ms =
        YamlNodeUtil::as_or<uint32_t>(md_node, "stale_md_ms", static_cast<uint32_t>(3000));
    if (out.transport == gateway_factory::MdTransport::Shm && out.shm_ring.empty()) {
        validation_errors.push_back("runtime.md.transport=shm 需要非空 runtime.md.shm_ring");
    }
    // 多盘组合盘 (transport=shm) 必须显式声明 per-book runtime.instance_id。该单一身份派生
    // safety / aggregator / baseline 三类 per-instance state 子目录；缺失会令同 venue 多 runner
    // 静默回退共享 md_provider 派生路径，互覆写 kill_switch latch / peak_equity / today-PnL，
    // 破坏 per-book 隔离。fail-closed：组合盘配置缺该键即记 validation_error，
    // runner 启动期拒绝（见 RunnerBootCommon::load_live_config_or_report）。direct（单盘现状）不受影响。
    if (out.transport == gateway_factory::MdTransport::Shm) {
        const std::string instance_id =
            YamlNodeUtil::as_or<std::string>(runtime_node, "instance_id", std::string());
        if (instance_id.empty()) {
            validation_errors.push_back(
                "runtime.md.transport=shm (multibook) 需要显式 runtime.instance_id —— "
                "缺失会令同 venue 多 runner 共享默认 state 子目录，互覆写 "
                "kill_switch/peak_equity/today-PnL");
        }
    }
}

// Strategy init cfg helpers：负责 strategies-vector 装配时的兼容字段注入/清理。
XtStrategyInitConfig make_strategy_init_cfg(const YAML::Node& strategy_node) {
    XtStrategyInitConfig cfg;
    XtConfigYamlUtil::flatten_yaml_node_to_map(strategy_node, "", cfg.values, true);
    return cfg;
}

void erase_strategy_init_key(XtStrategyInitConfig& cfg, const std::string& key) {
    auto it = cfg.values.find(key);
    if (it != cfg.values.end()) cfg.values.erase(it);
}

void erase_strategy_init_prefix(XtStrategyInitConfig& cfg, const std::string& prefix) {
    const std::string with_dot = prefix + ".";
    for (auto it = cfg.values.begin(); it != cfg.values.end();) {
        if (it->first.rfind(with_dot, 0) == 0) {
            it = cfg.values.erase(it);
            continue;
        }
        ++it;
    }
}

std::string resolve_path_lexical_from_config(const std::string& cfg_path,
                                             const std::string& path) {
    if (path.empty()) return path;
    const std::filesystem::path p(path);
    if (p.is_absolute()) return path;
    const std::filesystem::path base = std::filesystem::path(cfg_path).parent_path();
    if (base.empty()) return path;
    return (base / p).lexically_normal().string();
}

void resolve_runtime_output_paths(XtLiveConfig& out) {
    const std::string& cfg_path = out.parsed.cfg_path;
    auto& control = out.runtime.control_cfg;

    control.unified_report_dir =
        resolve_path_lexical_from_config(cfg_path, control.unified_report_dir);
    out.bootstrap.recording_cfg.file_path =
        resolve_path_lexical_from_config(cfg_path, out.bootstrap.recording_cfg.file_path);
    out.bootstrap.event_store_cfg.root_path =
        resolve_path_lexical_from_config(cfg_path, out.bootstrap.event_store_cfg.root_path);
}

void inject_strategy_single_entry_cfg(XtStrategyInitConfig& cfg,
                                      const YAML::Node& instrument_node,
                                      const YAML::Node& session_node,
                                      const YAML::Node& trading_node) {
    const std::string entry_symbol = cfg.get_string("symbol", "");
    erase_strategy_init_key(cfg, "symbol");
    erase_strategy_init_key(cfg, "allow_short");
    erase_strategy_init_prefix(cfg, "active_sections");

    std::string symbol = entry_symbol;
    if (symbol.empty()) {
        symbol = YamlNodeUtil::as_or<std::string>(instrument_node, "symbol", std::string(""));
    }
    if (!symbol.empty()) cfg.values["symbol"] = symbol;

    const bool allow_short = YamlNodeUtil::as_or<bool>(trading_node, "allow_short", false);
    cfg.values["allow_short"] = allow_short ? "true" : "false";

    std::vector<std::pair<int, int>> active_sections;
    parse_session_sections(session_node["active_sections"], active_sections);
    for (size_t i = 0; i < active_sections.size(); ++i) {
        cfg.values["active_sections." + std::to_string(i) + ".start"] = std::to_string(active_sections[i].first);
        cfg.values["active_sections." + std::to_string(i) + ".end"] = std::to_string(active_sections[i].second);
    }
}

void fill_strategy_pnl_emit_targets(XtLiveConfig& out) {
    out.runtime.control_cfg.strategy_pnl_emit_targets.clear();
    for (const auto& entry : out.parsed.strategies) {
        const std::string sid = entry.factory_cfg.id;
        const std::string symbol = entry.init_cfg.get_string("symbol", "");
        if (sid.empty() || symbol.empty()) continue;
        auto& targets = out.runtime.control_cfg.strategy_pnl_emit_targets;
        const bool exists = std::any_of(targets.begin(), targets.end(),
            [&](const XtStrategyPnlEmitTarget& target) {
                return target.strategy_id == sid && target.symbol == symbol;
            });
        if (!exists) targets.push_back({sid, symbol});
    }
}

// ── Param backend / latency cfg makers (简单 yaml field-mapping) ──────────────
// 与 fill_misc_runtime_cfg 紧耦合 (one-shot 装配),没必要单独 export,留 anon-ns。
XtLiveParamDomain::BackendConfig make_param_backend_cfg(const YAML::Node& runtime_node) {
    XtLiveParamDomain::BackendConfig cfg;
    const YAML::Node backend = runtime_node["params_backend"];
    if (!backend || backend.IsNull()) return cfg;
    cfg.type = YamlNodeUtil::as_or<std::string>(backend, "type", cfg.type);
    cfg.domain = YamlNodeUtil::as_or<std::string>(backend, "domain", cfg.domain);
    return cfg;
}

XtLiveLatencyTracker::Config make_latency_cfg(const YAML::Node& debug_node) {
    XtLiveLatencyTracker::Config cfg;
    cfg.enabled = YamlNodeUtil::as_or<bool>(debug_node, "latency_enabled", true);
    const int64_t ttl_us = YamlNodeUtil::as_or<int64_t>(debug_node, "latency_stale_ttl_us", 60000000LL);
    cfg.stale_ttl_us = ttl_us > 0 ? static_cast<uint64_t>(ttl_us) : 60000000ULL;
    // rolling window 秒数, 默认 600s (10 min). 0 退化累计.
    const int64_t win_s = YamlNodeUtil::as_or<int64_t>(debug_node, "latency_rolling_window_s", 600LL);
    cfg.rolling_window_s = win_s > 0 ? static_cast<uint64_t>(win_s) : 0;
    return cfg;
}

template <typename T, typename ParserFn>
// 封装冷路径 parser 调用，把异常统一转换为 validation_errors。
bool parse_typed_cfg(const YAML::Node& node,
                     const std::string& error_prefix,
                     T& out,
                     std::vector<std::string>& validation_errors,
                     ParserFn parser) {
    if (!node || node.IsNull()) return false;
    try {
        out = parser(node);
        return true;
    } catch (const std::exception& e) {
        validation_errors.push_back(error_prefix + ": " + e.what());
        return false;
    } catch (...) {
        validation_errors.push_back(error_prefix);
        return false;
    }
}

/// @brief `load_live_config()` 中复用的 YAML 节点视图。
struct LiveConfigNodes {
    YAML::Node runtime_node;
    YAML::Node log_node;
    YAML::Node queue_node;
    YAML::Node md_node;
    YAML::Node td_node;
    YAML::Node strategy_node;
    YAML::Node instrument_node;
    YAML::Node session_node;
    YAML::Node trading_node;
    YAML::Node debug_node;
    YAML::Node commodities_node;
    YAML::Node fees_node;
};

/// @brief 标记各子配置是否解析成功，避免下游使用未初始化 cfg。
struct ParsedFlags {
    bool log_cfg_ok = false;
    bool stra_cfg_ok = false;
    bool md_cfg_ok = false;
    bool td_cfg_ok = false;
    bool runtime_state_cfg_ok = false;
    bool order_gateway_cfg_ok = false;
};

LiveConfigNodes collect_live_config_nodes(const YAML::Node& root) {
    LiveConfigNodes nodes;
    nodes.runtime_node = XtConfigYamlUtil::get_path_node(root, "runtime");
    nodes.log_node = XtConfigYamlUtil::get_path_node(root, "log");
    nodes.queue_node = XtConfigYamlUtil::get_path_node(root, "queue");
    nodes.md_node =
        (nodes.runtime_node && !nodes.runtime_node.IsNull()) ? nodes.runtime_node["md"] : YAML::Node();
    nodes.td_node =
        (nodes.runtime_node && !nodes.runtime_node.IsNull()) ? nodes.runtime_node["td"] : YAML::Node();
    {
        const YAML::Node strategies =
            (nodes.runtime_node && !nodes.runtime_node.IsNull()) ? nodes.runtime_node["strategies"] : YAML::Node();
        nodes.strategy_node =
            (strategies && strategies.IsSequence() && strategies.size() > 0)
                ? strategies[0]
                : YAML::Node();
    }
    nodes.instrument_node =
        (nodes.runtime_node && !nodes.runtime_node.IsNull()) ? nodes.runtime_node["instrument"] : YAML::Node();
    nodes.session_node =
        (nodes.runtime_node && !nodes.runtime_node.IsNull()) ? nodes.runtime_node["session"] : YAML::Node();
    nodes.trading_node =
        (nodes.runtime_node && !nodes.runtime_node.IsNull()) ? nodes.runtime_node["trading"] : YAML::Node();
    nodes.debug_node =
        (nodes.runtime_node && !nodes.runtime_node.IsNull()) ? nodes.runtime_node["debug"] : YAML::Node();
    nodes.commodities_node = XtConfigYamlUtil::get_path_node(root, "commodities");
    nodes.fees_node = XtConfigYamlUtil::get_path_node(root, "fees");
    return nodes;
}

ParsedFlags parse_and_validate_base_cfg(const LiveConfigNodes& nodes,
                                        XtLiveConfig& out,
                                        std::vector<std::string>& validation_errors) {
    ParsedFlags flags;
    flags.log_cfg_ok = parse_typed_cfg(
        nodes.log_node,
        "log 配置解析失败",
        out.parsed.log_cfg,
        validation_errors,
        [](const YAML::Node& n) { return parse_logger_config(n); });
    flags.stra_cfg_ok = parse_typed_cfg(
        nodes.strategy_node,
        "runtime.strategies[0] 配置解析失败",
        out.parsed.strategy_cfg,
        validation_errors,
        [](const YAML::Node& n) { return parse_strategy_factory_config(n); });
    flags.md_cfg_ok = parse_typed_cfg(
        nodes.md_node,
        "runtime.md 配置解析失败",
        out.parsed.md_cfg,
        validation_errors,
        [](const YAML::Node& n) { return parse_md_gateway_config(n); });
    flags.td_cfg_ok = parse_typed_cfg(
        nodes.td_node,
        "runtime.td 配置解析失败",
        out.parsed.td_cfg,
        validation_errors,
        [](const YAML::Node& n) { return parse_trade_gateway_config(n); });

    // Common cfg 先解析 provider-neutral 字段；此处补 provider-specific variant。
    const std::string md_provider_str =
        YamlNodeUtil::as_or<std::string>(nodes.md_node, "provider", std::string());
    const std::string td_provider_str =
        YamlNodeUtil::as_or<std::string>(nodes.td_node, "provider", std::string());
    if (md_provider_str == "binance_u") {
        out.parsed.md_provider_cfg.extra = parse_binance_md_cfg(nodes.md_node);
    } else if (md_provider_str == "xtp") {
        out.parsed.md_provider_cfg.extra = parse_xtp_md_cfg(nodes.md_node);
    } else if (md_provider_str == "ctp") {
        out.parsed.md_provider_cfg.extra = parse_ctp_md_cfg(nodes.md_node);
    }
    // md.transport（direct 默认=现状 / shm=从 connector 广播 ring 读行情）。shm 时
    // factory 忽略 venue extra 改返 XtShmMdGateway；venue 名仍驱动 venue_kind 推导。
    parse_md_transport_cfg(nodes.md_node, nodes.runtime_node, out.parsed.md_provider_cfg, validation_errors);
    if (td_provider_str == "binance_u") {
        out.parsed.td_provider_cfg.extra = parse_binance_td_cfg(nodes.td_node);
    } else if (td_provider_str == "xtp") {
        out.parsed.td_provider_cfg.extra = parse_xtp_td_cfg(nodes.td_node);
    } else if (td_provider_str == "ctp") {
        out.parsed.td_provider_cfg.extra = parse_ctp_td_cfg(nodes.td_node);
    }

    validate_live_config(nodes.runtime_node,
                         nodes.queue_node,
                         nodes.commodities_node,
                         nodes.fees_node,
                         flags.log_cfg_ok ? &out.parsed.log_cfg : nullptr,
                         nodes.strategy_node,
                         flags.stra_cfg_ok ? &out.parsed.strategy_cfg : nullptr,
                         nodes.md_node,
                         flags.md_cfg_ok ? &out.parsed.md_cfg : nullptr,
                         nodes.td_node,
                         flags.td_cfg_ok ? &out.parsed.td_cfg : nullptr,
                         validation_errors);
    return flags;
}

void parse_runtime_runner_cfg(const LiveConfigNodes& nodes,
                              XtLiveConfig& out,
                              ParsedFlags& flags,
                              std::vector<std::string>& validation_errors) {
    flags.runtime_state_cfg_ok = parse_typed_cfg(
        nodes.runtime_node,
        "runtime/account 配置解析失败",
        out.bootstrap.runtime_state_cfg,
        validation_errors,
        [&](const YAML::Node& n) { return parse_live_runtime_state_config(n, nodes.commodities_node); });
    flags.order_gateway_cfg_ok = parse_typed_cfg(
        nodes.runtime_node,
        "runtime risk/closeout 配置解析失败",
        out.bootstrap.order_gateway_cfg,
        validation_errors,
        [](const YAML::Node& n) { return parse_live_order_gateway_config(n); });
    if (nodes.queue_node && !nodes.queue_node.IsNull()) {
        try {
            out.bootstrap.event_loop_cfg =
                parse_event_loop_cfg(nodes.queue_node, nodes.md_node);
        } catch (const std::exception& e) {
            validation_errors.push_back(std::string("queue 配置解析失败: ") + e.what());
        } catch (...) {
            validation_errors.push_back("queue 配置解析失败");
        }
    }

    const YAML::Node cost_node =
        (nodes.runtime_node && !nodes.runtime_node.IsNull()) ? nodes.runtime_node["cost"] : YAML::Node();
    if (cost_node && !cost_node.IsNull()) {
        XtTradeCostConfig live_cost_cfg;
        if (parse_typed_cfg(cost_node,
                            "runtime.cost 配置解析失败",
                            live_cost_cfg,
                            validation_errors,
                            [](const YAML::Node& n) { return engine_config::parse_trade_cost_config(n); })) {
            out.bootstrap.live_cost = live_cost_cfg;
        }
    }

    XtFeeRuleBook::Config live_fees_cfg;
    if (!nodes.fees_node || nodes.fees_node.IsNull()) {
        validation_errors.push_back("fees 缺失（请在 market.yaml 配置）");
    } else if (parse_typed_cfg(nodes.fees_node,
                               "fees 配置解析失败",
                               live_fees_cfg,
                               validation_errors,
                               [](const YAML::Node& n) { return parse_fee_rule_book_config(n); })) {
        if (live_fees_cfg.rules.empty()) {
            validation_errors.push_back("fees 不能为空");
        } else {
            out.bootstrap.live_fees.set_config(live_fees_cfg);
        }
    }
    if (flags.td_cfg_ok) {
        out.parsed.td_cfg.recovery_enabled = YamlNodeUtil::as_or<bool>(nodes.td_node, "recovery", true);
    }
    // runtime.td.reconcile.* → venue-neutral reconcile loop 配置（缺节走 struct 默认）。
    parse_typed_cfg(
        nodes.td_node,
        "runtime.td.reconcile 配置解析失败",
        out.bootstrap.reconcile_cfg,
        validation_errors,
        [](const YAML::Node& n) { return parse_reconcile_cfg(n); });
}

void parse_strategy_runtime_cfg(const LiveConfigNodes& nodes,
                                const ParsedFlags& flags,
                                XtLiveConfig& out,
                                std::vector<std::string>& validation_errors) {
    if (!flags.runtime_state_cfg_ok || !flags.order_gateway_cfg_ok || !flags.stra_cfg_ok) return;
    out.bootstrap.strategy_history_ticks = YamlNodeUtil::as_positive_size_or(nodes.strategy_node, "history_ticks", 2048);
    out.bootstrap.strategy_history_bars = YamlNodeUtil::as_positive_size_or(nodes.strategy_node, "history_bars", 512);
    out.bootstrap.strategy_debug_signal_log = YamlNodeUtil::as_or<bool>(nodes.strategy_node, "debug_signal_log", false);
    out.bootstrap.strategy_debug_signal_log_every_n_ticks = std::max<uint32_t>(
        1,
        YamlNodeUtil::as_or<uint32_t>(nodes.strategy_node, "debug_signal_log_every_n_ticks", 1));
    try {
        out.bootstrap.strategy_contract_info = parse_stra_contract_info(nodes.commodities_node);
    } catch (const std::exception& e) {
        validation_errors.push_back(std::string("commodities 策略合约信息解析失败: ") + e.what());
    } catch (...) {
        validation_errors.push_back("commodities 策略合约信息解析失败");
    }
}

// fill_misc_runtime_cfg helpers：每个 helper 只写 out 的一个子域。

// 控制面只走 `runtime.control_grpc`；yaml 老 `runtime.control_panel` /
// `runtime.control_cfg` block 由 validator fail-closed 拒启动。
// fill_control_grpc_cfg 定义在本文件 anon namespace 之外（validator 共用，见 header）。

void fill_safety_state_cfg(const YAML::Node& runtime_node,
                           const std::string& md_provider,
                           XtRunnerControlConfig& ctrl) {
    // runtime.instance_id 是单一身份来源：safety / aggregator / baseline 三类 per-instance
    // state 全部派生自它（空=回退 md_provider，再空=_default，见 resolved_instance_id）。
    ctrl.instance_id =
        YamlNodeUtil::as_or<std::string>(runtime_node, "instance_id", std::string());
    // safety.bin 路径派生（非直接配置）：instance_state_dir/<id>/safety.bin。单盘 direct
    // 无 instance_id → 回退 /state/<md_provider>/safety.bin（有意全迁移，无 compat）。
    ctrl.safety_state_path = instance_state_dir(md_provider, ctrl.instance_id) + "/safety.bin";
    // save_interval 仍可调（默认 60s）。
    const YAML::Node ss_node =
        (runtime_node && !runtime_node.IsNull()) ? runtime_node["safety_state"] : YAML::Node();
    ctrl.safety_state_save_interval_ms =
        YamlNodeUtil::as_or<int>(ss_node, "save_interval_ms", 60000);
}

void fill_metrics_cfg(const YAML::Node& runtime_node, XtRunnerControlConfig& ctrl) {
    // metrics exposer port per-profile
    // 默认 9101 保持现 CTP / Binance profile bit-exact; XTP 或多 runner 同 host
    // 可在 yaml 设 `runtime.metrics.exposer_port: 9102` 避免 bind 冲突。
    // 0 = 跳过 exposer 启动 (caller 依据 port==0 决定是否调 start_exposer)。
    const YAML::Node m_node =
        (runtime_node && !runtime_node.IsNull()) ? runtime_node["metrics"] : YAML::Node();
    ctrl.metrics_exposer_port =
        YamlNodeUtil::as_or<int>(m_node, "exposer_port", 9101);
    ctrl.metrics_exposer_bind_addr =
        YamlNodeUtil::as_or<std::string>(m_node, "exposer_bind_addr",
                                          std::string("0.0.0.0"));
}

void fill_portfolio_router_flag(const YAML::Node& runtime_node, XtLiveParsedConfig& parsed) {
    parsed.portfolio_router = YamlNodeUtil::as_or<bool>(
        runtime_node, "portfolio_router", true);
}

// ── selector.tasks[]（SEL live 调度任务）────────────────────────────────────

bool parse_sel_task_period(const std::string& s, TaskPeriod& out) {
    if (s == "minute")  { out = TaskPeriod::Minute;  return true; }
    if (s == "daily")   { out = TaskPeriod::Daily;   return true; }
    if (s == "weekly")  { out = TaskPeriod::Weekly;  return true; }
    if (s == "monthly") { out = TaskPeriod::Monthly; return true; }
    if (s == "yearly")  { out = TaskPeriod::Yearly;  return true; }
    return false;
}

/// @brief 解析顶层 `selector.tasks[]`（字段对齐 BT `selector.task` 单数节）。
/// 非法 entry 记 validation_error（runner 启动 fail-fast），不静默跳过。
void fill_selector_tasks(const YAML::Node& root,
                         XtLiveParsedConfig& parsed,
                         std::vector<std::string>& validation_errors) {
    parsed.sel_tasks.clear();
    const YAML::Node sel = root["selector"];
    if (!sel || sel.IsNull()) return;
    if (!sel.IsMap()) {
        validation_errors.push_back("selector 必须是 map（含 tasks: [...]）");
        return;
    }
    const YAML::Node tasks = sel["tasks"];
    if (!tasks || tasks.IsNull()) return;
    if (!tasks.IsSequence()) {
        validation_errors.push_back(
            "selector.tasks 必须是 list（each entry: {strategy_id, period, ...}）");
        return;
    }
    for (size_t i = 0; i < tasks.size(); ++i) {
        const YAML::Node t = tasks[i];
        const std::string prefix = "selector.tasks[" + std::to_string(i) + "]";
        if (!t || !t.IsMap()) {
            validation_errors.push_back(prefix + " 必须是 map");
            continue;
        }
        XtSelLiveTaskCfg cfg;
        cfg.strategy_id = YamlNodeUtil::as_or<std::string>(t, "strategy_id", std::string());
        if (cfg.strategy_id.empty()) {
            validation_errors.push_back(prefix + ".strategy_id 不能为空");
            continue;
        }
        if (!t["period"] || t["period"].IsNull()) {
            validation_errors.push_back(
                prefix + ".period 必填（minute / daily / weekly / monthly / yearly）");
            continue;
        }
        const std::string period = t["period"].as<std::string>("");
        if (!parse_sel_task_period(period, cfg.period)) {
            validation_errors.push_back(
                prefix + ".period 只支持 minute / daily / weekly / monthly / yearly，得到 '" +
                period + "'");
            continue;
        }
        cfg.day         = YamlNodeUtil::as_or<uint32_t>(t, "day", cfg.day);
        cfg.time        = YamlNodeUtil::as_or<uint32_t>(t, "time", cfg.time);
        cfg.strict_time = YamlNodeUtil::as_or<bool>(t, "strict_time", cfg.strict_time);
        cfg.trdtpl      = YamlNodeUtil::as_or<std::string>(t, "trdtpl", cfg.trdtpl);
        cfg.session     = YamlNodeUtil::as_or<std::string>(t, "session", cfg.session);
        cfg.name        = YamlNodeUtil::as_or<std::string>(t, "name", std::string());
        parsed.sel_tasks.push_back(std::move(cfg));
    }
}

void fill_strategies_vector(const LiveConfigNodes& nodes, XtLiveParsedConfig& parsed) {
    // ── 装配 strategies vector（single/multi unified path） ────────────────────────
    //
    // `runtime.strategies: [{...}, {...}]` 是唯一策略入口；N 可为 1。
    // strategy_cfg + strategy_init_cfg 镜像到 strategies[0]，供既有 runner 字段读取。
    parsed.strategies.clear();
    const YAML::Node strategies_node =
        (nodes.runtime_node && !nodes.runtime_node.IsNull()) ? nodes.runtime_node["strategies"] : YAML::Node();
    if (strategies_node && !strategies_node.IsNull() && strategies_node.IsSequence()) {
        // 多策略：解析 N 个 entry
        for (size_t i = 0; i < strategies_node.size(); ++i) {
            const auto& e = strategies_node[i];
            if (!e || !e.IsMap()) continue;  // validate 已在 validate_strategy_or_strategies_cfg 中拦截
            LiveStrategyEntry entry;
            entry.factory_cfg = parse_strategy_factory_config(e);
            entry.init_cfg    = make_strategy_init_cfg(e);
            inject_strategy_single_entry_cfg(
                entry.init_cfg, nodes.instrument_node, nodes.session_node, nodes.trading_node);
            parsed.strategies.push_back(std::move(entry));
        }
        if (!parsed.strategies.empty()) {
            parsed.strategy_cfg      = parsed.strategies[0].factory_cfg;
            parsed.strategy_init_cfg = parsed.strategies[0].init_cfg;
        }
    }
}

void fill_roll_engine_cfg(const YAML::Node& runtime_node,
                          const std::string& cfg_path,
                          XtLiveBootstrapPlan& bootstrap,
                          std::vector<std::string>& validation_errors) {
    // 换月引擎配置（可选）——内联解析，避免链接 xt_bt_hft
    if (!runtime_node || !runtime_node["roll"]) return;
    const std::size_t errors_before = validation_errors.size();
    auto& rcfg = bootstrap.roll_cfg;
    const auto& roll_node = runtime_node["roll"];
    rcfg.active                  = YamlNodeUtil::as_or<bool>(roll_node, "active", false);
    rcfg.logical_symbol          = YamlNodeUtil::as_or<std::string>(roll_node, "logical_symbol", "");
    rcfg.window_before_days      = YamlNodeUtil::as_or<int>(roll_node, "window_before_days", 0);
    rcfg.window_after_days       = YamlNodeUtil::as_or<int>(roll_node, "window_after_days", 0);
    rcfg.dispatch_parallel_ticks = YamlNodeUtil::as_or<bool>(roll_node, "dispatch_parallel_ticks", false);
    // roll path 文件解析（简化版，复用 XtTimeParse）
    if (roll_node["path"]) {
        const std::string raw_path = roll_node["path"].as<std::string>();
        const std::string roll_path = resolve_path_lexical_from_config(cfg_path, raw_path);
        try {
            std::ifstream in(roll_path);
            if (!in.is_open()) {
                if (rcfg.active) {
                    validation_errors.push_back(
                        "runtime.roll.path cannot open: " + raw_path +
                        (raw_path == roll_path ? "" : " (resolved: " + roll_path + ")"));
                }
                bootstrap.has_roll_cfg = false;
                return;
            }
            std::string line;
            while (std::getline(in, line)) {
                auto trim_s = line;
                while (!trim_s.empty() && std::isspace(static_cast<unsigned char>(trim_s.front()))) trim_s.erase(trim_s.begin());
                while (!trim_s.empty() && std::isspace(static_cast<unsigned char>(trim_s.back()))) trim_s.pop_back();
                if (trim_s.empty() || trim_s[0] == '#') continue;
                std::stringstream ss(trim_s);
                std::string d, sym, ko, oc, nc;
                if (!std::getline(ss, d, ',') || !std::getline(ss, sym, ',')) continue;
                (void)std::getline(ss, ko, ',');
                (void)std::getline(ss, oc, ',');
                (void)std::getline(ss, nc, ',');
                XtRollEngine::SwitchPoint sp;
                try { sp.date = static_cast<uint32_t>(std::stoul(d)); } catch (...) { continue; }
                sp.symbol = sym;
                while (!sp.symbol.empty() && std::isspace(static_cast<unsigned char>(sp.symbol.front()))) sp.symbol.erase(sp.symbol.begin());
                while (!sp.symbol.empty() && std::isspace(static_cast<unsigned char>(sp.symbol.back()))) sp.symbol.pop_back();
                if (sp.date == 0 || sp.symbol.empty()) continue;
                if (!ko.empty()) { try { sp.known_on_date = static_cast<uint32_t>(std::stoul(ko)); } catch (...) {} }
                if (!oc.empty() && !nc.empty()) {
                    try { sp.old_close = std::stod(oc); sp.new_close = std::stod(nc); } catch (...) {}
                }
                rcfg.switches.push_back(std::move(sp));
            }
        } catch (const std::exception& e) {
            rcfg.switches.clear();
            if (rcfg.active) {
                validation_errors.push_back(std::string("runtime.roll.path load failed: ") + e.what());
            }
        } catch (...) {
            rcfg.switches.clear();
            if (rcfg.active) {
                validation_errors.push_back("runtime.roll.path load failed");
            }
        }
    } else if (rcfg.active) {
        validation_errors.push_back("runtime.roll.path required when runtime.roll.active=true");
    }
    if (rcfg.active && rcfg.switches.empty() && validation_errors.size() == errors_before) {
        validation_errors.push_back("runtime.roll.path has no switch entries");
    }
    bootstrap.has_roll_cfg = rcfg.active && !rcfg.switches.empty();
}

void fill_misc_runtime_cfg(const LiveConfigNodes& nodes,
                           XtLiveConfig& out,
                           std::vector<std::string>& validation_errors) {
    const YAML::Node account_node =
        (nodes.runtime_node && !nodes.runtime_node.IsNull()) ? nodes.runtime_node["account"] : YAML::Node();
    out.runtime.control_cfg =
        parse_runner_control_config(
            nodes.md_node, nodes.debug_node, nodes.strategy_node, nodes.instrument_node, account_node);
    // md_provider 先于 fill_safety_state_cfg 解析：后者用它在 instance_id 缺省时派生 state 路径。
    // 纯幂等读 nodes.md_node["provider"]，不依赖中间任何 fill_* 调用。
    out.parsed.md_provider = YamlNodeUtil::as_or<std::string>(nodes.md_node, "provider", std::string());
    out.parsed.td_provider = YamlNodeUtil::as_or<std::string>(nodes.td_node, "provider", std::string());
    fill_control_grpc_cfg(nodes.runtime_node, out.runtime.control_grpc_cfg);
    fill_safety_state_cfg(nodes.runtime_node, out.parsed.md_provider, out.runtime.control_cfg);
    fill_metrics_cfg(nodes.runtime_node, out.runtime.control_cfg);

    out.bootstrap.process_tuning_cfg = parse_runner_process_tuning_config(nodes.debug_node, nodes.md_node);
    out.bootstrap.param_backend_cfg = make_param_backend_cfg(nodes.runtime_node);
    out.bootstrap.latency_cfg = make_latency_cfg(nodes.debug_node);
    out.bootstrap.params_wait_timeout_ms = std::max(
        1,
        YamlNodeUtil::as_or<int>(nodes.debug_node, "params_wait_timeout_ms", 1000));
    fill_portfolio_router_flag(nodes.runtime_node, out.parsed);

    // venue_label 供 HftRunnerControl 1Hz account-level metric 使用。
    out.runtime.control_cfg.venue_label = out.parsed.md_provider;

    // venue_kind 决定下单路径是否启用 A 股集合竞价限制。
    if (out.parsed.md_provider == "xtp") {
        out.bootstrap.order_gateway_cfg.venue_kind = xt_core::XtVenueKind::AShare;
    } else if (out.parsed.md_provider == "ctp") {
        out.bootstrap.order_gateway_cfg.venue_kind = xt_core::XtVenueKind::FuturesCN;
    } else if (out.parsed.md_provider == "binance_u") {
        out.bootstrap.order_gateway_cfg.venue_kind = xt_core::XtVenueKind::Continuous24h;
    } else {
        out.bootstrap.order_gateway_cfg.venue_kind = xt_core::XtVenueKind::Unknown;
    }

    out.parsed.param_seed_values.clear();
    XtConfigYamlUtil::flatten_yaml_node_to_pairs(nodes.runtime_node["params"], "", out.parsed.param_seed_values, true);
    const std::string instrument_symbol =
        YamlNodeUtil::as_or<std::string>(nodes.instrument_node, "symbol", std::string(""));
    out.parsed.strategy_init_cfg = make_strategy_init_cfg(nodes.strategy_node);
    inject_strategy_single_entry_cfg(
        out.parsed.strategy_init_cfg, nodes.instrument_node, nodes.session_node, nodes.trading_node);

    fill_strategies_vector(nodes, out.parsed);
    fill_strategy_pnl_emit_targets(out);

    out.bootstrap.recording_cfg = parse_recording_cfg(nodes.runtime_node);
    out.bootstrap.event_store_cfg = parse_event_store_cfg(nodes.runtime_node);
    out.bootstrap.risk_monitor_cfg = parse_risk_monitor_cfg(nodes.runtime_node);

    fill_roll_engine_cfg(nodes.runtime_node, out.parsed.cfg_path, out.bootstrap, validation_errors);
    // DeclaredUniverse 一次成型：roll 派生合约是其构成之一，故装配点排在 roll
    // 配置解析之后。规模超上限 → validation_error → runner 启动前 abort。
    out.parsed.md_cfg.subs = xt_live::collect_declared_universe(out, instrument_symbol);
    validate_declared_universe_size(out.parsed.md_cfg.subs, validation_errors);

    // local_id 发号器 per-instance 域身份注入 —— resolved instance id
    // 钉进 TD provider cfg（binance/XTP 用它派生 per-instance state file +
    // 高位 instance tag，multibook 跨进程 id 唯一）。CTP 不用该发号器（OrderRef）。
    {
        const std::string inst = resolved_instance_id(
            out.parsed.md_provider, out.runtime.control_cfg.instance_id);
        if (auto* bn = std::get_if<BinanceTdCfg>(&out.parsed.td_provider_cfg.extra)) {
            bn->local_id_instance = inst;
        } else if (auto* xtp = std::get_if<XTPTdCfg>(&out.parsed.td_provider_cfg.extra)) {
            xtp->local_id_instance = inst;
        }
    }
}

} // namespace

void fill_control_grpc_cfg(const YAML::Node& runtime_node, XtLiveControlGrpcConfig& cfg) {
    // control plane 默认开启；dry-run / 单测 profile 需显式配置 enabled=false。
    const YAML::Node cg_node =
        (runtime_node && !runtime_node.IsNull()) ? runtime_node["control_grpc"] : YAML::Node();
    const YAML::Node tls_node = cg_node ? cg_node["tls"] : YAML::Node();
    cfg.enabled_explicit = cg_node ? static_cast<bool>(cg_node["enabled"]) : false;
    cfg.enabled =
        XtConfigYamlUtil::as_or<bool>(cg_node, "enabled", cfg.enabled);
    cfg.listen_addr =
        XtConfigYamlUtil::as_or<std::string>(cg_node, "listen_addr", cfg.listen_addr);
    cfg.tls_mode =
        XtConfigYamlUtil::as_or<std::string>(tls_node, "mode", cfg.tls_mode);
    cfg.tls_ca_path =
        XtConfigYamlUtil::as_or<std::string>(tls_node, "ca_path", cfg.tls_ca_path);
    cfg.tls_cert_path =
        XtConfigYamlUtil::as_or<std::string>(tls_node, "cert_path", cfg.tls_cert_path);
    cfg.tls_key_path =
        XtConfigYamlUtil::as_or<std::string>(tls_node, "key_path", cfg.tls_key_path);
    cfg.audit_log_path =
        XtConfigYamlUtil::as_or<std::string>(cg_node, "audit_log_path", cfg.audit_log_path);
#ifndef XT_HAVE_CONTROL_GRPC
    // grpc-OFF build（开发/回测二进制）下，enabled 若只是 struct 默认而非 yaml
    // 显式请求 → 降级 disabled（warn 不 fatal），否则任何无 control_grpc 节的
    // live 配置在 runner 装配层启动即死；显式 enabled:true 仍由
    // validate_control_grpc_node fail-closed 拒启动。
    if (cfg.enabled && !cfg.enabled_explicit) {
        std::fprintf(stderr,
                     "[CFG WARN] runtime.control_grpc.enabled 未显式配置且本 binary "
                     "未编译 XT_HAVE_CONTROL_GRPC — 控制面降级为 disabled\n");
        cfg.enabled = false;
    }
#endif
}

bool load_live_config(int argc,
                      char** argv,
                      XtLiveConfig& out,
                      std::string& fatal_error,
                      std::vector<std::string>& validation_errors) {
    out = XtLiveConfig{};
    fatal_error.clear();
    validation_errors.clear();

    out.parsed.cfg_path = xt_locate_config(argc, argv, "XT_CONFIG", "live.yaml");
    if (out.parsed.cfg_path.empty()) {
        fatal_error = "config not found";
        return false;
    }

    // secrets.yaml 文件权限 0600 强制 fail-closed:含 CTP password / binance
    // api_secret 明文,误 chmod 644 → 同机其他 user 可读 → secret 泄漏。
    // load 前 stat() 验证 0600 / 0400 否则 fatal_error 退出, systemd
    // OnFailure= 触发告警。检查放 load_live_config 入口而非 YAMLMergeUtil
    // (live + backtest 共用,backtest 不读 secrets 影响面 0)。Windows dev
    // 环境跳过 (_stat 不返回 unix 权限位)。
#if defined(__unix__) || defined(__APPLE__)
    {
        // secrets.yaml 与 profile 同目录（见 YAMLMergeUtil::load_profile_with_common_and_market）
        std::string secrets_path = out.parsed.cfg_path;
        const size_t pos = secrets_path.find_last_of("/\\");
        if (pos == std::string::npos) {
            secrets_path = "secrets.yaml";
        } else {
            secrets_path = secrets_path.substr(0, pos) + "/secrets.yaml";
        }
        struct ::stat st{};
        if (::stat(secrets_path.c_str(), &st) == 0) {
            // secrets.yaml 只允许 owner 访问；group/other 任意权限位都拒绝。
            const mode_t bad_bits = st.st_mode & (S_IRWXG | S_IRWXO);
            if (bad_bits != 0) {
                std::ostringstream oss;
                oss << "secrets.yaml insecure mode: "
                    << std::oct << (st.st_mode & 0777)
                    << " (must be 0600, group/other bits forbidden); path=" << secrets_path;
                fatal_error = oss.str();
                std::fprintf(stderr, "[CFG FATAL] %s\n", fatal_error.c_str());
                return false;
            }
        }
        // 文件不存在 → 跳过（secrets.yaml optional,见 YAMLMergeUtil 注释）
    }
#endif

    YAML::Node root;
    std::string load_err;
    if (!XtConfigYamlUtil::load_profile_with_common_and_market(
            out.parsed.cfg_path, "common.yaml", "market.yaml", root, &load_err)) {
        // 前缀匹配：loader 的 not-found 消息携带具体路径（"config not found: <path>"）。
        const bool cfg_not_found = load_err.rfind("config not found", 0) == 0;
        fatal_error = cfg_not_found ? load_err : "config parse failed";
        if (!load_err.empty() && !cfg_not_found) fatal_error += ": " + load_err;
        return false;
    }

    validate_unknown_config_keys(root, build_live_schema_rules(), validation_errors);

    const LiveConfigNodes nodes = collect_live_config_nodes(root);
    ParsedFlags flags = parse_and_validate_base_cfg(nodes, out, validation_errors);
    parse_runtime_runner_cfg(nodes, out, flags, validation_errors);
    parse_strategy_runtime_cfg(nodes, flags, out, validation_errors);
    fill_misc_runtime_cfg(nodes, out, validation_errors);
    fill_selector_tasks(root, out.parsed, validation_errors);
    resolve_runtime_output_paths(out);
    // STP 自成交防护安全门 fail-closed：关闭须显式给理由，否则 validation_error → runner abort。
    {
        const auto& gw = out.bootstrap.order_gateway_cfg;
        if (!gw.stp_enabled && gw.stp_disabled_reason.empty()) {
            validation_errors.push_back(
                "runtime.risk.stp_enabled=false requires a non-empty runtime.risk.stp_disabled_reason "
                "(self-trade prevention is a safety gate; explicit opt-out reason is mandatory)");
        }
    }
    // multibook identity 前置自证：merge 后（含 per-dir secrets overlay 的
    // runtime.td[.ctp].user）生效 TD 账号与 expected_investor_id 不一致 = 凭证/部署
    // 错配，连接之前拒绝启动。空值任一侧 = 不校验（第二道权威校验在
    // TraderCTP::OnRspUserLogin 用登录回包 UserID 兜底）。
    {
        const auto& td = out.parsed.td_cfg;
        if (!td.expected_investor_id.empty() && !td.user.empty() &&
            td.expected_investor_id != td.user) {
            validation_errors.push_back(
                "runtime.td.expected_investor_id ('" + td.expected_investor_id +
                "') 与生效 runtime.td user ('" + td.user +
                "') 不一致 — 凭证与部署身份错配，拒绝启动");
        }
    }
    return true;
}
