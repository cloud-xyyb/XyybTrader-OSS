#include "LiveConfigValidators.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "xt_core/order/XtOffsetPolicy.h"

#include "XtLiveRuntimeStateCfg.h"
#include "ConfigValidationCtx.h"
#include "xt_config/yaml/ConfigYamlUtil.h"
#include "xt_util/net/XtLoopbackHost.h"
#include "xt_util/time/CtHHMM.h"
#include "xt_config/catalog/XtFeeRuleBook.h"
#include "xt_live/hft/runtime/XtHftEventLoop.h"

#include "api/XtStrategyRuntimeTypes.h"

extern std::unordered_map<std::string, xt_runtime::XtStraContractInfo>
parse_stra_contract_info(const YAML::Node& node);
extern XtLiveRuntimeStateCfg parse_live_runtime_state_config(const YAML::Node& runtime_node,
                                                            const YAML::Node& commodities_node);

namespace {

bool str_in(const std::string& v, std::initializer_list<const char*> cands) {
    for (const char* c : cands) {
        if (v == c) return true;
    }
    return false;
}

bool is_supported_provider(const std::string& provider) {
    return str_in(provider, {"ctp", "binance_u", "xtp"});
}

bool read_required_provider(const YAML::Node& node,
                            const std::string& path,
                            XtConfigValidationCtx& v,
                            std::string& provider) {
    if (!node || node.IsNull()) return false;
    const YAML::Node provider_node = node["provider"];
    if (!provider_node || provider_node.IsNull()) {
        v.add(path + " 缺失");
        return false;
    }
    if (!v.read_string(provider_node, path, provider)) return false;
    v.add_if(provider.empty(), path + " 不能为空");
    v.add_if(!provider.empty() && !is_supported_provider(provider),
             path + " 仅支持 ctp / binance_u / xtp");
    return !provider.empty() && is_supported_provider(provider);
}

void to_lower_ascii(std::string& s) {
    std::transform(s.begin(),
                   s.end(),
                   s.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
}

using xt_hhmm::valid_hhmm;
namespace YamlNodeUtil = ::XtConfigYamlUtil;
using ValidationCtx = XtConfigValidationCtx;

void validate_per_strategy_risk_sid_keys(const YAML::Node& runtime_node,
                                         const YAML::Node& risk_node,
                                         ValidationCtx& v);
void validate_portfolio_router_policy(const YAML::Node& runtime_node, ValidationCtx& v);

bool validate_optional_positive_size_field(const YAML::Node& entry,
                                           size_t index,
                                           const char* field,
                                           std::string& err) {
    const YAML::Node value = entry[field];
    if (!value || value.IsNull()) return true;
    try {
        const long long parsed = value.as<long long>();
        if (parsed <= 0) {
            err = "runtime.strategies[" + std::to_string(index) + "]." + field + " 必须 > 0";
            return false;
        }
    } catch (...) {
        err = "runtime.strategies[" + std::to_string(index) + "]." + field + " 必须是正整数";
        return false;
    }
    return true;
}

bool validate_strategies_entries(const YAML::Node& strategies, std::string& err) {

    std::unordered_map<std::string, size_t> seen_ids;
    for (size_t i = 0; i < strategies.size(); ++i) {
        const auto& e = strategies[i];
        if (!e || !e.IsMap()) {
            err = "runtime.strategies[" + std::to_string(i) + "] 必须是 map";
            return false;
        }
        const std::string id = e["id"] ? e["id"].as<std::string>("") : "";
        if (id.empty()) {
            err = "runtime.strategies[" + std::to_string(i) + "].id 必填";
            return false;
        }
        const std::string so = e["so"] ? e["so"].as<std::string>("") : "";
        if (so.empty()) {
            err = "runtime.strategies[" + std::to_string(i) + "].so 必填";
            return false;
        }
        if (!validate_optional_positive_size_field(e, i, "history_ticks", err)) return false;
        if (!validate_optional_positive_size_field(e, i, "history_bars", err)) return false;
        auto [it, inserted] = seen_ids.emplace(id, i);
        if (!inserted) {
            err = "runtime.strategies[" + std::to_string(i) + "].id='" + id +
                  "' 与 strategies[" + std::to_string(it->second) +
                  "] 重复 (engine routing 用 strategy_id 做 demux key,必须全局唯一)";
            return false;
        }
    }
    return true;
}

bool validate_strategies_b1_symbol_guard(const YAML::Node& runtime_node,
                                         const YAML::Node& strategies,
                                         std::string& err) {

    const YAML::Node risk_node = runtime_node["risk"];
    if (risk_node && risk_node.IsMap()) {
        const YAML::Node allow = risk_node["allow_strategy_symbol_overlap"];
        if (allow && !allow.IsNull()) {
            bool allow_overlap = false;
            try { allow_overlap = allow.as<bool>(); } catch (...) { allow_overlap = false; }
            if (allow_overlap) return true;
        }
    }
    const YAML::Node instrument_node = runtime_node["instrument"];
    const std::string global_symbol =
        (instrument_node && !instrument_node.IsNull())
            ? YamlNodeUtil::as_or<std::string>(instrument_node, "symbol", std::string(""))
            : std::string("");

    static constexpr std::array<const char*, 3> kSameSymbolRequiredPhases = {
        "R3",
        "R8",
        "R-CALLBACK-EMISSION",
    };
    (void)kSameSymbolRequiredPhases;
    if (strategies.size() <= 1) return true;

    std::unordered_map<std::string, size_t> seen_symbols;
    for (size_t i = 0; i < strategies.size(); ++i) {
        const auto& e = strategies[i];
        std::string sym = e["symbol"] ? e["symbol"].as<std::string>("") : "";
        if (sym.empty()) sym = global_symbol;
        if (sym.empty()) continue;
        auto [it, inserted] = seen_symbols.emplace(sym, i);
        if (!inserted) {
            err = "runtime.strategies[" + std::to_string(i) + "].symbol='" + sym +
                  "' 与 strategies[" + std::to_string(it->second) +
                  "] 同 (B1: 多策略 SAME symbol 共享 account position pool,交易所无 per-strategy "
                  "仓位维 → over-close 不可对账)。同 symbol 多策略已不再支持,请拆成不同 symbol。";
            return false;
        }
    }
    return true;
}

void validate_log_cfg(const XtLoggerCfg* log_cfg, ValidationCtx& v) {
    if (!log_cfg) {
        v.add("log 配置缺失或非法");
        return;
    }
    v.add_if(log_cfg->path.empty(), "log.path 不能为空");
    v.add_if(log_cfg->max_size == 0, "log.max_size 必须 > 0");
    v.add_if(log_cfg->max_files == 0, "log.max_files 必须 > 0");
    v.add_if(log_cfg->queue_size == 0, "log.queue_size 必须 > 0");
    v.add_if(log_cfg->thread_count == 0, "log.thread_count 必须 > 0");
}

void validate_md_cfg(const YAML::Node& md_node, const IMdGateway::Config* md_cfg, ValidationCtx& v) {
    v.add_if(!md_node || md_node.IsNull(), "runtime.md 缺失");
    if (!md_cfg) return;

    std::string provider;
    const bool provider_ok = read_required_provider(md_node, "runtime.md.provider", v, provider);
    v.add_if(md_cfg->front.empty(), "runtime.md.md_front 不能为空");
    if (!provider_ok) return;

    v.add_if(md_cfg->user.empty() && provider == "ctp", "runtime.md.user 不能为空");
    if (provider == "xtp") {

        const auto& x = md_node["xtp"];
        v.add_if(!x || !x["software_key"] || x["software_key"].as<std::string>("").empty(),
                 "runtime.md.xtp.software_key 不能为空");
    } else if (provider == "ctp") {

        v.add_if(!md_node["broker"] || md_node["broker"].as<std::string>("").empty(),
                 "runtime.md.broker 不能为空");
        const std::string flow = YamlNodeUtil::as_or<std::string>(md_node, "flow_dir", std::string("CTP_MD_FLOW"));
        v.add_if(flow.empty(), "runtime.md.flow_dir 不能为空");
    } else if (provider == "binance_u") {
        const auto& b = md_node["binance"];
        v.add_if(!b || !b["base_ws_url"] || b["base_ws_url"].as<std::string>("").empty(),
                 "runtime.md.binance.base_ws_url 不能为空");
        v.add_if(!b || !b["base_rest_url"] || b["base_rest_url"].as<std::string>("").empty(),
                 "runtime.md.binance.base_rest_url 不能为空（depth snapshot 用）");
    }
}

void validate_td_cfg(const YAML::Node& td_node, const ITradeGateway::Config* td_cfg, ValidationCtx& v) {
    v.add_if(!td_node || td_node.IsNull(), "runtime.td 缺失");
    if (!td_cfg) return;

    std::string provider;
    const bool provider_ok = read_required_provider(td_node, "runtime.td.provider", v, provider);

    v.add_if(td_cfg->front.empty(), "runtime.td.td_front 不能为空");
    if (!provider_ok) return;

    if (td_node && !td_node.IsNull() && td_node["recovery"]) {
        try {
            (void)td_node["recovery"].as<bool>();
        } catch (...) {
            v.add("runtime.td.recovery 必须是布尔值");
        }
    }

    v.add_if(td_cfg->user.empty() && provider == "ctp", "runtime.td.user 不能为空");
    if (provider == "xtp") {

        const auto& x = td_node["xtp"];
        v.add_if(!x || !x["software_key"] || x["software_key"].as<std::string>("").empty(),
                 "runtime.td.xtp.software_key 不能为空");
        v.add_if(!x || !x["software_version"] || x["software_version"].as<std::string>("").empty(),
                 "runtime.td.xtp.software_version 不能为空");
        v.add_if(!x || !x["account_key"] || x["account_key"].as<std::string>("").empty(),
                 "runtime.td.xtp.account_key 不能为空");
    } else if (provider == "ctp") {

        v.add_if(!td_node["broker"] || td_node["broker"].as<std::string>("").empty(),
                 "runtime.td.broker 不能为空");
        const std::string flow = YamlNodeUtil::as_or<std::string>(td_node, "flow_dir", std::string("CTP_TD_FLOW"));
        v.add_if(flow.empty(), "runtime.td.flow_dir 不能为空");
        const int rq_to = YamlNodeUtil::as_or<int>(td_node, "recovery_query_timeout_ms", 8000);
        v.add_if(rq_to < 0, "runtime.td.recovery_query_timeout_ms 不能为负");
        const int rq_mx = YamlNodeUtil::as_or<int>(td_node, "recovery_query_max_timeouts", 6);
        v.add_if(rq_mx <= 0, "runtime.td.recovery_query_max_timeouts 必须 > 0");

        const int rr_bo = YamlNodeUtil::as_or<int>(td_node, "recovery_rearm_backoff_ms", 5000);
        v.add_if(rr_bo < 0, "runtime.td.recovery_rearm_backoff_ms 不能为负（<=0 表示关闭 re-arm）");

        const int rr_ar = YamlNodeUtil::as_or<int>(td_node, "recovery_rearm_alert_rounds", 10);
        v.add_if(rr_ar < 0, "runtime.td.recovery_rearm_alert_rounds 不能为负");
    } else if (provider == "binance_u") {
        const auto& b = td_node["binance"];
        v.add_if(!b || !b["base_rest_url"] || b["base_rest_url"].as<std::string>("").empty(),
                 "runtime.td.binance.base_rest_url 不能为空");
        v.add_if(!b || !b["base_ws_url"] || b["base_ws_url"].as<std::string>("").empty(),
                 "runtime.td.binance.base_ws_url 不能为空");
    }
    v.add_if(td_cfg->reconnect_interval_ms < 0, "runtime.td.reconnect_interval_ms 不能为负");
    v.add_if(td_cfg->account_refresh_ms < 0, "runtime.td.account_refresh_ms 不能为负");
    v.add_if(td_cfg->position_refresh_ms < 0, "runtime.td.position_refresh_ms 不能为负");
    v.add_if(!std::isfinite(td_cfg->account_snapshot_max_abs) || td_cfg->account_snapshot_max_abs <= 0.0,
             "runtime.td.account_snapshot_max_abs 必须为正且有限");

    v.add_if(td_cfg->stale_reconnect_ms != 0 && td_cfg->stale_reconnect_ms < 5000,
             "runtime.td.stale_reconnect_ms 必须 = 0 (禁用) 或 >= 5000 (5s)");

    {
        const YAML::Node rec = td_node["reconcile"];
        if (rec && !rec.IsNull()) {
            const int64_t cyc = YamlNodeUtil::as_or<int64_t>(rec, "cycle_ms", 30'000);
            v.add_if(cyc < 1000,
                     "runtime.td.reconcile.cycle_ms 必须 >= 1000（防 busy-loop / REST 限流）");
            const int64_t rc_n = YamlNodeUtil::as_or<int64_t>(rec, "reconnect_after_n", 2);
            v.add_if(rc_n < 0 || rc_n > 255,
                     "runtime.td.reconcile.reconnect_after_n 必须在 [0,255]（0 = 禁用 resnap 阶梯）");
            const int64_t kill_n = YamlNodeUtil::as_or<int64_t>(rec, "kill_after_n", 8);
            v.add_if(kill_n < 0 || kill_n > 255,
                     "runtime.td.reconcile.kill_after_n 必须在 [0,255]（0 = 禁用 kill 阶梯）");
            const int64_t thr = YamlNodeUtil::as_or<int64_t>(rec, "resync_throttle_ms", 60'000);
            v.add_if(thr < 0, "runtime.td.reconcile.resync_throttle_ms 不能为负");
            const int64_t eps = YamlNodeUtil::as_or<int64_t>(rec, "position_epsilon_fp", 1);
            v.add_if(eps < 0, "runtime.td.reconcile.position_epsilon_fp 不能为负");
            const int64_t age = YamlNodeUtil::as_or<int64_t>(rec, "order_min_age_ms", 30'000);
            v.add_if(age < 0, "runtime.td.reconcile.order_min_age_ms 不能为负");
        }
    }
}

void validate_account_node(const YAML::Node& runtime_node, ValidationCtx& v) {
    const YAML::Node account_node = runtime_node["account"];
    if (!account_node || account_node.IsNull()) return;

    if (account_node["mode"]) {
        try {
            const std::string mode = account_node["mode"].as<std::string>();
            v.add_if(!str_in(mode, {"cash", "futures"}), "runtime.account.mode 仅支持 cash/futures");
        } catch (...) {
            v.add("runtime.account.mode 必须是字符串");
        }
    }

    double cash = 0.0;
    if (v.read_double(account_node["cash"], "runtime.account.cash", cash)) {
        v.add_if(!std::isfinite(cash) || cash < 0.0, "runtime.account.cash 必须是非负有限值");
    }

    double margin_rate = 1.0;
    if (v.read_double(account_node["margin_rate"], "runtime.account.margin_rate", margin_rate)) {
        v.add_if(!std::isfinite(margin_rate) || margin_rate <= 0.0, "runtime.account.margin_rate 必须 > 0");
    }

    const YAML::Node margin_by_symbol = account_node["margin_by_symbol"];
    if (!margin_by_symbol || margin_by_symbol.IsNull()) return;
    if (!margin_by_symbol.IsMap()) {
        v.add("runtime.account.margin_by_symbol 必须是 map");
        return;
    }
    for (auto it = margin_by_symbol.begin(); it != margin_by_symbol.end(); ++it) {
        try {
            const std::string key = it->first.as<std::string>();
            double value = 0.0;
            if (v.read_double(it->second, "runtime.account.margin_by_symbol." + key, value)) {
                v.add_if(!std::isfinite(value) || value <= 0.0,
                         "runtime.account.margin_by_symbol." + key + " 必须 > 0");
            }
        } catch (...) {
            v.add("runtime.account.margin_by_symbol 键必须是字符串");
        }
    }
}

void validate_risk_node(const YAML::Node& runtime_node, ValidationCtx& v) {
    const YAML::Node risk_node = runtime_node["risk"];
    if (!risk_node || risk_node.IsNull()) {
        v.add("runtime.risk 缺失");
        return;
    }
    if (!risk_node.IsMap()) {
        v.add("runtime.risk 必须是 map");
        return;
    }
    validate_per_strategy_risk_sid_keys(runtime_node, risk_node, v);

    double d = 0.0;
    int i = 0;
    if (v.read_double(risk_node["max_order_qty"], "runtime.risk.max_order_qty", d)) {
        v.add_if(!std::isfinite(d) || d < 0.0, "runtime.risk.max_order_qty 不能为负");
    }
    if (v.read_double(risk_node["max_order_notional"], "runtime.risk.max_order_notional", d)) {
        v.add_if(!std::isfinite(d) || d < 0.0, "runtime.risk.max_order_notional 不能为负");
    }
    if (v.read_double(risk_node["min_order_notional"], "runtime.risk.min_order_notional", d)) {
        v.add_if(!std::isfinite(d) || d < 0.0, "runtime.risk.min_order_notional 不能为负");
    }
    if (v.read_double(risk_node["max_pos_qty"], "runtime.risk.max_pos_qty", d)) {
        v.add_if(!std::isfinite(d) || d < 0.0, "runtime.risk.max_pos_qty 不能为负");
    }
    if (v.read_double(risk_node["max_daily_loss"], "runtime.risk.max_daily_loss", d)) {
        v.add_if(!std::isfinite(d) || d < 0.0, "runtime.risk.max_daily_loss 不能为负");
    }
    if (v.read_int(risk_node["max_orders_per_sec"], "runtime.risk.max_orders_per_sec", i)) {
        v.add_if(i < 0, "runtime.risk.max_orders_per_sec 不能为负");
    }
    if (v.read_int(risk_node["max_cancels_per_sec"], "runtime.risk.max_cancels_per_sec", i)) {
        v.add_if(i < 0, "runtime.risk.max_cancels_per_sec 不能为负");
    }
    if (v.read_int(risk_node["max_rejects_per_min"], "runtime.risk.max_rejects_per_min", i)) {
        v.add_if(i < 0, "runtime.risk.max_rejects_per_min 不能为负");
    }
    if (v.read_int(risk_node["cooldown_ms"], "runtime.risk.cooldown_ms", i)) {
        v.add_if(i < 0, "runtime.risk.cooldown_ms 不能为负");
    }
    if (v.read_int(risk_node["position_snapshot_stale_ms"], "runtime.risk.position_snapshot_stale_ms", i)) {
        v.add_if(i <= 0, "runtime.risk.position_snapshot_stale_ms 必须 > 0");
    }
    bool b = false;
    (void)v.read_bool(risk_node["enable_position_snapshot_stale_guard"],
                      "runtime.risk.enable_position_snapshot_stale_guard",
                      b);
    (void)v.read_bool(risk_node["require_l2_orderflow"], "runtime.risk.require_l2_orderflow", b);

    if (v.read_int(risk_node["max_order_count_per_symbol"],
                   "runtime.risk.max_order_count_per_symbol", i)) {
        v.add_if(i < 0, "runtime.risk.max_order_count_per_symbol 不能为负");
    }
    if (v.read_int(risk_node["max_order_rate_per_symbol"],
                   "runtime.risk.max_order_rate_per_symbol", i)) {
        v.add_if(i < 0, "runtime.risk.max_order_rate_per_symbol 不能为负");
    }
    if (v.read_int(risk_node["order_rate_window_ms"],
                   "runtime.risk.order_rate_window_ms", i)) {
        v.add_if(i <= 0, "runtime.risk.order_rate_window_ms 必须 > 0");
    }
    (void)v.read_bool(risk_node["blacklist_persist_across_session"],
                      "runtime.risk.blacklist_persist_across_session", b);

    bool auto_liq = false;
    (void)v.read_bool(risk_node["per_strategy_auto_liquidate"],
                      "runtime.risk.per_strategy_auto_liquidate", auto_liq);

    (void)v.read_bool(risk_node["per_strategy_auto_liquidate_dry_run"],
                      "runtime.risk.per_strategy_auto_liquidate_dry_run", b);
    const YAML::Node liq_map = risk_node["per_strategy_liquidate_pct"];
    if (auto_liq) {
        v.add_if(!liq_map || liq_map.IsNull() || !liq_map.IsMap() || liq_map.size() == 0,
                 "runtime.risk.per_strategy_auto_liquidate=true 必须配 "
                 "per_strategy_liquidate_pct (sid -> 比例)");
        bool overlap = false;
        const YAML::Node allow = risk_node["allow_strategy_symbol_overlap"];
        if (allow && !allow.IsNull()) {
            try { overlap = allow.as<bool>(); } catch (...) { overlap = false; }
        }
        v.add_if(overlap,
                 "runtime.risk.per_strategy_auto_liquidate 不能与 "
                 "allow_strategy_symbol_overlap=true 同用：自动强平要求 1:1 sid:symbol，"
                 "same-symbol 多策略下账户净仓无 per-sid 归属。");
    }

    if (liq_map && liq_map.IsMap()) {
        for (auto it = liq_map.begin(); it != liq_map.end(); ++it) {
            std::string key;
            try { key = it->first.as<std::string>(); } catch (...) { continue; }
            double pct = 0.0;
            try { pct = it->second.as<double>(); }
            catch (...) {
                v.add("runtime.risk.per_strategy_liquidate_pct." + key + " 必须是数值");
                continue;
            }
            v.add_if(!(std::isfinite(pct) && pct > 0.0 && pct < 1.0),
                     "runtime.risk.per_strategy_liquidate_pct." + key + " 必须在 (0,1) 区间");
        }
    }
}

void validate_instrument_node(const YAML::Node& runtime_node, ValidationCtx& v) {
    const YAML::Node instrument_node = runtime_node["instrument"];
    if (!instrument_node || instrument_node.IsNull()) {
        v.add("runtime.instrument 缺失");
        return;
    }
    if (!instrument_node.IsMap()) {
        v.add("runtime.instrument 必须是 map");
        return;
    }
    std::string symbol;
    if (!v.read_string(instrument_node["symbol"], "runtime.instrument.symbol", symbol)) {
        v.add("runtime.instrument.symbol 缺失");
        return;
    }
    v.add_if(symbol.empty(), "runtime.instrument.symbol 不能为空");
}

void validate_session_node(const YAML::Node& runtime_node, ValidationCtx& v) {
    const YAML::Node session_node = runtime_node["session"];
    if (!session_node || session_node.IsNull()) {
        v.add("runtime.session 缺失");
        return;
    }
    if (!session_node.IsMap()) {
        v.add("runtime.session 必须是 map");
        return;
    }
    const YAML::Node sections_node = session_node["active_sections"];
    if (!sections_node || sections_node.IsNull()) {
        v.add("runtime.session.active_sections 缺失");
        return;
    }
    if (!sections_node.IsSequence()) {
        v.add("runtime.session.active_sections 必须是 sequence");
        return;
    }
    for (size_t i = 0; i < sections_node.size(); ++i) {
        const YAML::Node item = sections_node[i];
        const std::string path_prefix = "runtime.session.active_sections[" + std::to_string(i) + "]";
        if (!item || !item.IsMap()) {
            v.add(path_prefix + " 必须是 map");
            continue;
        }
        int start = 0;
        int end = 0;
        if (v.read_int(item["start"], path_prefix + ".start", start)) {
            v.add_if(!valid_hhmm(start), path_prefix + ".start 必须是 HHMM（0000~2359）");
        }
        if (v.read_int(item["end"], path_prefix + ".end", end)) {
            v.add_if(!valid_hhmm(end), path_prefix + ".end 必须是 HHMM（0000~2359）");
        }
    }
}

void validate_trading_node(const YAML::Node& runtime_node, ValidationCtx& v) {
    const YAML::Node trading_node = runtime_node["trading"];
    if (!trading_node || trading_node.IsNull()) {
        v.add("runtime.trading 缺失");
        return;
    }
    if (!trading_node.IsMap()) {
        v.add("runtime.trading 必须是 map");
        return;
    }
    bool allow_short = false;
    if (!v.read_bool(trading_node["allow_short"], "runtime.trading.allow_short", allow_short)) {
        v.add("runtime.trading.allow_short 缺失");
    }

    if (trading_node["offset_policy_default"]) {
        std::string s;
        XtOffsetPolicy p{};
        if (v.read_string(trading_node["offset_policy_default"],
                          "runtime.trading.offset_policy_default", s)) {
            v.add_if(!parse_offset_policy(s, p),
                     "runtime.trading.offset_policy_default 必须是 close_first | open_allowed");
        }
    }
}

void validate_closeout_window_fields(const YAML::Node& node,
                                     const std::string& path_prefix,
                                     ValidationCtx& v) {
    int no_open = 1455;
    int force_flat = 1459;
    if (v.read_int(node["no_open_hhmm"], path_prefix + ".no_open_hhmm", no_open)) {
        v.add_if(!valid_hhmm(no_open), path_prefix + ".no_open_hhmm 必须是 HHMM（0000~2359）");
    }
    if (v.read_int(node["force_flat_hhmm"], path_prefix + ".force_flat_hhmm", force_flat)) {
        v.add_if(!valid_hhmm(force_flat), path_prefix + ".force_flat_hhmm 必须是 HHMM（0000~2359）");
    }

    if (valid_hhmm(no_open) && valid_hhmm(force_flat)) {
        v.add_if(force_flat <= no_open,
                 path_prefix + " closeout 窗口要求 force_flat_hhmm > no_open_hhmm（不支持跨午夜窗口）");
    }
}

void validate_closeout_windows_node(const YAML::Node& closeout_node, ValidationCtx& v) {
    const YAML::Node windows_node = closeout_node["windows"];
    if (!windows_node || windows_node.IsNull()) {
        validate_closeout_window_fields(closeout_node, "runtime.closeout", v);
        return;
    }
    if (!windows_node.IsSequence()) {
        v.add("runtime.closeout.windows 必须是 sequence");
        return;
    }
    for (size_t i = 0; i < windows_node.size(); ++i) {
        const YAML::Node item = windows_node[i];
        const std::string path_prefix = "runtime.closeout.windows[" + std::to_string(i) + "]";
        if (!item || !item.IsMap()) {
            v.add(path_prefix + " 必须是 map");
            continue;
        }
        validate_closeout_window_fields(item, path_prefix, v);
    }
}

void validate_closeout_session_gate_node(const YAML::Node& closeout_node, ValidationCtx& v) {
    const YAML::Node gate_node = closeout_node["session_gate"];
    if (!gate_node || gate_node.IsNull()) return;

    bool gate_enabled = false;
    (void)v.read_bool(gate_node["enabled"], "runtime.closeout.session_gate.enabled", gate_enabled);
}

void validate_closeout_node(const YAML::Node& runtime_node, ValidationCtx& v) {
    const YAML::Node closeout_node = runtime_node["closeout"];
    if (!closeout_node || closeout_node.IsNull()) {
        v.add("runtime.closeout 缺失");
        return;
    }
    if (!closeout_node.IsMap()) {
        v.add("runtime.closeout 必须是 map");
        return;
    }

    bool enabled = false;
    if (closeout_node["enabled"]) {
        try {
            enabled = closeout_node["enabled"].as<bool>();
        } catch (...) {
            v.add("runtime.closeout.enabled 必须是布尔值");
        }
    }
    (void)enabled;

    validate_closeout_windows_node(closeout_node, v);
    int retry_ms = 1000;
    if (v.read_int(closeout_node["retry_ms"], "runtime.closeout.retry_ms", retry_ms)) {
        v.add_if(retry_ms < 0, "runtime.closeout.retry_ms 不能为负");
    }
    validate_closeout_session_gate_node(closeout_node, v);
    if (YamlNodeUtil::as_or<bool>(closeout_node["session_gate"], "enabled", true)) {
        const YAML::Node sections_node = runtime_node["session"]["active_sections"];
        v.add_if(!sections_node || !sections_node.IsSequence() || sections_node.size() == 0,
                 "runtime.closeout.session_gate.enabled=true 时 runtime.session.active_sections 不能为空");
    }
}

void validate_recording_node(const YAML::Node& runtime_node, ValidationCtx& v) {
    const YAML::Node node = runtime_node["recording"];
    if (!node || node.IsNull()) return;
    if (!node.IsMap()) {
        v.add("runtime.recording 必须是 map");
        return;
    }

    bool b = false;
    std::string s;
    int queue_capacity = 0;
    const bool enabled_set = v.read_bool(node["enabled"], "runtime.recording.enabled", b);
    (void)v.read_string(node["file_path"], "runtime.recording.file_path", s);
    if (v.read_int(node["queue_capacity"], "runtime.recording.queue_capacity", queue_capacity)) {
        v.add_if(queue_capacity < 0, "runtime.recording.queue_capacity 不能为负");
    }

    if (enabled_set && b) {
        const std::string path = YamlNodeUtil::as_or<std::string>(node, "file_path", std::string());
        v.add_if(path.empty(), "runtime.recording.enabled=true 时 file_path 不能为空");
    }

    const YAML::Node intent = node["intent_trace"];
    if (intent && !intent.IsNull()) {
        if (!intent.IsMap()) {
            v.add("runtime.recording.intent_trace 必须是 map");
        } else {
            bool intent_enabled = false;
            bool include_wire_debug = false;
            double sample_rate = 0.0;
            (void)v.read_bool(intent["enabled"],
                              "runtime.recording.intent_trace.enabled",
                              intent_enabled);
            if (v.read_double(intent["sample_rate"],
                              "runtime.recording.intent_trace.sample_rate",
                              sample_rate)) {
                v.add_if(!std::isfinite(sample_rate) || sample_rate < 0.0 || sample_rate > 1.0,
                         "runtime.recording.intent_trace.sample_rate 必须在 [0.0, 1.0] 范围内");
            }
            (void)v.read_bool(intent["include_wire_debug"],
                              "runtime.recording.intent_trace.include_wire_debug",
                              include_wire_debug);
        }
    }

    const YAML::Node es = node["event_store"];
    if (es && !es.IsNull()) {
        if (!es.IsMap()) {
            v.add("runtime.recording.event_store 必须是 map");
        } else {
            bool es_enabled = false;
            std::string es_root;
            const bool es_enabled_set = v.read_bool(es["enabled"],
                                                   "runtime.recording.event_store.enabled",
                                                   es_enabled);
            (void)v.read_string(es["root_path"],
                                "runtime.recording.event_store.root_path", es_root);
            if (es_enabled_set && es_enabled) {
                const std::string rp = YamlNodeUtil::as_or<std::string>(
                    es, "root_path", std::string());
                v.add_if(rp.empty(),
                         "runtime.recording.event_store.enabled=true 时 root_path 不能为空");
            }
        }
    }
}

void validate_risk_monitor_node(const YAML::Node& runtime_node, ValidationCtx& v) {
    const YAML::Node node = runtime_node["risk_monitor"];
    if (!node || node.IsNull()) return;
    if (!node.IsMap()) {
        v.add("runtime.risk_monitor 必须是 map");
        return;
    }
    bool b = false;
    int  i = 0;
    (void)v.read_bool(node["enabled"], "runtime.risk_monitor.enabled", b);
    if (v.read_int(node["idd_scale_threshold_bp"], "runtime.risk_monitor.idd_scale_threshold_bp", i)) {
        v.add_if(i < 0, "runtime.risk_monitor.idd_scale_threshold_bp 不能为负");
    }
    if (v.read_int(node["idd_halt_threshold_bp"], "runtime.risk_monitor.idd_halt_threshold_bp", i)) {
        v.add_if(i < 0, "runtime.risk_monitor.idd_halt_threshold_bp 不能为负");
    }
    if (v.read_int(node["idd_scale_factor_fp"], "runtime.risk_monitor.idd_scale_factor_fp", i)) {
        v.add_if(i < 0 || i > 1000000,
                 "runtime.risk_monitor.idd_scale_factor_fp 必须在 [0, 1000000] (1e6 = 1.0x)");
    }
    if (v.read_int(node["stale_quote_ms"], "runtime.risk_monitor.stale_quote_ms", i)) {
        v.add_if(i < 0, "runtime.risk_monitor.stale_quote_ms 不能为负");
    }
}

enum class IntCheck {
    Positive,
    NonNegative,
    AtLeastMinusOne,
};

struct TypedFieldSpec {
    const char* key = "";
    const char* path = "";
};

struct IntFieldSpec {
    const char* key = "";
    const char* path = "";
    int* out = nullptr;
    IntCheck check = IntCheck::NonNegative;
    const char* err_msg = "";
};

bool int_check_failed(int value, IntCheck check) {
    switch (check) {
        case IntCheck::Positive:
            return value <= 0;
        case IntCheck::NonNegative:
            return value < 0;
        case IntCheck::AtLeastMinusOne:
            return value < -1;
    }
    return false;
}

void validate_bool_fields(const YAML::Node& node,
                          ValidationCtx& v,
                          std::initializer_list<TypedFieldSpec> fields) {
    for (const TypedFieldSpec& field : fields) {
        bool out = false;
        (void)v.read_bool(node[field.key], field.path, out);
    }
}

void validate_string_fields(const YAML::Node& node,
                            ValidationCtx& v,
                            std::initializer_list<TypedFieldSpec> fields) {
    for (const TypedFieldSpec& field : fields) {
        std::string out;
        (void)v.read_string(node[field.key], field.path, out);
    }
}

void validate_int_fields(const YAML::Node& node,
                         ValidationCtx& v,
                         std::initializer_list<IntFieldSpec> fields) {
    for (const IntFieldSpec& field : fields) {
        int scratch = 0;
        int& out = field.out ? *field.out : scratch;
        if (!v.read_int(node[field.key], field.path, out)) continue;
        v.add_if(int_check_failed(out, field.check), field.err_msg);
    }
}

void validate_queue_mode_fields(const YAML::Node& queue_node, ValidationCtx& v) {
    const TypedFieldSpec fields[] = {
        {"mode", "queue.mode"},
        {"td_event_mode", "queue.td_event_mode"},
        {"flow_event_mode", "queue.flow_event_mode"},
    };
    for (const TypedFieldSpec& field : fields) {
        std::string mode;
        if (!v.read_string(queue_node[field.key], field.path, mode)) continue;
        v.add_if(!str_in(mode, {"spsc", "mpsc", "mpmc"}), std::string(field.path) + " 仅支持 spsc/mpsc/mpmc");
    }
}

void validate_queue_node(const YAML::Node& queue_node, ValidationCtx& v) {
    if (!queue_node || queue_node.IsNull()) return;

    validate_queue_mode_fields(queue_node, v);

    int cap = 0;
    int tick_hwm = 0;
    int td_event_cap = 0;
    int flow_event_cap = 0;
    int td_hwm = 0;
    int flow_hwm = 0;
    int td_backlog_warn_threshold = 0;
    int tick_batch = 0;
    int td_batch = 0;
    int flow_batch = 0;

    validate_int_fields(queue_node,
                        v,
                        {
                            {"capacity", "queue.capacity", &cap, IntCheck::Positive, "queue.capacity 必须 > 0"},
                            {"tick_high_watermark", "queue.tick_high_watermark", &tick_hwm, IntCheck::Positive, "queue.tick_high_watermark 必须 > 0"},
                            {"event_capacity", "queue.event_capacity", nullptr, IntCheck::Positive, "queue.event_capacity 必须 > 0"},
                            {"td_event_capacity", "queue.td_event_capacity", &td_event_cap, IntCheck::Positive, "queue.td_event_capacity 必须 > 0"},
                            {"flow_event_capacity", "queue.flow_event_capacity", &flow_event_cap, IntCheck::Positive, "queue.flow_event_capacity 必须 > 0"},
                            {"td_event_high_watermark", "queue.td_event_high_watermark", &td_hwm, IntCheck::Positive, "queue.td_event_high_watermark 必须 > 0"},
                            {"flow_event_high_watermark", "queue.flow_event_high_watermark", &flow_hwm, IntCheck::Positive, "queue.flow_event_high_watermark 必须 > 0"},
                            {"td_event_budget_per_loop", "queue.td_event_budget_per_loop", nullptr, IntCheck::Positive, "queue.td_event_budget_per_loop 必须 > 0"},
                            {"flow_event_budget_per_loop", "queue.flow_event_budget_per_loop", nullptr, IntCheck::Positive, "queue.flow_event_budget_per_loop 必须 > 0"},
                            {"tick_max_wait_us", "queue.tick_max_wait_us", nullptr, IntCheck::NonNegative, "queue.tick_max_wait_us 不能为负"},
                            {"tick_starvation_warn_loops", "queue.tick_starvation_warn_loops", nullptr, IntCheck::NonNegative, "queue.tick_starvation_warn_loops 不能为负"},
                            {"td_backlog_warn_threshold", "queue.td_backlog_warn_threshold", &td_backlog_warn_threshold, IntCheck::Positive, "queue.td_backlog_warn_threshold 必须 > 0"},
                        });
    if (v.read_int(queue_node["tick_batch_size"], "queue.tick_batch_size", tick_batch)) {
        v.add_if(tick_batch <= 0, "queue.tick_batch_size 必须 > 0");
        v.add_if(
            tick_batch > static_cast<int>(XtHftEventLoop::kTickBatchMax),
            std::string("queue.tick_batch_size 不能大于 ") +
                std::to_string(XtHftEventLoop::kTickBatchMax) +
                "（避免 worker 栈占用过大）");
    }
    if (v.read_int(queue_node["td_event_batch_size"], "queue.td_event_batch_size", td_batch)) {
        v.add_if(td_batch <= 0, "queue.td_event_batch_size 必须 > 0");
        v.add_if(
            td_batch > static_cast<int>(XtHftEventLoop::kTdEventBatchMax),
            std::string("queue.td_event_batch_size 不能大于 ") +
                std::to_string(XtHftEventLoop::kTdEventBatchMax) +
                "（避免 worker 栈占用过大）");
    }
    if (v.read_int(queue_node["flow_event_batch_size"], "queue.flow_event_batch_size", flow_batch)) {
        v.add_if(flow_batch <= 0, "queue.flow_event_batch_size 必须 > 0");
        v.add_if(
            flow_batch > static_cast<int>(XtHftEventLoop::kFlowEventBatchMax),
            std::string("queue.flow_event_batch_size 不能大于 ") +
                std::to_string(XtHftEventLoop::kFlowEventBatchMax) +
                "（避免 worker 栈占用过大）");
    }

    if (cap > 0 && tick_hwm > 0) {
        v.add_if(tick_hwm > cap, "queue.tick_high_watermark 不能大于 queue.capacity");
    }
    if (td_event_cap > 0 && td_hwm > 0) {
        v.add_if(td_hwm > td_event_cap, "queue.td_event_high_watermark 不能大于 queue.td_event_capacity");
    }
    if (td_event_cap > 0 && td_backlog_warn_threshold > 0) {
        v.add_if(
            td_backlog_warn_threshold > td_event_cap,
            "queue.td_backlog_warn_threshold 不能大于 queue.td_event_capacity");
    }
    if (flow_event_cap > 0 && flow_hwm > 0) {
        v.add_if(flow_hwm > flow_event_cap, "queue.flow_event_high_watermark 不能大于 queue.flow_event_capacity");
    }
}

void validate_debug_node(const YAML::Node& debug_node, ValidationCtx& v) {
    if (!debug_node || debug_node.IsNull()) return;

    validate_bool_fields(debug_node,
                         v,
                         {
                             {"tick_count", "runtime.debug.tick_count"},
                             {"fail_on_critical_td_drop", "runtime.debug.fail_on_critical_td_drop"},
                             {"mlockall", "runtime.debug.mlockall"},
                         });
    validate_string_fields(debug_node,
                           v,
                           {
                               {"report_contract_dir", "runtime.debug.report_contract_dir"},
                               {"report_run_id", "runtime.debug.report_run_id"},
                               {"report_config_hash", "runtime.debug.report_config_hash"},
                               {"report_code_version", "runtime.debug.report_code_version"},
                           });

    validate_int_fields(debug_node,
                        v,
                        {
                            {"heartbeat_ms", "runtime.debug.heartbeat_ms", nullptr, IntCheck::NonNegative, "runtime.debug.heartbeat_ms 不能为负"},
                            {"consistency_warn_loops", "runtime.debug.consistency_warn_loops", nullptr, IntCheck::Positive, "runtime.debug.consistency_warn_loops 必须 > 0"},
                            {"consistency_block_loops", "runtime.debug.consistency_block_loops", nullptr, IntCheck::Positive, "runtime.debug.consistency_block_loops 必须 > 0"},
                            {"consistency_kill_loops", "runtime.debug.consistency_kill_loops", nullptr, IntCheck::Positive, "runtime.debug.consistency_kill_loops 必须 > 0"},
                            {"report_contract_flush_ms", "runtime.debug.report_contract_flush_ms", nullptr, IntCheck::NonNegative, "runtime.debug.report_contract_flush_ms 不能为负"},
                            {"main_affinity", "runtime.debug.main_affinity", nullptr, IntCheck::AtLeastMinusOne, "runtime.debug.main_affinity 必须 >= -1"},
                        });

    int warn_loops = 0;
    int block_loops = 0;
    int kill_loops = 0;
    const bool has_warn = v.read_int(debug_node["consistency_warn_loops"],
                                     "runtime.debug.consistency_warn_loops",
                                     warn_loops);
    const bool has_block = v.read_int(debug_node["consistency_block_loops"],
                                      "runtime.debug.consistency_block_loops",
                                      block_loops);
    const bool has_kill = v.read_int(debug_node["consistency_kill_loops"],
                                     "runtime.debug.consistency_kill_loops",
                                     kill_loops);
    if (has_warn && has_block) {
        v.add_if(block_loops < warn_loops, "runtime.debug.consistency_block_loops 不能小于 consistency_warn_loops");
    }
    if (has_block && has_kill) {
        v.add_if(kill_loops < block_loops, "runtime.debug.consistency_kill_loops 不能小于 consistency_block_loops");
    }
}

void validate_control_grpc_node(const YAML::Node& runtime_node, ValidationCtx& v) {
    if (runtime_node["control_panel"]) {
        v.add("runtime.control_panel 已退役；请迁移到 runtime.control_grpc");
    }
    if (runtime_node["control_cfg"]) {
        v.add("runtime.control_cfg 已退役；请迁移到 runtime.control_grpc");
    }

    const YAML::Node cg_node = runtime_node["control_grpc"];
    if (!cg_node || cg_node.IsNull()) return;
    if (!cg_node.IsMap()) {
        v.add("runtime.control_grpc 必须是 map");
        return;
    }

    bool enabled = false;
    (void)v.read_bool(cg_node["enabled"], "runtime.control_grpc.enabled", enabled);
    if (!enabled) return;

    std::string listen_addr;
    if (v.read_string(cg_node["listen_addr"], "runtime.control_grpc.listen_addr", listen_addr)) {
        v.add_if(listen_addr.empty(), "runtime.control_grpc.listen_addr 不能为空");
    } else {
        v.add("runtime.control_grpc.listen_addr 缺失");
    }
    std::string audit_log_path;
    if (v.read_string(cg_node["audit_log_path"], "runtime.control_grpc.audit_log_path", audit_log_path)) {
        v.add_if(audit_log_path.empty(), "runtime.control_grpc.audit_log_path 不能为空");
    } else {
        v.add("runtime.control_grpc.audit_log_path 缺失");
    }

    const YAML::Node tls_node = cg_node["tls"];
    v.add_if(!tls_node || tls_node.IsNull(), "runtime.control_grpc.tls 缺失");
    if (!tls_node || tls_node.IsNull()) return;
    if (!tls_node.IsMap()) {
        v.add("runtime.control_grpc.tls 必须是 map");
        return;
    }
    for (const auto* key : {"ca_path", "cert_path", "key_path"}) {
        std::string value;
        const std::string path = std::string("runtime.control_grpc.tls.") + key;
        if (v.read_string(tls_node[key], path, value)) {
            v.add_if(value.empty(), path + " 不能为空");
        } else {
            v.add(path + " 缺失");
        }
    }

    std::string tls_mode = "mtls";
    (void)v.read_string(tls_node["mode"], "runtime.control_grpc.tls.mode", tls_mode);
    if (tls_mode != "mtls" && tls_mode != "dev_insecure") {
        v.add("runtime.control_grpc.tls.mode 必须是 mtls 或 dev_insecure");
    }
    if (tls_mode == "dev_insecure") {

        v.add_if(!xt_net::listen_addr_is_loopback_literal(listen_addr),
                 "runtime.control_grpc.tls.mode=dev_insecure 仅允许 loopback "
                 "listen_addr，当前=" + listen_addr);
    }

#ifndef XT_HAVE_CONTROL_GRPC
    v.add("runtime.control_grpc.enabled=true 但当前 binary 未编译 XT_HAVE_CONTROL_GRPC");
#endif
}

void validate_runtime_cross(const YAML::Node& runtime_node,
                            const YAML::Node& md_node,
                            const YAML::Node& queue_node,
                            const YAML::Node& commodities_node,
                            const YAML::Node& fees_node,
                            ValidationCtx& v) {
    std::string single_strategy_err;
    if (!validate_strategy_or_strategies_cfg(runtime_node, single_strategy_err)) {
        v.add(single_strategy_err);
    }

    if (commodities_node && !commodities_node.IsNull() && !commodities_node.IsMap()) {
        v.add("commodities 必须是 map");
    }
    if (!fees_node || fees_node.IsNull()) {
        v.add("fees 缺失（请在 market.yaml 配置）");
    } else if (!fees_node.IsMap()) {
        v.add("fees 必须是 map");
    } else if (parse_fee_rule_book_config(fees_node).rules.empty()) {
        v.add("fees 不能为空");
    }
    try {
        (void)parse_live_runtime_state_config(runtime_node, commodities_node);
    } catch (const std::exception& e) {
        v.add(std::string("runtime/commodities 配置解析失败: ") + e.what());
    } catch (...) {
        v.add("runtime/commodities 配置解析失败");
    }
    try {
        (void)parse_stra_contract_info(commodities_node);
    } catch (const std::exception& e) {
        v.add(std::string("commodities 策略合约信息解析失败: ") + e.what());
    } catch (...) {
        v.add("commodities 策略合约信息解析失败");
    }

    validate_queue_node(queue_node, v);
    validate_debug_node(runtime_node["debug"], v);
    validate_control_grpc_node(runtime_node, v);

    if (md_node && !md_node.IsNull()) {
        int md_log_interval = 0;
        if (v.read_int(md_node["log_interval_ms"], "runtime.md.log_interval_ms", md_log_interval)) {
            v.add_if(md_log_interval <= 0, "runtime.md.log_interval_ms 必须 > 0");
        }
    }
}

std::unordered_set<std::string> collect_declared_strategy_ids(const YAML::Node& runtime_node) {
    std::unordered_set<std::string> ids;
    const YAML::Node strategies = runtime_node["strategies"];
    if (strategies && strategies.IsSequence()) {
        for (const auto& item : strategies) {
            if (!item || !item.IsMap() || !item["id"]) continue;
            try {
                const std::string id = item["id"].as<std::string>();
                if (!id.empty()) ids.insert(id);
            } catch (...) {}
        }
    }
    return ids;
}

void validate_per_strategy_risk_sid_keys(const YAML::Node& runtime_node,
                                         const YAML::Node& risk_node,
                                         ValidationCtx& v) {
    if (!risk_node || risk_node.IsNull()) return;
    const auto ids = collect_declared_strategy_ids(runtime_node);
    auto check_map = [&](const char* field) {
        const YAML::Node node = risk_node[field];
        if (!node || node.IsNull()) return;
        if (!node.IsMap()) {
            v.add(std::string("runtime.risk.") + field + " 必须是 map");
            return;
        }
        for (auto it = node.begin(); it != node.end(); ++it) {
            std::string sid;
            try {
                sid = it->first.as<std::string>();
            } catch (...) {
                v.add(std::string("runtime.risk.") + field + " 键必须是 strategy id 字符串");
                continue;
            }
            if (sid.empty() || ids.find(sid) == ids.end()) {
                v.add(std::string("runtime.risk.") + field + "." + sid
                    + " 未在 runtime.strategies 中声明");
            }
        }
    };
    check_map("per_strategy_max_pos_qty");
    check_map("per_strategy_daily_loss_pct");
    check_map("per_strategy_liquidate_pct");
}

void validate_portfolio_router_policy(const YAML::Node& runtime_node, ValidationCtx& v) {
    bool router_enabled = true;
    if (runtime_node["portfolio_router"]) {
        try {
            router_enabled = runtime_node["portfolio_router"].as<bool>();
        } catch (...) {
            v.add("runtime.portfolio_router 必须是 bool");
            return;
        }
    }
    if (router_enabled) return;

    const YAML::Node strategies = runtime_node["strategies"];
    if (strategies && strategies.IsSequence() && strategies.size() > 1) {
        v.add("runtime.portfolio_router: false 不允许用于 multi-strategy live profile");
    }
    const YAML::Node risk_node = runtime_node["risk"];
    auto has_key = [&](const char* k) {
        return risk_node[k] && !risk_node[k].IsNull();
    };

    if (risk_node && risk_node.IsMap() &&
        (has_key("per_strategy_max_pos_qty") ||
         has_key("per_strategy_daily_loss_pct") ||
         has_key("per_strategy_liquidate_pct") ||
         (has_key("per_strategy_auto_liquidate") &&
          risk_node["per_strategy_auto_liquidate"].as<bool>(false)))) {
        v.add("runtime.portfolio_router: false 不允许与 per-strategy risk 配置同时使用");
    }
}

void validate_per_strategy_pos_qty_same_symbol_guard(const YAML::Node& runtime_node,
                                                     ValidationCtx& v) {
    const YAML::Node risk_node = runtime_node["risk"];
    if (!risk_node || !risk_node.IsMap()) return;
    const YAML::Node quota = risk_node["per_strategy_max_pos_qty"];
    if (!quota || quota.IsNull()) return;
    bool overlap = false;
    const YAML::Node allow = risk_node["allow_strategy_symbol_overlap"];
    if (allow && !allow.IsNull()) {
        try { overlap = allow.as<bool>(); } catch (...) { overlap = false; }
    }
    if (overlap) {
        v.add("runtime.risk.per_strategy_max_pos_qty 不能与 allow_strategy_symbol_overlap=true "
              "同用：当前不支持同账户多策略同 symbol，risk 只读账户级 symbol 净仓。"
              "移除 per_strategy_max_pos_qty，或用不同 symbol 拆分策略。");
    }
}

}

bool validate_strategy_or_strategies_cfg(const YAML::Node& runtime_node,
                                         std::string& err) {
    const YAML::Node legacy_strategy = runtime_node["strategy"];
    if (legacy_strategy && !legacy_strategy.IsNull()) {
        err = "runtime.strategy 已退役，请改用 runtime.strategies: [{id, so, ...}]";
        return false;
    }
    const YAML::Node strategies = runtime_node["strategies"];
    if (!strategies || strategies.IsNull()) {
        err = "runtime.strategies 必填";
        return false;
    }
    if (!strategies.IsSequence()) {
        err = "runtime.strategies 必须是 list（each entry: {id, so, ...}）";
        return false;
    }
    if (strategies.size() == 0) {
        err = "runtime.strategies 不能为空";
        return false;
    }
    if (!validate_strategies_entries(strategies, err)) return false;
    if (!validate_strategies_b1_symbol_guard(runtime_node, strategies, err)) return false;
    err.clear();
    return true;
}

bool live_selector_section_present(const YAML::Node& root) {
    const YAML::Node sel = root["selector"];
    if (!sel || sel.IsNull()) return false;

    if (sel.IsMap() || sel.IsSequence()) return sel.size() > 0;
    return true;
}

void validate_live_config(const YAML::Node& runtime_node,
                          const YAML::Node& queue_node,
                          const YAML::Node& commodities_node,
                          const YAML::Node& fees_node,
                          const XtLoggerCfg* log_cfg,
                          const YAML::Node& stra_node,
                          const XtStrategyFactoryCfg* stra_cfg,
                          const YAML::Node& md_node,
                          const IMdGateway::Config* md_cfg,
                          const YAML::Node& td_node,
                          const ITradeGateway::Config* td_cfg,
                          std::vector<std::string>& errors) {
    (void)stra_node;
    (void)stra_cfg;
    ValidationCtx v(errors);
    v.add_if(!runtime_node || runtime_node.IsNull(), "runtime 配置缺失");
    if (!runtime_node || runtime_node.IsNull()) return;

    validate_log_cfg(log_cfg, v);
    validate_md_cfg(md_node, md_cfg, v);
    validate_td_cfg(td_node, td_cfg, v);
    validate_instrument_node(runtime_node, v);
    validate_session_node(runtime_node, v);
    validate_trading_node(runtime_node, v);
    validate_account_node(runtime_node, v);
    validate_risk_node(runtime_node, v);
    validate_portfolio_router_policy(runtime_node, v);
    validate_per_strategy_pos_qty_same_symbol_guard(runtime_node, v);
    validate_closeout_node(runtime_node, v);
    validate_recording_node(runtime_node, v);
    validate_risk_monitor_node(runtime_node, v);
    validate_runtime_cross(runtime_node, md_node, queue_node, commodities_node, fees_node, v);
}
