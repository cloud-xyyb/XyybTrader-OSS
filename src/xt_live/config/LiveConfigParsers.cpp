// Live config leaf parsers；public contracts 见 LiveConfigParsers.h。

#include "LiveConfigParsers.h"

#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <string>

#include "xt_config/yaml/ConfigYamlUtil.h"
#include "xt_util/time/CtHHMM.h"
#include "xt_util/string/secure_string.h"
#include "xt_util/string/secure_string_yaml.h"
#include "xt_core/session/XtSessionWindow.h"

namespace {

namespace YamlNodeUtil = ::XtConfigYamlUtil;
using xt_hhmm::valid_hhmm;

void to_lower_ascii(std::string& s) {
    std::transform(s.begin(),
                   s.end(),
                   s.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
}

std::string read_secret_via_secure_string(const YAML::Node& node) {
    if (!node) return std::string{};
    XtUtil::secure_string s = XtUtil::secure_string_from_yaml(node);
    return s.unsafe_to_string();
}

YAML::Node provider_secret_node(const YAML::Node& node) {
    if (!node || !node.IsMap()) return YAML::Node();
    const std::string provider =
        node["provider"] ? node["provider"].as<std::string>() : std::string();

    if (provider.empty()) return YAML::Node();
    if (provider == "binance_u") return node["binance"];
    return node[provider];
}

template <typename ConfigT>
void apply_provider_credential_overrides(const YAML::Node& node, ConfigT& cfg) {
    const YAML::Node provider_node = provider_secret_node(node);
    if (!provider_node || !provider_node.IsMap()) return;
    if (provider_node["user"]) cfg.user = provider_node["user"].as<std::string>();
    if (provider_node["password"]) {
        cfg.password = read_secret_via_secure_string(provider_node["password"]);
    }
}

}

IMdGateway::Config parse_md_gateway_config(const YAML::Node& node) {
    IMdGateway::Config cfg;
    cfg.front = node["md_front"] ? node["md_front"].as<std::string>() : "";
    cfg.user = node["user"] ? node["user"].as<std::string>() : "";
    cfg.password = read_secret_via_secure_string(node["password"]);
    apply_provider_credential_overrides(node, cfg);
    if (node["stale_reconnect_ms"]) cfg.stale_reconnect_ms = node["stale_reconnect_ms"].as<uint32_t>();
    return cfg;
}

ITradeGateway::Config parse_trade_gateway_config(const YAML::Node& node) {
    ITradeGateway::Config cfg;
    cfg.front = node["td_front"] ? node["td_front"].as<std::string>() : "";
    cfg.user = node["user"] ? node["user"].as<std::string>() : "";
    cfg.password = read_secret_via_secure_string(node["password"]);
    apply_provider_credential_overrides(node, cfg);
    if (node["reconnect"]) cfg.reconnect_enabled = node["reconnect"].as<bool>();
    if (node["reconnect_interval_ms"]) cfg.reconnect_interval_ms = node["reconnect_interval_ms"].as<int>();
    if (node["account_refresh_ms"]) cfg.account_refresh_ms = node["account_refresh_ms"].as<int>();
    if (node["position_refresh_ms"]) cfg.position_refresh_ms = node["position_refresh_ms"].as<int>();
    if (node["account_snapshot_max_abs"]) cfg.account_snapshot_max_abs = node["account_snapshot_max_abs"].as<double>();

    if (node["stale_reconnect_ms"]) {
        const int v = node["stale_reconnect_ms"].as<int>();
        cfg.stale_reconnect_ms = v > 0 ? static_cast<uint32_t>(v) : 0u;
    }

    if (node["expected_investor_id"]) {
        cfg.expected_investor_id = node["expected_investor_id"].as<std::string>();
    }
    if (node["expected_api_key_sha8"]) {
        cfg.expected_api_key_sha8 = node["expected_api_key_sha8"].as<std::string>();
    }
    return cfg;
}

xt_live::control::XtHftReconcileLoopCfg parse_reconcile_cfg(const YAML::Node& td_node) {
    xt_live::control::XtHftReconcileLoopCfg cfg;
    if (!td_node || !td_node["reconcile"]) return cfg;
    const YAML::Node r = td_node["reconcile"];
    if (r["enabled"]) cfg.enabled = r["enabled"].as<bool>();
    if (r["cycle_ms"]) {
        cfg.cycle_ms = static_cast<uint32_t>(std::max<int64_t>(0, r["cycle_ms"].as<int64_t>()));
    }

    if (r["reconnect_after_n"]) {
        cfg.reconnect_after_n =
            static_cast<uint8_t>(std::clamp(r["reconnect_after_n"].as<int>(), 0, 255));
    }
    if (r["kill_after_n"]) {
        cfg.kill_after_n =
            static_cast<uint8_t>(std::clamp(r["kill_after_n"].as<int>(), 0, 255));
    }
    if (r["resync_throttle_ms"]) {
        cfg.resync_throttle_ms =
            static_cast<uint32_t>(std::max<int64_t>(0, r["resync_throttle_ms"].as<int64_t>()));
    }
    if (r["position_epsilon_fp"]) cfg.position_epsilon_fp = r["position_epsilon_fp"].as<int64_t>();
    if (r["order_min_age_ms"]) {
        cfg.order_min_age_ms =
            static_cast<uint32_t>(std::max<int64_t>(0, r["order_min_age_ms"].as<int64_t>()));
    }
    return cfg;
}

CTPMdCfg parse_ctp_md_cfg(const YAML::Node& node) {
    CTPMdCfg cfg;
    cfg.broker = node["broker"] ? node["broker"].as<std::string>() : "";
    cfg.flow_dir = node["flow_dir"] ? node["flow_dir"].as<std::string>() : cfg.flow_dir;
    return cfg;
}

CTPTdCfg parse_ctp_td_cfg(const YAML::Node& node) {
    CTPTdCfg cfg;
    cfg.broker = node["broker"] ? node["broker"].as<std::string>() : "";
    cfg.flow_dir = node["flow_dir"] ? node["flow_dir"].as<std::string>() : cfg.flow_dir;
    if (node["confirm_settlement"]) cfg.confirm_settlement = node["confirm_settlement"].as<bool>();
    if (node["settlement_confirm_timeout_ms"]) cfg.settlement_confirm_timeout_ms = node["settlement_confirm_timeout_ms"].as<int>();
    if (node["recovery_query_timeout_ms"]) cfg.recovery_query_timeout_ms = node["recovery_query_timeout_ms"].as<int>();
    if (node["recovery_query_max_timeouts"]) cfg.recovery_query_max_timeouts = node["recovery_query_max_timeouts"].as<int>();
    if (node["recovery_rearm_backoff_ms"]) cfg.recovery_rearm_backoff_ms = node["recovery_rearm_backoff_ms"].as<int>();
    if (node["recovery_rearm_alert_rounds"]) cfg.recovery_rearm_alert_rounds = node["recovery_rearm_alert_rounds"].as<int>();
    return cfg;
}

static BinanceMdCfg::TickSource parse_binance_tick_source(const YAML::Node& b) {
    if (!b || !b["tick_source"]) return BinanceMdCfg::TickSource::TradeAndQuote;
    const std::string raw = b["tick_source"].as<std::string>();
    if (raw == "trade+quote") return BinanceMdCfg::TickSource::TradeAndQuote;
    if (raw == "trade") return BinanceMdCfg::TickSource::TradeOnly;
    throw std::invalid_argument(
        "md.binance.tick_source: expected 'trade' or 'trade+quote', got: '" + raw + "'");
}

BinanceMdCfg parse_binance_md_cfg(const YAML::Node& node) {
    BinanceMdCfg cfg;
    if (node["binance"]) {
        const auto& b = node["binance"];
        if (b["base_ws_url"]) cfg.base_ws_url = b["base_ws_url"].as<std::string>();
        if (b["base_rest_url"]) cfg.base_rest_url = b["base_rest_url"].as<std::string>();
        if (b["testnet"]) cfg.testnet = b["testnet"].as<bool>();
        cfg.tick_source = parse_binance_tick_source(b);
    }
    return cfg;
}

namespace {

BinanceTdCfg::Transport parse_binance_td_transport(const YAML::Node& node) {
    if (!node || !node["transport"]) return BinanceTdCfg::Transport::kRest;
    const std::string raw = node["transport"].as<std::string>();
    if (raw == "rest") return BinanceTdCfg::Transport::kRest;
    if (raw == "ws") return BinanceTdCfg::Transport::kWs;
    throw std::invalid_argument("expected 'rest' or 'ws', got: '" + raw + "'");
}

}

BinanceTdCfg parse_binance_td_cfg(const YAML::Node& node) {
    BinanceTdCfg cfg;
    if (node["binance"]) {
        const auto& b = node["binance"];
        cfg.transport = parse_binance_td_transport(b);
        if (b["base_rest_url"]) cfg.base_rest_url = b["base_rest_url"].as<std::string>();
        if (b["base_ws_url"]) cfg.base_ws_url = b["base_ws_url"].as<std::string>();
        if (b["base_ws_trade_url"]) cfg.base_ws_trade_url = b["base_ws_trade_url"].as<std::string>();
        if (b["api_key"]) cfg.api_key = b["api_key"].as<std::string>();
        if (b["api_secret"]) cfg.api_secret = b["api_secret"].as<std::string>();
        if (b["listen_key_refresh_ms"]) cfg.listen_key_refresh_ms = b["listen_key_refresh_ms"].as<uint32_t>();
        if (b["testnet"]) cfg.testnet = b["testnet"].as<bool>();
        if (b["recv_window_ms"]) cfg.recv_window_ms = b["recv_window_ms"].as<int>();
        if (b["time_sync_interval_ms"]) cfg.time_sync_interval_ms = b["time_sync_interval_ms"].as<uint32_t>();
        if (b["ws_submit_timeout_ms"]) cfg.ws_submit_timeout_ms = b["ws_submit_timeout_ms"].as<uint32_t>();
        if (b["ws_stale_drop_ms"]) cfg.ws_stale_drop_ms = b["ws_stale_drop_ms"].as<uint32_t>();
        if (b["ws_send_queue_cap"]) cfg.ws_send_queue_cap = b["ws_send_queue_cap"].as<uint32_t>();
        if (b["ws_reconnect_min_ms"]) cfg.ws_reconnect_min_ms = b["ws_reconnect_min_ms"].as<uint32_t>();
        if (b["ws_reconnect_max_ms"]) cfg.ws_reconnect_max_ms = b["ws_reconnect_max_ms"].as<uint32_t>();
        if (b["uds_first_effective_ack"]) cfg.uds_first_effective_ack = b["uds_first_effective_ack"].as<bool>();
        if (b["close_reject_plain_fallback"]) cfg.close_reject_plain_fallback = b["close_reject_plain_fallback"].as<bool>();
    }
    return cfg;
}

namespace {

XTPMdCfg::Protocol parse_xtp_md_protocol(const YAML::Node& node) {
    if (!node || !node["protocol"]) return XTPMdCfg::Protocol::kTcp;
    const std::string raw = node["protocol"].as<std::string>();
    if (raw == "tcp") return XTPMdCfg::Protocol::kTcp;
    if (raw == "udp") return XTPMdCfg::Protocol::kUdp;
    throw std::invalid_argument("xtp.protocol: expected 'tcp' or 'udp', got: '" + raw + "'");
}

}

XTPMdCfg parse_xtp_md_cfg(const YAML::Node& node) {
    XTPMdCfg cfg;
    if (node["xtp"]) {
        const auto& x = node["xtp"];
        cfg.protocol = parse_xtp_md_protocol(x);
        if (x["client_id"]) cfg.client_id = x["client_id"].as<uint8_t>();
        if (x["software_key"]) cfg.software_key = x["software_key"].as<std::string>();
        if (x["heartbeat_interval_s"]) cfg.heartbeat_interval_s = x["heartbeat_interval_s"].as<uint32_t>();
        if (x["buffer_size_mb"]) cfg.buffer_size_mb = x["buffer_size_mb"].as<uint32_t>();
        if (x["log_dir"]) cfg.log_dir = x["log_dir"].as<std::string>();
        if (x["log_level"]) cfg.log_level = x["log_level"].as<int>();
    }
    return cfg;
}

XTPTdCfg parse_xtp_td_cfg(const YAML::Node& node) {
    XTPTdCfg cfg;
    if (node["xtp"]) {
        const auto& x = node["xtp"];
        if (x["client_id"]) cfg.client_id = x["client_id"].as<uint8_t>();
        if (x["software_key"]) cfg.software_key = x["software_key"].as<std::string>();
        if (x["software_version"]) cfg.software_version = x["software_version"].as<std::string>();
        if (x["account_key"]) cfg.account_key = x["account_key"].as<std::string>();
        if (x["heartbeat_interval_s"]) cfg.heartbeat_interval_s = x["heartbeat_interval_s"].as<uint32_t>();
        if (x["buffer_size_mb"]) cfg.buffer_size_mb = x["buffer_size_mb"].as<uint32_t>();
        if (x["log_dir"]) cfg.log_dir = x["log_dir"].as<std::string>();
        if (x["log_level"]) cfg.log_level = x["log_level"].as<int>();
        if (x["confirm_init_query"]) cfg.confirm_init_query = x["confirm_init_query"].as<bool>();
        if (x["init_query_timeout_ms"]) cfg.init_query_timeout_ms = x["init_query_timeout_ms"].as<int>();
        if (x["recovery_query_timeout_ms"]) cfg.recovery_query_timeout_ms = x["recovery_query_timeout_ms"].as<int>();
        if (x["recovery_query_max_timeouts"]) cfg.recovery_query_max_timeouts = x["recovery_query_max_timeouts"].as<int>();
    }
    return cfg;
}

namespace {

std::unordered_map<std::string, XtCommodityInfo> parse_commodity_map(const YAML::Node& node) {

    return parse_commodity_catalog_config(node).map;
}

}

std::unordered_map<std::string, xt_runtime::XtStraContractInfo>
parse_stra_contract_info(const YAML::Node& node) {
    std::unordered_map<std::string, xt_runtime::XtStraContractInfo> out;
    if (!node || !node.IsMap()) return out;
    for (auto it = node.begin(); it != node.end(); ++it) {
        std::string key = it->first.as<std::string>();
        YAML::Node v = it->second;
        if (!v || !v.IsMap()) continue;
        xt_runtime::XtStraContractInfo info;
        if (v["price_tick"]) info.price_tick = v["price_tick"].as<double>();
        if (v["vol_scale"]) info.vol_scale = v["vol_scale"].as<double>();
        if (v["allow_short"]) info.allow_short = v["allow_short"].as<bool>();
        out.emplace(std::move(key), info);
    }
    return out;
}

XtLiveRuntimeStateCfg parse_live_runtime_state_config(const YAML::Node& runtime_node,
                                                      const YAML::Node& commodities_node) {
    XtLiveRuntimeStateCfg cfg;
    const YAML::Node account_node = runtime_node["account"];
    cfg.initial_cash = YamlNodeUtil::as_or<double>(account_node, "cash", 0.0);
    cfg.t1_enabled = YamlNodeUtil::as_or<bool>(account_node, "t1_enabled", false);
    const std::string account_mode = YamlNodeUtil::as_or<std::string>(account_node, "mode", std::string("cash"));
    cfg.futures_mode = (account_mode == "futures");
    cfg.margin_rate = YamlNodeUtil::as_or<double>(account_node, "margin_rate", 1.0);

    const YAML::Node risk_node = runtime_node["risk"];
    cfg.enable_position_snapshot_stale_guard =
        YamlNodeUtil::as_or<bool>(risk_node, "enable_position_snapshot_stale_guard", true);
    cfg.position_snapshot_stale_ms = std::max(
        1,
        YamlNodeUtil::as_or<int>(risk_node, "position_snapshot_stale_ms", 120000));

    if (account_node && account_node["margin_by_symbol"]) {
        const YAML::Node margin_by_symbol = account_node["margin_by_symbol"];
        if (margin_by_symbol && margin_by_symbol.IsMap()) {
            for (auto it = margin_by_symbol.begin(); it != margin_by_symbol.end(); ++it) {
                try {
                    cfg.margin_by_symbol[it->first.as<std::string>()] = it->second.as<double>();
                } catch (...) {
                }
            }
        }
    }

    const YAML::Node trading_node = runtime_node["trading"];
    if (trading_node && trading_node["offset_policy_default"]) {
        const std::string v = trading_node["offset_policy_default"].as<std::string>();
        if (!parse_offset_policy(v, cfg.offset_policy_default))

            throw std::runtime_error(
                "runtime.trading.offset_policy_default 非法值 '" + v +
                "'（合法: close_first | open_allowed）");
    }

    cfg.conditional_orders_enabled =
        YamlNodeUtil::as_or<bool>(trading_node, "conditional_orders_enabled", false);

    cfg.commodity_map = parse_commodity_map(commodities_node);
    return cfg;
}

namespace {

void parse_closeout_windows(const YAML::Node& closeout_node,
                            std::vector<XtSessionWindow>& out) {
    out.clear();
    const YAML::Node windows_node = closeout_node["windows"];
    if (!windows_node || !windows_node.IsSequence()) return;
    for (const auto& item : windows_node) {
        if (!item || !item.IsMap()) continue;
        const int no_open = YamlNodeUtil::as_or<int>(item, "no_open_hhmm", 1455);
        const int force = YamlNodeUtil::as_or<int>(item, "force_flat_hhmm", 1459);
        if (!valid_hhmm(no_open) || !valid_hhmm(force)) continue;
        out.push_back(XtSessionWindow{no_open, force});
    }
}

}

void parse_session_sections(const YAML::Node& sections_list_node,
                            std::vector<std::pair<int, int>>& out) {
    out.clear();
    const YAML::Node sections_node = sections_list_node;
    if (!sections_node || !sections_node.IsSequence()) return;
    for (const auto& item : sections_node) {
        if (!item || !item.IsMap()) continue;
        const int start = YamlNodeUtil::as_or<int>(item, "start", -1);
        const int end = YamlNodeUtil::as_or<int>(item, "end", -1);
        if (!valid_hhmm(start) || !valid_hhmm(end)) continue;
        out.emplace_back(start, end);
    }
}

XtLiveOrderGatewayCfg parse_live_order_gateway_config(const YAML::Node& runtime_node) {
    XtLiveOrderGatewayCfg cfg;
    const YAML::Node raw_risk_node =
        (runtime_node && runtime_node["risk"]) ? runtime_node["risk"] : YAML::Node();
    const YAML::Node risk_node =
        (raw_risk_node && raw_risk_node.IsMap()) ? raw_risk_node : YAML::Node();

    cfg.risk.max_order_qty      = YamlNodeUtil::as_or<double>(risk_node, "max_order_qty", 0.0);
    cfg.risk.max_order_notional = YamlNodeUtil::as_or<double>(risk_node, "max_order_notional", 0.0);
    cfg.risk.min_order_notional = YamlNodeUtil::as_or<double>(risk_node, "min_order_notional", 0.0);
    cfg.risk.max_pos_qty        = YamlNodeUtil::as_or<double>(risk_node, "max_pos_qty", 0.0);
    cfg.risk.allow_short        = YamlNodeUtil::as_or<bool>(runtime_node["trading"], "allow_short", false);
    cfg.risk.enforce_long_t_plus_1 = YamlNodeUtil::as_or<bool>(risk_node, "enforce_long_t_plus_1", false);
    cfg.risk.max_daily_loss     = YamlNodeUtil::as_or<double>(risk_node, "max_daily_loss", 0.0);
    cfg.risk.max_order_count_per_symbol = YamlNodeUtil::as_or<uint64_t>(
        risk_node, "max_order_count_per_symbol", 0ULL);
    cfg.risk.max_order_rate_per_symbol = YamlNodeUtil::as_or<uint64_t>(
        risk_node, "max_order_rate_per_symbol", 0ULL);
    cfg.risk.order_rate_window_ms = YamlNodeUtil::as_or<uint32_t>(
        risk_node, "order_rate_window_ms", 10000U);
    cfg.risk.blacklist_persist_across_session = YamlNodeUtil::as_or<bool>(
        risk_node, "blacklist_persist_across_session", false);

    const YAML::Node quota_node =
        (risk_node && risk_node["per_strategy_max_pos_qty"])
            ? risk_node["per_strategy_max_pos_qty"]
            : YAML::Node();
    if (quota_node && quota_node.IsMap()) {
        for (auto it = quota_node.begin(); it != quota_node.end(); ++it) {
            std::string sid_str;
            try {
                sid_str = it->first.as<std::string>();
            } catch (...) { continue; }
            double qty = 0.0;
            try {
                qty = it->second.as<double>();
            } catch (...) { continue; }
            if (sid_str.empty()) continue;
            cfg.risk.per_strategy_max_pos_qty_by_sid_string[sid_str] = qty;
        }
    }

    const YAML::Node dll_node =
        (risk_node && risk_node["per_strategy_daily_loss_pct"])
            ? risk_node["per_strategy_daily_loss_pct"]
            : YAML::Node();
    if (dll_node && dll_node.IsMap()) {
        for (auto it = dll_node.begin(); it != dll_node.end(); ++it) {
            std::string sid_str;
            try { sid_str = it->first.as<std::string>(); } catch (...) { continue; }
            double pct = 0.0;
            try { pct = it->second.as<double>(); } catch (...) { continue; }
            if (sid_str.empty() || pct <= 0.0) continue;
            cfg.risk.per_strategy_daily_loss_pct_by_sid_string[sid_str] = pct;
        }
    }

    cfg.risk.per_strategy_auto_liquidate =
        YamlNodeUtil::as_or<bool>(risk_node, "per_strategy_auto_liquidate", false);
    cfg.risk.per_strategy_auto_liquidate_dry_run =
        YamlNodeUtil::as_or<bool>(risk_node, "per_strategy_auto_liquidate_dry_run", false);
    const YAML::Node liq_node =
        (risk_node && risk_node["per_strategy_liquidate_pct"])
            ? risk_node["per_strategy_liquidate_pct"]
            : YAML::Node();
    if (liq_node && liq_node.IsMap()) {
        for (auto it = liq_node.begin(); it != liq_node.end(); ++it) {
            std::string sid_str;
            try { sid_str = it->first.as<std::string>(); } catch (...) { continue; }
            double pct = 0.0;
            try { pct = it->second.as<double>(); } catch (...) { continue; }
            if (sid_str.empty() || pct <= 0.0) continue;
            cfg.risk.per_strategy_liquidate_pct_by_sid_string[sid_str] = pct;
        }
    }

    cfg.risk.allow_strategy_symbol_overlap =
        YamlNodeUtil::as_or<bool>(risk_node, "allow_strategy_symbol_overlap", false);

    cfg.max_orders_per_sec = YamlNodeUtil::as_or<int>(risk_node, "max_orders_per_sec", 0);
    cfg.max_cancels_per_sec = YamlNodeUtil::as_or<int>(risk_node, "max_cancels_per_sec", 0);
    cfg.max_rejects_per_min = YamlNodeUtil::as_or<int>(risk_node, "max_rejects_per_min", 0);
    cfg.cooldown_ms = YamlNodeUtil::as_or<int>(risk_node, "cooldown_ms", 0);
    cfg.require_l2_orderflow = YamlNodeUtil::as_or<bool>(risk_node, "require_l2_orderflow", false);
    cfg.stp_enabled = YamlNodeUtil::as_or<bool>(risk_node, "stp_enabled", true);
    cfg.stp_disabled_reason = YamlNodeUtil::as_or<std::string>(risk_node, "stp_disabled_reason", std::string(""));

    cfg.velocity_enabled = YamlNodeUtil::as_or<bool>(risk_node, "velocity_enabled", false);
    cfg.velocity_window_ms = YamlNodeUtil::as_or<int64_t>(risk_node, "velocity_window_ms", 100);
    cfg.velocity_max_qty = YamlNodeUtil::as_or<double>(risk_node, "velocity_max_qty", 0.0);

    const YAML::Node raw_closeout_node =
        (runtime_node && runtime_node["closeout"]) ? runtime_node["closeout"] : YAML::Node();
    const YAML::Node closeout_node =
        (raw_closeout_node && raw_closeout_node.IsMap()) ? raw_closeout_node : YAML::Node();
    cfg.closeout_enabled = YamlNodeUtil::as_or<bool>(closeout_node, "enabled", false);
    parse_closeout_windows(closeout_node, cfg.closeout_windows);
    if (cfg.closeout_windows.empty()) {
        const int no_open = YamlNodeUtil::as_or<int>(closeout_node, "no_open_hhmm", 1455);
        const int force = YamlNodeUtil::as_or<int>(closeout_node, "force_flat_hhmm", 1459);
        cfg.closeout_windows.push_back(XtSessionWindow{no_open, force});
    }
    cfg.closeout_retry_ms = std::max(0, YamlNodeUtil::as_or<int>(closeout_node, "retry_ms", 1000));
    cfg.closeout_escalate_after_ms = std::max(0, YamlNodeUtil::as_or<int>(closeout_node, "escalate_after_ms", 3000));
    cfg.closeout_aggressive_buffer_bps = std::max(1, YamlNodeUtil::as_or<int>(closeout_node, "aggressive_buffer_bps", 10));

    const YAML::Node recovery_node =
        (runtime_node && runtime_node["recovery"]) ? runtime_node["recovery"] : YAML::Node();
    cfg.orphan_reap_enabled = YamlNodeUtil::as_or<bool>(recovery_node, "orphan_reap_enabled", true);
    cfg.orphan_reap_grace_ms = std::max<int64_t>(
        0, YamlNodeUtil::as_or<int64_t>(recovery_node, "orphan_reap_grace_ms", 60'000));

    cfg.orphan_reap_interval_ms = std::max<int64_t>(
        1'000, YamlNodeUtil::as_or<int64_t>(recovery_node, "orphan_reap_interval_ms", 30'000));

    cfg.orphan_reap_max_per_sweep = std::min(
        256, std::max(1, YamlNodeUtil::as_or<int>(recovery_node, "orphan_reap_max_per_sweep", 50)));
    cfg.lost_terminal_reconcile_enabled =
        YamlNodeUtil::as_or<bool>(recovery_node, "lost_terminal_reconcile_enabled", true);
    const YAML::Node gate_node =
        (closeout_node && closeout_node["session_gate"]) ? closeout_node["session_gate"] : YAML::Node();
    cfg.session_gate_enabled = YamlNodeUtil::as_or<bool>(gate_node, "enabled", true);
    const YAML::Node session_node =
        (runtime_node && runtime_node["session"]) ? runtime_node["session"] : YAML::Node();
    const YAML::Node active_sections_node =
        (session_node && session_node["active_sections"]) ? session_node["active_sections"] : YAML::Node();
    parse_session_sections(active_sections_node, cfg.session_gate_sections);

    cfg.reject_on_position_unknown =
        YamlNodeUtil::as_or<bool>(risk_node, "reject_on_position_unknown", false);

    const YAML::Node gateway_node = runtime_node["gateway"];
    if (gateway_node && !gateway_node.IsNull()) {
        cfg.order_strategy_grace_us = YamlNodeUtil::as_or<int64_t>(
            gateway_node, "order_strategy_grace_us", cfg.order_strategy_grace_us);
    }
    return cfg;
}

XtRunnerControlConfig parse_runner_control_config(const YAML::Node& md_node,
                                                  const YAML::Node& debug_node,
                                                  const YAML::Node& strategy_node,
                                                  const YAML::Node& instrument_node,
                                                  const YAML::Node& account_node) {
    using namespace YamlNodeUtil;

    XtRunnerControlConfig cfg;

    cfg.log_stats = as_or<bool>(md_node, "log_stats", false);
    const int interval_ms = as_or<int>(md_node, "log_interval_ms", 1000);
    cfg.sleep_ms  = interval_ms > 0 ? interval_ms : 1000;

    cfg.heartbeat_tick_count     = as_or<bool>(debug_node, "tick_count",               false);
    cfg.heartbeat_ms             = std::max(0, as_or<int>(debug_node, "heartbeat_ms",   0));
    cfg.fail_on_critical_td_drop = as_or<bool>(debug_node, "fail_on_critical_td_drop", true);

    {
        const int warn  = std::max(1, as_or<int>(debug_node, "consistency_warn_loops",  1));
        const int block =             as_or<int>(debug_node, "consistency_block_loops", 3);
        const int kill  =             as_or<int>(debug_node, "consistency_kill_loops",  20);
        if (block < warn || kill < block) {
            throw std::runtime_error(
                "consistency thresholds 必须满足 warn ≤ block ≤ kill，"
                "当前: warn=" + std::to_string(warn) +
                " block=" + std::to_string(block) +
                " kill="  + std::to_string(kill));
        }
        cfg.consistency_warn_loops  = warn;
        cfg.consistency_block_loops = block;
        cfg.consistency_kill_loops  = kill;
    }

    {
        const YAML::Node ub_node = debug_node ? debug_node["unmatched_budget"] : YAML::Node();
        cfg.unmatched_budget_window_sec = as_or<int>(ub_node, "window_sec", 60);
        cfg.unmatched_budget_max_drops  = as_or<int>(ub_node, "max_drops", 3);
        cfg.unmatched_budget_max_ratio  = as_or<double>(ub_node, "max_ratio", 0.001);
    }

    cfg.unified_report_flush_ms    = std::max(0, as_or<int>(debug_node, "report_contract_flush_ms", 1000));
    cfg.strategy_metrics_flush_sec = std::max(1, as_or<int>(debug_node, "strategy_metrics_flush_sec", 5));
    cfg.unified_report_dir    = as_or<std::string>(debug_node,    "report_contract_dir",   std::string("var/live_metrics/report_contract"));
    cfg.unified_run_id        = as_or<std::string>(debug_node,    "report_run_id",         std::string(""));
    cfg.unified_strategy_id   = as_or<std::string>(strategy_node, "id",                    std::string(""));
    cfg.unified_symbol        = as_or<std::string>(instrument_node,"symbol",               std::string(""));
    cfg.unified_config_hash   = as_or<std::string>(debug_node,    "report_config_hash",    std::string(""));
    cfg.unified_code_version  = as_or<std::string>(debug_node,    "report_code_version",   std::string(""));

    cfg.account_reconcile_on_stop = as_or<bool>(account_node, "reconcile_on_stop", false);

    return cfg;
}

XtProcessUtil::RuntimeTuningConfig parse_runner_process_tuning_config(const YAML::Node& debug_node,
                                                                      const YAML::Node& md_node) {
    XtProcessUtil::RuntimeTuningConfig cfg;
    cfg.main_affinity = YamlNodeUtil::as_or<int>(debug_node, "main_affinity", -1);
    cfg.main_priority = YamlNodeUtil::as_or<std::string>(debug_node, "main_priority", std::string(""));
    cfg.enable_mlockall = YamlNodeUtil::as_or<bool>(
        debug_node,
        "mlockall",
        YamlNodeUtil::as_or<bool>(md_node, "md_mlock", YamlNodeUtil::as_or<bool>(md_node, "mlock", false)));
    return cfg;
}

XtHftEventLoop::Config parse_event_loop_cfg(const YAML::Node& queue_node,
                                            const YAML::Node& md_node) {
    using namespace YamlNodeUtil;
    using CfgQueueMode = XtHftEventLoopCfg::QueueMode;

    auto to_mode = [](const std::string& s, const char* field) -> CfgQueueMode {
        if (s == "spsc")                return CfgQueueMode::SPSC;
        if (s == "mpsc" || s == "mpmc") return CfgQueueMode::MPSC;
        throw std::runtime_error(
            std::string(field) + ": 无效队列模式 '" + s + "'，合法值: spsc / mpsc / mpmc");
    };

    XtHftEventLoop::Config cfg;

    const size_t tick_cap  = as_positive_size_or(queue_node, "capacity",            65536);
    const size_t event_cap = as_positive_size_or(queue_node, "event_capacity",      tick_cap);
    const size_t td_cap    = as_positive_size_or(queue_node, "td_event_capacity",   event_cap);
    const size_t flow_cap  = as_positive_size_or(queue_node, "flow_event_capacity", event_cap);

    cfg.tick_queue_capacity    = tick_cap;
    cfg.td_event_queue_capacity   = td_cap;
    cfg.flow_event_queue_capacity = flow_cap;

    cfg.tick_queue_mode       = to_mode(as_or<std::string>(queue_node, "mode",            std::string("spsc")), "queue.mode");
    cfg.td_event_queue_mode   = to_mode(as_or<std::string>(queue_node, "td_event_mode",   std::string("mpsc")), "queue.td_event_mode");
    cfg.flow_event_queue_mode = to_mode(as_or<std::string>(queue_node, "flow_event_mode", std::string("mpsc")), "queue.flow_event_mode");

    cfg.tick_high_watermark      = std::min(as_positive_size_or(queue_node, "tick_high_watermark",      tick_cap * XtHftEventLoopCfg::kTickHighWatermarkPctDefault    / 100), tick_cap);
    cfg.td_event_high_watermark  = std::min(as_positive_size_or(queue_node, "td_event_high_watermark",  td_cap   * XtHftEventLoopCfg::kTdEventHighWatermarkPctDefault  / 100), td_cap);
    cfg.flow_event_high_watermark= std::min(as_positive_size_or(queue_node, "flow_event_high_watermark",flow_cap * XtHftEventLoopCfg::kFlowEventHighWatermarkPctDefault / 100), flow_cap);

    cfg.tick_batch_size            = as_positive_size_or(queue_node, "tick_batch_size",         XtHftEventLoopCfg::kTickBatchDefault);
    cfg.td_event_batch_size        = as_positive_size_or(queue_node, "td_event_batch_size",     XtHftEventLoopCfg::kTdEventBatchDefault);
    cfg.flow_event_batch_size      = as_positive_size_or(queue_node, "flow_event_batch_size",   XtHftEventLoopCfg::kFlowEventBatchDefault);
    cfg.td_event_budget_per_loop   = as_positive_size_or(queue_node, "td_event_budget_per_loop",XtHftEventLoopCfg::kTdEventBudgetPerLoopDefault);
    cfg.flow_event_budget_per_loop = as_positive_size_or(queue_node, "flow_event_budget_per_loop", XtHftEventLoopCfg::kFlowEventBudgetPerLoopDefault);

    cfg.tick_max_wait_us = static_cast<uint64_t>(std::max<int64_t>(
        0, as_or<int64_t>(queue_node, "tick_max_wait_us",
                          static_cast<int64_t>(XtHftEventLoopCfg::kTickMaxWaitUsDefault))));
    cfg.tick_starvation_warn_loops = static_cast<uint32_t>(std::max<int64_t>(
        0, as_or<int64_t>(queue_node, "tick_starvation_warn_loops",
                          static_cast<int64_t>(XtHftEventLoopCfg::kTickStarvationWarnLoopsDefault))));
    cfg.td_backlog_warn_threshold = std::min(
        as_positive_size_or(queue_node, "td_backlog_warn_threshold", cfg.td_event_high_watermark),
        td_cap);

    cfg.worker_affinity = as_or<int>(md_node, "worker_affinity",
                          as_or<int>(md_node, "md_worker_affinity", -1));
    cfg.worker_priority = as_or<std::string>(md_node, "worker_priority",
                          as_or<std::string>(md_node, "md_worker_priority", std::string("")));
    cfg.enable_pure_spin = as_or<bool>(md_node, "enable_pure_spin", true);

    return cfg;
}

XtLiveRecorder::Config parse_recording_cfg(const YAML::Node& runtime_node) {
    XtLiveRecorder::Config cfg;
    const YAML::Node node = runtime_node["recording"];
    if (!node || node.IsNull()) return cfg;

    cfg.enabled = YamlNodeUtil::as_or<bool>(node, "enabled", cfg.enabled);
    cfg.file_path = YamlNodeUtil::as_or<std::string>(node, "file_path", std::string());
    const int qc = YamlNodeUtil::as_or<int>(node, "queue_capacity",
                                            static_cast<int>(cfg.queue_capacity));
    if (qc > 0) cfg.queue_capacity = static_cast<size_t>(qc);

    const int64_t rb = YamlNodeUtil::as_or<int64_t>(node, "rotate_bytes",
                                                    static_cast<int64_t>(cfg.rotate_bytes));
    cfg.rotate_bytes = rb > 0 ? static_cast<uint64_t>(rb) : 0;
    const int ri = YamlNodeUtil::as_or<int>(node, "rotate_interval_sec",
                                            static_cast<int>(cfg.rotate_interval_sec));
    cfg.rotate_interval_sec = ri > 0 ? static_cast<uint32_t>(ri) : 0;
    const YAML::Node intent_node = node["intent_trace"];
    if (intent_node && !intent_node.IsNull()) {
        cfg.intent_trace.enabled =
            YamlNodeUtil::as_or<bool>(intent_node, "enabled", cfg.intent_trace.enabled);
        cfg.intent_trace.sample_rate =
            YamlNodeUtil::as_or<double>(intent_node, "sample_rate", cfg.intent_trace.sample_rate);
        cfg.intent_trace.include_wire_debug =
            YamlNodeUtil::as_or<bool>(intent_node, "include_wire_debug",
                                      cfg.intent_trace.include_wire_debug);
    }
    return cfg;
}

XtEventStoreCfg parse_event_store_cfg(const YAML::Node& runtime_node) {
    XtEventStoreCfg cfg;
    const YAML::Node rec = runtime_node["recording"];
    if (!rec || rec.IsNull()) return cfg;
    const YAML::Node node = rec["event_store"];
    if (!node || node.IsNull()) return cfg;
    cfg.enabled = YamlNodeUtil::as_or<bool>(node, "enabled", cfg.enabled);
    cfg.root_path = YamlNodeUtil::as_or<std::string>(node, "root_path", std::string());
    return cfg;
}

xt_risk::XtRiskMonitor::Config parse_risk_monitor_cfg(const YAML::Node& runtime_node) {
    xt_risk::XtRiskMonitor::Config cfg;
    const YAML::Node node = runtime_node["risk_monitor"];
    if (!node || node.IsNull()) return cfg;
    cfg.enabled = YamlNodeUtil::as_or<bool>(node, "enabled", cfg.enabled);
    cfg.idd_scale_threshold_bp = YamlNodeUtil::as_or<int64_t>(
        node, "idd_scale_threshold_bp", cfg.idd_scale_threshold_bp);
    cfg.idd_halt_threshold_bp = YamlNodeUtil::as_or<int64_t>(
        node, "idd_halt_threshold_bp", cfg.idd_halt_threshold_bp);
    cfg.idd_scale_factor_fp = YamlNodeUtil::as_or<int64_t>(
        node, "idd_scale_factor_fp", cfg.idd_scale_factor_fp);
    cfg.stale_quote_ms = YamlNodeUtil::as_or<uint32_t>(
        node, "stale_quote_ms", cfg.stale_quote_ms);
    return cfg;
}
