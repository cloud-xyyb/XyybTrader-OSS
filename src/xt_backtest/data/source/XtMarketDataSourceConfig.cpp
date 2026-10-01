#include "xt_backtest/data/source/XtMarketDataSourceConfig.h"

#include "xt_config/yaml/ConfigYamlUtil.h"

#include <yaml-cpp/yaml.h>
#include <initializer_list>
#include <unordered_set>

namespace {
bool str_in(const std::string& v, std::initializer_list<const char*> candidates) {
    for (const char* c : candidates) {
        if (v == c) return true;
    }
    return false;
}

// 兼容旧配置的 scalar 写法与新配置的 sequence 写法；非 scalar 元素按缺失处理。
std::vector<std::string> parse_string_list(const YAML::Node& parent, const char* key) {
    std::vector<std::string> out;
    if (!parent || !parent[key]) return out;
    const YAML::Node node = parent[key];
    if (node.IsSequence()) {
        for (const auto& it : node) {
            if (!it.IsScalar()) continue;
            try {
                out.push_back(it.as<std::string>());
            } catch (...) {
            }
        }
        return out;
    }
    if (node.IsScalar()) {
        try {
            out.push_back(node.as<std::string>());
        } catch (...) {
        }
    }
    return out;
}

void append_string_list(std::vector<std::string>& out, const std::vector<std::string>& in) {
    out.insert(out.end(), in.begin(), in.end());
}

void dedupe_keep_order(std::vector<std::string>& items) {
    std::unordered_set<std::string> seen;
    std::vector<std::string> deduped;
    deduped.reserve(items.size());
    for (const auto& it : items) {
        if (it.empty()) continue;
        if (seen.insert(it).second) deduped.push_back(it);
    }
    items.swap(deduped);
}

} // namespace

void XtMarketDataSourceConfig::validate(std::vector<std::string>& errors) const {
    if (!str_in(mode, {"tick", "bar"})) {
        errors.push_back("backtest.datasource.mode 仅支持 tick/bar");
    }
    if (!str_in(data_mode, {"stream", "memory"})) {
        errors.push_back("backtest.datasource.data_mode 仅支持 stream/memory");
    }
    if (!str_in(replay_mode, {"tick_only", "event_stream"})) {
        errors.push_back("backtest.datasource.replay_mode 仅支持 tick_only/event_stream");
    }
    if (replay_mode == "event_stream" && mode != "tick") {
        errors.push_back("backtest.datasource.replay_mode=event_stream 要求 backtest.datasource.mode=tick");
    }
    if (!str_in(tick_cache_format, {"v2", "v3"})) {
        errors.push_back("backtest.tick_cache.format 仅支持 v2/v3");
    }
    if (simtick_spread < 0.0) {
        errors.push_back("backtest.datasource.bar.simtick.spread 不能为负");
    }
    if (qty_base <= 0.0) {
        errors.push_back("backtest.datasource.tick.qty_base / backtest.datasource.bar.simtick.qty_base 必须 > 0");
    }
    if (bar_interval_sec <= 0) {
        errors.push_back("backtest.datasource.bar.interval_sec 必须 > 0");
    }
    if (!str_in(simtick_format, {"bin", "csv"})) {
        errors.push_back("backtest.datasource.bar.simtick.format 仅支持 bin/csv");
    }
    if (start_date != 0 && end_date != 0 && start_date > end_date) {
        errors.push_back("backtest.start_date 不能晚于 backtest.end_date");
    }

    if (mode == "tick") {
        if (has_bar_path || !bar_path.empty()) {
            errors.push_back("tick 模式不支持 backtest.datasource.bar_path；请使用 backtest.datasource.tick.*");
        }
        if (has_bar_section) {
            errors.push_back("tick 模式不支持 backtest.datasource.bar；bar 数据源只允许 mode=bar");
        }
        const bool has_csv_input = !tick_root.empty() || !tick_files.empty();
        const bool has_fbs_input = !tick_fbs_root.empty();
        const bool has_events_parquet_input = !tick_events_parquet_root.empty();
        const bool has_orderflow_input = !order_queue_files.empty() ||
                                         !order_detail_files.empty() ||
                                         !transaction_files.empty();
        const int input_count = (has_csv_input ? 1 : 0) +
                                (has_fbs_input ? 1 : 0) +
                                (has_events_parquet_input ? 1 : 0);
        if (input_count == 0) {
            errors.push_back("tick 模式要求配置 tick.files / tick.root / tick.fbs_root / tick.events_parquet_root 之一");
        }
        if (input_count > 1) {
            errors.push_back("tick.fbs_root / tick.events_parquet_root 与 tick.files / tick.root 互斥（避免歧义）");
        }
        if ((has_fbs_input || has_events_parquet_input) && (start_date == 0 || end_date == 0)) {
            errors.push_back("tick.fbs_root / tick.events_parquet_root 要求同时配置 backtest.start_date 与 backtest.end_date");
        }
        if (replay_mode == "event_stream" && !has_orderflow_input) {
            errors.push_back("backtest.datasource.replay_mode=event_stream 要求配置 orderflow 文件");
        }
        // event_stream 读端只消费 memory 物化的 tick 缓存，而 fbs / events-parquet
        // 只支持流式读取 → 该组合到不了 load_memory_source 的任何物化分支，只会撞进
        // 防御性兜底并以「no data source configured for memory load」收场。在配置层
        // 直接拒绝，替代那条指向不明的运行期报错。
        if (replay_mode == "event_stream" && (has_fbs_input || has_events_parquet_input)) {
            errors.push_back("backtest.datasource.replay_mode=event_stream 不支持 "
                             "tick.fbs_root / tick.events_parquet_root（这两个源仅支持 stream，"
                             "无法为 event_stream 物化 tick）；请改用 tick.files / tick.root");
        }
        return;
    }
    if (mode == "bar") {
        const bool has_direct_bar_input = !bar_path.empty() || !hot_path.empty();
        const bool has_symbol_range_input = !bar_symbol.empty() &&
                                            !bar_root.empty() &&
                                            start_date != 0 &&
                                            end_date != 0;
        if (!(has_direct_bar_input || has_symbol_range_input)) {
            errors.push_back("bar 模式要求：bar_path / bar.hot_path / (bar.root + bar.symbol + backtest.start_date/end_date)");
        }
    }
}

XtMarketDataSourceConfig parse_market_data_source_config(
    const YAML::Node& node,
    const XtMarketDataSourceConfig& defaults) {
    XtMarketDataSourceConfig c = defaults;

    YAML::Node cfg = node["datasource"] ? node["datasource"] : node;
    YAML::Node bar = cfg["bar"];
    YAML::Node tick = cfg["tick"];

    c.mode = XtConfigYamlUtil::as_or<std::string>(cfg, "mode", defaults.mode);
    c.data_mode = XtConfigYamlUtil::as_or<std::string>(cfg, "data_mode", defaults.data_mode);
    {
        // legacy `stream` 同时需要值与“是否显式出现”两个状态；as_or 只能返回值。
        c.stream_override = false;
        c.stream_override_set = false;
        const YAML::Node sn = (cfg && !cfg.IsNull()) ? cfg["stream"] : YAML::Node();
        if (sn && !sn.IsNull()) {
            try { c.stream_override = sn.as<bool>(); c.stream_override_set = true; } catch (...) {}
        }
    }
    c.replay_mode = XtConfigYamlUtil::as_or<std::string>(cfg, "replay_mode", defaults.replay_mode);

    c.bar_interval_sec = XtConfigYamlUtil::as_or<int>(bar, "interval_sec", defaults.bar_interval_sec);
    c.simtick_enable = XtConfigYamlUtil::as_or<bool>(bar["simtick"], "enable", defaults.simtick_enable);
    c.simtick_dir = XtConfigYamlUtil::as_or<std::string>(bar["simtick"], "dir", defaults.simtick_dir);
    c.simtick_format = XtConfigYamlUtil::as_or<std::string>(bar["simtick"], "format", defaults.simtick_format);
    c.simtick_spread = XtConfigYamlUtil::as_or<double>(bar["simtick"], "spread", defaults.simtick_spread);

    c.has_bar_code = (bar && bar["code"] && bar["code"].IsDefined() && !bar["code"].IsNull());
    c.has_bar_section = (bar && bar.IsDefined() && !bar.IsNull());
    c.bar_code = c.has_bar_code ? XtConfigYamlUtil::as_or<std::string>(bar, "code", defaults.bar_code) : defaults.bar_code;
    c.use_hot = XtConfigYamlUtil::as_or<bool>(bar, "use_hot", defaults.use_hot);
    c.hot_path = XtConfigYamlUtil::as_or<std::string>(bar, "hot_path", defaults.hot_path);

    c.tick_root = XtConfigYamlUtil::as_or<std::string>(tick, "root", defaults.tick_root);
    c.tick_fbs_root = XtConfigYamlUtil::as_or<std::string>(tick, "fbs_root", defaults.tick_fbs_root);
    c.tick_events_parquet_root = XtConfigYamlUtil::as_or<std::string>(tick, "events_parquet_root", defaults.tick_events_parquet_root);
    c.tick_cache_dir = XtConfigYamlUtil::as_or<std::string>(tick, "cache_dir", defaults.tick_cache_dir);
    c.tick_use_bin_cache = XtConfigYamlUtil::as_or<bool>(tick, "use_bin_cache", defaults.tick_use_bin_cache);
    // cache 格式挂在 backtest 根下（backtest.tick_cache.format），不在 datasource 节内：
    // 它选的是 cache 的落盘布局，与「数据从哪来」正交。传入 datasource 子节点时读不到
    // 该键，沿用 defaults（v2）——与不写该键同义。
    c.tick_cache_format =
        XtConfigYamlUtil::as_or<std::string>(node["tick_cache"], "format", defaults.tick_cache_format);
    c.tick_volume_mode = XtConfigYamlUtil::as_or<std::string>(tick, "volume_mode", defaults.tick_volume_mode);
    c.qty_base = XtConfigYamlUtil::as_or<double>(tick, "qty_base", defaults.qty_base);
    c.tick_main_symbol = XtConfigYamlUtil::as_or<std::string>(tick, "main_symbol", defaults.tick_main_symbol);
    c.tick_auto_main = XtConfigYamlUtil::as_or<bool>(tick, "auto_main", defaults.tick_auto_main);
    if (bar["simtick"] && bar["simtick"]["qty_base"]) {
        c.qty_base = XtConfigYamlUtil::as_or<double>(bar["simtick"], "qty_base", c.qty_base);
    }

    c.tick_files = parse_string_list(tick, "files");
    c.tick_symbols = parse_string_list(tick, "symbols");
    // orderflow 文件支持嵌套与平铺两套 YAML 形态；去重时保留首个出现顺序。
    YAML::Node orderflow = cfg["orderflow"];
    append_string_list(c.order_queue_files, parse_string_list(orderflow, "order_queue_files"));
    append_string_list(c.order_detail_files, parse_string_list(orderflow, "order_detail_files"));
    append_string_list(c.transaction_files, parse_string_list(orderflow, "transaction_files"));
    append_string_list(c.order_queue_files, parse_string_list(orderflow["order_queue"], "files"));
    append_string_list(c.order_detail_files, parse_string_list(orderflow["order_detail"], "files"));
    append_string_list(c.transaction_files, parse_string_list(orderflow["transaction"], "files"));
    append_string_list(c.order_queue_files, parse_string_list(cfg, "order_queue_files"));
    append_string_list(c.order_detail_files, parse_string_list(cfg, "order_detail_files"));
    append_string_list(c.transaction_files, parse_string_list(cfg, "transaction_files"));
    dedupe_keep_order(c.order_queue_files);
    dedupe_keep_order(c.order_detail_files);
    dedupe_keep_order(c.transaction_files);

    c.bar_root = XtConfigYamlUtil::as_or<std::string>(bar, "root", defaults.bar_root);
    c.bar_symbol = XtConfigYamlUtil::as_or<std::string>(bar, "symbol", defaults.bar_symbol);
    c.bar_suffix = XtConfigYamlUtil::as_or<std::string>(bar, "suffix", defaults.bar_suffix);
    c.has_bar_path = (cfg && cfg["bar_path"] && cfg["bar_path"].IsDefined() && !cfg["bar_path"].IsNull());
    c.bar_path = XtConfigYamlUtil::as_or<std::string>(cfg, "bar_path", defaults.bar_path);
    return c;
}
