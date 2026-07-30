#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "xt_core/session/XtSessionFilter.h"

namespace YAML {
class Node;
}

/**
 * @brief 回测行情数据源配置（market data source config）。
 *
 * 描述回测数据加载方式、数据文件路径、日期范围、SimTick 生成参数及订单流文件列表。
 * 由 `XtMarketDataSource::load()` 消费后进入回放流程，不含运行时游标或打开的文件句柄。
 * `validate()` 只追加错误信息，不修改配置；调用方负责在继续回放前处理 errors。
 */
struct XtMarketDataSourceConfig {

    std::string mode = "bar";
    std::string data_mode = "stream";
    bool stream_override_set = false;
    bool stream_override = false;
    std::string replay_mode = "tick_only";

    int bar_interval_sec = 300;
    bool simtick_enable = false;
    std::string simtick_dir = "data/simtick";
    std::string simtick_format = "bin";
    double simtick_spread = 0.2;

    bool has_bar_code = false;
    bool has_bar_section = false;
    bool has_bar_path = false;
    std::string bar_code = "BAR";
    bool use_hot = false;
    std::string hot_path;

    std::string tick_root;
    std::vector<std::string> tick_files;
    std::vector<std::string> tick_symbols;
    std::string tick_main_symbol;
    bool tick_auto_main = false;
    std::string tick_cache_dir = "data/ticks/cache";
    bool tick_use_bin_cache = true;

    std::string tick_cache_format = "v3";
    std::string tick_volume_mode = "cumulative";
    double qty_base = 100.0;

    std::string tick_fbs_root;

    std::string tick_events_parquet_root;

    std::vector<std::string> order_queue_files;
    std::vector<std::string> order_detail_files;
    std::vector<std::string> transaction_files;

    std::string bar_root = "data/bars/csv";
    std::string bar_symbol;
    std::string bar_suffix = ".csv";
    std::string bar_path;

    uint32_t start_date = 0;
    uint32_t end_date = 0;

    bool skip_last_tick_per_day = false;

    xt_core::Config session;

    void validate(std::vector<std::string>& errors) const;
};

XtMarketDataSourceConfig parse_market_data_source_config(
    const YAML::Node& node,
    const XtMarketDataSourceConfig& defaults);
