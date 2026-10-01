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
    // ── 运行模式（runtime mode） ──────────────────────────────────────────────
    std::string mode = "bar"; ///< `bar` 或 `tick`。
    std::string data_mode = "stream"; ///< `stream` 或 `memory`；legacy stream 字段可覆盖。
    bool stream_override_set = false; ///< true 表示 YAML 显式配置了 legacy `stream` 字段。
    bool stream_override = false; ///< legacy `stream` 字段值；由 prepare 阶段映射到 data_mode。
    std::string replay_mode = "tick_only"; ///< `tick_only` 或 `event_stream`。

    // ── Bar -> Tick 生成（generation） ──────────────────────────────────────────────
    int bar_interval_sec = 300; ///< 单根 bar 的秒数，用于生成 simtick 时间点。
    bool simtick_enable = false; ///< true 表示 bar 模式启用 bar->simtick 合成与缓存。
    std::string simtick_dir = "data/simtick"; ///< simtick cache 输出目录。
    std::string simtick_format = "bin"; ///< `bin` 或 `csv`。
    double simtick_spread = 0.2; ///< 生成盘口 bid/ask 的基础 spread。

    // ── 主力合约映射（hot symbol mapping） ──────────────────────────────────────────────
    bool has_bar_code = false; ///< YAML 中是否显式出现 bar.code。
    bool has_bar_section = false; ///< YAML 中是否显式出现 datasource.bar 节。
    bool has_bar_path = false; ///< YAML 中是否显式出现 legacy bar_path。
    std::string bar_code = "BAR"; ///< bar->simtick 生成 tick 时写入 XtRawTick::code。
    bool use_hot = false; ///< true 表示 bar 模式走 hot_path 主力合约映射。
    std::string hot_path; ///< 主力合约 CSV 路径。

    // ── Tick 数据源（tick source） ──────────────────────────────────────────────
    std::string tick_root; ///< tick 文件根目录；prepare 阶段按日期范围扫描。
    std::vector<std::string> tick_files; ///< 显式 tick 文件列表，优先级高于 tick_root。
    std::vector<std::string> tick_symbols; ///< tick symbol 白名单；空表示不过滤。
    std::string tick_main_symbol; ///< 单主力合约过滤；非空时覆盖 tick_symbols。
    bool tick_auto_main = false; ///< true 表示从输入文件首个 symbol 推断 main symbol。
    std::string tick_cache_dir = "data/ticks/cache"; ///< CSV->bin cache 目录。
    bool tick_use_bin_cache = true; ///< true 表示允许构建或复用 tick bin cache。
    /// @brief tick cache 落盘格式：`v3`（RawShardV3 按交易日分片）或 `v2`（DualV2 单文件）。
    ///
    /// 默认 v3（按日分片：窗口共享分片、增量只建缺失日、raw-only 盘占减半）；
    /// v3 不可用（建库失败/缺片/索引损坏）时逐级回落 v2 → CSV 直读，
    /// 交付的 tick 序列与显式 `format: v2` 逐条一致（shadow digest 钉背书）。
    std::string tick_cache_format = "v3";
    std::string tick_volume_mode = "cumulative"; ///< `cumulative` 或 `incremental`。
    double qty_base = 100.0; ///< simple CSV 或 simtick 缺省盘口数量。

    // ── fbs event store tick source ─────────────────────────────
    //
    // 非空时走 XtFbsTickReader (ct::data::LocalFsEventStore)，与 CSV tick source
    // 互斥（validation 期检查）。配合 live fbs sink 实现 live -> fbs -> backtest 同源 replay。
    std::string tick_fbs_root; ///< fbs event-store 根目录；非空时与 CSV/parquet tick source 互斥。

    // ── canonical events parquet tick source ────────────────
    //
    // 非空时走 XtEventParquetReader，从 vendor-agnostic book/trades events parquet
    // replay L5 tick；与 fbs/csv tick source 互斥。
    std::string tick_events_parquet_root; ///< canonical events parquet 根目录；非空时与 CSV/fbs 互斥。

    // ── 订单流数据源（orderflow source） ──────────────────────────────────────────────
    std::vector<std::string> order_queue_files; ///< event_stream 的 order_queue CSV 文件列表。
    std::vector<std::string> order_detail_files; ///< event_stream 的 order_detail CSV 文件列表。
    std::vector<std::string> transaction_files; ///< event_stream 的 transaction CSV 文件列表。

    // ── Bar 数据源（bar source） ──────────────────────────────────────────────
    std::string bar_root = "data/bars/csv"; ///< 按 symbol/month 推导 bar 文件的根目录。
    std::string bar_symbol; ///< bar 文件 symbol，用于推导路径和默认 bar_code。
    std::string bar_suffix = ".csv"; ///< bar 文件后缀；参与月份路径拼接。
    std::string bar_path; ///< legacy 单文件 bar 输入路径。

    // ── 日期范围（date range） ──────────────────────────────────────────────
    uint32_t start_date = 0; ///< YYYYMMDD inclusive；0 表示未配置。
    uint32_t end_date = 0; ///< YYYYMMDD inclusive；0 表示未配置。

    // ── WT 对齐（wt alignment） ──────────────────────────────────────────────
    bool skip_last_tick_per_day = false; ///< 过滤每个 trading_day 的最后 1 条 tick（对齐 WT store_ticks 行为）。

    // ── Session 过滤（session filter） ──────────────────────────────────────
    xt_core::Config session; ///< tick memory/bin load 阶段应用的 session filter 配置。

    // ── 参数校验（validation） ──────────────────────────────────────────────
    void validate(std::vector<std::string>& errors) const;
};

/// @brief 从 `backtest.datasource` YAML 节解析行情数据源配置。
///
/// 支持传入完整 backtest node 或 datasource 子节点；缺失字段沿用 `defaults`。
/// 本函数只做字段提取和兼容形态归一化，合法性由 `XtMarketDataSourceConfig::validate()` 判断。
XtMarketDataSourceConfig parse_market_data_source_config(
    const YAML::Node& node,
    const XtMarketDataSourceConfig& defaults);
