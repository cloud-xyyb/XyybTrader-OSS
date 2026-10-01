#pragma once

#include <string>
#include <vector>

#include "XtStrategyInitConfig.h"
#include "XtLoggerCfg.h"
#include "XtStrategyFactoryCfg.h"
#include "xt_backtest/data/source/XtMarketDataSourceConfig.h"
#include "xt_backtest/infra/XtReplayer.h"
#include "XtHftBacktestEngineCfg.h"
#include "xt_backtest/reporter/XtReportCfg.h"
#include "xt_backtest/reporter/XtEventStreamSinkCfg.h"
#include "xt_risk/config/XtRiskCfg.h"
#include "xt_cost/XtTradeCostConfig.h"
#include "HFTMatchEngineCfg.h"
#include "xt_backtest/infra/ReplayFilterCalendar.h"
#include "xt_core/session/XtSessionFilter.h"
#include "xt_core/market/XtRollEngine.h"
#include "xt_config/catalog/XtCommodityCatalog.h"
#include "xt_config/catalog/XtFeeRuleBook.h"

/// @brief HFT backtest 的单个 strategy 装配单元。
///
/// `backtest.strategies[]` 中每个 entry 解析为一个 `BtStrategyEntry`。
/// `factory_cfg.id` 必填且在同一回测内唯一，用作 multi-strategy routing 的 sid。
/// `init_cfg.values` 由该 entry 的 YAML map flatten 得到，传给 strategy context 初始化。
struct BtStrategyEntry {
    XtStrategyFactoryCfg factory_cfg;  ///< plugin 加载配置，包含 strategy id / so_path
    XtStrategyInitConfig init_cfg;     ///< strategy 初始化参数，来自该 entry 的 flattened YAML
};

/// @brief HFT backtest runner 的一次运行配置快照。
///
/// 覆盖策略装配、行情回放、撮合/风控/成本、交易日过滤和报告输出等子系统配置。
/// `load_backtest_config()` 完成 schema 校验、默认值补全和跨字段派生后产出本结构；
/// runner 后续只读取该快照装配 runtime，不再回读 YAML。
struct XtHftBacktestConfig {
    // 配置来源与基础设施。
    std::string cfg_path;
    XtLoggerCfg log_cfg;

    // 单策略兼容镜像；新装配逻辑以 strategies 为准。
    XtStrategyFactoryCfg strategy_cfg;
    XtStrategyInitConfig strategy_init_cfg;

    // Multi-strategy 装配列表；解析成功后至少 1 个 entry。
    // 多策略下相同 symbol 会被拒绝，避免共享 account position pool 造成归因串扰。
    std::vector<BtStrategyEntry> strategies;

    // 数据回放与回测执行。
    XtMarketDataSourceConfig datasource_cfg;
    XtReplayer::Config replay_cfg;
    XtHftBacktestEngineCfg engine_cfg;

    // 输出与观测。
    xt_report::XtReportCfg report_cfg;
    XtEventStreamSinkCfg event_stream_cfg;

    // 交易、风控和撮合模型。
    XtRiskCfg risk_cfg;
    XtTradeCostConfig cost_cfg;
    XtHftMatchEngineCfg match_engine_cfg;
    bool self_match_check = false;  ///< submit 前 STP 自成交预防（backtest.risk.self_match_check，默认关）

    // 交易日、session、换月和合约元数据。
    xt_core::Config session_cfg;
    XtBacktestTradingCalendar::Config calendar_cfg;
    XtRollEngine::Config roll_cfg;
    XtCommodityCatalog::Config commodities_cfg;
    XtFeeRuleBook::Config fees_cfg;
};

/// @brief 加载并校验 HFT backtest 配置。
///
/// `argc/argv` 只用于定位配置文件；查找顺序由 `xt_locate_config()` 决定，并支持
/// `XT_CONFIG` 环境变量。成功时 `out` 是已完成派生字段补全和跨子系统校验的配置。
/// 失败时返回 false，`err` 写入第一条可读错误；函数不向外抛 YAML 解析异常。
bool load_backtest_config(int argc, char** argv, XtHftBacktestConfig& out, std::string& err);
