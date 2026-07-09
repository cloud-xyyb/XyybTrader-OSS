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
    XtStrategyFactoryCfg factory_cfg;
    XtStrategyInitConfig init_cfg;
};

/// @brief HFT backtest runner 的一次运行配置快照。
///
/// 覆盖策略装配、行情回放、撮合/风控/成本、交易日过滤和报告输出等子系统配置。
/// `load_backtest_config()` 完成 schema 校验、默认值补全和跨字段派生后产出本结构；
/// runner 后续只读取该快照装配 runtime，不再回读 YAML。
struct XtHftBacktestConfig {

    std::string cfg_path;
    XtLoggerCfg log_cfg;

    XtStrategyFactoryCfg strategy_cfg;
    XtStrategyInitConfig strategy_init_cfg;

    std::vector<BtStrategyEntry> strategies;

    XtMarketDataSourceConfig datasource_cfg;
    XtReplayer::Config replay_cfg;
    XtHftBacktestEngineCfg engine_cfg;

    xt_report::XtReportCfg report_cfg;
    XtEventStreamSinkCfg event_stream_cfg;

    XtRiskCfg risk_cfg;
    XtTradeCostConfig cost_cfg;
    XtHftMatchEngineCfg match_engine_cfg;
    bool self_match_check = false;

    xt_core::Config session_cfg;
    XtBacktestTradingCalendar::Config calendar_cfg;
    XtRollEngine::Config roll_cfg;
    XtCommodityCatalog::Config commodities_cfg;
    XtFeeRuleBook::Config fees_cfg;
};

bool load_backtest_config(int argc, char** argv, XtHftBacktestConfig& out, std::string& err);
