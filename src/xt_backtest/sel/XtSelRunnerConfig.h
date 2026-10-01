#pragma once
//
// SEL backtest runner 配置（yaml 解析后的 POD struct）。
//
// yaml schema（最小可行版）：
//
//   datalake:
//     root: /path/to/datalake           # 可选；空则 ENV XT_DATALAKE_ROOT 兜底，仍空 fail-fast
//     allow_schema_skew: false          # 可选；true → schema 不匹配 warn 不 throw
//
//   selector:
//     trading_dates_csv: data/regression/sel_demo/trading_dates.csv  # 简化
//     tradability_csv: data/sel/tradability.csv  # 可选；date,code,is_open,is_limit_up,is_limit_down
//     corporate_actions_csv: data/sel/corporate_actions.csv  # 可选；见 XtSelCorporateAction.h
//     rights_policy: subscribe          # 可选；subscribe(默认,缴款认购) / forfeit(弃权)
//     allow_skip_rights: false          # 可选；true = 配股行整条跳过（不除权、不认购）
//     date_from: 20260109               # 可选；闭区间过滤
//     date_to:   20260208
//     task:
//       strategy_id: sel_demo
//       period: monthly                 # minute/daily/weekly/monthly/yearly
//       day: 1                          # weekly:1-7, monthly:1-31, yearly:MMDD
//       time: 1500                      # HHMM
//       strict_time: true
//     strategy:
//       so:    var/plugins/strategies/libsel_weights_csv_strategy.so
//       id:    sel_demo
//       params:
//         weights_path: data/regression/sel_demo/weights.csv
//         normalize: true
//
//   account:
//     initial_cash: 1000000.0
//
//   backtest:
//     trace_mode: off                   # 可选；off(默认) / full → events_full.csv
//
//   execution:                          # 可选；整节缺席 = 全部默认（产物与未配置逐位一致）
//     integer_projection: floor         # floor(默认) / largest_remainder
//     max_participation: 0.1            # 当日成交量参与率上限（(0,1]），需 close_prices 带 volume 列
//     carry_max_days: 5                 # 未成量跨日复投的最大天数，到限丢弃并计数
//
//   report:
//     output_dir: ./reports/sel_demo    # trades.csv / summary.json 输出目录
//

#include <yaml-cpp/yaml.h>

#include <cstdint>
#include <string>
#include <vector>

#include "XtSelCorporateAction.h"        // RightsPolicy
#include "api/XtStrategyRuntimeTypes.h"   // TaskPeriod
#include "xt_config/catalog/XtCommodityCatalog.h"
#include "xt_config/catalog/XtFeeRuleBook.h"
#include "xt_risk/config/XtRiskCfg.h"
#include "xt_cost/XtTradeCostConfig.h"

namespace xt_bt_sel {

/// @brief 单个 SEL task 的 yaml 配置（mirror SelTaskInfo 字段，不含 runtime state）。
struct SelTaskCfg {
    std::string strategy_id;
    TaskPeriod  period = TaskPeriod::Monthly;
    uint32_t    day = 1;
    uint32_t    time = 1500;
    bool        strict_time = true;
    std::string trdtpl = "CHINA";
    std::string session = "default";
};

/// @brief 单个 SEL strategy plugin 的 yaml 配置。
struct SelStrategyCfg {
    std::string strategy_id;
    std::string lib_path;            ///< plugin .so 绝对/相对路径；空 = external strategy 注入（run(cfg, strat)）
    YAML::Node  params;              ///< 透传给 strategy.init() 的 flat key→value
};

/// @brief 顶层 runner 配置。
struct XtSelRunnerConfig {
    // datalake
    std::string datalake_root;            ///< 数据湖根（empty → ENV XT_DATALAKE_ROOT 兜底，仍空 fail-fast）
    bool        allow_schema_skew = false;

    // selector
    std::string trading_dates_csv;        ///< trading dates fallback（后续走 calendar.parquet）
    std::string close_prices_csv;         ///< 日 close 价 csv (date,code,close)；空则 sim 撮合无价格
    std::string tradability_csv;          ///< 可选 tradability csv (date,code,is_open,is_limit_up,is_limit_down)；空 = gate 不启用
    std::string corporate_actions_csv;    ///< 可选 corporate action csv（三日期 schema，见 XtSelCorporateAction.h）；空 = CA 不启用
    /// 配股处置口径（`selector.rights_policy`）：缴款认购 or 弃权；两者除权参考价同式。
    RightsPolicy rights_policy = RightsPolicy::kSubscribe;
    /// `selector.allow_skip_rights`：true = 配股行整条跳过（不除权、不认购、不摊薄）
    /// + 计数披露。与 `rights_policy` 正交——它跳过的是整个事件而非只是认购腿。
    bool        allow_skip_rights = false;
    uint32_t    date_from = 0;            ///< 闭区间过滤；0 = 无下限
    uint32_t    date_to   = 0;            ///< 闭区间过滤；0 = 无上限
    std::vector<SelTaskCfg>     tasks;
    std::vector<SelStrategyCfg> strategies;

    /// @brief 输入路径的 yaml 原始写法（display/summary 用）。
    ///
    /// 上面的路径字段在解析末被绝对化（消 cwd 依赖）；产物里的 source 字段必须写
    /// 用户在 config 里写的原串，否则同一 config 在不同机器/目录下 summary 字节不同。
    struct DisplayPaths {
        std::string trading_dates_csv;
        std::string close_prices_csv;
        std::string tradability_csv;
        std::string corporate_actions_csv;
    } display;

    // account
    double initial_cash = 1'000'000.0;

    // sim 撮合接入所需配置（与 cta runner 同 schema）
    XtRiskCfg                    risk_cfg;       ///< max_order_qty / enforce_long_t_plus_1 等
    XtFeeRuleBook::Config        fee_cfg;        ///< 印花税 / 过户费 / 最低佣金
    XtCommodityCatalog::Config   commodities_cfg; ///< vol_scale / min_lot / has_open_interest
    XtTradeCostConfig            trade_cost_cfg; ///< slippage（默认 0）

    // corporate action（BT-only；XtRiskCfg / XtFeeRuleBook 是 live 共享类型，不为
    // 回测语义扩字段——两项都只由 SEL BT 消费）
    /// `fees.dividend_tax_rate`：现金分红所得税率 [0,1)。默认 0；A 股按持有期
    /// 0%（>1 年）/10%（1 月–1 年）/20%（<1 月）差异化，本模型是单一常率近似。
    double dividend_tax_rate = 0.0;
    /// `risk.delist_haircut_pct`：退市清算折价 [0,1)。默认 0 = 按最后可得 close
    /// 全额清算（乐观假设，summary/研究门禁需披露）。
    double delist_haircut_pct = 0.0;

    // backtest（BT-only 事件流导出）
    /// `backtest.trace_mode`：off = 不产 events_full.csv（默认，产物集合与未配置时
    /// 逐位一致）；full = 产 SEL 原生门禁链事件流（见 XtSelTraceSink）。
    bool trace_full = false;

    // execution（BT-only 执行真实度；整节缺席时全部保持默认，行为与本节落地前逐位一致）
    /// `execution:` 节在 yaml 里出现过（用于 summary 的 execution 块是否写出——
    /// 配了参数却看不到生效面比默认值本身更容易误判）。
    bool execution_configured = false;
    /// `execution.integer_projection: largest_remainder`：权重→整手股数改走
    /// 「费用感知 + 最大余数法 + 残余现金约束」投影；默认 floor = 逐 code 向零截断。
    bool projection_largest_remainder = false;
    /// `execution.max_participation`：单 code 单日成交量占当日 volume 的上限，(0,1]。
    /// 仅当 close_prices.csv 带 volume 列时生效（无量数据无从判定参与率）。
    double max_participation = 0.1;
    /// `execution.carry_max_days`：被参与率截下的未成量最多跨几个交易日复投；
    /// 到限仍未成则丢弃并计数（现实里挂太久的调仓意图会被重新评估而不是无限追）。
    uint32_t carry_max_days = 5;

    // report
    std::string report_dir = "./reports";

    // 原始 yaml 路径（log）
    std::string cfg_path;
};

/// @brief 解析 yaml；失败时填 err，返回 false。
bool load_sel_config(int argc, char** argv,
                     XtSelRunnerConfig& out, std::string& err);

}  // namespace xt_bt_sel
