#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

/// @file
/// @brief `XtRiskEngine` 与 live/backtest guard 共用的风控配置结构。
///
/// 数值字段使用配置层的单位，engine 装载后转换为 fixed-point cache；
/// 除字段注释另有说明外，0 或空 map 表示对应规则关闭。

inline constexpr int64_t kFrozenDisagreementEpsilonFp = 1'000'000LL;

/// @brief 风控规则配置。
///
/// 字段由配置解析层填充；`XtRiskEngine` 读取 sid_id keyed map，`*_by_sid_string`
/// 仅作为 YAML strategy id 翻译前的 staging map。
struct XtRiskCfg {
    double max_order_qty         = 0.0;
    double max_order_notional    = 0.0;
    double min_order_notional    = 0.0;
    double max_pos_qty           = 0.0;
    bool   allow_short           = true;

    bool   enforce_long_t_plus_1 = false;

    double max_daily_loss        = 0.0;

    double max_annual_loss_pct   = 0.0;

    std::unordered_map<uint32_t, double> per_strategy_max_pos_qty;

    std::unordered_map<std::string, double> per_strategy_max_pos_qty_by_sid_string;

    uint64_t max_order_count_per_symbol = 0;

    uint64_t max_order_rate_per_symbol = 0;

    uint32_t order_rate_window_ms = 10000;

    bool blacklist_persist_across_session = false;

    std::unordered_map<uint32_t, double> per_strategy_daily_loss_pct;

    std::unordered_map<std::string, double> per_strategy_daily_loss_pct_by_sid_string;

    bool allow_strategy_symbol_overlap = false;

    bool per_strategy_auto_liquidate = false;

    bool per_strategy_auto_liquidate_dry_run = false;

    std::unordered_map<uint32_t, double> per_strategy_liquidate_pct;

    std::unordered_map<std::string, double> per_strategy_liquidate_pct_by_sid_string;
};
