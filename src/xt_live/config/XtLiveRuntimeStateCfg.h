#pragma once
#include <cstddef>
#include <string>
#include <unordered_map>

#include "xt_config/catalog/XtCommodityCatalog.h"

/// @brief Live bootstrap 阶段注入账户、合约目录和 market cache 的状态配置。
///
/// parse_live_runtime_state_config() 从配置文件填充 XtLiveRuntimeStateCfg；
/// HFT runner 会用 strategy history 配置覆盖 tick/bar 默认容量。
struct XtLiveRuntimeStateCfg {
    double      initial_cash                         = 0.0;
    bool        t1_enabled                           = false;
    bool        futures_mode                         = false;
    double      margin_rate                          = 1.0;

    std::size_t tick_history_cap                     = 2048;
    std::size_t bar_history_cap                      = 512;
    std::size_t orderflow_history_cap                = 2048;

    bool        enable_position_snapshot_stale_guard = true;
    int         position_snapshot_stale_ms           = 120000;

    std::unordered_map<std::string, double>          margin_by_symbol;
    std::unordered_map<std::string, XtCommodityInfo> commodity_map;

    XtOffsetPolicy offset_policy_default = XtOffsetPolicy::OpenAllowed;

    bool conditional_orders_enabled = false;
};
