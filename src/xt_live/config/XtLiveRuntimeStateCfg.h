#pragma once
#include <cstddef>
#include <string>
#include <unordered_map>

#include "xt_config/catalog/XtCommodityCatalog.h"  // XtCommodityInfo for commodity_map

/// @brief Live bootstrap 阶段注入账户、合约目录和 market cache 的状态配置。
///
/// parse_live_runtime_state_config() 从配置文件填充 XtLiveRuntimeStateCfg；
/// HFT runner 会用 strategy history 配置覆盖 tick/bar 默认容量。
struct XtLiveRuntimeStateCfg {
    double      initial_cash                         = 0.0;   ///< AccountState 初始 cash，实盘快照到达后由账户流覆盖
    bool        t1_enabled                           = false; ///< T+1 closeable guard，A 股路径启用
    bool        futures_mode                         = false; ///< futures accounting path，影响保证金和权益计算
    double      margin_rate                          = 1.0;   ///< 默认保证金率；symbol override / commodity entry 优先

    std::size_t tick_history_cap                     = 2048;  ///< tick ring cap；HFT 可由 strategy_history_ticks 覆盖
    std::size_t bar_history_cap                      = 512;   ///< bar ring cap；HFT 可由 strategy_history_bars 覆盖
    std::size_t orderflow_history_cap                = 2048;  ///< order_queue/detail/transaction ring cap

    bool        enable_position_snapshot_stale_guard = true;   ///< true 时 stale position snapshot 会阻断新委托
    int         position_snapshot_stale_ms           = 120000; ///< last_position_snapshot_ms 允许的最大年龄

    std::unordered_map<std::string, double>          margin_by_symbol; ///< per-symbol margin override
    std::unordered_map<std::string, XtCommodityInfo> commodity_map;     ///< `commodities` 静态合约信息

    /// venue 默认开平路由（`runtime.trading.offset_policy_default`）；品种 Inherit 时生效。
    XtOffsetPolicy offset_policy_default = XtOffsetPolicy::OpenAllowed;

    /// 本地条件单总闸（`runtime.trading.conditional_orders_enabled`）；默认关。
    /// runner 把此值同时喂给 engine Config 与每个 XtStrategyContext::Config。
    bool conditional_orders_enabled = false;
};
