#pragma once

/// @file
/// @brief HFT runner control-loop 配置结构。
///
/// 字段由 live yaml 解析后传入 runner；时间单位除特别说明外为毫秒。

#include <cstdlib>
#include <string>
#include <vector>

/// @brief per-strategy PnL zero-heartbeat 的 emit 目标（strategy × symbol）。
struct XtStrategyPnlEmitTarget {
    std::string strategy_id;
    std::string symbol;
};

inline std::string resolved_instance_id(const std::string& md_provider,
                                        const std::string& instance_id) {
    if (!instance_id.empty()) return instance_id;
    return md_provider.empty() ? std::string("_default") : md_provider;
}

inline std::string var_root() {
    const char* env = std::getenv("XT_VAR_ROOT");
    return (env && *env) ? std::string(env) : std::string("/opt/xyybtrader/var");
}

inline std::string state_root() { return var_root() + "/state"; }

inline std::string instance_state_dir(const std::string& md_provider,
                                      const std::string& instance_id) {
    return state_root() + "/" + resolved_instance_id(md_provider, instance_id);
}

inline std::string instance_audit_dir(const std::string& md_provider,
                                      const std::string& instance_id) {
    return var_root() + "/audit/" + resolved_instance_id(md_provider, instance_id);
}

/// @brief 配置 runner 控制帧循环、告警阈值与 per-instance 持久化。
struct XtRunnerControlConfig {
    bool        log_stats                       = false;
    int         sleep_ms                        = 1000;
    bool        heartbeat_tick_count            = false;
    int         heartbeat_ms                    = 0;
    bool        fail_on_critical_td_drop        = true;
    int         consistency_warn_loops          = 1;
    int         consistency_block_loops         = 3;
    int         consistency_kill_loops          = 20;
    int         closeout_warn_streak           = 3;
    int         closeout_block_streak          = 10;

    int         unmatched_budget_window_sec    = 60;
    int         unmatched_budget_max_drops     = 3;
    double      unmatched_budget_max_ratio     = 0.001;
    int         unified_report_flush_ms         = 1000;
    int         strategy_metrics_flush_sec      = 5;
    std::string unified_report_dir              = "var/live_metrics/report_contract";
    std::string unified_run_id;
    std::string unified_strategy_id;
    std::string unified_symbol;
    std::vector<XtStrategyPnlEmitTarget> strategy_pnl_emit_targets;
    std::string unified_config_hash;
    std::string unified_code_version;

    bool        account_reconcile_on_stop       = false;

    double      account_reconcile_abs_tol       = 1.0;
    double      account_reconcile_rel_tol       = 1e-4;

    std::string instance_id;

    std::string safety_state_path = state_root() + "/safety.bin";

    int safety_state_save_interval_ms = 60000;

    int         metrics_exposer_port            = 9101;

    std::string metrics_exposer_bind_addr       = "0.0.0.0";

    std::string venue_label;

    std::string account_label = "_default";

    std::string profile_label;

    int trading_day_tz_offset_h = 0;
};
