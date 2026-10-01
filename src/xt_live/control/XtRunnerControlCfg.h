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
    std::string strategy_id;  ///< 空字符串由 emit 侧映射为 "_default"。
    std::string symbol;       ///< 需要输出 zero-heartbeat 的合约。
};

/// @brief 解析 per-instance 身份：显式 instance_id 优先，否则回退 md_provider，
///        两者皆空归一化为 "_default"。多盘组合盘所有 per-instance state（safety /
///        aggregator / baseline）的唯一身份来源。
inline std::string resolved_instance_id(const std::string& md_provider,
                                        const std::string& instance_id) {
    if (!instance_id.empty()) return instance_id;
    return md_provider.empty() ? std::string("_default") : md_provider;
}

/// @brief runner 可变状态根目录（state / audit 同根派生）。env `XT_VAR_ROOT`
///        覆盖（本地栈/开发机可写目录）；默认生产布局 /opt/xyybtrader/var。
///        所有派生路径必须同源本函数，勿各自硬编码。
/// @note 已知例外（不随本函数）：ControlGrpcCommon / XtKillSwitchAuditLog 的
///       默认参与 `audit_log_path`——runner 路径下分别经 instance_audit_dir()
///       注入、profile 配置面本地化覆盖，默认值实际不落盘。
inline std::string var_root() {
    const char* env = std::getenv("XT_VAR_ROOT");
    return (env && *env) ? std::string(env) : std::string("/opt/xyybtrader/var");
}

/// @brief per-instance state 根目录（safety / aggregator / baseline）：<var_root()>/state。
inline std::string state_root() { return var_root() + "/state"; }

/// @brief per-instance state 子目录：<state_root()>/<resolved instance_id>。
inline std::string instance_state_dir(const std::string& md_provider,
                                      const std::string& instance_id) {
    return state_root() + "/" + resolved_instance_id(md_provider, instance_id);
}

/// @brief per-instance kill-switch 审计目录：<var_root()>/audit/<resolved id>。
///        同 host 多 runner（multibook）共用根目录会混写 JSONL 且事件无 instance
///        归属；各 runner 经 instance_audit_dir() 注入 CommonGrpcDeps.audit_dir。
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
    int         closeout_warn_streak           = 3;   ///< 强平连续被阻塞 N 轮后 error 告警
    int         closeout_block_streak          = 10;  ///< 强平连续被阻塞 N 轮后禁止开仓
    // ── unmatched drop 预算（runtime.debug.unmatched_budget，AND 语义见 HftUnmatchedBudget.h）─
    int         unmatched_budget_window_sec    = 60;    ///< tumbling window 长度；<=0 关闭预算判定
    int         unmatched_budget_max_drops     = 3;     ///< 绝对阈值；<=0 时该条件恒真（纯比例模式）
    double      unmatched_budget_max_ratio     = 0.001; ///< 比例阈值 drops/dispatch；<=0 时恒真（纯绝对模式）
    int         unified_report_flush_ms         = 1000;
    int         strategy_metrics_flush_sec      = 5;
    std::string unified_report_dir              = "var/live_metrics/report_contract";
    std::string unified_run_id;
    std::string unified_strategy_id;
    std::string unified_symbol;
    std::vector<XtStrategyPnlEmitTarget> strategy_pnl_emit_targets;
    std::string unified_config_hash;
    std::string unified_code_version;
    // 停机账户对账：runtime snapshot vs broker 最后快照直比（XtHftAccountReconcile）。
    bool        account_reconcile_on_stop       = false;
    // 停机对账混合容差：fresh REST 直比后残差来自「最后 anchor 之后」的
    // fee/realized 定点舍入与 venue 精度差（anchor 缓存回退路径残差更大）；
    // abs 兜小额（≈ 2-3 笔典型单笔 fee），rel 兜大账户（1e-4 = 1bp，
    // 买方现金 break 常用 0.1-1bp 口径）。
    double      account_reconcile_abs_tol       = 1.0;
    double      account_reconcile_rel_tol       = 1e-4;

    // ── per-instance 身份 ────────────────────────────────────────────────
    /// @brief runner 实例身份 override；空=按 md_provider 派生。safety / aggregator /
    ///        baseline 三类 per-instance state 文件全部落 instance_state_dir() 子目录。
    ///        多盘组合盘 (transport=shm) 同 venue 多 runner 须显式 per-book id，避免互覆写
    ///        kill_switch latch / peak_equity / today-PnL（cumulative + opening）。
    std::string instance_id;

    // ── 持久化 latched safety state ───────────────────────────────────────
    /// @brief safety.bin 持久化路径；由 instance_id 派生（非直接配置），空字符串表示关闭。
    std::string safety_state_path = state_root() + "/safety.bin";

    /// @brief safety state 周期写盘间隔 ms。
    int safety_state_save_interval_ms = 60000;

    // ── 每 profile 的 metrics exposer 端口 ────────────────────────────────
    /// @brief Prometheus /metrics bind port；0 表示不启动 exposer。
    int         metrics_exposer_port            = 9101;

    /// @brief Prometheus /metrics bind 地址。
    std::string metrics_exposer_bind_addr       = "0.0.0.0";

    // venue/account label for Prometheus metric emit。
    /// @brief venue label；空表示不 emit account-level metric。
    std::string venue_label;

    /// @brief account label for account-level metric (e.g. "_default" / 子账户 ID)。
    /// 默认 "_default" — 单账户 deployment 不需要区分。
    std::string account_label = "_default";

    // ── baseline persistence config ───────────────────────────────────────
    /// @brief Profile label，用作 var/state/ 下的 per-instance 子目录。
    std::string profile_label;

    /// @brief trading-day boundary timezone offset hours。
    int trading_day_tz_offset_h = 0;
};
