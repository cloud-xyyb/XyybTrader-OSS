#pragma once
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "xt_core/session/XtSessionPhase.h"    // venue_kind classification
#include "xt_core/session/XtSessionWindow.h"   // 共享 XtSessionWindow
#include "xt_risk/config/XtRiskCfg.h"

/// @brief 实盘下单网关的前置门禁、限流和风控配置。
struct XtLiveOrderGatewayCfg {
    /// @brief 与 backtest 共用的风控规则配置。
    XtRiskCfg risk;

    int    max_orders_per_sec     = 0; ///< 每秒最大报单数；0 表示不限
    int    max_cancels_per_sec    = 0; ///< 每秒最大撤单数；0 表示不限
    int    max_rejects_per_min    = 0; ///< 每分钟最大拒单数；0 表示不限
    int    cooldown_ms            = 0; ///< 拒单熔断冷却 ms；0 表示不冷却
    bool   require_l2_orderflow   = false;

    bool                               closeout_enabled   = false;
    std::vector<XtSessionWindow>       closeout_windows;
    int                                closeout_retry_ms  = 1000;
    int                                closeout_escalate_after_ms = 3000; ///< 收盘强平被动 GTC@mark 维持 ms，超时升级 IOC@aggressive；0=立即激进
    int                                closeout_aggressive_buffer_bps = 10; ///< 升级激进价穿价 buffer（基点 bps）
    bool                               closeout_force_exit_latch_enabled = true; ///< panel ForceExit CancelledOnly 后由 control frame 续帧重平（HFT）；false=不装配（恢复一次性语义）
    int                                closeout_force_exit_latch_ttl_ms = 30'000; ///< 续帧重试 TTL ms；<=0=永不过期

    // ── Recovery orphan reaper（cross-restart 孤儿在途单回收）────────────
    // reverse-sync 灌入且本进程无策略归属的在途单，multi-strategy 下被 sid filter
    // 挡住永远无人能撤，跨重启堆到 venue per-symbol 上限（binance -2025）。
    bool     orphan_reap_enabled = true;       ///< 周期撤掉无归属 recovered 在途单（默认开，fail-safe）
    int64_t  orphan_reap_grace_ms = 60'000;    ///< reverse-sync 后静默窗口 ms（single-strategy 策略自行处置窗口）
    int64_t  orphan_reap_interval_ms = 30'000; ///< sweep 节拍 ms
    int      orphan_reap_max_per_sweep = 50;   ///< 单次 sweep 撤单上限（venue 限频保护）
    bool     lost_terminal_reconcile_enabled = true; ///< 周期对账 venue-terminal 但 book-open 的 owned 单 → 合成终态（默认开，fail-safe）
    bool                               session_gate_enabled = true; ///< 非交易时段拒绝报单
    std::vector<std::pair<int, int>>   session_gate_sections;      ///< 允许报单时段，{start HHMM, end HHMM}
    /// @brief 持仓未确认时是否拒绝 close order。
    ///
    /// 默认 false：信任策略账本继续交易；true 时返回 PositionStateUnknown。
    bool                               reject_on_position_unknown = false;

    bool stp_enabled = true;             ///< 启用 STP 自成交防护（默认开，fail-safe）
    std::string stp_disabled_reason;     ///< 仅 stp_enabled=false 时必填的显式关闭理由；为空则启动期 throw

    bool     velocity_enabled         = false;
    int64_t  velocity_window_ms       = 100; ///< velocity 滑动窗口 ms
    double   velocity_max_qty         = 0.0; ///< 窗口内最大累计下单量；0 表示不限

    bool bypass_risk_for_bench   = false; ///< 仅 benchmark 使用，生产禁止开启
    bool single_threaded_submit  = false; ///< 跳过 submit mutex；调用方必须保证无并发提交

    /// @brief order_id → strategy_id sidecar 的终态保留窗口 us。
    ///
    /// 终态后先 mark，超过 grace 后 prune；窗口内允许 recovery stale rtn 继续反查 sid。
    int64_t order_strategy_grace_us = 60'000'000;

    /// @brief venue session 分类；Unknown 时 submit fail-closed。
    xt_core::XtVenueKind venue_kind = xt_core::XtVenueKind::Unknown;
};
