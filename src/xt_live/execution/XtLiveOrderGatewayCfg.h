#pragma once
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "xt_core/session/XtSessionPhase.h"
#include "xt_core/session/XtSessionWindow.h"
#include "xt_risk/config/XtRiskCfg.h"

/// @brief 实盘下单网关的前置门禁、限流和风控配置。
struct XtLiveOrderGatewayCfg {

    XtRiskCfg risk;

    int    max_orders_per_sec     = 0;
    int    max_cancels_per_sec    = 0;
    int    max_rejects_per_min    = 0;
    int    cooldown_ms            = 0;
    bool   require_l2_orderflow   = false;

    bool                               closeout_enabled   = false;
    std::vector<XtSessionWindow>       closeout_windows;
    int                                closeout_retry_ms  = 1000;
    int                                closeout_escalate_after_ms = 3000;
    int                                closeout_aggressive_buffer_bps = 10;

    bool     orphan_reap_enabled = true;
    int64_t  orphan_reap_grace_ms = 60'000;
    int64_t  orphan_reap_interval_ms = 30'000;
    int      orphan_reap_max_per_sweep = 50;
    bool     lost_terminal_reconcile_enabled = true;
    bool                               session_gate_enabled = true;
    std::vector<std::pair<int, int>>   session_gate_sections;

    bool                               reject_on_position_unknown = false;

    bool stp_enabled = true;
    std::string stp_disabled_reason;

    bool     velocity_enabled         = false;
    int64_t  velocity_window_ms       = 100;
    double   velocity_max_qty         = 0.0;

    bool bypass_risk_for_bench   = false;
    bool single_threaded_submit  = false;

    int64_t order_strategy_grace_us = 60'000'000;

    xt_core::XtVenueKind venue_kind = xt_core::XtVenueKind::Unknown;
};
