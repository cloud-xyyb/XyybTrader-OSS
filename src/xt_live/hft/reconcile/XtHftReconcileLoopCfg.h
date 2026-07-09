#pragma once

#include <cstdint>

namespace xt_live { namespace control {

/// @brief Reconcile loop 配置（yaml: `runtime.td.reconcile.*`）。
///
/// 独立轻头：config 层（XtLiveBootstrapPlan / LiveConfigParsers）与 loop 本体
/// 共用同一 struct，字段初始化器是默认值唯一来源（防两处默认漂移）。
/// 阶梯阈值 `reconnect_after_n` / `kill_after_n` 取 0 = 禁用该级阶梯
/// （calibrate 借用共享账户场景关 kill 保 resnap+metrics 用）。
struct XtHftReconcileLoopCfg {
    uint32_t cycle_ms = 30'000;
    uint8_t  reconnect_after_n = 2;
    uint8_t  kill_after_n = 8;
    uint32_t resync_throttle_ms = 60'000;
    int64_t  position_epsilon_fp = 1;
    uint32_t order_min_age_ms = 30'000;

    bool enabled = true;
};

}}
