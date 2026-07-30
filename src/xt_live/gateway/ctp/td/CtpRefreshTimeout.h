#pragma once

/// @file
/// @brief Ready 稳态周期 refresh 查询（account/position）超时自愈的纯决策 helper。
///
/// recovery 阶段的查询超时守卫见 ctp/recovery/CtpRecoveryRearm.h + arm_recovery_query_timeout；
/// 本文件只服务 SessionPhase::Ready 之后的周期刷新查询：某 QueryKind 发出后长时间无
/// bIsLast 回包（丢包 / 柜台无回）时，query_once 的 inflight CAS 恒失败 → 后续周期刷新
/// 全 no-op（永久 wedge）。timer 到期后据本决策重发并按连续超时轮数升级 wedged 观测状态。
/// 决策与 timer/线程解耦，纯函数便于单测。

namespace ctp_refresh {

/// @brief 一次 refresh 查询超时的自愈决策结果。
struct RefreshTimeoutDecision {
    bool reissue     = false;
    bool raise_wedge = false;
};

inline RefreshTimeoutDecision refresh_timeout_decision(int consecutive_timeouts, int wedge_rounds) {
    RefreshTimeoutDecision d;
    d.reissue     = true;
    d.raise_wedge = (wedge_rounds > 0 && consecutive_timeouts >= wedge_rounds);
    return d;
}

}
