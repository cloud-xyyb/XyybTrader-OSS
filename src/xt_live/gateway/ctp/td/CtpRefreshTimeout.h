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
    bool reissue     = false;  ///< 解开 stuck inflight 后是否重发 query（page-then-continue 恒重发）
    bool raise_wedge = false;  ///< 连续超时轮数达阈值 → 置 refresh wedged 观测状态
};

/// @brief 计算一次 refresh 查询超时的自愈决策。
///
/// @param consecutive_timeouts 自上次成功回包以来的连续超时次数（本次 ≥ 1）。
/// @param wedge_rounds         连续超时达此值置 wedged；<=0 表示恒不置（仅重发）。
/// @return reissue 恒 true（不放弃重试）；raise_wedge = (wedge_rounds > 0 且达阈值)。
///
/// @note 有意保持无状态：wedge 仅作观测告警，不改变重发行为（超阈仍继续重试，
///       与 recovery re-arm 的 page-then-continue 语义一致）。
inline RefreshTimeoutDecision refresh_timeout_decision(int consecutive_timeouts, int wedge_rounds) {
    RefreshTimeoutDecision d;
    d.reissue     = true;
    d.raise_wedge = (wedge_rounds > 0 && consecutive_timeouts >= wedge_rounds);
    return d;
}

}  // namespace ctp_refresh
