#pragma once

/// @file
/// @brief CTP 恢复查询重试的 backoff 与告警升级决策函数 helper。
///
/// - CTP 登录后要查持仓、资金、委托、成交来恢复本地状态，如果失败需要重新尝试
/// - CtpRecoveryRearm.h 提供查询间隔和重试次数上限的判断支持
/// 不是真正的 recovery 流程，只是 helper

#include <algorithm>
#include <climits>

namespace ctp_recovery {

/// @brief 一轮 re-arm 的决策结果。
struct RecoveryRearmDecision {
    int  backoff_ms  = 0;      ///< 本轮重发前等待的 backoff（指数退避封顶）
    bool raise_alert = false;  ///< 轮数是否达到 paging 阈值（置 wedged gauge）
};

/// @brief 计算一轮 re-arm 的 backoff 与是否 paging。
///
/// @param round           本 session 第几轮 re-arm（从 1 起）。
/// @param alert_rounds    轮数 ≥ 此值置 wedged 告警；<=0 表示恒告警。
/// @param backoff_base_ms 基准 backoff（caller 已保证 > 0；<=0 = 关闭 re-arm 不调本函数）。
/// @param backoff_max_ms  backoff 封顶；<=0 表示无上限。
/// @return backoff_ms ∈ [backoff_base_ms, min(base<<(round-1), cap)]，clamp 到 INT_MAX；
///         raise_alert = (round >= alert_rounds)。
///
/// @note shift 截到 30 防负移位 / 溢出 UB；round<1 当 1 处理（防御）。
inline RecoveryRearmDecision recovery_rearm_decision(int round, int alert_rounds,
                                                     int backoff_base_ms, int backoff_max_ms) {
    RecoveryRearmDecision d;
    const int r     = round < 1 ? 1 : round;
    const int shift = std::min(r - 1, 30);
    long long raw   = static_cast<long long>(backoff_base_ms) << shift;
    const long long cap = backoff_max_ms > 0 ? static_cast<long long>(backoff_max_ms)
                                             : static_cast<long long>(LLONG_MAX);
    long long ms = raw < cap ? raw : cap;
    if (ms < backoff_base_ms) ms = backoff_base_ms;  // 不低于基准
    if (ms > INT_MAX) ms = INT_MAX;
    d.backoff_ms  = static_cast<int>(ms);
    d.raise_alert = (round >= alert_rounds);
    return d;
}

/// @brief re-arm backoff 指数退避封顶（60s）。
inline constexpr int kRecoveryRearmBackoffMaxMs = 60000;

}  // namespace ctp_recovery
