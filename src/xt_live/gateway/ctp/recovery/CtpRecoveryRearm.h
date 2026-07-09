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
    int  backoff_ms  = 0;
    bool raise_alert = false;
};

inline RecoveryRearmDecision recovery_rearm_decision(int round, int alert_rounds,
                                                     int backoff_base_ms, int backoff_max_ms) {
    RecoveryRearmDecision d;
    const int r     = round < 1 ? 1 : round;
    const int shift = std::min(r - 1, 30);
    long long raw   = static_cast<long long>(backoff_base_ms) << shift;
    const long long cap = backoff_max_ms > 0 ? static_cast<long long>(backoff_max_ms)
                                             : static_cast<long long>(LLONG_MAX);
    long long ms = raw < cap ? raw : cap;
    if (ms < backoff_base_ms) ms = backoff_base_ms;
    if (ms > INT_MAX) ms = INT_MAX;
    d.backoff_ms  = static_cast<int>(ms);
    d.raise_alert = (round >= alert_rounds);
    return d;
}

inline constexpr int kRecoveryRearmBackoffMaxMs = 60000;

}
