#pragma once

#include <cstddef>

enum class XtLiveRecoveryError {
    Unknown = 0,
    TdQueryFailed,
    TdQueryThrottled,
    TdQueryRetryExhausted,
    TdQueryRetryFailed,
    RecoveryQueryTimeout,
    SettlementConfirmReqFailed,
    SettlementConfirmRspFailed,
    Count
};

inline constexpr size_t kLiveRecoveryErrorCount = static_cast<size_t>(XtLiveRecoveryError::Count);

inline const char* live_recovery_error_str(XtLiveRecoveryError code) {
    switch (code) {
    case XtLiveRecoveryError::Unknown:                    return "UNKNOWN";
    case XtLiveRecoveryError::TdQueryFailed:              return "TD_QUERY_FAILED";
    case XtLiveRecoveryError::TdQueryThrottled:           return "TD_QUERY_THROTTLED";
    case XtLiveRecoveryError::TdQueryRetryExhausted:      return "TD_QUERY_RETRY_EXHAUSTED";
    case XtLiveRecoveryError::TdQueryRetryFailed:         return "TD_QUERY_RETRY_FAILED";
    case XtLiveRecoveryError::RecoveryQueryTimeout:       return "TD_RECOVERY_QUERY_TIMEOUT";
    case XtLiveRecoveryError::SettlementConfirmReqFailed: return "SETTLEMENT_CONFIRM_REQ_FAILED";
    case XtLiveRecoveryError::SettlementConfirmRspFailed: return "SETTLEMENT_CONFIRM_RSP_FAILED";
    default:                                              return "UNKNOWN";
    }
}
