#pragma once

#include <cstddef>

/// @brief TD 恢复流程各步骤失败的错误码，用在日志和 metric 的 reason 字段。
///
/// 这些错误都发生在恢复/查询阶段（下单之前），跟下单被拒的 XtLiveRejectReason 是两套。
enum class XtLiveRecoveryError {
    Unknown = 0,
    TdQueryFailed,              ///< ReqQry* 同步发送失败（非流控）
    TdQueryThrottled,           ///< CTP 查询流控，等待 retry worker 重发
    TdQueryRetryExhausted,      ///< 同类查询重试次数耗尽
    TdQueryRetryFailed,         ///< retry worker 再次发送失败
    RecoveryQueryTimeout,       ///< recovery 阶段等待查询 is_last 超时
    SettlementConfirmReqFailed, ///< 结算确认请求发送失败
    SettlementConfirmRspFailed, ///< 结算确认应答失败或超时
    Count
};

inline constexpr size_t kLiveRecoveryErrorCount = static_cast<size_t>(XtLiveRecoveryError::Count);

/// @brief 转为稳定的 metric/log reason code 字符串。
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
