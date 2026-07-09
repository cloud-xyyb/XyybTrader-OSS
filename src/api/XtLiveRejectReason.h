#pragma once

#include <cstddef>

enum class XtLiveRejectReason {
    Unknown = 0,

    Cooldown,
    RateLimited,
    InvalidInput,
    UnknownStrategySid,
    InvalidFlag,
    PriceTickMismatch,
    TradingNotReady,
    CloseoutNoOpen,
    TdRecovering,
    MaxOrderQty,
    MaxOrderNotional,
    MinOrderNotional,
    PositionLimit,
    ShortNotAllowed,
    InsufficientMargin,
    TdNotInitialized,
    TdCapCloseTodayUnsupported,
    TdCapL2OrderFlowUnsupported,
    TdCapIocFokUnsupported,

    CancelCooldown,
    CancelRateLimited,
    CancelTradingNotReady,
    CancelTdNotInitialized,
    CancelSendFailed,

    TdFrontDisconnected,
    TdLoginFailed,
    PositionSyncStale,
    CloseSyncPending,
    InsufficientCloseToday,
    InsufficientCloseYesterday,

    KillSwitchActive,
    HardCircuitBreakerTripped,
    SelfTradeDetected,
    VelocityLimitExceeded,
    DailyLossExceeded,
    GateDisabled,
    SymbolBlacklisted,
    OrderCountExceeded,
    OrderRateExceeded,
    SelfMatchPostTrade,
    OpenHalted,
    AnnualLossExceeded,
    StrategyExposureExceeded,
    SessionClosed,
    InCallAuctionLimitNotAllowed,

    StrategyMaxPosQtyExceeded,
    StrategyMarginPctExceeded,
    StrategyDailyLossExceeded,
    StrategyKillSwitchActive,
    StrategyBlockedByWatchdog,
    SiblingForceFlattenInProgress,

    OppositeOpenPending,
    AutoOffsetUnresolved,

    Count
};

inline constexpr size_t kLiveRejectReasonCount = static_cast<size_t>(XtLiveRejectReason::Count);

inline constexpr size_t live_reject_reason_index(XtLiveRejectReason code) {
    return static_cast<size_t>(code);
}

inline const char* live_reject_reason_str(XtLiveRejectReason code) {
    switch (code) {
    case XtLiveRejectReason::Unknown:                    return "UNKNOWN";
    case XtLiveRejectReason::Cooldown:                   return "COOLDOWN";
    case XtLiveRejectReason::RateLimited:                return "RATE_LIMITED";
    case XtLiveRejectReason::InvalidInput:               return "INVALID_INPUT";
    case XtLiveRejectReason::UnknownStrategySid:         return "UNKNOWN_STRATEGY_SID";
    case XtLiveRejectReason::InvalidFlag:                return "INVALID_FLAG";
    case XtLiveRejectReason::PriceTickMismatch:          return "PRICE_TICK_MISMATCH";
    case XtLiveRejectReason::TradingNotReady:            return "TRADING_NOT_READY";
    case XtLiveRejectReason::CloseoutNoOpen:             return "CLOSEOUT_NO_OPEN";
    case XtLiveRejectReason::TdRecovering:               return "TD_RECOVERING";
    case XtLiveRejectReason::MaxOrderQty:                return "MAX_ORDER_QTY";
    case XtLiveRejectReason::MaxOrderNotional:           return "MAX_ORDER_NOTIONAL";
    case XtLiveRejectReason::MinOrderNotional:           return "MIN_ORDER_NOTIONAL";
    case XtLiveRejectReason::PositionLimit:              return "POSITION_LIMIT";
    case XtLiveRejectReason::ShortNotAllowed:            return "SHORT_NOT_ALLOWED";
    case XtLiveRejectReason::InsufficientMargin:         return "INSUFFICIENT_MARGIN";
    case XtLiveRejectReason::TdNotInitialized:           return "TD_NOT_INITIALIZED";
    case XtLiveRejectReason::TdCapCloseTodayUnsupported: return "TD_CAP_CLOSE_TODAY_UNSUPPORTED";
    case XtLiveRejectReason::TdCapL2OrderFlowUnsupported:return "TD_CAP_L2_ORDERFLOW_UNSUPPORTED";
    case XtLiveRejectReason::TdCapIocFokUnsupported:     return "TD_CAP_IOC_FOK_UNSUPPORTED";
    case XtLiveRejectReason::CancelCooldown:             return "CANCEL_COOLDOWN";
    case XtLiveRejectReason::CancelRateLimited:          return "CANCEL_RATE_LIMITED";
    case XtLiveRejectReason::CancelTradingNotReady:      return "CANCEL_TRADING_NOT_READY";
    case XtLiveRejectReason::CancelTdNotInitialized:     return "CANCEL_TD_NOT_INITIALIZED";
    case XtLiveRejectReason::CancelSendFailed:           return "CANCEL_SEND_FAILED";
    case XtLiveRejectReason::TdFrontDisconnected:        return "TD_FRONT_DISCONNECTED";
    case XtLiveRejectReason::TdLoginFailed:              return "TD_LOGIN_FAILED";
    case XtLiveRejectReason::PositionSyncStale:          return "POSITION_SYNC_STALE";
    case XtLiveRejectReason::CloseSyncPending:           return "CLOSE_SYNC_PENDING";
    case XtLiveRejectReason::InsufficientCloseToday:     return "INSUFFICIENT_CLOSE_TODAY";
    case XtLiveRejectReason::InsufficientCloseYesterday: return "INSUFFICIENT_CLOSE_YESTERDAY";
    case XtLiveRejectReason::KillSwitchActive:           return "KILL_SWITCH_ACTIVE";
    case XtLiveRejectReason::HardCircuitBreakerTripped:  return "HARD_CIRCUIT_BREAKER_TRIPPED";
    case XtLiveRejectReason::SelfTradeDetected:          return "SELF_TRADE_DETECTED";
    case XtLiveRejectReason::VelocityLimitExceeded:      return "VELOCITY_LIMIT_EXCEEDED";
    case XtLiveRejectReason::DailyLossExceeded:          return "DAILY_LOSS_EXCEEDED";
    case XtLiveRejectReason::GateDisabled:               return "GATE_DISABLED";
    case XtLiveRejectReason::SymbolBlacklisted:          return "SYMBOL_BLACKLISTED";
    case XtLiveRejectReason::OrderCountExceeded:         return "ORDER_COUNT_EXCEEDED";
    case XtLiveRejectReason::OrderRateExceeded:          return "ORDER_RATE_EXCEEDED";
    case XtLiveRejectReason::SelfMatchPostTrade:         return "SELF_MATCH_POST_TRADE";
    case XtLiveRejectReason::OpenHalted:                 return "OPEN_HALTED";
    case XtLiveRejectReason::AnnualLossExceeded:         return "ANNUAL_LOSS_EXCEEDED";
    case XtLiveRejectReason::StrategyExposureExceeded:   return "STRATEGY_EXPOSURE_EXCEEDED";
    case XtLiveRejectReason::InCallAuctionLimitNotAllowed: return "IN_CALL_AUCTION_LIMIT_NOT_ALLOWED";
    case XtLiveRejectReason::SessionClosed:              return "SESSION_CLOSED";

    case XtLiveRejectReason::StrategyMaxPosQtyExceeded:    return "STRATEGY_MAX_POS_QTY_EXCEEDED";
    case XtLiveRejectReason::StrategyMarginPctExceeded:    return "STRATEGY_MARGIN_PCT_EXCEEDED";
    case XtLiveRejectReason::StrategyDailyLossExceeded:    return "STRATEGY_DAILY_LOSS_EXCEEDED";
    case XtLiveRejectReason::StrategyKillSwitchActive:     return "STRATEGY_KILL_SWITCH_ACTIVE";
    case XtLiveRejectReason::StrategyBlockedByWatchdog:    return "STRATEGY_BLOCKED_BY_WATCHDOG";
    case XtLiveRejectReason::SiblingForceFlattenInProgress: return "SIBLING_FORCE_FLATTEN_IN_PROGRESS";
    case XtLiveRejectReason::OppositeOpenPending:          return "OPPOSITE_OPEN_PENDING";
    case XtLiveRejectReason::AutoOffsetUnresolved:         return "AUTO_OFFSET_UNRESOLVED";
    default:                                             return "UNKNOWN";
    }
}
