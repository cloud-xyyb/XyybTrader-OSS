#pragma once

#include "api/XtLiveRejectReason.h"
#include "xt_risk/engine/XtRiskEngine.h"

/// @file
/// @brief risk reason 到 live reject bucket 的稳定映射。

namespace live_reject_reason {

inline XtLiveRejectReason from_risk(XtRiskReasonCode code) {
    switch (code) {
    case XtRiskReasonCode::Cooldown: return XtLiveRejectReason::Cooldown;
    case XtRiskReasonCode::RateLimited: return XtLiveRejectReason::RateLimited;
    case XtRiskReasonCode::PriceTickMismatch: return XtLiveRejectReason::PriceTickMismatch;
    case XtRiskReasonCode::TradingNotReady: return XtLiveRejectReason::TradingNotReady;
    case XtRiskReasonCode::CloseoutNoOpen: return XtLiveRejectReason::CloseoutNoOpen;
    case XtRiskReasonCode::TdRecovering: return XtLiveRejectReason::TdRecovering;
    case XtRiskReasonCode::TdNotInitialized: return XtLiveRejectReason::TdNotInitialized;
    case XtRiskReasonCode::TdCapCloseTodayUnsupported: return XtLiveRejectReason::TdCapCloseTodayUnsupported;
    case XtRiskReasonCode::TdCapL2OrderFlowUnsupported: return XtLiveRejectReason::TdCapL2OrderFlowUnsupported;
    case XtRiskReasonCode::TdCapIocFokUnsupported: return XtLiveRejectReason::TdCapIocFokUnsupported;
    case XtRiskReasonCode::MaxOrderQty: return XtLiveRejectReason::MaxOrderQty;
    case XtRiskReasonCode::MaxOrderNotional: return XtLiveRejectReason::MaxOrderNotional;
    case XtRiskReasonCode::MinOrderNotional: return XtLiveRejectReason::MinOrderNotional;
    case XtRiskReasonCode::MaxPosQty: return XtLiveRejectReason::PositionLimit;
    case XtRiskReasonCode::ShortNotAllowed: return XtLiveRejectReason::ShortNotAllowed;
    case XtRiskReasonCode::InsufficientMargin: return XtLiveRejectReason::InsufficientMargin;

    case XtRiskReasonCode::InsufficientAvailablePosition: return XtLiveRejectReason::PositionLimit;
    case XtRiskReasonCode::PositionSyncStale: return XtLiveRejectReason::PositionSyncStale;
    case XtRiskReasonCode::CloseSyncPending: return XtLiveRejectReason::CloseSyncPending;
    case XtRiskReasonCode::InsufficientCloseToday: return XtLiveRejectReason::InsufficientCloseToday;
    case XtRiskReasonCode::InsufficientCloseYesterday: return XtLiveRejectReason::InsufficientCloseYesterday;

    case XtRiskReasonCode::InCallAuctionLimitNotAllowed: return XtLiveRejectReason::InCallAuctionLimitNotAllowed;
    case XtRiskReasonCode::EmptyCode:
    case XtRiskReasonCode::InvalidPriceOrQty: return XtLiveRejectReason::InvalidInput;
    case XtRiskReasonCode::UnknownStrategySid: return XtLiveRejectReason::UnknownStrategySid;
    case XtRiskReasonCode::KillSwitchActive: return XtLiveRejectReason::KillSwitchActive;

    case XtRiskReasonCode::HardCircuitBreakerTripped: return XtLiveRejectReason::HardCircuitBreakerTripped;
    case XtRiskReasonCode::SelfTradeDetected: return XtLiveRejectReason::SelfTradeDetected;
    case XtRiskReasonCode::VelocityLimitExceeded: return XtLiveRejectReason::VelocityLimitExceeded;
    case XtRiskReasonCode::CancelRateLimited: return XtLiveRejectReason::CancelRateLimited;
    case XtRiskReasonCode::DailyLossExceeded: return XtLiveRejectReason::DailyLossExceeded;
    case XtRiskReasonCode::AnnualLossExceeded: return XtLiveRejectReason::AnnualLossExceeded;
    case XtRiskReasonCode::StrategyExposureExceeded: return XtLiveRejectReason::StrategyExposureExceeded;

    case XtRiskReasonCode::StrategyMaxPosQtyExceeded: return XtLiveRejectReason::StrategyMaxPosQtyExceeded;

    case XtRiskReasonCode::StrategyMarginPctExceeded: return XtLiveRejectReason::StrategyMarginPctExceeded;
    case XtRiskReasonCode::StrategyDailyLossExceeded: return XtLiveRejectReason::StrategyDailyLossExceeded;
    case XtRiskReasonCode::StrategyKillSwitchActive: return XtLiveRejectReason::StrategyKillSwitchActive;
    case XtRiskReasonCode::StrategyBlockedByWatchdog: return XtLiveRejectReason::StrategyBlockedByWatchdog;

    case XtRiskReasonCode::SiblingForceFlattenInProgress: return XtLiveRejectReason::SiblingForceFlattenInProgress;
    case XtRiskReasonCode::SymbolBlacklisted: return XtLiveRejectReason::SymbolBlacklisted;
    case XtRiskReasonCode::OrderCountExceeded: return XtLiveRejectReason::OrderCountExceeded;
    case XtRiskReasonCode::OrderRateExceeded: return XtLiveRejectReason::OrderRateExceeded;
    case XtRiskReasonCode::SelfMatchPostTrade: return XtLiveRejectReason::SelfMatchPostTrade;
    case XtRiskReasonCode::SessionClosed: return XtLiveRejectReason::SessionClosed;

    case XtRiskReasonCode::PositionStateUnknown:
        return XtLiveRejectReason::Unknown;

    case XtRiskReasonCode::PriceOutOfDailyLimit:
        return XtLiveRejectReason::Unknown;
    default: return XtLiveRejectReason::Unknown;
    }
}

}
