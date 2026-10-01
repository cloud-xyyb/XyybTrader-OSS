#pragma once

#include "api/XtLiveRejectReason.h"
#include "xt_risk/engine/XtRiskEngine.h"

/// @file
/// @brief risk reason 到 live reject bucket 的稳定映射。

namespace live_reject_reason {

/// @brief 将风控核心 reason 映射为 live submit reject 统计桶。
///
/// @note 这里保持观测口径稳定；少数新 risk reason 暂无专用 live bucket 时，
///       会显式映射到既有 bucket 或 Unknown，并在对应 case 旁说明原因。
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
    // A 股 T+1 可卖量不足沿用 PositionLimit bucket，避免拆出新 live 统计口径。
    case XtRiskReasonCode::InsufficientAvailablePosition: return XtLiveRejectReason::PositionLimit;
    case XtRiskReasonCode::PositionSyncStale: return XtLiveRejectReason::PositionSyncStale;
    case XtRiskReasonCode::CloseSyncPending: return XtLiveRejectReason::CloseSyncPending;
    case XtRiskReasonCode::InsufficientCloseToday: return XtLiveRejectReason::InsufficientCloseToday;
    case XtRiskReasonCode::InsufficientCloseYesterday: return XtLiveRejectReason::InsufficientCloseYesterday;
    // enum 名保持外部口径；实际拦截集合竞价 IOC/FOK，GTC LIMIT 仍允许。
    case XtRiskReasonCode::InCallAuctionLimitNotAllowed: return XtLiveRejectReason::InCallAuctionLimitNotAllowed;
    case XtRiskReasonCode::EmptyCode:
    case XtRiskReasonCode::InvalidPriceOrQty: return XtLiveRejectReason::InvalidInput;
    case XtRiskReasonCode::UnknownStrategySid: return XtLiveRejectReason::UnknownStrategySid;
    case XtRiskReasonCode::KillSwitchActive: return XtLiveRejectReason::KillSwitchActive;
    // 兼容 replay payload；live submit path 不 emit 该 risk reason。
    case XtRiskReasonCode::HardCircuitBreakerTripped: return XtLiveRejectReason::HardCircuitBreakerTripped;
    case XtRiskReasonCode::SelfTradeDetected: return XtLiveRejectReason::SelfTradeDetected;
    case XtRiskReasonCode::VelocityLimitExceeded: return XtLiveRejectReason::VelocityLimitExceeded;
    case XtRiskReasonCode::CancelRateLimited: return XtLiveRejectReason::CancelRateLimited;
    case XtRiskReasonCode::DailyLossExceeded: return XtLiveRejectReason::DailyLossExceeded;
    case XtRiskReasonCode::AnnualLossExceeded: return XtLiveRejectReason::AnnualLossExceeded;
    case XtRiskReasonCode::StrategyExposureExceeded: return XtLiveRejectReason::StrategyExposureExceeded;
    // per-strategy 风控扩展: 6 reason code mapping
    case XtRiskReasonCode::StrategyMaxPosQtyExceeded: return XtLiveRejectReason::StrategyMaxPosQtyExceeded;
    // 兼容 replay payload；live submit path 不 emit 该 risk reason。
    case XtRiskReasonCode::StrategyMarginPctExceeded: return XtLiveRejectReason::StrategyMarginPctExceeded;
    case XtRiskReasonCode::StrategyDailyLossExceeded: return XtLiveRejectReason::StrategyDailyLossExceeded;
    case XtRiskReasonCode::StrategyKillSwitchActive: return XtLiveRejectReason::StrategyKillSwitchActive;
    case XtRiskReasonCode::StrategyBlockedByWatchdog: return XtLiveRejectReason::StrategyBlockedByWatchdog;
    // 兼容 replay payload；live submit path 不 emit 该 risk reason。
    case XtRiskReasonCode::SiblingForceFlattenInProgress: return XtLiveRejectReason::SiblingForceFlattenInProgress;
    case XtRiskReasonCode::SymbolBlacklisted: return XtLiveRejectReason::SymbolBlacklisted;
    case XtRiskReasonCode::OrderCountExceeded: return XtLiveRejectReason::OrderCountExceeded;
    case XtRiskReasonCode::OrderRateExceeded: return XtLiveRejectReason::OrderRateExceeded;
    case XtRiskReasonCode::SelfMatchPostTrade: return XtLiveRejectReason::SelfMatchPostTrade;
    case XtRiskReasonCode::SessionClosed: return XtLiveRejectReason::SessionClosed;
    // CTP bootstrap 可平口径未分配专用 live bucket，统一归 Unknown。
    case XtRiskReasonCode::PositionStateUnknown:
        return XtLiveRejectReason::Unknown;
    // 涨跌停拒单 live enum 无专用 bucket（live 由 venue 拒，本地不产生）；保持 Unknown。
    // BT 侧的策略可见映射走 gateway 直返 XtRejectCode::PriceExceedsLimit，不经本函数。
    case XtRiskReasonCode::PriceOutOfDailyLimit:
        return XtLiveRejectReason::Unknown;
    default: return XtLiveRejectReason::Unknown;
    }
}

} // namespace live_reject_reason
