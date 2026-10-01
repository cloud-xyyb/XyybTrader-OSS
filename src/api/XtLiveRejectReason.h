#pragma once

// ---------------------------------------------------------------------------
// api/XtLiveRejectReason.h — Live 拒单原因码 (port layer enum)
// ---------------------------------------------------------------------------

#include <cstddef>

/// @brief CT 实盘订单/撤单 reject 原因码（submit/cancel reject reason）。
///
/// @note 这些是在 submit/cancel 热路径中产生的拒单原因，进入 OrderRtn.err_msg
///       以及 reject 统计计数器。与 recovery 路径内部错误（XtLiveRecoveryError）严格区分。
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

    TdFrontDisconnected,    ///< 订单在 front 断时 submit 被拒（submit 路径 reject，非 recovery error）
    TdLoginFailed,          ///< 订单在 login 失败时 submit 被拒（同上）
    PositionSyncStale,
    CloseSyncPending,
    InsufficientCloseToday,
    InsufficientCloseYesterday,

    KillSwitchActive,           ///< 紧急 kill switch 激活（人工/程序化紧急停止）
    HardCircuitBreakerTripped,  ///< 保留 ordinal + journal replay；运行期不再产生
    SelfTradeDetected,          ///< 自成交防护（STP）拒绝
    VelocityLimitExceeded,      ///< 仓位变化速率超限（qty/window）
    DailyLossExceeded,          ///< 日内亏损超过 max_daily_loss 限额
    GateDisabled,               ///< 报单门禁关闭（param domain "trading.submit.enabled"=false）
    SymbolBlacklisted,          ///< 合约已在 per-symbol 黑名单
    OrderCountExceeded,         ///< 单合约累计订单数超限（触发黑名单）
    OrderRateExceeded,          ///< 单合约滑动窗口频率超限（触发黑名单）
    SelfMatchPostTrade,         ///< post-trade self-match 兜底检测触发
    OpenHalted,                 ///< Active risk monitor 触发开仓硬停（drawdown/stale）
    AnnualLossExceeded,         ///< 年度亏损超过 max_annual_loss_pct 限额
    StrategyExposureExceeded,   ///< 保留 ordinal + log replay；运行期不再产生
    SessionClosed,              ///< 非活跃时段 submit 被拒 (夜盘/日盘间歇期)
    InCallAuctionLimitNotAllowed, ///< A 股集合竞价时段 IOC/FOK 不允许（仅限价 GTC 通过）

    // ── Per-strategy 风控扩展 ──────────────────────────────────────────
    StrategyMaxPosQtyExceeded,    ///< 单 sid max_pos_qty 上限
    StrategyMarginPctExceeded,    ///< 保留 ordinal + log replay；运行期不再产生
    StrategyDailyLossExceeded,    ///< 单 sid 日内亏损 pct 超 starting_balance × per_strategy_daily_loss_pct
    StrategyKillSwitchActive,     ///< 单 sid kill_switch (vs global KillSwitchActive)
    StrategyBlockedByWatchdog,    ///< 保留码；运行期不再产生
    SiblingForceFlattenInProgress,///< 保留码；运行期不再产生

    // ── offset 路由 ────────────────────────────────
    OppositeOpenPending,          ///< close_first 品种：对侧在途 Open 未决，本地拒（策略下 tick 重试）
    AutoOffsetUnresolved,         ///< XtOffset::Auto 来自非策略源（Manual/Closeout/Recovery），无解析语义 fail-closed 拒

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
    // Per-strategy 风控扩展
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
