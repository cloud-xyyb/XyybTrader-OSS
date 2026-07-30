#pragma once

namespace xt_runtime {

enum class XtRejectCode {
    NoError = 0,
    Unknown = 99,

    InvalidInput,
    UnknownStrategySid,
    InvalidFlag,
    PriceTickMismatch,

    TradingNotReady,
    Cooldown,
    RateLimited,
    CapabilityUnsupported,

    CloseoutNoOpen,
    MaxOrderQty,
    MaxOrderNotional,
    MinOrderNotional,
    PositionLimit,
    ShortNotAllowed,
    InsufficientMargin,
    InsufficientCloseToday,
    InsufficientCloseYesterday,
    InCallAuctionLimitNotAllowed,
    PositionSyncStale,
    CloseSyncPending,

    KillSwitchActive,
    HardCircuitBreakerTripped,
    SelfTradeDetected,
    VelocityLimitExceeded,
    DailyLossExceeded,

    Gated,
    OpenHalted,

    OppositeOpenPending,

    SymbolBlacklisted,
    OrderCountExceeded,
    OrderRateExceeded,
    SelfMatchPostTrade,
    AnnualLossExceeded,
    StrategyMaxPosQtyExceeded,
    StrategyDailyLossExceeded,
    StrategyKillSwitchActive,
    StrategyBlockedByWatchdog,

    PriceExceedsLimit,

    MarginCallLiquidation,
    AutoOffsetUnresolved,

    BacktestEofClosing,
};

}
