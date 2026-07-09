#pragma once

#include <cstdint>
#include <string>

enum class XtRiskReasonCode {
    None = 0,

    EmptyCode,
    InvalidPriceOrQty,
    UnknownStrategySid,
    Cooldown,
    RateLimited,
    PriceTickMismatch,
    TradingNotReady,
    CloseoutNoOpen,
    TdRecovering,

    SessionClosed,

    KillSwitchActive,

    SelfTradeDetected,

    VelocityLimitExceeded,

    CancelRateLimited,

    HardCircuitBreakerTripped,

    TdNotInitialized,
    TdCapCloseTodayUnsupported,
    TdCapL2OrderFlowUnsupported,
    TdCapIocFokUnsupported,

    MaxOrderQty,
    MaxOrderNotional,
    MinOrderNotional,
    MaxPosQty,
    ShortNotAllowed,
    InsufficientAvailablePosition,
    InsufficientMargin,

    DailyLossExceeded,

    AnnualLossExceeded,

    StrategyExposureExceeded,

    StrategyMaxPosQtyExceeded,

    StrategyMarginPctExceeded,
    StrategyDailyLossExceeded,
    StrategyKillSwitchActive,

    StrategyBlockedByWatchdog,

    SiblingForceFlattenInProgress,

    SymbolBlacklisted,
    OrderCountExceeded,
    OrderRateExceeded,
    SelfMatchPostTrade,

    PositionSyncStale,
    CloseSyncPending,
    InsufficientCloseToday,
    InsufficientCloseYesterday,

    PositionStateUnknown,

    InCallAuctionLimitNotAllowed,

    PriceOutOfDailyLimit,

    Count
};

enum class XtRiskStage {
    InputGuard,
    CoreRule,
    PositionPrecheck,
    MarginPrecheck,
    CapabilityGuard,
};

const char* risk_reason_text(XtRiskReasonCode code);

const char* risk_stage_text(XtRiskStage stage);

/**
 * @brief 单次风控评估上下文（per-request eval context）。
 *
 * @details 承载单次 evaluate() 调用的时序与行为元信息。
 * 分为两类字段（见注释分区）：
 *   - 当前生效：已被实现层读取，填错会影响风控行为。
 *   - 预留扩展：当前实现层不读取，填任何值均无副作用，仅为未来扩展预留接口。
 */
struct XtRiskEvalContext {

    int64_t now_ms = 0;

    bool run_input_guards = true;

    bool is_forced_close = false;

    bool is_in_call_auction = false;

    bool dry_run = false;

    uint32_t strategy_id = 0;

    int64_t now_ns = 0;
    uint64_t trace_id = 0;
};

/** @brief 风控决策结果（risk decision）。守卫层与规则引擎共用同一类型。
 *
 * @note reason_text 是 reject 路径诊断字段，承载 reason_code 无法重建的动态信息
 *       （涨跌停命中的方向 limit_up/limit_down、平今/平昨变体、gateway-unbound、
 *       caller 自定义 override），故保留 std::string 而非塌缩为 POD。放行路径不构造
 *       字符串；string 成本仅落在 reject 诊断路径。
 */
struct XtRiskResult {
    bool passed = true;
    XtRiskReasonCode reason_code = XtRiskReasonCode::None;
    XtRiskStage stage = XtRiskStage::CoreRule;
    std::string reason_text;
    int64_t required_margin_fp = 0;
    int64_t per_unit_margin_fp = 0;

    int64_t available_cash_fp = 0;
};

using XtRiskPrecheck = XtRiskResult;
