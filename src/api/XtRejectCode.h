#pragma once

namespace xt_runtime {

/// @brief 通用报单拒绝原因码（generic submit reject code）。
///
/// 用于 SubmitResult::reject_reason，对 live/backtest 调用方中立。
/// live 内部专属状态码（TdFrontDisconnected、TdLoginFailed、TdQuery* 等连接/
/// 结算级错误）在 xt_live 内消化，不通过 SubmitResult 向上透出。
enum class XtRejectCode {
    NoError = 0,   ///< 报单已受理，无拒绝（submit accepted, no rejection）
    Unknown = 99,  ///< 未分类拒绝原因（unclassified rejection）

    // ── 输入验证（input validation） ──────────────────────────────────────────────
    InvalidInput,
    UnknownStrategySid,
    InvalidFlag,
    PriceTickMismatch,

    // ── 通道状态（gateway/channel state） ──────────────────────────────────────────────
    TradingNotReady,       ///< 通道未就绪（含 TdRecovering / TdNotInitialized）
    Cooldown,              ///< 报单冷却期（submit cooldown）
    RateLimited,           ///< 速率超限（rate limit exceeded）
    CapabilityUnsupported, ///< 通道能力不支持（如 CloseToday/L2/IOC-FOK）

    // ── 风控（risk） ──────────────────────────────────────────────
    CloseoutNoOpen,
    MaxOrderQty,
    MaxOrderNotional,
    MinOrderNotional,
    PositionLimit,
    ShortNotAllowed,
    InsufficientMargin,
    InsufficientCloseToday,
    InsufficientCloseYesterday,
    InCallAuctionLimitNotAllowed,  ///< A 股集合竞价时段 IOC/FOK 不允许
    PositionSyncStale,
    CloseSyncPending,

    // ── 安全熔断（circuit breaker） ──────────────────────────────────────────────
    KillSwitchActive,
    HardCircuitBreakerTripped, ///< 保留 ordinal + journal replay；运行期不再产生
    SelfTradeDetected,
    VelocityLimitExceeded,     ///< 仓位变化速率超限（qty/window）
    DailyLossExceeded,
    // ── 门禁（gate） ──────────────────────────────────────────────
    Gated,                     ///< 报单门禁关闭（submit gate disabled via param domain）
    OpenHalted,                ///< Active risk monitor 触发开仓硬停（drawdown / stale 等）；平仓仍允许
    // ── offset 路由（offset routing）────────────────────────────────
    OppositeOpenPending,       ///< close_first 品种：对侧在途 Open 未决，本地拒（策略下 tick 重试）

    // ── per-symbol throttle / STP / per-strategy 风控 ──────────────────────────────
    // live-reachable 拒因必须显式映射，避免落入默认桶污染 last_reject_code() 诊断。
    SymbolBlacklisted,         ///< 合约在 per-symbol 黑名单（自成交/超限触发）
    OrderCountExceeded,        ///< 单合约累计订单数超限
    OrderRateExceeded,         ///< 单合约滑动窗口频率超限
    SelfMatchPostTrade,        ///< post-trade 自成交兜底检测
    AnnualLossExceeded,        ///< 年度亏损超 max_annual_loss_pct
    StrategyMaxPosQtyExceeded, ///< 单 sid 持仓 abs 上限
    StrategyDailyLossExceeded, ///< 单 sid 日内亏损 pct 超限
    StrategyKillSwitchActive,  ///< 单 sid kill_switch（vs 全局 KillSwitchActive）
    StrategyBlockedByWatchdog, ///< 保留码；运行期不再产生
    // ── 价格边界（price band）────────────────────────────
    PriceExceedsLimit,         ///< 限价越涨跌停板（strict >upper / <lower）；BT 本地拒，live 由 venue 拒
    // ── 爆仓接管（margin call）────────────────────
    MarginCallLiquidation,     ///< 强平在途，账户被接管；非强平单一律拒（BT margin_call_enabled 专用）
    AutoOffsetUnresolved,      ///< XtOffset::Auto 来自非策略源，提交入口 fail-closed 拒
    // ── 回测 EOF 收尾（backtest end-of-run closeout）────────────────
    BacktestEofClosing,        ///< EOF 收尾栅栏期新提交一律本地拒（BT-only，防合成终态触发重挂遗留活单）
};

} // namespace xt_runtime
