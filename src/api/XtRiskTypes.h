#pragma once

// ---------------------------------------------------------------------------
// api/XtRiskTypes.h — 风控类型 POD 层 (Risk types port layer)
// ---------------------------------------------------------------------------

#include <cstdint>
#include <string>

// ─── Reason code (拒绝原因码) ───────────────────────────────────────────────

enum class XtRiskReasonCode {
    None = 0,

    // InputGuard
    EmptyCode,
    InvalidPriceOrQty,
    UnknownStrategySid,
    Cooldown,
    RateLimited,
    PriceTickMismatch,
    TradingNotReady,
    CloseoutNoOpen,
    TdRecovering,
    // 非活跃时段禁止任何 submit (vs CloseoutNoOpen 仅限开仓分支).
    // 复用 session_gate_sections (YAML runtime.session.active_sections).
    // 阻止策略在夜盘收盘→日盘开盘间歇期反复发单被 broker reject, 消除 risk_block
    // 激增和 reject_ratio 破警戒的误报.
    SessionClosed,

    /**
     * @brief 紧急 kill switch 已激活（kill switch active）。
     *
     * @details [HFT 工业实践]
     * KillSwitch 与 TradingNotReady 的语义区分：
     * - TradingNotReady：系统/session 未就绪（自动恢复）。
     * - KillSwitchActive：人工或程序化紧急停止（必须显式复位才能继续）。
     *
     * KillSwitch 在 precheck_input 中作为**绝对第一检查**执行，
     * 甚至先于 cooldown/rate-limit，确保任何情况下都能立即阻断下单。
     * 对应的 mass cancel（撤所有挂单）由编排层（orchestrator）负责，
     * 不在风控层内执行，遵循单一职责原则。
     */
    KillSwitchActive,

    /**
     * @brief 自成交检测（self-trade prevention, STP）。
     *
     * @details [HFT 工业实践]
     * 同一账户的买卖单在同一价位相撞（自成交）会导致：
     * - 部分交易所罚款或取消资格（CTP 上期所有自成交管控）。
     * - 做市策略的 spread 实质归零（成本损失）。
     * 由 ISTPChecker 实现具体逻辑（pluggable），风控层仅对接接口。
     * 推荐用 XtSymbolPriceBoundSTP 实现（per-symbol 原子量，O(1) 判断）。
     */
    SelfTradeDetected,

    /**
     * @brief 仓位变化速率超限（position velocity limit exceeded）。
     *
     * @details [HFT 工业实践 — Velocity 检测]
     * 滑动时间窗口（如 100ms）内累计下单量超过阈值。
     * 与 RateLimited（orders/sec 频率限制）互补：
     * - RateLimited：控制下单频率（笔数）。
     * - VelocityLimitExceeded：控制仓位变化速率（qty/window），
     *   防止单笔大量订单绕过频率限制造成仓位暴增。
     * 由 XtVelocityLimiter 实现，通过 VelocityLimiterFn 注入 guard context。
     */
    VelocityLimitExceeded,

    /**
     * @brief 取消订单速率超限（cancel rate limited）。
     *
     * @details [HFT 工业实践]
     * 撤单（cancel）请求无限速时，失控策略可以饱和交易所网关的撤单通道，
     * 即使不产生新持仓，也会引发交易所拒绝或封号风险。
     * 撤单速率限制与下单速率限制（RateLimited）独立配置，
     * 因为两者的业务含义和交易所管控维度不同。
     */
    CancelRateLimited,

    /// @deprecated 保留 ordinal 与 replay 文本桶；运行期不再产生。
    ///             进程级硬停止由 kill_switch + SafetyStateStore latch 承担。
    HardCircuitBreakerTripped,

    // CapabilityGuard
    TdNotInitialized,
    TdCapCloseTodayUnsupported,
    TdCapL2OrderFlowUnsupported,
    TdCapIocFokUnsupported,

    // CoreRule
    MaxOrderQty,
    MaxOrderNotional,
    MinOrderNotional,
    MaxPosQty,
    ShortNotAllowed,
    InsufficientAvailablePosition,
    InsufficientMargin,

    /**
     * @brief 日内亏损超限（daily loss exceeded）。
     *
     * @details 对应 XtRiskCfg::max_daily_loss。
     * 仅拦截开仓；平仓单始终放行，以便策略能止损出场。
     */
    DailyLossExceeded,

    /**
     * @brief 年度亏损超限（annual loss exceeded）。
     *
     * @details 对应 XtRiskCfg::max_annual_loss_pct。
     * 年内权益回撤超过 year_start_equity * max_annual_loss_pct 时触发。
     * 仅拦截开仓；平仓单始终放行，以便策略能止损出场。
     * 一旦触发锁定状态，年内不再恢复（即使后续权益回升）。
     */
    AnnualLossExceeded,

    /// @deprecated 保留 ordinal 与 reject log replay bucket；运行期不再产生。
    ///             不加 [[deprecated]] attribute——from_risk/risk_reason_text 仍保留
    ///             case 处理 replay，加 attribute 会产生编译警告。
    StrategyExposureExceeded,

    // ── Per-strategy 风控扩展 ──────────────────────────────────────────────
    StrategyMaxPosQtyExceeded,        ///< 单 sid 持仓 abs 上限 (跟全局 MaxPosQty 区分)
    /// @deprecated 保留 ordinal 与 journal/log replay bucket；运行期不再产生。
    StrategyMarginPctExceeded,
    StrategyDailyLossExceeded,        ///< 单 sid 日内亏损 pct 超 starting_balance × per_strategy_daily_loss_pct
    StrategyKillSwitchActive,         ///< 单 sid kill_switch (vs global KillSwitchActive)
    /// @deprecated 保留 ordinal 与 journal/log replay bucket；运行期不再产生。
    StrategyBlockedByWatchdog,
    /// @deprecated 保留 ordinal 与 journal/log replay bucket；运行期不再产生。
    SiblingForceFlattenInProgress,

    // per-symbol blacklist
    SymbolBlacklisted,      ///< 合约已在黑名单（violated 后冻结）
    OrderCountExceeded,     ///< 单合约累计订单数超限（触发 blacklist）
    OrderRateExceeded,      ///< 单合约滑动窗口频率超限（触发 blacklist）
    SelfMatchPostTrade,     ///< post-trade 自成交检测（仅 insert_blacklist 用 reason_tag）

    // PositionPrecheck
    PositionSyncStale,
    CloseSyncPending,
    InsufficientCloseToday,
    InsufficientCloseYesterday,
    // account_store could not confirm authoritative
    // closeable today/yesterday split (e.g. CTP ReqQryInvestorPosition
    // unconfirmed at bootstrap) AND operator policy is reject-not-fallback
    // (Config::reject_on_position_unknown=true). Distinct from
    // PositionSyncStale (timestamp-based staleness) — this is a
    // never-confirmed condition, not a went-stale condition.
    PositionStateUnknown,

    /**
     * @brief 集合竞价时段限价单 reject (A 股专属)。
     *
     * @details A 股 9:15-9:25 / 14:57-15:00 集合竞价时段不接受限价 LIMIT 之外的
     * 时效（IOC/FOK 在集合竞价无意义）。strategy 提交 IOC/FOK 在该时段会被 venue
     * 直接 reject — 这里在 risk 层提前拦截避免无效 RTT。
     * 触发条件：XtRiskEvalContext::is_in_call_auction=true && tif != GTC（即非限价）。
     */
    InCallAuctionLimitNotAllowed,

    /**
     * @brief 限价单价格触及涨跌停 (price out of daily limit, A 股专属)。
     *
     * @details A 股涨跌停 ±10%（普通股）/ ±5%（ST）/ ±20%（创业板/STAR）。
     * 限价单价格超 limit_up_fp 或低于 limit_down_fp → venue 直接 reject。
     * 风控层提前拦截避免无效 RTT + reject_ratio 噪音。
     *
     * 填写责任：caller 在 XtRiskSnapshot::limit_up_fp / limit_down_fp 填实
     * （由 XtCommodityInfo::daily_price_limit_pct + prev_close 计算）。
     * 0 = bypass（期货 CTP / 加密永续 default，行为不变 bit-exact）。
     */
    PriceOutOfDailyLimit,

    /// @brief 枚举计数 sentinel（非真实 reason）。
    /// 仅供穷尽性测试迭代 [0, Count)；不参与 from_risk / risk_reason_text 映射。
    Count
};

// ─── Reject stage (拒绝阶段) ────────────────────────────────────────────────

enum class XtRiskStage {
    InputGuard,
    CoreRule,
    PositionPrecheck,
    MarginPrecheck,
    CapabilityGuard,
};

/** @brief 风控拒绝原因文本（stable reason text）。实现在 xt_risk/XtRiskEngine.cpp。 */
const char* risk_reason_text(XtRiskReasonCode code);
/** @brief 风控决策阶段文本（stable stage text）。实现在 xt_risk/XtRiskEngine.cpp。 */
const char* risk_stage_text(XtRiskStage stage);

// ─── Risk evaluation context (单次评估上下文) ────────────────────────────────

/**
 * @brief 单次风控评估上下文（per-request eval context）。
 *
 * @details 承载单次 evaluate() 调用的时序与行为元信息。
 * 分为两类字段（见注释分区）：
 *   - 当前生效：已被实现层读取，填错会影响风控行为。
 *   - 预留扩展：当前实现层不读取，填任何值均无副作用，仅为未来扩展预留接口。
 */
struct XtRiskEvalContext {
    // ─── 当前生效字段 ─────────────────────────────────────────────────────────

    /**
     * @brief 评估时间戳（ms）。
     *
     * @details 用于 cooldown 冷却窗口、order rate limit、velocity 滑窗的时间比较。
     * 精度约束：
     *   - 实盘（live）：来自 XtSteadyClock，CTP 网关侧 ms 级精度。
     *   - 回测（backtest）：合成编码 YYYYMMDD×1e9+HHMMSSmmm，为单调递增整数，
     *     可直接与 live 时间戳比较大小（回测内部语义一致即可）。
     *
     * [升级路径] 更高精度时钟源（RDTSC/PTP/FPGA）接入后，
     * 使用预留扩展字段 now_ns 替换本字段（见下方预留扩展区）。
     */
    int64_t now_ms = 0;

    /**
     * @brief 是否执行限频类 input guards（rate/cooldown/velocity/STP 等）。
     *
     * @details
     * - true（默认）：完整执行所有 input guards。
     * - false：跳过限频/冷却/STP，适用于拆单后续切片（前一切片已通过输入门禁）。
     *
     * 注意：无论此字段取何值，kill switch 和 hard circuit breaker 始终执行。
     */
    bool run_input_guards = true;

    /**
     * @brief 强制平仓通道标志（forced liquidation path）。
     *
     * @details
     * true 时绕过以下限制，确保仓位能在任何风控状态下平出：
     *   - cooldown 冷却窗口
     *   - order rate limit（下单频率限制）
     *   - velocity limiter（仓位速率限制）
     *   - DailyLossExceeded（日内亏损超限；平仓单必须放行，否则策略困死仓）
     *
     * 以下检查**不受影响**（仍完整执行）：
     *   - kill switch / hard circuit breaker（绝对优先，永不跳过）
     *   - 参数合法性（code/price/qty）
     *   - TradingNotReady / TdRecovering
     *   - 通道能力（capability guard）
     *   - 可平量与保证金（guard_before_submit）
     *
     * 典型场景：日内亏损触发强制减仓、清仓通道、紧急平仓指令。
     *
     * 实现：
     *   - XtLiveRiskGuardContext::precheck_input 根据此标志跳过 cooldown/rate/velocity。
     *   - XtRiskGateway::evaluate 据此将 XtRiskPolicy::enforce_daily_loss 置为 false。
     *
     * 注意：is_forced_close 与 dry_run 互斥，同时为 true 时 is_forced_close 优先。
     */
    bool is_forced_close = false;

    /**
     * @brief A 股集合竞价时段标志 (call auction)。
     *
     * @details
     * - true：当前在 9:15-9:25（开盘集合竞价）或 14:57-15:00（收盘集合竞价）窗口。
     *   IOC/FOK 时效单会被 venue reject — 提前拦截。
     * - false（默认）：连续竞价时段，无限制。
     *
     * 填写责任：caller (XtLiveRiskGuardContext) 按 session schedule 推断；
     * 期货/加密路径无意义保持 false。
     */
    bool is_in_call_auction = false;

    /**
     * @brief 干跑模式（dry run / pre-flight check）。
     *
     * @details
     * true 时执行完整风控评估，但**不消耗**以下状态资源：
     *   - cooldown slot（冷却窗口不推进）
     *   - order rate limit slot（频率计数器不递增）
     *   - velocity limiter 滑窗（累计量不增加）
     *
     * 用途：策略在真正下单前做 "能否下单" 的预判，不污染实际限速状态。
     * 纯规则评估（max_order_qty、max_pos_qty、DailyLoss 等）仍完整执行。
     *
     * 实现：XtLiveRiskGuardContext::precheck_input 根据此标志跳过消耗型调用。
     */
    bool dry_run = false;

    /**
     * @brief 策略标识（per-strategy 规则用，0 = no-sid/single-strategy，全部 bypass）。
     *
     * @details evaluate() 在 strategy_id > 0 时执行 per-strategy 规则：
     * max_pos_qty（per_strategy_max_pos_qty）、daily_loss
     * （per_strategy_daily_loss_pct × starting_balance）、kill_switch
     * （snapshot.strategy_killed）。
     */
    uint32_t strategy_id = 0;

    // ─── 预留扩展字段 ──────────────────────────────────────────────
    // @note 以下字段实现层不读取，填入任何值不影响现有逻辑，仅预留接口。

    int64_t now_ns = 0;        ///< @note 预留字段（ns 级时钟升级路径）
    uint64_t trace_id = 0;     ///< @note 预留字段（全链路审计追踪）
};

// ─── Risk decision result (风控决策结果) ────────────────────────────────────

/** @brief 风控决策结果（risk decision）。守卫层与规则引擎共用同一类型。
 *
 * @note reason_text 是 reject 路径诊断字段，承载 reason_code 无法重建的动态信息
 *       （涨跌停命中的方向 limit_up/limit_down、平今/平昨变体、gateway-unbound、
 *       caller 自定义 override），故保留 std::string 而非塌缩为 POD。放行路径不构造
 *       字符串；string 成本仅落在 reject 诊断路径。
 */
struct XtRiskResult {
    bool passed = true;                                       ///< 是否通过风控（true = 放行）
    XtRiskReasonCode reason_code = XtRiskReasonCode::None;    ///< 拒绝原因码（reason code）
    XtRiskStage stage = XtRiskStage::CoreRule;                ///< 拒绝发生的阶段（reject stage）
    std::string reason_text;                                  ///< 可读拒绝描述（reject 路径动态诊断；见类注释）
    int64_t required_margin_fp = 0;                           ///< 本单所需保证金（required margin, fp）
    int64_t per_unit_margin_fp = 0;                           ///< 每手保证金（per-unit margin, fp）
    /// 当时账户可用现金（available cash, fp）。仅 MarginPrecheck 路径填充, 其它
    /// 阶段为 0 (== 不可信)。 用于 reject log 显式打印 "available X < required Y"
    /// 让 oncall 一眼诊断 wallet starvation vs. 保证金率配置错误等场景。
    int64_t available_cash_fp = 0;
};

// 守卫层（precheck）与规则引擎（evaluate）返回相同结构，保留别名以保持调用侧可读性。
using XtRiskPrecheck = XtRiskResult;
