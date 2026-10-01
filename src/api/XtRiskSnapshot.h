#pragma once

#include <cstdint>
#include <type_traits>

/** @brief 风控快照输入（snapshot input）。 */
struct XtRiskSnapshot {
    int64_t cash_available_fp = 0;       ///< 可用资金（available cash, fixed-point）
    /**
     * @brief 当日已实现盈亏（realized P&L，负值表示亏损）。
     *
     * @details 由 guard context 在 make_snapshot() 中填入。
     * 用于 DailyLossExceeded 检查：`today_pnl_fp < -to_fp(max_daily_loss)` 时拒绝开仓。
     * 若 guard context 未配置 pnl 数据源，此字段为 0（跳过检查）。
     */
    int64_t today_pnl_fp = 0;
    int64_t position_qty_fp = 0;         ///< 当前净持仓量（net position, fixed-point）
    int64_t available_close_qty_fp = 0;  ///< 可平仓数量（available close qty, fixed-point）

    /**
     * @brief 当前账户权益（current equity, fixed-point）。
     *
     * @details 用于 AnnualLossExceeded 检查：
     * year_start_equity - equity > year_start_equity * max_annual_loss_pct 时拒绝开仓。
     * 若未填入（0），跳过年度亏损检查。
     */
    int64_t equity_fp = 0;

    /**
     * @brief 本进程累计交易净盈亏（process trading net PnL, fixed-point）。
     *
     * @details = account.realized_pnl − account.fees + account.unrealized_pnl
     *   (GuardContext 从 snapshot_account() 算)。**零 cashflow**（realized/fees/
     *   unrealized 都不含出入金）→ wt 式净值，供 annual-loss 免疫出入金。
     *   XtRiskEngine 加持久化 nmf_offset 得跨重启连续的 net_pnl。
     */
    int64_t process_net_pnl_fp = 0;

    /**
     * @brief 昨仓多头可卖量（A 股 T+1 用，default 0）。
     *
     * @details A 股 T+1 settlement
     * 不允许卖出当日买入仓位 → sell 量必须 ≤ yesterday_long_qty_fp。
     *
     * 期货/加密路径不依赖此字段（XtRiskCfg::enforce_long_t_plus_1=false → bypass）。
     *
     * 填写责任：`xt_risk::project_risk_snapshot` 对所有 venue 无条件填，判据
     * `min(available_sell_total_qty, long_qty − long_today_qty)`——与
     * `xt_account::closeable_view` 的昨仓分量同解。未经该投影的手工快照保 default 0
     * → enforce_long_t_plus_1=true 时 sell 全 reject（fail-safe）。
     */
    int64_t yesterday_long_qty_fp = 0;

    /**
     * @brief 今日涨停价 fp（daily upper limit, fp）。A 股专属。
     *
     * @details A 股 ±10%（普通股）/ ±5%（ST）/ ±20%（创业板/STAR）涨跌停。
     * caller (guard context) 按 `prev_close × (1 + daily_price_limit_pct)`
     * 圆整到 price_tick 计算后填入。
     *
     * 0 = 无限制（期货 CTP 由 broker 推送实际 limit，不走此 check；加密永续无 limit）。
     * 限价单价格 > limit_up_fp → 风控层 PriceOutOfDailyLimit reject，
     * 避免无效 RTT + venue reject_ratio 噪音。
     *
     * 填写责任：guard context make_snapshot() 时按 catalog.daily_price_limit_pct
     * 与 prev_close 计算（pct=0 → 不填 → bypass，期货/加密行为 bit-exact）。
     */
    int64_t limit_up_fp = 0;

    /**
     * @brief 今日跌停价 fp（daily lower limit, fp）。A 股专属。
     *
     * @details 与 limit_up_fp 同源（caller 计算 `prev_close × (1 - pct)`）。
     * 0 = 无限制。限价单价格 < limit_down_fp → 风控层 PriceOutOfDailyLimit reject。
     */
    int64_t limit_down_fp = 0;

    // ── Per-strategy 风控扩展 ──────────────────────────────────────────────
    //
    // 以下字段不由 xt_risk::project_risk_snapshot 填 (default 0),
    // 由 caller (IRiskGuardContext::make_snapshot 实现层) 多源汇编后写入;
    // 漏填字段保 default 0 → fail-safe gate bypass (per-strategy caller-builds 模式)。
    // ─────────────────────────────────────────────────────────────────────

    /**
     * @brief 单策略今日 PnL (realized + unrealized, fp)。
     *
     * @details 用于 StrategyDailyLossExceeded 检查:
     * per_strategy_today_pnl_fp < -per_strategy_daily_loss_pct[sid] × starting_balance_fp。
     * 0 = 跳过 (跟 today_pnl_fp 同语义, fail-safe)。
     *
     * @warning caller MUST 显式 fill: out.per_strategy_today_pnl_fp =
     *   tracker.today_pnl_fp(eval_ctx.strategy_id)。
     *   xt_risk::project_risk_snapshot **不**填本字段。
     */
    int64_t per_strategy_today_pnl_fp = 0;

    /**
     * @brief 账户起始余额, session_begin 当下 freeze (fp)。
     *
     * @details prop firm balance-based DLL 语义: 当日 deposit/withdraw 不计入。
     * caller 走 AccountStateStore.starting_balance_fp() atomic acquire-load 拿最新 freeze 值。
     * 0 = caller 未填或 store 未 freeze (跳过 daily_loss_pct 检查, fail-safe)。
     *
     * @warning caller MUST 显式 fill: out.starting_balance_fp = account_store->starting_balance_fp()。
     *   xt_risk::project_risk_snapshot **不**填本字段 (避免 hazard publish stale race)。
     */
    int64_t starting_balance_fp = 0;

    /**
     * @brief 单 sid kill_switch active 标志。
     *
     * @details caller (XtLiveRiskGuardContext.make_snapshot) 走
     *   `monitor_state->per_strategy.is_strategy_killed(eval_ctx.strategy_id)` 读取。
     *   true → evaluate 立即 reject StrategyKillSwitchActive (vs global KillSwitchActive)。
     *   false (default) = 跳过 (跟 default 字段同语义, fail-safe)。
     *   sid=0 / monitor_state==nullptr → caller 填 false → bypass。
     *
     * @warning caller MUST 显式 fill: out.strategy_killed = monitor_state ?
     *   monitor_state->per_strategy.is_strategy_killed(eval_ctx.strategy_id) : false。
     */
    bool strategy_killed = false;

    // sizeof = 11 int64 + 1 bool + padding = 96；
    // AccountSnapshotTypesTest.PodTypesKeepExpectedLayout 同步断言 96。
};

// co-located ABI tripwire：改本 header 字段即时 trip，对齐 xt_core XtTickBinCodec 的
// sizeof + trivially_copyable 模式。
static_assert(sizeof(XtRiskSnapshot) == 96,
              "XtRiskSnapshot ABI size changed (sync AccountSnapshotTypesTest)");
static_assert(std::is_trivially_copyable<XtRiskSnapshot>::value,
              "XtRiskSnapshot must stay trivially_copyable (cross-module POD)");
