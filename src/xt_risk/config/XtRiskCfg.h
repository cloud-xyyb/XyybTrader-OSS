#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

/// @file
/// @brief `XtRiskEngine` 与 live/backtest guard 共用的风控配置结构。
///
/// 数值字段使用配置层的单位，engine 装载后转换为 fixed-point cache；
/// 除字段注释另有说明外，0 或空 map 表示对应规则关闭。

/// @brief close quota 双源取严的 disagreement 容忍阈值（1 lot fp）。
/// 过滤 broker frozen push 与本地 in-flight 累计的毫秒级同步抖动；
/// live / backtest guard 共用同一口径（SSOT，勿在 guard 内重复定义）。
inline constexpr int64_t kFrozenDisagreementEpsilonFp = 1'000'000LL;

/// @brief 风控规则配置。
///
/// 字段由配置解析层填充；`XtRiskEngine` 读取 sid_id keyed map，`*_by_sid_string`
/// 仅作为 YAML strategy id 翻译前的 staging map。
struct XtRiskCfg {
    double max_order_qty         = 0.0;  ///< 单笔最大数量。
    double max_order_notional    = 0.0;  ///< 单笔最大名义金额。
    double min_order_notional    = 0.0;  ///< 增仓最小名义金额；减仓/reduce-only 豁免。
    double max_pos_qty           = 0.0;  ///< 单品种净持仓绝对值上限。
    bool   allow_short           = true; ///< 是否允许净空头。

    /// @brief 启用 A 股 T+1 长仓约束：sell qty <= yesterday_long_qty_fp。
    /// @note 依赖 XtRiskSnapshot::yesterday_long_qty_fp；缺失时按 0 处理并保守拒卖。
    bool   enforce_long_t_plus_1 = false;

    /// @brief 开仓日内亏损绝对值上限；平仓路径不拦截。
    /// @note 触发条件为 snapshot.today_pnl_fp < -to_fp(max_daily_loss)。
    double max_daily_loss        = 0.0;

    /// @brief 开仓年度亏损比例上限；0.10 表示 10%。
    /// @note 依赖 XtRiskEngine annual latch；未通过 on_year_change()/restore arm 时规则 bypass。
    double max_annual_loss_pct   = 0.0;

    /// @brief sid_id -> 单品种净持仓绝对值上限。
    /// @note Engine 比较账户级 symbol 净仓；live profile 不支持同账户多策略同 symbol，
    ///       启动期 validator 负责拒绝破坏 1:1 sid:symbol 的配置。未配置 sid 回退全局上限。
    std::unordered_map<uint32_t, double> per_strategy_max_pos_qty;

    /// @brief YAML staging：strategy id 字符串 -> per-sid max_pos_qty。
    /// @note Runner boot 期翻译到 per_strategy_max_pos_qty；XtRiskEngine 不读取此字段。
    std::unordered_map<std::string, double> per_strategy_max_pos_qty_by_sid_string;

    // ── per-symbol throttle / blacklist ───────────────────────────────────
    /// @brief 单合约 session 内累计订单数上限。
    /// @note 超限时仅非减仓订单触发拒单和黑名单；减仓订单豁免以避免困仓。
    uint64_t max_order_count_per_symbol = 0;

    /// @brief 单合约滑动窗口订单频率上限，单位 orders/window。
    /// @note 使用 XtRiskEvalContext::now_ms；超限处理同 max_order_count_per_symbol。
    uint64_t max_order_rate_per_symbol = 0;

    /// @brief max_order_rate_per_symbol 的滑动窗口长度，单位 ms。
    uint32_t order_rate_window_ms = 10000;

    /// @brief session_begin 时是否保留黑名单（默认 false = 清空）。
    bool blacklist_persist_across_session = false;

    // ── per-strategy 风控扩展 ──────────────────────────────────────────────
    /// @brief sid_id -> 单策略日内亏损比例上限。
    /// @note 触发条件为 per_strategy_today_pnl < -pct * starting_balance；engine 只拒单。
    std::unordered_map<uint32_t, double> per_strategy_daily_loss_pct;

    /// @brief YAML staging：strategy id 字符串 -> per_strategy_daily_loss_pct。
    /// @note Runner boot 期翻译到 per_strategy_daily_loss_pct；XtRiskEngine 不读取此字段。
    std::unordered_map<std::string, double> per_strategy_daily_loss_pct_by_sid_string;

    /// @brief 保留字段：是否允许多策略声明同一 symbol。
    /// @note live 配置 schema 不开放该 key；同 symbol 多 sid 启动期 fail-closed。
    bool allow_strategy_symbol_overlap = false;

    // ── per-strategy 主动强平（auto-liquidation，reject-only daily-loss 之上的平仓层）──
    /// @brief 总开关：per-sid 自动强平。false 时 PerSidLiquidator 整段 no-op（默认关）。
    bool per_strategy_auto_liquidate = false;

    /// @brief dry-run：评估+latch+log 但不发强平单（0 订单 / 0 order-book 变更）。
    bool per_strategy_auto_liquidate_dry_run = false;

    /// @brief sid_id -> 自动强平硬阈值比例。
    /// @note 触发条件 (today_realized + unrealized) < -pct * starting_balance；
    ///       与 reject-only daily-loss 同公式但含 unrealized（mark-to-market 主动层）。
    std::unordered_map<uint32_t, double> per_strategy_liquidate_pct;

    /// @brief YAML staging：strategy id 字符串 -> per_strategy_liquidate_pct。
    /// @note Runner boot 期翻译到 per_strategy_liquidate_pct；PerSidLiquidator 不读取此字段。
    std::unordered_map<std::string, double> per_strategy_liquidate_pct_by_sid_string;
};

/// @brief 是否配置了任一生效（pct > 0）的 per-strategy 日内亏损上限。
///
/// 同时检查已翻译的 sid_id keyed map 与 YAML staging（string keyed）map——boot 期
/// 翻译可能晚于某些 wiring 调用点，任一有正阈值即视为启用。caller（PnL state
/// fail-closed 决策）据此判定「损坏的风控输入是否必须阻断启动」。
inline bool has_active_per_strategy_daily_loss(const XtRiskCfg& cfg) {
    for (const auto& kv : cfg.per_strategy_daily_loss_pct)
        if (kv.second > 0.0) return true;
    for (const auto& kv : cfg.per_strategy_daily_loss_pct_by_sid_string)
        if (kv.second > 0.0) return true;
    return false;
}
