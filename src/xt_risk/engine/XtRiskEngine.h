#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "xt_core/order/XtOrder.h"
#include "api/XtRiskSnapshot.h"
#include "api/XtRiskTypes.h"
#include "xt_risk/config/XtRiskCfg.h"

/// @brief 控制单次风控评估启用哪些核心规则。
///
/// 默认值对应常规 submit 路径；gateway 可在强平、dry-run 等路径按调用语义派生
/// 局部 policy，而不修改引擎配置。
struct XtRiskPolicy {
    bool enforce_position    = true;  ///< 仓位上限与做空规则。
    bool enforce_short_t1    = true;  ///< A 股/短仓 T+1 规则。
    /// A 股 stock-mode 现金占用检查（notional+fee）。不经 yaml 暴露（无解析入口，
    /// 仅代码/测试赋值）；期货/加密资金门禁在 guard 层且恒生效，不受此开关影响。
    bool enforce_margin      = true;
    bool enforce_daily_loss  = true;  ///< 开仓日内止损；强平路径由 gateway 关闭。
    bool enforce_annual_loss = true;  ///< 开仓年度止损；强平路径由 gateway 关闭。
};

#include "api/IRiskGuardContext.h"

class IFeeRuleProvider;
class XtCommodityCatalog;
struct XtCommodityInfo;

/// @brief 同步 pre-trade 风控规则引擎。
///
/// `evaluate()` 消费订单、`XtRiskSnapshot` 与 `XtRiskPolicy`，返回首个命中的
/// `XtRiskResult`。引擎持有配置快照、per-symbol 计数/黑名单和年度止损 latch；
/// input/capability/submit-resource 等运行态检查由 gateway guard 提供。
class XtRiskEngine {
public:
    /// @brief 构造期一次性注入的外部只读依赖。
    struct InitDeps {
        InitDeps() noexcept
            : fee_provider(nullptr),
              commodity_catalog(nullptr) {}
        InitDeps(const IFeeRuleProvider* fee_provider_in,
                 const XtCommodityCatalog* commodity_catalog_in) noexcept
            : fee_provider(fee_provider_in),
              commodity_catalog(commodity_catalog_in) {}

        const IFeeRuleProvider* fee_provider;       ///< stock mode BUY 现金占用费率源。
        const XtCommodityCatalog* commodity_catalog; ///< stock mode 判定与合约属性源。
    };

    explicit XtRiskEngine(const XtRiskCfg& cfg, InitDeps deps = InitDeps());

    /// @brief 线程安全替换运行配置，并刷新 fixed-point cache。
    void set_config(const XtRiskCfg& cfg);

    /// @brief 返回线程安全配置快照；返回 by value 避免锁外引用 cfg_。
    XtRiskCfg config() const;

    /// @brief 年份切换时冻结年度止损基准，并清除 same-year latch。
    /// @param year 当前年份；与 current_year_ 相同则 no-op。
    /// @param equity_fp 年初权益基准（fixed-point）。
    /// @param process_net_pnl_fp 年切时的 process_net_pnl，冻结为 year_start_net_pnl anchor。
    void on_year_change(int year, int64_t equity_fp, int64_t process_net_pnl_fp);

    /// @brief 查询并 latch 年度亏损是否超限。
    /// @param process_net_pnl_fp 本进程累计交易净盈亏（realized - fees + unrealized）。
    /// @return true 表示年度亏损已超限，新开仓应被拒绝。
    bool is_annual_loss_exceeded(int64_t process_net_pnl_fp);

    // ── annual latch persistence accessors ─────────────────────────────────

    /// @brief 正在跟踪的年份。0 = 未初始化或未恢复有效年份。
    int current_year() const;

    /// @brief 年初权益基准 fp。0 = 未初始化 → `is_annual_loss_exceeded` 始终 bypass。
    int64_t year_start_equity_fp() const;

    /// @brief 年度熔断是否已 latched（一旦 true 在 same-year 内不自动清除）。
    bool annual_cutoff_active() const;

    /// @brief 年切冻结的 net_pnl anchor（wt 净值模型）。
    int64_t year_start_net_pnl_fp() const;

    /// @brief 跨重启 cum(realized - fees) offset，用于抵消 Account 重启归零。
    int64_t nmf_offset_fp() const;

    /// @brief 启动期恢复 SafetyStateStore 持久化的 annual latch。
    /// @note year <= 0 或 year_start_equity_fp <= 0 时 no-op；运行期用 on_year_change()/is_annual_loss_exceeded()。
    /// @param year 持久化年份（YYYY）。
    /// @param year_start_equity_fp 持久化年初权益基准。
    /// @param cutoff_active 持久化年度熔断 latch。
    /// @param year_start_net_pnl_fp 持久化年切净盈亏基准。
    /// @param nmf_offset_fp 持久化跨重启净盈亏 offset。
    void restore_annual_state(int year, int64_t year_start_equity_fp, bool cutoff_active,
                              int64_t year_start_net_pnl_fp, int64_t nmf_offset_fp);

    /// @brief annual latch 复合快照；单次 shared_lock 临界区读出 5 字段。
    /// @note 持久化采样必须用本方法；逐个调散 getter 的组合读可与
    ///       on_year_change/restore/latch 推进交错，产生字段不一致的复合快照。
    struct AnnualLatchSnapshot {
        int     current_year          = 0;
        int64_t year_start_equity_fp  = 0;
        bool    annual_cutoff_active  = false;
        int64_t year_start_net_pnl_fp = 0;
        int64_t nmf_offset_fp         = 0;
    };
    AnnualLatchSnapshot annual_latch_snapshot() const;

    /// @brief 已配置 per-strategy daily-loss 规则但 session baseline 未冻结的评估次数。
    ///
    /// `XtRiskSnapshot::starting_balance_fp == 0` 表示 session 起点权益尚未冻结，
    /// 此时 daily-loss 阈值无分母、检查被跳过（放行，避免 bootstrap 时序窗口误伤）。
    /// 该计数把「规则已配置却在放行」与「规则未配置」区分开：持续增长即规则静默失效。
    [[nodiscard]] uint64_t per_strategy_daily_loss_baseline_missing_count() const noexcept;

    /// @brief 同步执行规则引擎风控评估。
    ///
    /// 只消费订单、快照和 policy，不访问 gateway/account runtime。提交前资源门禁
    /// （可平量、在途占用、现金/保证金实时扣减）由 IRiskGuardContext 负责。
    ///
    /// @warning 非 dry-run 调用会推进 per-symbol count/rate 状态，并可能 latch
    ///          annual cutoff 或 blacklist。
    XtRiskResult evaluate(const xt_core::XtOrder& order,
                          const XtRiskSnapshot& snapshot,
                          const XtRiskPolicy& policy = XtRiskPolicy{});

    /// @brief 带单次评估上下文的入口；now_ms、strategy_id 和 dry_run 参与状态型规则。
    XtRiskResult evaluate(const xt_core::XtOrder& order,
                          const XtRiskSnapshot& snapshot,
                          const XtRiskPolicy& policy,
                          const XtRiskEvalContext& eval_ctx);

    // ── per-symbol blacklist API ─────────────────────────────────────────
    /// @brief 外部检测点将合约加入黑名单，后续非减仓订单在 CoreRule 阶段拒绝。
    /// @param code 合约代码。
    /// @param reason_tag 预留给日志/指标标签；规则判定不依赖该值。
    void insert_blacklist(const char* code, const char* reason_tag);

    /// @brief 查询合约是否在黑名单。
    bool is_blacklisted(const char* code) const;

    /// @brief Session 切换时清空 order count/rate window，并按配置处理黑名单。
    void on_session_begin(uint32_t trading_date);

private:
    /// @brief 从 cfg_ 预转换 fixed-point cache；caller 已持 cfg_mu_。
    void refresh_fp_cache();

    /// @brief 黑名单查询的内部版本；caller 已持 cfg_mu_，避免重入 shared_mutex。
    bool is_blacklisted_unlocked_(const char* code) const;

    /// @brief 年度止损查询的内部版本；caller 已持 unique_lock。
    bool is_annual_loss_exceeded_unlocked_(int64_t process_net_pnl_fp);

    // ── evaluate() compose-method 拆分 ─────────────────────────────────────
    /// evaluate() 是 reject pipeline，按以下顺序逐段检查；每个 helper 返回
    /// nullopt = 放行继续，有值 = 该段命中的 reject 结果（顺序即优先级，不可重排）。
    /// @pre 除 eval_input_guard_（锁外）外，caller 持 cfg_mu_ unique_lock —— helper 读
    ///      cfg_/fp cache/blacklist；throttle/open_loss 还 mutate count/rate/blacklist/annual latch。

    /// @brief 锁外输入 guard：空 code / 非法价量。
    static std::optional<XtRiskResult> eval_input_guard_(const xt_core::XtOrder& order);

    /// @brief per-symbol 黑名单命中 + 累计计数 + 频率窗口（减仓单一律豁免）。
    std::optional<XtRiskResult> eval_symbol_throttle_(const xt_core::XtOrder& order,
                                                      const XtRiskSnapshot& snapshot,
                                                      const std::string& code_key,
                                                      const XtRiskEvalContext& eval_ctx);

    /// @brief 单笔上限：max_order_qty / max_order_notional / 涨跌停价（平仓也受约束）。
    /// @param notional_fp 名义金额 = price × qty × vol_scale（由 evaluate 统一计算）。
    std::optional<XtRiskResult> eval_order_limits_(const xt_core::XtOrder& order,
                                                   const XtRiskSnapshot& snapshot,
                                                   int64_t notional_fp) const;

    /// @brief 开仓专属：min_notional（减仓豁免）+ 日内止损 + 年度止损。
    std::optional<XtRiskResult> eval_open_loss_guards_(const xt_core::XtOrder& order,
                                                       const XtRiskSnapshot& snapshot,
                                                       const XtRiskPolicy& policy,
                                                       int64_t notional_fp);

    /// @brief per-strategy guard：kill_switch + daily_loss_pct。
    std::optional<XtRiskResult> eval_per_strategy_(const xt_core::XtOrder& order,
                                                   const XtRiskSnapshot& snapshot,
                                                   const XtRiskEvalContext& eval_ctx) const;

    /// @brief 持仓方向规则：max_pos_qty（per-sid + 全局，减仓豁免）/ allow_short / T+1 / 集合竞价。
    /// @param comm evaluate 已 find 的品种规格（可空 → 卖空许可只看全局 risk.allow_short）；
    ///             非空时品种级 allow_short 与全局取严。
    std::optional<XtRiskResult> eval_position_rules_(const xt_core::XtOrder& order,
                                                     const XtRiskSnapshot& snapshot,
                                                     const XtRiskPolicy& policy,
                                                     const XtRiskEvalContext& eval_ctx,
                                                     const XtCommodityInfo* comm) const;

    /// @brief A 股 stock-mode 现金占用 check + 终态 pass；期货/加密资金门禁归 guard 层。
    /// @param comm evaluate 已 find 的品种规格（可空）；传入复用避免同一 evaluate 二次查表。
    XtRiskResult eval_margin_and_pass_(const xt_core::XtOrder& order,
                                       const XtRiskSnapshot& snapshot,
                                       const XtRiskPolicy& policy,
                                       int64_t notional_fp,
                                       const XtCommodityInfo* comm) const;

    /// @brief 保护 cfg_、fixed-point cache、annual latch、blacklist/counters。
    mutable std::shared_mutex cfg_mu_;

    XtRiskCfg cfg_; ///< 风控规则配置快照；cfg_mu_ 保护。

    // 预转换 fixed-point cache，避免 evaluate() 重复 to_fp；cfg_mu_ 保护。
    int64_t max_order_qty_fp_      = 0;
    int64_t max_order_notional_fp_ = 0;
    int64_t min_order_notional_fp_ = 0;
    int64_t max_daily_loss_fp_     = 0;
    int64_t max_pos_qty_fp_        = 0;

    /// @brief key = sid_id；空 map 表示沿用全局 max_pos_qty。
    std::unordered_map<uint32_t, int64_t> per_strategy_max_pos_qty_fp_;

    /// @brief key = sid_id，value = pct fixed-point；空 map 表示禁用 per-strategy daily loss。
    std::unordered_map<uint32_t, int64_t> per_strategy_daily_loss_pct_fp_;

    /// 规则已配置但 baseline 未冻结的放行次数与 warn-once 闩；evaluate 为 const，故 mutable。
    mutable std::atomic<uint64_t> per_strategy_dll_baseline_missing_{0};
    mutable std::atomic<bool>     per_strategy_dll_baseline_warned_{false};

    int64_t year_start_equity_fp_  = 0;     ///< 年初权益基准 fp；0 表示 annual loss 未 arm。
    int     current_year_          = 0;     ///< 正在跟踪的年份；0 表示未初始化。
    bool    annual_cutoff_         = false; ///< 年度熔断 latch。
    int64_t year_start_net_pnl_fp_ = 0;     ///< 年切冻结的 net_pnl anchor。
    int64_t nmf_offset_fp_         = 0;     ///< 跨重启 cum(realized - fees) offset。

    /// @brief stock mode BUY 现金检查使用的 pre-trade fee provider；构造期注入，nullptr 时不估 fee。
    const IFeeRuleProvider*  fee_provider_      = nullptr;
    const XtCommodityCatalog* commodity_catalog_ = nullptr;

    std::unordered_set<std::string> blacklisted_codes_; ///< per-symbol blacklist；cfg_mu_ 保护。
    std::unordered_map<std::string, uint64_t> symbol_order_count_; ///< session 内订单数。
    std::unordered_map<std::string, std::deque<int64_t>> symbol_recent_orders_; ///< rate window。
};
