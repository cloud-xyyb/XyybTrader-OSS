#pragma once

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
    bool enforce_position    = true;
    bool enforce_short_t1    = true;

    bool enforce_margin      = true;
    bool enforce_daily_loss  = true;
    bool enforce_annual_loss = true;
};

#include "api/IRiskGuardContext.h"

class IFeeRuleProvider;
class XtCommodityCatalog;

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

        const IFeeRuleProvider* fee_provider;
        const XtCommodityCatalog* commodity_catalog;
    };

    explicit XtRiskEngine(const XtRiskCfg& cfg, InitDeps deps = InitDeps());

    void set_config(const XtRiskCfg& cfg);

    XtRiskCfg config() const;

    void on_year_change(int year, int64_t equity_fp, int64_t process_net_pnl_fp);

    bool is_annual_loss_exceeded(int64_t process_net_pnl_fp);

    int current_year() const;

    int64_t year_start_equity_fp() const;

    bool annual_cutoff_active() const;

    int64_t year_start_net_pnl_fp() const;

    int64_t nmf_offset_fp() const;

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

    XtRiskResult evaluate(const xt_core::XtOrder& order,
                          const XtRiskSnapshot& snapshot,
                          const XtRiskPolicy& policy = XtRiskPolicy{});

    XtRiskResult evaluate(const xt_core::XtOrder& order,
                          const XtRiskSnapshot& snapshot,
                          const XtRiskPolicy& policy,
                          const XtRiskEvalContext& eval_ctx);

    void insert_blacklist(const char* code, const char* reason_tag);

    bool is_blacklisted(const char* code) const;

    void on_session_begin(uint32_t trading_date);

private:

    void refresh_fp_cache();

    bool is_blacklisted_unlocked_(const char* code) const;

    bool is_annual_loss_exceeded_unlocked_(int64_t process_net_pnl_fp);

    static std::optional<XtRiskResult> eval_input_guard_(const xt_core::XtOrder& order);

    std::optional<XtRiskResult> eval_symbol_throttle_(const xt_core::XtOrder& order,
                                                      const XtRiskSnapshot& snapshot,
                                                      const std::string& code_key,
                                                      const XtRiskEvalContext& eval_ctx);

    std::optional<XtRiskResult> eval_order_limits_(const xt_core::XtOrder& order,
                                                   const XtRiskSnapshot& snapshot,
                                                   int64_t notional_fp) const;

    std::optional<XtRiskResult> eval_open_loss_guards_(const xt_core::XtOrder& order,
                                                       const XtRiskSnapshot& snapshot,
                                                       const XtRiskPolicy& policy,
                                                       int64_t notional_fp);

    std::optional<XtRiskResult> eval_per_strategy_(const xt_core::XtOrder& order,
                                                   const XtRiskSnapshot& snapshot,
                                                   const XtRiskEvalContext& eval_ctx) const;

    std::optional<XtRiskResult> eval_position_rules_(const xt_core::XtOrder& order,
                                                     const XtRiskSnapshot& snapshot,
                                                     const XtRiskPolicy& policy,
                                                     const XtRiskEvalContext& eval_ctx) const;

    XtRiskResult eval_margin_and_pass_(const xt_core::XtOrder& order,
                                       const XtRiskSnapshot& snapshot,
                                       const XtRiskPolicy& policy,
                                       int64_t notional_fp) const;

    mutable std::shared_mutex cfg_mu_;

    XtRiskCfg cfg_;

    int64_t max_order_qty_fp_      = 0;
    int64_t max_order_notional_fp_ = 0;
    int64_t min_order_notional_fp_ = 0;
    int64_t max_daily_loss_fp_     = 0;
    int64_t max_pos_qty_fp_        = 0;

    std::unordered_map<uint32_t, int64_t> per_strategy_max_pos_qty_fp_;

    std::unordered_map<uint32_t, int64_t> per_strategy_daily_loss_pct_fp_;

    int64_t year_start_equity_fp_  = 0;
    int     current_year_          = 0;
    bool    annual_cutoff_         = false;
    int64_t year_start_net_pnl_fp_ = 0;
    int64_t nmf_offset_fp_         = 0;

    const IFeeRuleProvider*  fee_provider_      = nullptr;
    const XtCommodityCatalog* commodity_catalog_ = nullptr;

    std::unordered_set<std::string> blacklisted_codes_;
    std::unordered_map<std::string, uint64_t> symbol_order_count_;
    std::unordered_map<std::string, std::deque<int64_t>> symbol_recent_orders_;
};
