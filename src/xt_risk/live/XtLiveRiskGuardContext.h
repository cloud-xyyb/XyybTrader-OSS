#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "xt_core/order/XtOffset.h"
#include "xt_risk/engine/XtRiskEngine.h"
#include "api/ISTPChecker.h"
#include "xt_core/session/XtSessionWindow.h"
#include "live/XtVelocityLimiter.h"

class IMdGateway;
class ITradeGateway;
class XtCommodityCatalog;
namespace xt_account { class AccountStateStore; }
namespace xt_risk { class XtRiskMonitorState; }
namespace ct { namespace metrics { class XtStrategyPnlAggregator; } }
namespace ct { namespace metrics { class XtStrategyPnlBaseline; } }

namespace xt_runtime {
namespace order { class XtOrderRuntimeBook; }
}

/// @brief 实盘风控守卫的运行态输入汇集器。
///
/// 风控引擎只做纯规则判定，但判定需要 Context，例如：通道ready、是否在交易时段、
/// 账户和在途订单什么状态、下单频率是否超限等。
/// XtLiveRiskGuardContext 把这些状态集中起来，供 IRiskGuardContext 的各判定阶段取用。
/// 构造参数和 setter 注入对象均为 non-owning，调用方负责生命周期。
class XtLiveRiskGuardContext final : public IRiskGuardContext {
public:
    struct Config {
        using CloseoutWindow = XtSessionWindow;

        int max_orders_per_sec = 0;
        int max_cancels_per_sec = 0;
        bool require_l2_orderflow = false;
        bool closeout_enabled = false;
        bool session_gate_enabled = false;

        bool reject_on_position_unknown = false;
        std::vector<XtSessionWindow>     closeout_windows;
        std::vector<std::pair<int, int>> session_gate_sections;
    };

    using OrderRateLimitFn = std::function<bool(int limit, int64_t now_ms)>;

    using VelocityLimiterFn = std::function<bool(std::string_view code, int64_t qty_fp, int64_t now_ms)>;

    using ExchangeTimeFn = std::function<int()>;

    using TodayPnlFn = std::function<int64_t()>;

    /// @brief boot 期一次性注入的早绑定依赖集合（见 wire()）。
    /// @note optional 字段缺省 null/empty 即表示该 feature 不启用（presence-gated）；
    ///       genuinely-late 的 strategy_pnl_aggr / monitor_state 不在此，走各自 atomic setter。
    struct GuardDeps {
        const std::atomic<bool>* kill_switch = nullptr;
        const ISTPChecker* stp_checker = nullptr;
        TodayPnlFn today_pnl_fn;
        VelocityLimiterFn velocity_limiter_fn;
        ExchangeTimeFn exchange_time_fn;
        const XtCommodityCatalog* catalog = nullptr;
        const xt_account::AccountStateStore* account_store = nullptr;
        xt_runtime::order::XtOrderRuntimeBook* order_book = nullptr;
        const std::atomic<uint64_t>* last_position_snapshot_ms = nullptr;
        bool stale_guard_enabled = true;
        int  stale_ms = 120000;
    };

    XtLiveRiskGuardContext(Config cfg,
                           std::atomic<bool>* trading_ready,
                           std::atomic<uint32_t>* last_tick_action_time,
                           std::atomic<bool>* td_position_ready,
                           const std::atomic<ITradeGateway*>* td,
                           const std::atomic<IMdGateway*>* md,
                           const std::atomic<int64_t>* block_until_ms,
                           OrderRateLimitFn order_rate_limit_fn);

    void set_strategy_pnl_aggregator(const ct::metrics::XtStrategyPnlAggregator* aggr) noexcept;

    void set_strategy_pnl_baseline(const ct::metrics::XtStrategyPnlBaseline* baseline) noexcept;

    void set_per_strategy_risk_monitor_state(const xt_risk::XtRiskMonitorState* monitor_state) noexcept;

    void wire(GuardDeps deps);

    std::string validate(const XtRiskCfg& risk_cfg) const;

    XtRiskSnapshot make_snapshot(const xt_core::XtOrder& req,
                                 const XtRiskEvalContext& eval_ctx) const override;

    XtRiskResult precheck_input(const xt_core::XtOrder& req,
                                  const XtRiskEvalContext& eval_ctx) const override;

    XtRiskResult precheck_capability(const xt_core::XtOrder& req,
                                       const XtRiskEvalContext& eval_ctx) const override;

    XtRiskResult guard_before_submit(const xt_core::XtOrder& req,
                                       const XtRiskEvalContext& eval_ctx) const override;

private:

    int resolve_gate_hhmm() const;

    std::optional<XtRiskResult> guard_kill_switch_(const xt_core::XtOrder& req,
                                                   const XtRiskEvalContext& eval_ctx) const;
    std::optional<XtRiskResult> guard_session_and_throttle_(const xt_core::XtOrder& req,
                                                            const XtRiskEvalContext& eval_ctx) const;
    std::optional<XtRiskResult> guard_input_validity_(const xt_core::XtOrder& req) const;
    std::optional<XtRiskResult> guard_trading_ready_and_tick_(const xt_core::XtOrder& req) const;
    std::optional<XtRiskResult> guard_td_and_closeout_(const xt_core::XtOrder& req) const;

    Config cfg_;

    std::atomic<bool>* trading_ready_ = nullptr;
    std::atomic<uint32_t>* last_tick_action_time_ = nullptr;
    std::atomic<bool>* td_position_ready_ = nullptr;
    const std::atomic<ITradeGateway*>* td_ = nullptr;
    const std::atomic<IMdGateway*>* md_ = nullptr;
    const std::atomic<int64_t>* block_until_ms_ = nullptr;
    OrderRateLimitFn order_rate_limit_fn_;

    const std::atomic<bool>* kill_switch_ = nullptr;
    const ISTPChecker* stp_checker_ = nullptr;
    TodayPnlFn today_pnl_fn_;

    std::atomic<const ct::metrics::XtStrategyPnlAggregator*> strategy_pnl_aggr_{nullptr};

    std::atomic<const ct::metrics::XtStrategyPnlBaseline*> strategy_pnl_baseline_{nullptr};
    std::atomic<const xt_risk::XtRiskMonitorState*> per_strategy_monitor_state_{nullptr};

    const XtCommodityCatalog* catalog_ = nullptr;
    const xt_account::AccountStateStore* account_store_submit_ = nullptr;
    xt_runtime::order::XtOrderRuntimeBook* order_book_submit_ = nullptr;
    const std::atomic<uint64_t>* last_position_snapshot_ms_ = nullptr;
    bool stale_guard_enabled_ = true;
    int  stale_ms_ = 120000;

    VelocityLimiterFn velocity_limiter_fn_;
    ExchangeTimeFn exchange_time_fn_;

    bool wired_ = false;

    mutable std::atomic<int8_t> cached_l2_support_{-1};
};
