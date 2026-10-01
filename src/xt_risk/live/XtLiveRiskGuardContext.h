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

        int max_orders_per_sec = 0;         ///< 每秒最大下单笔数；0 = 不限
        int max_cancels_per_sec = 0;        ///< 撤单限频配置副本；由 cancel 路径消费
        bool require_l2_orderflow = false;  ///< 策略要求 MD 支持 L2 orderflow
        bool closeout_enabled = false;      ///< 启用收盘前 no-open 窗口
        bool session_gate_enabled = false;  ///< 启用交易时段门禁；空 sections 会被 validate() 拒绝
        /// 保留字段；closeable_known=false 时统一拒为 PositionStateUnknown。
        bool reject_on_position_unknown = false;
        std::vector<XtSessionWindow>     closeout_windows;       ///< 收盘平仓时间窗口列表
        std::vector<std::pair<int, int>> session_gate_sections;  ///< 允许交易的时段区间 [start_hhmm, end_hhmm]
    };

    using OrderRateLimitFn = std::function<bool(int limit, int64_t now_ms)>;

    /// @brief 仓位变化速率检查回调；返回 false 表示 qty/window 超限。
    /// @param code 直接引用 req.code，回调不得保存该 string_view。
    /// @param qty_fp 本次下单数量（fixed-point）。
    /// @param now_ms 评估时间戳。
    using VelocityLimiterFn = std::function<bool(std::string_view code, int64_t qty_fp, int64_t now_ms)>;

    /// @brief session/closeout 时间源；返回 HHMM，未注入时 fallback 到本地 OS 时间。
    using ExchangeTimeFn = std::function<int()>;

    /// @brief 当日已实现盈亏数据源；返回 fixed-point，负值表示亏损。
    using TodayPnlFn = std::function<int64_t()>;

    /// @brief broker-frozen 时效窗（steady ms）：超窗的 frozen 降级为 local-only。
    /// SSOT：config 层要求 venue 的 position 刷新周期严格小于本窗（否则 frozen
    /// 恒 stale = 结构性旁路），validator 与本判据引同一常量。
    static constexpr uint64_t kBrokerFrozenStaleMs = 30'000;

    /// @brief boot 期一次性注入的早绑定依赖集合（见 wire()）。
    /// @note optional 字段缺省 null/empty 即表示该 feature 不启用（presence-gated）；
    ///       genuinely-late 的 strategy_pnl_aggr / monitor_state 不在此，走各自 atomic setter。
    struct GuardDeps {
        const std::atomic<bool>* kill_switch = nullptr;                ///< 全局 kill switch；nullptr=不启用
        const ISTPChecker* stp_checker = nullptr;                      ///< STP 检查器；nullptr=跳过
        TodayPnlFn today_pnl_fn;                                       ///< 全局 daily-loss PnL 源
        VelocityLimiterFn velocity_limiter_fn;                         ///< 仓位速率检查；空=跳过
        ExchangeTimeFn exchange_time_fn;                               ///< session/closeout 时间源
        const XtCommodityCatalog* catalog = nullptr;                   ///< price_tick/vol_scale/margin_rate
        const xt_account::AccountStateStore* account_store = nullptr;  ///< 资金/可平量/registered-symbol
        xt_runtime::order::XtOrderRuntimeBook* order_book = nullptr;    ///< 在途平仓占用（close outstanding）
        const std::atomic<uint64_t>* last_position_snapshot_ms = nullptr;  ///< 最近持仓快照时间戳
        bool stale_guard_enabled = true;                               ///< 持仓快照新鲜度检查开关
        int  stale_ms = 120000;                                        ///< 持仓快照过期阈值（ms）
    };

    XtLiveRiskGuardContext(Config cfg,
                           std::atomic<bool>* trading_ready,
                           std::atomic<uint32_t>* last_tick_action_time,
                           std::atomic<bool>* td_position_ready,
                           const std::atomic<ITradeGateway*>* td,
                           const std::atomic<IMdGateway*>* md,
                           const std::atomic<int64_t>* block_until_ms,
                           OrderRateLimitFn order_rate_limit_fn);

    /// @brief 注入单策略 realized PnL 聚合器，用于 StrategyDailyLossExceeded。
    /// @note nullptr 时 per_strategy_today_pnl_fp 保持 0；对应分支 fail-safe bypass。
    void set_strategy_pnl_aggregator(const ct::metrics::XtStrategyPnlAggregator* aggr) noexcept;

    /// @brief 注入日初 PnL baseline，用于把 per_strategy_today_pnl_fp 从 lifetime 累计
    ///        校正为 today realized（aggregator 累计 − 日初 baseline）。
    /// @note nullptr 或 baseline 未 ready 时不做减法，字段退化为 lifetime 累计；
    ///       启动早期不因 baseline 缺失扩大拒单范围。
    void set_strategy_pnl_baseline(const ct::metrics::XtStrategyPnlBaseline* baseline) noexcept;

    /// @brief 注入 per-strategy kill_switch/watchdog 状态。
    /// @note nullptr 时 strategy_killed 保持 false；对应分支 bypass。
    void set_per_strategy_risk_monitor_state(const xt_risk::XtRiskMonitorState* monitor_state) noexcept;

    /// @brief boot 期一次性装配早绑定依赖（providers + optional features）。
    /// @note 唯一调用点为 XtLiveOrderGateway::inject_components（provider 到齐时）。
    ///       重复调用被 runtime guard 忽略并告警；这是 misuse-guard，非 publication fence
    ///       —— 早绑定 dep 的可见性靠"装配先于消费线程创建"的 happens-before 保证。
    void wire(GuardDeps deps);

    /// @brief 启动期校验配置与 provider 注入是否一致。
    /// @return 空字符串表示合法；非空字符串为应拒绝启动的错误说明。
    std::string validate(const XtRiskCfg& risk_cfg) const;

    /// @brief 构建 XtRiskEngine.evaluate() 使用的实盘快照。
    /// @note 缺少 account_store 时返回空快照；PnL/monitor provider 缺失时对应字段保持默认值。
    XtRiskSnapshot make_snapshot(const xt_core::XtOrder& req,
                                 const XtRiskEvalContext& eval_ctx) const override;

    /// @brief 实盘 input guard：kill switch、限频、输入合法性、session/closeout、STP。
    /// @note kill switch 不受 run_input_guards 控制（绝对门禁）。
    /// @note is_forced_close/dry_run 会跳过消耗型限速，但不跳过合法性和就绪检查。
    XtRiskResult precheck_input(const xt_core::XtOrder& req,
                                  const XtRiskEvalContext& eval_ctx) const override;

    /// @brief 通道能力守卫：TD 初始化、CloseToday、IOC/FOK、可选 L2 orderflow。
    XtRiskResult precheck_capability(const xt_core::XtOrder& req,
                                       const XtRiskEvalContext& eval_ctx) const override;

    /// @brief 提交前资源守卫：持仓新鲜度、可平量、在途占用和保证金。
    XtRiskResult guard_before_submit(const xt_core::XtOrder& req,
                                       const XtRiskEvalContext& eval_ctx) const override;

private:
    /// @brief 解析当前 HHMM；优先级：exchange_time_fn_ > last_tick_action_time_ > OS 本地时间。
    int resolve_gate_hhmm() const;

    // ── precheck_input compose-method 拆分 ─────────────────────────────────
    /// precheck_input 是 input guard reject pipeline；每 helper 返回 nullopt=放行继续，
    /// 有值=该段命中的 reject（顺序即优先级，不可重排）。全部 const，只读 runtime signal。
    std::optional<XtRiskResult> guard_kill_switch_(const xt_core::XtOrder& req,
                                                   const XtRiskEvalContext& eval_ctx) const;
    std::optional<XtRiskResult> guard_session_and_throttle_(const xt_core::XtOrder& req,
                                                            const XtRiskEvalContext& eval_ctx) const;
    std::optional<XtRiskResult> guard_input_validity_(const xt_core::XtOrder& req) const;
    std::optional<XtRiskResult> guard_trading_ready_and_tick_(const xt_core::XtOrder& req) const;
    std::optional<XtRiskResult> guard_td_and_closeout_(const xt_core::XtOrder& req) const;

    Config cfg_;

    // 构造期注入的 live runtime signal；全部 non-owning，通常由 runner/gateway 持有。
    std::atomic<bool>* trading_ready_ = nullptr;
    std::atomic<uint32_t>* last_tick_action_time_ = nullptr;
    std::atomic<bool>* td_position_ready_ = nullptr;
    const std::atomic<ITradeGateway*>* td_ = nullptr;
    const std::atomic<IMdGateway*>* md_ = nullptr;
    const std::atomic<int64_t>* block_until_ms_ = nullptr;  // epoch ms；submit cooldown 截止时间。
    OrderRateLimitFn order_rate_limit_fn_;

    // 早绑定 optional 风控 provider；wire() 在消费线程创建前一次性装配。
    const std::atomic<bool>* kill_switch_ = nullptr;
    const ISTPChecker* stp_checker_ = nullptr;
    TodayPnlFn today_pnl_fn_;
    // genuinely-late deps：消费线程启动后由 runner 注入，atomic 保证发布可见性（store-release/acquire-load）。
    std::atomic<const ct::metrics::XtStrategyPnlAggregator*> strategy_pnl_aggr_{nullptr};
    // 日初 baseline：与 aggregator 配对，make_snapshot 内 aggr_cum − baseline = today realized。
    std::atomic<const ct::metrics::XtStrategyPnlBaseline*> strategy_pnl_baseline_{nullptr};
    std::atomic<const xt_risk::XtRiskMonitorState*> per_strategy_monitor_state_{nullptr};

    // submit 资源由 XtLiveOrderGateway 启动期注入；运行期只读，指针均 non-owning。
    const XtCommodityCatalog* catalog_ = nullptr;
    const xt_account::AccountStateStore* account_store_submit_ = nullptr;
    xt_runtime::order::XtOrderRuntimeBook* order_book_submit_ = nullptr;
    const std::atomic<uint64_t>* last_position_snapshot_ms_ = nullptr;  // steady ms；0 表示未知。
    bool stale_guard_enabled_ = true;
    int  stale_ms_ = 120000;

    VelocityLimiterFn velocity_limiter_fn_;
    ExchangeTimeFn exchange_time_fn_;

    bool wired_ = false;  ///< wire() 单发 misuse-guard（runtime 检查，非 publication fence）。

    mutable std::atomic<int8_t> cached_l2_support_{-1};  ///< -1 未缓存，0 不支持，1 支持。
};
