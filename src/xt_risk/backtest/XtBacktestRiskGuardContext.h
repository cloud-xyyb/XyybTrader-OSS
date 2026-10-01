#pragma once

#include "state/AccountStateStore.h"
#include "xt_risk/engine/XtRiskEngine.h"

namespace xt_runtime { namespace order { class XtOrderRuntimeBook; } }
class XtCommodityCatalog;
class ISTPChecker;

/// @brief 回测侧 `IRiskGuardContext` 实现。
///
/// XtBacktestRiskGuardContext 把账户状态、在途订单簿、涨跌停、STP 和当日 PnL 组织成
/// `XtRiskEngine` 可消费的 snapshot 与提交前资源门禁。
class XtBacktestRiskGuardContext final : public IRiskGuardContext {
public:
    /// @param catalog 期货 Open margin 门禁的 per-unit margin 源；nullptr = 门禁 bypass
    ///        （轻量测试路径）。仅 account.futures_mode() 为真时参与检查。
    /// @param limit_up_fp 当前 tick 涨停价（fixed-point；0 = 无限制，engine
    ///        PriceOutOfDailyLimit 检查 bypass）。venue-truth 来自 tick 内嵌字段，
    ///        由 caller（BT gateway）每次 submit 时传入。
    /// @param limit_down_fp 当前 tick 跌停价，同上。
    /// @param stp 自成交预防检查器（submit 前价格交叉判定，对齐 live InputGuard
    ///        拒 SelfTradeDetected）；nullptr = 检查关闭（默认，既有行为）。
    /// @param today_pnl_fp 当日已实现盈亏（realized-delta 口径，对齐 live venue
    ///        close_profit 语义：不含 unrealized/fees）；0 + 未配 max_daily_loss =
    ///        engine 检查 bypass（既有行为）。由 caller（BT gateway）每次 submit
    ///        时传 realized_now − session_begin 基线。
    XtBacktestRiskGuardContext(const xt_account::AccountStateStore& account,
                               const xt_runtime::order::XtOrderRuntimeBook* order_book = nullptr,
                               const XtCommodityCatalog* catalog = nullptr,
                               int64_t limit_up_fp = 0,
                               int64_t limit_down_fp = 0,
                               const ISTPChecker* stp = nullptr,
                               int64_t today_pnl_fp = 0);

    XtRiskSnapshot make_snapshot(const xt_core::XtOrder& req,
                                 const XtRiskEvalContext& eval_ctx) const override;
    /// @brief 回测 input guard；STP 命中时拒单，其余基础字段交给 core evaluate。
    XtRiskResult precheck_input(const xt_core::XtOrder& req,
                                  const XtRiskEvalContext& eval_ctx) const override;
    /// @brief 能力守卫：回测不依赖 TD/MD 能力，默认放行。
    XtRiskResult precheck_capability(const xt_core::XtOrder& req,
                                       const XtRiskEvalContext& eval_ctx) const override;
    /// @brief 提交前最终守卫：Open 走期货 margin 门禁（futures-mode + catalog 时）；
    ///        平仓走可用量预检查（today/yesterday/total）+ 在途平仓量扣减。
    XtRiskResult guard_before_submit(const xt_core::XtOrder& req,
                                       const XtRiskEvalContext& eval_ctx) const override;

private:
    const xt_account::AccountStateStore& account_;                                    ///< non-owning ref；账户状态存储（account state store）
    const xt_runtime::order::XtOrderRuntimeBook* order_book_ = nullptr;   ///< non-owning；在途订单簿，用于扣减平仓量（outstanding order book）
    const XtCommodityCatalog* catalog_ = nullptr;                          ///< non-owning；期货 Open margin 门禁的 per-unit margin 源；nullptr = bypass
    int64_t limit_up_fp_ = 0;    ///< 当前 tick 涨停价（0 = 无限制）；make_snapshot 覆写进风控快照
    int64_t limit_down_fp_ = 0;  ///< 当前 tick 跌停价（0 = 无限制）
    const ISTPChecker* stp_ = nullptr;  ///< non-owning；自成交预防检查（nullptr = 关闭）
    int64_t today_pnl_fp_ = 0;  ///< 当日已实现盈亏（realized-delta）；make_snapshot 覆写进快照
};
