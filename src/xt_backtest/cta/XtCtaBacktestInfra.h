#pragma once

/// @file
/// @brief CTA 回测基础设施构建工具（CTA backtest infrastructure builder）。
///
/// `build_backtest_infra()` 只构建账户、风控、费率、订单簿和 gateway；engine 由
/// caller 装配，以便 bar/tick runner 决定 shared-domain 或 per-strategy 拓扑。

#include <memory>

#include "xt_account/state/AccountStateStore.h"
#include "xt_account/state/AccountState.h"
#include "xt_risk/engine/XtRiskEngine.h"
#include "xt_risk/gateway/XtRiskGateway.h"
#include "xt_risk/backtest/XtBacktestRiskGuardContext.h"
#include "xt_config/catalog/XtCommodityCatalog.h"
#include "xt_config/catalog/XtFeeRuleBook.h"
#include "xt_cost/XtFillCostApplier.h"
#include "xt_runtime/order/XtOrderRuntimeBook.h"
#include "XtBacktestOrderGateway.h"

/// @brief CTA 回测的基础设施 ownership 包。
///
/// Runner 持有本结构并把 non-owning 指针注入 gateway/risk/cost 组件。成员声明顺序
/// 编码析构约束：consumer 后声明，确保析构时先释放 gateway/risk/cost，再释放
/// fee book、commodities 与 account provider。
struct BacktestInfra {
    std::unique_ptr<XtCommodityCatalog>            commodities;   ///< 合约目录
    std::unique_ptr<XtFeeRuleBook>                 fee_book;      ///< 费率规则集（owning）
    std::unique_ptr<xt_account::AccountStateStore>            account;       ///< 账户状态（权威账本）
    std::unique_ptr<xt_fill_cost::FillCostApplier> cost_applier;  ///< 成本模型
    std::unique_ptr<xt_runtime::order::XtOrderRuntimeBook> order_book;  ///< 订单簿（unique_ptr：risk_guard_ctx 持其裸指针，须跨 BacktestInfra move 稳定）
    std::unique_ptr<XtRiskEngine>                  risk_engine;   ///< 风控引擎
    std::unique_ptr<XtRiskPolicy>                  risk_policy;   ///< 风控策略
    std::unique_ptr<XtRiskGateway>                 risk_gateway;  ///< 风控入口
    std::unique_ptr<XtBacktestRiskGuardContext>    risk_guard_ctx; ///< 风控上下文
    std::unique_ptr<XtCtaBacktestOrderGateway>  gw;            ///< 回测执行网关
};

/// 从 XtCtaBacktestOrderGateway::Config 构建完整基础设施（不含 Engine）。
inline BacktestInfra build_backtest_infra(const XtCtaBacktestOrderGateway::Config& cfg) {
    BacktestInfra infra;

    // commodities 先于 account 构建：AccountState::Config.catalog 指向该对象。
    infra.commodities = std::make_unique<XtCommodityCatalog>(cfg.commodities);

    AccountState::Config acfg = cfg.account;
    // 期货 PnL 依赖 catalog 中的 vol_scale。
    acfg.catalog = infra.commodities.get();
    infra.account = std::make_unique<xt_account::AccountStateStore>(acfg);

    // fee_book 由 infra 持有，cost_applier/risk_engine 仅引用其 IFeeRuleProvider 接口。
    infra.fee_book = std::make_unique<XtFeeRuleBook>(cfg.fees);

    infra.order_book = std::make_unique<xt_runtime::order::XtOrderRuntimeBook>();

    infra.risk_engine    = std::make_unique<XtRiskEngine>(
        cfg.risk, XtRiskEngine::InitDeps{infra.fee_book.get(), infra.commodities.get()});
    infra.risk_policy    = std::make_unique<XtRiskPolicy>();
    infra.risk_gateway   = std::make_unique<XtRiskGateway>();
    infra.risk_gateway->bind(*infra.risk_engine, *infra.risk_policy);
    // catalog 接入 guard 的期货 Open margin 门禁；SEL/A 股（futures_mode=false）下门禁 inert。
    // order_book 接入 guard 的平仓可用量预检查（扣减在途平仓量，防同 tick over-close）。
    infra.risk_guard_ctx = std::make_unique<XtBacktestRiskGuardContext>(
        *infra.account, infra.order_book.get(), infra.commodities.get());

    infra.cost_applier = std::make_unique<xt_fill_cost::FillCostApplier>();
    infra.cost_applier->init(cfg.cost, infra.fee_book.get(), infra.commodities.get());

    infra.gw = std::make_unique<XtCtaBacktestOrderGateway>();

    return infra;
}
