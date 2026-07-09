#pragma once

/// @file
/// @brief HFT runner 的控制环（约 1Hz 冷路径 frame loop）。
///
/// 交易热路径在 event loop worker 线程；控制环独立每秒执行一轮：
/// 采样运行状态、推进风控维护、执行收盘强平、刷新报表和 Prometheus 指标、
/// 持久化安全闩锁。panel 的 RPC 由 gRPC binder 承接，本文件不解析 RPC。

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "XtRunnerControlCfg.h"
#include "xt_core/container/XtLockFreeQueue.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/market/XtL2MarketStructs.h"

// HFT runner 装配依赖：本头只需要不完整类型，完整定义由使用方 .cpp include。
class ITradeGateway;
class XtHftEventLoop;
class XtLiveOrderGateway;
struct XtRunnerSignals;

class IMdGateway;
class IAccountTruthSource;
class XtLiveLatencyTracker;
class XtLiveParamDomain;
struct XtLiveOrderGatewayCfg;
class XtCommodityCatalog;

namespace xt_runtime {
namespace order { class XtOrderRuntimeBook; }
class XtRuntimeMarketCache;
}

namespace xt_account {
class AccountStateStore;
}

namespace xt_risk {
class XtRiskMonitor;
class XtRiskMonitorState;
}

namespace xt_report {
class XtLiveReporter;
}

namespace ct::metrics {
class XtStrategyPnlAggregator;
class XtStrategyPnlBaseline;
}

namespace xt_strategy_engine {
class XtDeclaredSymbolRegistry;
}

#include "xt_live/hft/control/PerSidLiquidator.h"

/// @brief 控制环依赖注入上下文。
///
/// 所有指针均为 non-owning；nullable 字段表示对应冷路径功能关闭或 dry-run。
/// 引用字段必须覆盖 HftRunnerControl::run() 的完整生命周期。
struct XtHftRunnerControlContext {

    ITradeGateway* td_gateway = nullptr;
    XtHftEventLoop& strategy_event_loop;
    XtLiveOrderGateway& live_order_gateway;

    XtRunnerSignals& runtime_shared_state;
    const xt_account::AccountStateStore* account_store = nullptr;

    xt_runtime::order::XtOrderRuntimeBook* order_book = nullptr;

    const XtLiveOrderGatewayCfg* order_gateway_cfg = nullptr;

    XtLiveLatencyTracker* latency_tracker = nullptr;
    XtLiveParamDomain* param_domain = nullptr;
    IAccountTruthSource* account_truth_source = nullptr;

    XtLockFreeQueue<XtTradeRecord>*     trade_drain_q        = nullptr;
    XtLockFreeQueue<XtOrderExecUpdate>* order_update_drain_q = nullptr;

    xt_risk::XtRiskMonitor* risk_monitor = nullptr;

    xt_risk::XtRiskMonitorState* risk_monitor_state = nullptr;

    IMdGateway* md_gateway = nullptr;

    const xt_runtime::XtRuntimeMarketCache* market_cache = nullptr;

    const XtCommodityCatalog* catalog = nullptr;

    const ct::metrics::XtStrategyPnlAggregator* strategy_pnl_aggr = nullptr;

    const xt_strategy_engine::XtDeclaredSymbolRegistry* declared_registry = nullptr;

    ct::metrics::XtStrategyPnlBaseline* strategy_pnl_baseline = nullptr;

    std::function<void(bool active, const std::string& actor,
                       const std::string& reason, int64_t ts_unix_ns)> record_kill_switch_trip;
};

XtHftRunnerControlContext make_runner_control_context(ITradeGateway* td_gateway,
                                                    XtHftEventLoop& strategy_event_loop,
                                                    XtLiveOrderGateway& live_order_gateway,
                                                    XtRunnerSignals& runtime_shared_state,
                                                    const xt_account::AccountStateStore& account_store,
                                                    xt_runtime::order::XtOrderRuntimeBook& order_book,
                                                    const XtLiveOrderGatewayCfg& order_gateway_cfg,
                                                    XtLiveLatencyTracker* latency_tracker = nullptr,
                                                    XtLiveParamDomain* param_domain = nullptr,
                                                    IAccountTruthSource* account_truth_source = nullptr);

/** @brief 跨域一致性检查报告（cross-domain consistency report，冷路径）。 */
struct ConsistencyReport {
    bool ok = true;
    double frozen_cash = 0.0;
    double frozen_cash_by_orders = 0.0;
    size_t open_order_count = 0;
    size_t tracked_code_count = 0;
    std::string reason;
};

/// @brief HFT runner 冷路径控制环。
///
/// 按 `cfg.sleep_ms` 周期采样 runtime/account/order 状态，驱动 active risk frame、
/// closeout / per-sid force-flat、live report / metrics flush 和 safety state 持久化。
///
/// 不拥有注入依赖；`cfg`、`ctx` 和 `Deps` 引用的生命周期必须覆盖 `run()`。
/// 不解析 operator transport；gRPC binder 负责接收命令并更新共享状态或调用控制 helper。
/// @note `run()` 单线程执行，回调线程只通过 `XtRunnerSignals`、order book、account store
///       等注入对象与本控制环交换状态。
class HftRunnerControl {
public:
    struct Deps {
        xt_report::XtLiveReporter& live_reporter;
    };

    HftRunnerControl(const XtRunnerControlConfig& cfg,
                  XtHftRunnerControlContext& ctx,
                  const Deps& deps);

    void run(std::atomic<bool>& shutdown);

    void configure_auto_liquidator(xt_live::control::PerSidLiquidatorCfg cfg) {
        liquidator_.configure(std::move(cfg));
    }

private:
    const XtRunnerControlConfig& cfg_;
    XtHftRunnerControlContext& ctx_;
    xt_report::XtLiveReporter& live_reporter_;
    xt_live::control::PerSidLiquidator liquidator_;

};
