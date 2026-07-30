#pragma once
/// @file
/// @brief HFT / CTA / SEL 共用的 panel 控制 helper。
///
/// 本文件暴露 destructive RPC 所需的跨 Runner 依赖和入口
/// 撤单、强平、manual-order 的具体流程封装在 `.cpp` 内部

#include <cstdint>
#include <cstddef>
#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace xt_account {
class AccountStateStore;
}
namespace xt_runtime {
namespace order {
class XtOrderRuntimeBook;
}
}
namespace xt_risk {
class XtRiskMonitor;
class XtRiskMonitorState;
}
namespace ct::metrics {
class XtStrategyPnlAggregator;
class XtStrategyPnlBaseline;
}
namespace xt_strategy_engine {
class XtDeclaredSymbolRegistry;
}
class ITradeGateway;
class XtCommodityCatalog;
class XtLiveOrderGateway;
class XtLiveParamDomain;
struct XtLiveConfig;
struct XtStrategyInitConfig;
struct XtRunnerSignals;

namespace xt_live::control {

class PanelForceExitLatch;
class ForceFlatSubmitFence;

/// @brief Panel helper 的非拥有依赖视图。
///
/// Runner bootstrap 单线程装配本结构；gRPC server 启动后只读。所有指针均指向
/// Runner 持有对象，生命周期必须覆盖 control wrapper。
struct XtPanelContext {
    ITradeGateway*                          td_gateway          = nullptr;
    xt_runtime::order::XtOrderRuntimeBook*  order_book          = nullptr;
    const xt_account::AccountStateStore*    account_store       = nullptr;
    XtLiveOrderGateway*                     live_order_gateway  = nullptr;
    XtRunnerSignals*                        runtime_shared_state = nullptr;
    XtLiveParamDomain*                      param_domain        = nullptr;
    const XtCommodityCatalog*               catalog             = nullptr;

    PanelForceExitLatch*                    force_exit_latch    = nullptr;

    ForceFlatSubmitFence*                   force_flat_fence    = nullptr;

    std::vector<std::string>                hosted_strategies;

    bool                                    attribution_router_mode = false;
};

enum class ForceExitOutcome : uint8_t {
    Unspecified        = 0,
    NoTd               = 1,
    AlreadyForceFlat   = 2,
    NoPosition         = 3,
    NoMarkPrice        = 4,
    CancelledOnly      = 5,
    ForceClosed        = 6,
    SubmitFailed       = 7,
    Count_,
};

enum class ManualOrderOutcome : uint8_t {
    NoTd            = 0,
    InvalidSide     = 1,
    InvalidType     = 2,
    InvalidQty      = 3,
    InvalidPrice    = 4,
    RiskRejected    = 5,
    Submitted       = 6,
    InvalidStrategy = 7,
};

/// @brief SubmitManualOrder 的 normalized 入参。
///
/// gRPC 层已把数量和价格转换为字符串形式，helper 再统一做校验和 risk gate。
struct ManualOrderRequest {
    std::string request_id;
    std::string venue;
    std::string strategy_id;
    std::string symbol;
    std::string side;
    std::string type;
    std::string qty;
    std::string price;
};

/// @brief flatten_all_panel_request() 的分项计数结果。
struct FlattenAllPanelResult {
    std::size_t cancelled_orders = 0;
    std::size_t closed_positions = 0;
    std::size_t deferred_symbols = 0;
    std::size_t failed_symbols = 0;
    std::string error;
};

std::size_t cancel_all_panel_request(XtPanelContext& ctx);

FlattenAllPanelResult flatten_all_panel_request(XtPanelContext& ctx);

std::pair<bool, bool> cancel_one_panel_request(XtPanelContext& ctx,
                                                const std::string& symbol,
                                                const std::string& oid_str,
                                                std::string& err_out);

ForceExitOutcome force_exit_panel_request(XtPanelContext& ctx,
                                          const std::string& symbol,
                                          bool& hedge_warn_out,
                                          uint64_t* force_order_id_out = nullptr);
ForceExitOutcome force_exit_panel_request(XtPanelContext& ctx,
                                          const std::string& symbol,
                                          const std::string& strategy_id,
                                          bool& hedge_warn_out,
                                          uint64_t* force_order_id_out = nullptr);

ManualOrderOutcome manual_order_panel_request(XtPanelContext& ctx,
                                              const ManualOrderRequest& req,
                                              std::string& reject_reason_out);

void wire_per_strategy_risk_state(
    xt_risk::XtRiskMonitor* risk_monitor,
    xt_account::AccountStateStore* account_store,
    XtLiveOrderGateway* order_gateway,
    const xt_risk::XtRiskMonitorState* risk_monitor_state) noexcept;

std::function<void(uint32_t  , const std::string&  )>
make_strategy_init_failure_handler(
    xt_risk::XtRiskMonitorState* risk_monitor_state) noexcept;

uint64_t strategy_init_failure_latched_total() noexcept;

bool wire_risk_state_and_validate(
    xt_risk::XtRiskMonitor* risk_monitor,
    xt_account::AccountStateStore* account_store,
    XtLiveOrderGateway* order_gateway,
    const xt_risk::XtRiskMonitorState* risk_monitor_state,
    const ct::metrics::XtStrategyPnlAggregator* strategy_pnl_aggr = nullptr,
    const ct::metrics::XtStrategyPnlBaseline* strategy_pnl_baseline = nullptr) noexcept;

bool validate_cross_strategy_symbol_overlap(
    const xt_strategy_engine::XtDeclaredSymbolRegistry* registry,
    const std::vector<std::string>& sid_list,
    bool allow_overlap) noexcept;

std::vector<std::string> collect_strategy_config_symbols(
    const ::XtStrategyInitConfig& cfg);

bool run_overlap_check(
    const xt_strategy_engine::XtDeclaredSymbolRegistry* registry,
    const std::vector<std::string>& sid_list,
    bool allow_overlap,
    const char* engine_tag,
    bool wait_for_workers) noexcept;

bool run_overlap_check_for_config(
    const xt_strategy_engine::XtDeclaredSymbolRegistry* registry,
    const ::XtLiveConfig& cfg,
    const char* engine_tag) noexcept;

}
