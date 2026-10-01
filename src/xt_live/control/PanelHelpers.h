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
class XtCommodityCatalog;  // force-exit 激进价 venue-legal tick snap
class XtLiveOrderGateway;
class XtLiveParamDomain;
struct XtLiveConfig;
struct XtStrategyInitConfig;
struct XtRunnerSignals;

namespace xt_live::control {

class PanelForceExitLatch;   // force-exit CancelledOnly 续帧 latch（xt_live/hft/control）
class ForceFlatSubmitFence;  // force-flat submit 同步 per-symbol fence（同目录）

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
    const XtCommodityCatalog*               catalog             = nullptr;  ///< 品种目录（force-exit 激进价 venue-legal tick 对齐）；nullptr 不对齐
    /// panel ForceExit 返 CancelledOnly 时挂账的续帧 latch（control frame 续平）。
    /// HFT runner 装配注入；nullptr = 关闭（CTA/SEL 不接 → 原一次性语义不变）。
    PanelForceExitLatch*                    force_exit_latch    = nullptr;
    /// force-flat submit 同步 fence：genuine 标记 marshal 异步可见前的 over-close
    /// 挡板（held 窗口内仅 (4a) submit 被闸返 AlreadyForceFlat；撤单轮与 flat/
    /// no-mark 早返路径照常）。HFT runner 装配注入；nullptr = 关闭（CTA/SEL
    /// 不穿线 → 双击窗口为 pre-existing 现状）。
    ForceFlatSubmitFence*                   force_flat_fence    = nullptr;
    /// 本 runner hosted 策略 id 集合（runner main 从 parsed.strategies wire）。
    /// update_param 的 strategy-scoped 写按此校验归属，拒绝非本机策略的孤儿
    /// key upsert；空 = 未注入 = 不校验放行（向后兼容测试基建/未 wire 路径）。
    std::vector<std::string>                hosted_strategies;
    /// attribution 路由模式（== portfolio_router）：runner main 在决定 gateway sid_registry
    /// 注入的同一处显式 wire。为 true 且 manual order 请求 strategy_id 为空时，改写归属为
    /// 保留名 `_manual`（见 manual_order_panel_request）。**不得**用 registry 存在性探测
    /// （registry 全模式装配）。false（非 router）= 不 stamp → 含 recorder strat_id 在内零 delta。
    bool                                    attribution_router_mode = false;
};

// ── force_exit / manual_order outcome 类型 ──

/// @brief force_exit 单 symbol 结果。
///
/// 强类型 outcome：内部层（XtRunnerPanelControl → grpc binder）以本枚举传递，
/// binder 经 `force_exit_outcome_to_proto`（ForceExitOutcomeProto.h，唯一
/// enum↔proto switch）映射到 proto `FORCE_EXIT_OUTCOME_*` 后填 wire。新增值
/// 须同步补 to_proto case；两侧枚举对齐由 xt_grpc_control_tests 的
/// `ForceExitOutcomeMapping.MirrorsProtoValues`（MAX 漂移守卫）钉住，且
/// `Count_` 哨兵在 `ForceExitOutcomeProto.h` 有 static_assert 与 proto
/// `ForceExitOutcome_ARRAYSIZE` 对齐（C++ 侧加值时编译期拦截漏同步）。
enum class ForceExitOutcome : uint8_t {
    Unspecified        = 0,  ///< 仅作 proto UNSPECIFIED 对齐占位；业务路径不产生
    NoTd               = 1,  ///< td 不可用 / recovering / trading_ready=false
    AlreadyForceFlat   = 2,  ///< 防 race: 该 symbol 已有未终态 force-flat 单
    NoPosition         = 3,  ///< 无持仓
    NoMarkPrice        = 4,  ///< mark price 不可用,不能 submit IOC
    CancelledOnly      = 5,  ///< 撤 working orders, 本帧未 submit（HFT 侧由 PanelForceExitLatch 续平；CTA/SEL 仍需 operator 再次触发）
    ForceClosed        = 6,  ///< 已 submit IOC@mark close
    SubmitFailed       = 7,  ///< td submit 返 0
    Count_,                  ///< 哨兵：仅编译期基数校验用，不是合法 outcome，不进 wire
};

/// @brief manual_order 提交结果分类。
enum class ManualOrderOutcome : uint8_t {
    NoTd            = 0,  ///< td/gateway/account_store 未注入或 td recovering
    InvalidSide     = 1,  ///< side 不是 BUY/SELL
    InvalidType     = 2,  ///< type 不是 MARKET/LIMIT
    InvalidQty      = 3,  ///< qty 解析失败 / <= 0
    InvalidPrice    = 4,  ///< LIMIT 时 price 解析失败 / <= 0,或 MARKET 时 mark 不可用
    RiskRejected    = 5,  ///< XtLiveOrderGateway::submit 返 SubmitResult.accepted()=false
    Submitted       = 6,  ///< 已提交, SubmitResult.order_id 非零
    InvalidStrategy = 7,  ///< strategy_id 提供但未在 sid registry 注册
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

// ── cross-engine panel entry point ──────────────────────────────────────────

/// @brief 撤销所有 working orders；返回已调用 cancel 的订单数。
std::size_t cancel_all_panel_request(XtPanelContext& ctx);

/// @brief 撤单后对所有非零本地持仓提交 IOC close；返回提交/延迟/失败计数。
FlattenAllPanelResult flatten_all_panel_request(XtPanelContext& ctx);

/// @brief symbol-scoped 单笔撤单；返回 (ok, was_open)。
std::pair<bool, bool> cancel_one_panel_request(XtPanelContext& ctx,
                                                const std::string& symbol,
                                                const std::string& oid_str,
                                                std::string& err_out);

/// @brief 单 symbol 先撤后平；有 working order 时只撤，无 working order 时提交 IOC close。
/// @param hedge_warn_out hedge mode 只关较大边时置 true。
ForceExitOutcome force_exit_panel_request(XtPanelContext& ctx,
                                          const std::string& symbol,
                                          bool& hedge_warn_out,
                                          uint64_t* force_order_id_out = nullptr);
ForceExitOutcome force_exit_panel_request(XtPanelContext& ctx,
                                          const std::string& symbol,
                                          const std::string& strategy_id,
                                          bool& hedge_warn_out,
                                          uint64_t* force_order_id_out = nullptr);

/// @brief 人工开仓入口；必须经 XtLiveOrderGateway::submit 通过完整 risk gate。
ManualOrderOutcome manual_order_panel_request(XtPanelContext& ctx,
                                              const ManualOrderRequest& req,
                                              std::string& reject_reason_out);

/// @brief 注入 per-strategy risk state 到 risk monitor / order gateway。
///
/// 任一可选指针为空时跳过对应 setter；调用期在 bootstrap 完成后、
/// risk monitor frame loop 启动前。
void wire_per_strategy_risk_state(
    xt_risk::XtRiskMonitor* risk_monitor,
    xt_account::AccountStateStore* account_store,
    XtLiveOrderGateway* order_gateway,
    const xt_risk::XtRiskMonitorState* risk_monitor_state) noexcept;

/// @brief 构造 engine `on_strategy_init_failure` 回调（HFT / CTA / SEL 共用）。
///
/// 策略 on_init 失败时 engine 已 fail-closed disable 该 ctx，随后调用返回的回调：
///   1. 置该 sid 的 per-strategy kill latch（runtime atomic；随既有 safety.bin per-sid
///      落盘机制自然跨重启持久——不改 schema）；sid_id==0 或 state 为空时跳过。
///      置位后**回读确认**：setter 对越界 sid 静默 no-op（装配期槽位数组常为空），
///      只有回读为 true 才记「已置位」；回读为假 → error 日志 +
///      `strategy_init_failure_latch_unverified_total` 递增（不假报）。
///   2. critical 日志（含 sid + 策略名 + fail-closed / kill-latch / close-bypass 语义 +
///      operator 处置提示）。
///   3. 进程内 atomic counter 递增（strategy_init_failure_latched_total）。
/// close bypass 保留：per-sid kill 只拒新开仓，平仓单天然通行（XtRiskEngine::evaluate）。
///
/// @param risk_monitor_state per-sid kill latch 宿主（非拥有）；nullptr 时仅日志 + counter。
/// @note per-sid kill 目前无持久化 source 字段（仅全局 kill_switch_source 有 manual/auto
///       区分）——**不加新 schema**；InitFailure 语义经 critical 日志 + counter 表达。
/// @note recovery 持仓额外告警：on_init 触发点（worker 启动 / 装配期）早于 venue position
///       snapshot 回灌，此刻按 sid 的持仓读路径尚未 populate → 折入基础 critical，由
///       operator 据日志核查该 sid 是否残留恢复持仓（无人管理）。
std::function<void(uint32_t /*sid_id*/, const std::string& /*strategy_name*/)>
make_strategy_init_failure_handler(
    xt_risk::XtRiskMonitorState* risk_monitor_state) noexcept;

/// @brief 进程内累计：被 fail-closed disable + kill-latch 的策略 on_init 失败次数。
uint64_t strategy_init_failure_latched_total() noexcept;

/// @brief 进程内累计：latch 置位后回读为 false 的次数（该 sid 开仓闸实际未生效）。
/// @note 非零 ≠ 最终失守：runner 在 per-sid risk state `init()` 之后按登记补置；
///       该计数用于确认「装配期置位本就无效」这一前提，以及补置缺失时的取证。
uint64_t strategy_init_failure_latch_unverified_total() noexcept;

/// @brief `wire_per_strategy_risk_state` + gateway risk config 校验合并入口。
///
/// aggregator/baseline 是 per-strategy daily-loss 的 PnL 源。**nullptr = 跳过
/// setter、不覆盖已注入值**（承重语义：CTA 在 runtime build 期自行注入
/// aggregator——无 baseline，today 退化 lifetime 口径——此处传 nullptr 必须
/// 保留之；SEL 无源，daily_loss_pct 保持 fail-safe bypass）。setter 必须先于
/// validate 注入——validate 检查「配 daily-loss 却无 PnL 源」，本函数内部保证
/// 该顺序。
/// @return false 表示 risk config 校验失败（已 log "RT"，caller fail-exit）。
bool wire_risk_state_and_validate(
    xt_risk::XtRiskMonitor* risk_monitor,
    xt_account::AccountStateStore* account_store,
    XtLiveOrderGateway* order_gateway,
    const xt_risk::XtRiskMonitorState* risk_monitor_state,
    const ct::metrics::XtStrategyPnlAggregator* strategy_pnl_aggr = nullptr,
    const ct::metrics::XtStrategyPnlBaseline* strategy_pnl_baseline = nullptr) noexcept;


/// @brief 校验多个 strategy 是否声明同一 symbol。
///
/// 默认不允许重叠，避免共仓归因模糊；`allow_overlap=true` 时仅告警并继续。
/// 返回 false 表示缺少 registry、声明覆盖不完整，或出现未允许的重叠。
bool validate_cross_strategy_symbol_overlap(
    const xt_strategy_engine::XtDeclaredSymbolRegistry* registry,
    const std::vector<std::string>& sid_list,
    bool allow_overlap) noexcept;

/// @brief 收集配置声明的 symbol，用于 overlap validation。
std::vector<std::string> collect_strategy_config_symbols(
    const ::XtStrategyInitConfig& cfg);

/// @brief Runner 启动期 overlap-check wrapper。
///
/// CTA/SEL 可等待 worker 完成 strategy 注册；超时或校验失败时 fail closed。
bool run_overlap_check(
    const xt_strategy_engine::XtDeclaredSymbolRegistry* registry,
    const std::vector<std::string>& sid_list,
    bool allow_overlap,
    const char* engine_tag,
    bool wait_for_workers) noexcept;

/// @brief `run_overlap_check` 的 live-config 入口：从 cfg 取 sid 列表与
///        allow 开关，wait_for_workers=true；失败时已 log abort 原因。
bool run_overlap_check_for_config(
    const xt_strategy_engine::XtDeclaredSymbolRegistry* registry,
    const ::XtLiveConfig& cfg,
    const char* engine_tag) noexcept;

}  // namespace xt_live::control
