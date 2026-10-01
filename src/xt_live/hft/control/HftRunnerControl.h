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
#include <memory>
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
namespace xt_ipc { class XtShmMdGateway; }  // concrete shm reader（typed，for attach 观测）
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

namespace xt_live::control {
class PanelForceExitLatch;   // panel force-exit 续帧 latch（同目录，完整定义由使用方 .cpp include）
class ForceFlatSubmitFence;  // force-flat submit 同步 fence（xt_live/control）
namespace detail { struct ControlSnapshot; }  // finalize_report 缓存的帧快照（完整定义由 .cpp include）
}

#include "xt_live/hft/control/PerSidLiquidator.h"

/// @brief 控制环依赖注入上下文。
///
/// 所有指针均为 non-owning；nullable 字段表示对应冷路径功能关闭或 dry-run。
/// 引用字段必须覆盖 HftRunnerControl::run() 的完整生命周期。
struct XtHftRunnerControlContext {
    // ── 通道侧依赖（channels） ──────────────────────────────────────────────
    ITradeGateway* td_gateway = nullptr;            ///< TD 网关（non-owning, nullable if dry-run）
    XtHftEventLoop& strategy_event_loop;       ///< 策略事件循环（non-owning ref）
    XtLiveOrderGateway& live_order_gateway;         ///< 下单网关（non-owning ref）

    // ── 运行态依赖（runtime） ──────────────────────────────────────────────
    XtRunnerSignals& runtime_shared_state;          ///< 跨线程共享信号（non-owning ref, e.g. shutdown flag）
    const xt_account::AccountStateStore* account_store = nullptr;    ///< 账户状态存储（non-owning, 只读）

    // ── 订单簿（order book） ──────────────────────────────────────────────
    xt_runtime::order::XtOrderRuntimeBook* order_book = nullptr;  ///< 订单运行时账本（non-owning，一致性检查 + 强平用）

    // ── 下单网关配置（order gateway config）──────────────────────────────
    const XtLiveOrderGatewayCfg* order_gateway_cfg = nullptr;  ///< 下单网关配置（non-owning，强平窗口参数来源）

    // ── 可选服务（optional services） ──────────────────────────────────────────────
    XtLiveLatencyTracker* latency_tracker = nullptr; ///< 延迟追踪器（non-owning, nullable）
    XtLiveParamDomain* param_domain = nullptr;       ///< 参数热更新域（non-owning, nullable）
    IAccountTruthSource* account_truth_source = nullptr; ///< 账户真值源（non-owning, nullable）

    // ── SPSC 诊断通道（消费端，对应 EventLoop 生产端） ──────────────────────────────────────────────
    XtLockFreeQueue<XtTradeRecord>*     trade_drain_q        = nullptr; ///< 成交诊断队列（non-owning，可 null）
    XtLockFreeQueue<XtOrderExecUpdate>* order_update_drain_q = nullptr; ///< 订单更新诊断队列（non-owning，可 null）

    // ── 帧尾钩子 ────────────────────────────────────────────────────────────
    /// @brief 1Hz 帧尾回调（nullable）；runner 装配时注入（如 TD handoff 重投缓冲 drain）。
    std::function<void()> frame_tail_hook;

    // ── Active risk monitor ─────────────────────────────────────────────
    /// @brief Drawdown / stale 算法层；nullptr 表示未启用，frame loop 跳过 on_frame。
    /// @note 由 Bootstrap 持有，XtHftRunner / XtCtaRunner 在装配时注入。
    xt_risk::XtRiskMonitor* risk_monitor = nullptr;

    /// @brief 4-stage force-flat per-sid watchdog 状态；nullptr 时走全 runner 路径。
    xt_risk::XtRiskMonitorState* risk_monitor_state = nullptr;

    // ── Gateway 健康检查（self-healing reconnect）──────────────────────
    IMdGateway* md_gateway = nullptr;  ///< MD 网关（non-owning, for check_health; 装配时注入）
    /// @brief 具体 shm reader 网关（non-owning，仅 md.transport=shm 时非空）。
    /// @note frame-tail attach 观测专用（IMdGateway 接口不暴露 attach 计数）；venue-direct
    ///       网关下为 nullptr。composition root 用 dynamic_cast 从 md_gateway 派生注入。
    xt_ipc::XtShmMdGateway* shm_md_gateway = nullptr;

    // ── 行情快照 (for bid/ask 回填) ────────────────────────────────────────
    /// @brief 行情缓存，只读用于 bid/ask 回填；nullptr 时回填 0。
    const xt_runtime::XtRuntimeMarketCache* market_cache = nullptr;

    // ── 合约目录 (for closeout aggressive tick snap) ──────────────
    /// @brief 品种规格目录，只读用于 closeout 强平价的 venue-legal tick 对齐；nullptr 时不对齐。
    const XtCommodityCatalog* catalog = nullptr;

    // ── per-strategy PnL aggregator ───────────────────────────────────────
    /// @brief HFT per-(sid, code) realized+fee aggregator；nullptr 时跳过 emit。
    const ct::metrics::XtStrategyPnlAggregator* strategy_pnl_aggr = nullptr;

    /// @brief sid->declared codes registry，用于 per-strategy PnL zero-heartbeat targets。
    const xt_strategy_engine::XtDeclaredSymbolRegistry* declared_registry = nullptr;

    // ── per-day baseline (today PnL reset) ────────────────────────────────
    /// @brief Per-(sid, code) PnL baseline；nullptr 时不 emit today_pnl_net_v2。
    ct::metrics::XtStrategyPnlBaseline* strategy_pnl_baseline = nullptr;


    // ── Kill-switch 因果链 hook (panel GetTradingState actor/reason) ──────
    /// @brief 记录 kill_switch 状态变化到 binder 的 causal-chain state（actor/reason/ts）。
    /// 由 runner 注入闭包包住 binder 持有的 XtKillSwitchState；空时 auto-trip/release 仅记日志/metric。
    /// @note xt_live/control 不直接依赖 xt_panel：closure 在 runner 装配处构造（避免层依赖 / xt_tests 链接）。
    std::function<void(bool active, const std::string& actor,
                       const std::string& reason, int64_t ts_unix_ns)> record_kill_switch_trip;

    // ── panel force-exit 续帧 latch（T1.155，HFT-only）───────────────────
    /// @brief panel ForceExit CancelledOnly 的续帧重试 latch；nullptr = 关闭。
    /// RPC 线程 arm（XtRunnerPanelControl 经 XtPanelContext 持同一指针），
    /// frame loop 每轮 step 直到 flat（Done）/ TTL 到期（Expired）。
    xt_live::control::PanelForceExitLatch* force_exit_latch = nullptr;

    /// @brief force-flat submit 同步 fence（over-close 缝挡板）；nullptr = 关闭。
    /// liquidator / latch / RPC 三路 XtPanelContext 投影共用同一实例。
    xt_live::control::ForceFlatSubmitFence* force_flat_fence = nullptr;

    // ── account 帧新鲜度（发布请求投递）─────────────────────────────────
    /// @brief 向 account worker 投递「执行在册发布请求」闭包（td 队列 WorkerTask）；
    ///        返回 false = 队列满。行情不再发布帧，这是 equity 帧刷新的周期来源。
    /// 未注入时兜底发布与 session-begin 同步一并跳过（见 RiskFrameDeps 同名字段）。
    std::function<bool()> post_account_publish_request;

    /// @brief 控制环停机信号；`HftRunnerControl::run()` 入口写入，供 session-begin
    ///        的有界等待立即退出。run() 之外为 nullptr。
    const std::atomic<bool>* shutdown_flag = nullptr;
};

// ── 控制环装配 ──────────────────────────────────────────────
/// @brief 构造控制环上下文（runner control context factory）。
/// @note 由 XtHftRunner 装配期调用；所有引用均为 non-owning，生命期由 HftEngineRuntime 管理。
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
    bool ok = true;                     ///< 一致性检查是否通过
    double frozen_cash = 0.0;           ///< 账户级冻结资金（account state 口径）
    double frozen_cash_by_orders = 0.0; ///< 订单冻结 ledger 影子总额（account order-slot 口径）
    size_t open_order_count = 0;        ///< 当前在途订单数
    size_t tracked_code_count = 0;      ///< 持仓跟踪合约数
    std::string reason;                 ///< ok=false 的不一致原因；或 ok=true 时的不可用说明（如快照未发布）
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
        xt_report::XtLiveReporter& live_reporter; ///< 报告器（non-owning ref）
    };

    HftRunnerControl(const XtRunnerControlConfig& cfg,
                  XtHftRunnerControlContext& ctx,
                  const Deps& deps);
    ~HftRunnerControl();  // out-of-line：last_snapshot_ 持不完整 ControlSnapshot 的 unique_ptr

    /// @brief 进入控制环主循环，直到 shutdown 信号置位。
    /// @note 单线程调用；函数内部按 cfg.sleep_ms 节拍采样、执行策略链并写报告。
    ///       循环退出时**只缓存**最后一帧快照，不再落 final KPI/RunSummary——收尾报表
    ///       由 finalize_report() 承担（须在 runner stop_runtime_threads 之后调用）。
    void run(std::atomic<bool>& shutdown);

    /// @brief 收尾报表 flush：stop latency tracker + fresh snapshot + KPI/RunSummary flush。
    ///
    /// **必须在 runner 的 event loop / gateway / channels 全部停机（stop_runtime_threads）
    /// 之后调用**：tracker->stop() 的 final drain 才能纳入这些线程停机前产出的尾批 latency
    /// 样本（run() 内 stop 会因 enabled=false 丢弃尾样）。run() 不再调用本方法。无缓存快照
    /// （从未进入循环体）时 no-op；对 tracker 幂等（TrackerStopGuard 后置兜底）。
    void finalize_report();

    /// @brief 配置 per-sid 主动强平 driver（runner 装配期一次性注入）。
    /// @note 默认 cfg.enabled=false → frame loop 内 step() 整段 no-op。HFT-only。
    void configure_auto_liquidator(xt_live::control::PerSidLiquidatorCfg cfg) {
        liquidator_.configure(std::move(cfg));
    }

private:
    const XtRunnerControlConfig& cfg_;                   ///< 控制环配置（non-owning ref）
    XtHftRunnerControlContext& ctx_;                        ///< 注入上下文（non-owning ref）
    xt_report::XtLiveReporter& live_reporter_;            ///< 报告器（non-owning ref）
    xt_live::control::PerSidLiquidator liquidator_;      ///< per-sid 主动强平（frame loop 持有状态）

    // run() 退出时缓存最后一帧，供 finalize_report()（在 runner 停机线程后）消费。
    bool finalize_ready_ = false;                        ///< 至少进入过一轮循环体（有可用快照）
    std::unique_ptr<xt_live::control::detail::ControlSnapshot> last_snapshot_;
    int64_t last_snapshot_ms_ = 0;
    // 本类只负责 control frame loop；命令面 RPC 由 XtRunnerPanelControl 处理。
};
