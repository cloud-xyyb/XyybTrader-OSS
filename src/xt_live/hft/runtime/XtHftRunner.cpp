#include <atomic>
#include "xt_engine/hft/XtHftEngine.h"
#include "xt_live/telemetry/observer/XtHftLiveObserverCombo.h"
#include "api/IHftEngineObserver.h"
#include "xt_live/telemetry/recording/XtLiveRecorder.h"

// gRPC completion callback 在本 TU 直接写 control-plane Prometheus 指标。
#include "XtMetricRegistry.h"
#include "xt_metrics/XtVenueLabel.h"
#include <cstdlib>
#include "xt_metrics/XtStrategyPnlAggregator.h"
#include "xt_strategy_engine/XtDeclaredSymbolRegistry.h"
#include "xt_metrics/XtStrategyPnlBaseline.h"

// control plane 运行路径：panel 通过 gRPC mTLS/TCP 进入 runner。
#ifdef XT_HAVE_CONTROL_GRPC
#include "XtControlGrpcServer.h"
#include "XtControlGrpcInterceptors.h"
#include "FillsCsvTail.h"            // xt_panel/query (GetRecentFills helper)
#include "OrderbookSnapshotBuilder.h"  // xt_panel/query (经 xt_panel PUBLIC include 解析)
#include "ControlGrpcBoot.h"         // make_control_grpc_server / start_control_grpc_or_log
#include "ControlGrpcCommon.h"
#include "HftControlGrpcBinder.h"
#include "XtRunnerPanelControl.h"    // xt_panel/command (panel 命令面)
#endif
#include "XtKillSwitchState.h"       // xt_live/control (kill-switch trip 因果链)

#include <algorithm>
#include <cassert>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <exception>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "XtStrategyContext.h"
#include "xt_identity/StrategySidRegistry.h"
#include "XtLogger.h"
#include "GatewayFactory.h"
#include "api/IMdGateway.h"
#include "xt_ipc/XtShmMdGateway.h"  // dynamic_cast for shm attach 观测（T1.141①）
#include "xt_live/hft/runtime/XtHftChannels.h"
#include "XtLiveConfig.h"
#include "XtLiveOrderGateway.h"
#include "XtLiveParamDomain.h"
#include "XtLiveParamPort.h"
#include "xt_risk/config/XtRiskCfg.h"
#include "xt_risk/engine/XtRiskEngine.h"
#include "xt_runtime/order/XtOrderRuntimeBook.h"
#include "XtLiveRuntimeStateCfg.h"
#include "xt_strategy_engine/view/XtRuntimeView.h"
#include "xt_runtime/market/XtRuntimeMarketCache.h"
#include "xt_report/live/XtLiveReporter.h"
#include "xt_report/core/XtCsvReportSink.h"
#include "xt_util/time/XtSteadyClock.h"
#include "xt_util/path/PathUtil.h"
#include "xt_core/XtFixedPoint.h"  // auto-liquidation pct double -> fp
#include "xt_live/hft/control/HftRunnerControl.h"
#include "xt_live/hft/control/PanelForceExitLatch.h"  // panel force-exit 续帧 latch（T1.155）
#include "xt_live/control/ForceFlatSubmitFence.h"     // force-flat submit 同步 fence（over-close 缝挡板）
#include "xt_live/hft/reconcile/XtHftAccountReconcile.h"
#include "xt_live/common/AccountTruthSourceFactory.h"
#include "xt_live/common/StrategyUserdataStore.h"  // 策略 userdata 跨重启持久化
#include "xt_live/common/VenueTraits.h"  // provider capability 单点（closeable bootstrap 可信度）
#include "xt_live/common/XtLiveStrategyTelemetrySink.h"  // report_metric/intent trace 下沉 adapter
#include "binance/rest/BinanceIncomeProvider.h"  // caller-side income provider 注入
#include "PanelHelpers.h"
#include "RunnerBootCommon.h"  // apply_initial_control_enabled（HFT/CTA/SEL 共用）
#include "xt_util/runtime/XtProcessUtil.h"
#include "api/XtRunnerSignals.h"
#include "xt_live/hft/runtime/XtHftEventLoop.h"
#include "xt_live/hft/telemetry/XtHftTelemetryWiring.h"
#include "xt_live/hft/reconcile/XtHftReconcileLoop.h"
#include "api/IReconcilableGateway.h"
// ── Runner 公共组件 ───────────────────────────────────────────────────────
#include "XtPluginHolder.h"
#include "xt_strategy_engine/loader/StrategyInstance.h"          // 跨 live/backtest 共享 StrategyInstance
#include "xt_strategy_engine/loader/StrategyAssemblyContract.h"  // 跨 live/backtest 共享 4-tuple 对齐检查
#include "XtLiveBootstrap.h"
#include "RunnerCommon.h"

using xt_runtime::XtRuntimeMarketCache;

namespace {

using HftStrategyInstance = xt_strategy_engine::StrategyInstance;

struct HftEngineBootResources {
    XtProcessUtil::SingleInstanceLock instance_lock;
    // 策略插件持有者；装配后至少 1 个。
    std::vector<HftStrategyInstance> strategies;

    // ── 公共基础设施（account / catalog / param / latency）─────────────
    XtLiveBootstrap base;

    // ── HFT-specific 风控 / 订单状态 ─────────────────────────────────────
    // 风控快照由 xt_account::AccountStateStore hazard snapshot 发布。
    xt_runtime::order::XtOrderRuntimeBook       order_book;

    std::unique_ptr<XtRuntimeMarketCache>       market_cache;
    std::unique_ptr<xt_strategy_engine::XtRuntimeView>  runtime_view;
};

struct HftEngineSharedResources {
    XtRunnerSignals shared_state;
    // Context-as-sink raw pointer vector；pointee 由 engine 的 slots_ 拥有（原子装配后
    // 集合封口不再变化）。本 view 在 engine 构造前一次性收集、engine 构造成功后注入
    // gateway，此后不再改动；teardown 必须先 clear_exec_sinks 再析构 engine。
    std::vector<xt_strategy_engine::IStrategyExecSink*> exec_sink_ptrs;
};

struct HftEngineRunResources {
    std::unique_ptr<XtLiveOrderGateway> order_gateway;
    std::unique_ptr<XtStrategyContext::IParamPort> param_port;
    std::unique_ptr<xt_strategy_engine::XtDeclaredSymbolRegistry> declared_registry;

    // sid_registry 是本进程内 strategy string 与 1-based sid_id 的唯一映射。
    // bootstrap 后 freeze，后续热路径只读；risk cfg、context 和 gateway 必须共享
    // 同一 sid_id 空间，否则分策略限仓/归因会错位。
    std::unique_ptr<xt_identity::StrategySidRegistry> sid_registry;
    std::unique_ptr<XtHftEngine> hft_engine;
    std::unique_ptr<xt_live::XtHftLiveObserverCombo> hft_observer;
    // 分策略 PnL 累计器：realized 只接 engine per-fill callback，fee 只接
    // fee_observer，HftRunnerControl 仅读 snapshot 后 emit today_pnl_net_v2。
    std::unique_ptr<ct::metrics::XtStrategyPnlAggregator> strategy_pnl_aggr;
    std::unique_ptr<xt_runtime::IHftEngineObserver>       fee_observer;
    // 每日 PnL baseline 按 trading_day_id 持久化；load 失败时 HftRunnerControl
    // fail-closed，不 emit 依赖 baseline 的 today_pnl_net_v2。
    std::unique_ptr<ct::metrics::XtStrategyPnlBaseline>   strategy_pnl_baseline;
    std::unique_ptr<XtHftEventLoop> event_loop;
    std::unique_ptr<IAccountTruthSource> account_truth_source;
    IAccountTruthSink* account_truth_sink = nullptr;  ///< 同一对象的写侧 port（非拥有）。
    // reconcile_loop 只在 gateway 明确支持 IReconcilableGateway 时构造；stop 顺序
    // 必须早于 channels/td 析构，避免后台 reconcile 线程访问已释放 gateway。
    std::unique_ptr<xt_live::control::XtHftReconcileLoop> reconcile_loop;
    // ── SPSC 诊断通道队列（owning，EventLoop 生产端 + HftRunnerControl 消费端共享）──
    std::unique_ptr<XtLockFreeQueue<XtTradeRecord>>     trade_drain_q;
    std::unique_ptr<XtLockFreeQueue<XtOrderExecUpdate>> order_update_drain_q;
    std::atomic<bool> param_watch_stop{false};
    std::thread param_watch_thread;
    std::unique_ptr<IMdGateway> md;
    std::unique_ptr<ITradeGateway> td;
    std::unique_ptr<XtHftChannels> channels;
};

struct HftEngineRuntime {
    HftEngineBootResources boot;
    HftEngineSharedResources shared;
    HftEngineRunResources run;
};

// ── 策略 userdata 跨重启持久化（graceful-stop dump + 装配期 restore）──────────
//
// SDK 零 IO：ctx 只提供 snapshot_userdata()/restore_userdata()，落盘/读回归 runner。
// userdata fail-safe（读失败空启动，绝不 abort）；HFT tick worker 内禁止文件 IO，
// restore 在 event loop 启动（worker 调 engine.start()）前、dump 在 worker join 后。

/// @brief 装配期回灌所有策略 userdata（event loop 启动前、worker 未跑 start()）。
void restore_all_strategy_userdata_(HftEngineRuntime& st, const XtLiveConfig& cfg) {
    if (!st.run.hft_engine) return;
    const std::string state_dir = xt_live::common::derive_userdata_state_dir(
        cfg.bootstrap.recording_cfg.file_path, cfg.parsed.cfg_path);
    // 遍历归 engine 窄口（context 可变操作不出 engine）；runner 只提供读盘策略。
    st.run.hft_engine->restore_strategy_userdata(
        [&](const std::string& sid, std::unordered_map<std::string, std::string>& out) {
            std::string err;
            if (xt_live::common::read_strategy_userdata(state_dir, sid, out, err, "RT")) return true;
            if (!err.empty()) {
                XtLogger::warn_m("RT", "userdata restore skipped strategy={} ({})", sid, err);
            }
            return false;
        });
}

/// @brief graceful stop（event loop worker 已 join）后 dump 各策略 userdata。
void dump_all_strategy_userdata_(HftEngineRuntime& st, const XtLiveConfig& cfg) {
    if (!st.run.hft_engine) return;
    const std::string state_dir = xt_live::common::derive_userdata_state_dir(
        cfg.bootstrap.recording_cfg.file_path, cfg.parsed.cfg_path);
    const uint32_t trading_date =
        st.shared.shared_state.last_tick_date.load(std::memory_order_acquire);
    st.run.hft_engine->dump_strategy_userdata(
        [&](const std::string& sid, std::unordered_map<std::string, std::string> kv) {
            std::string err;
            if (!xt_live::common::write_strategy_userdata(state_dir, sid, trading_date, kv, err)) {
                XtLogger::warn_m("RT", "userdata dump failed strategy={} ({})", sid, err);
            }
        });
}

// report sink/tracker 生命周期收口：reporter 持 sink&、Deps 持 reporter&，
// 两者必须 span control_loop.run()。声明序 sink→reporter 保证析构逆序安全
// （reporter 先于 sink 析构，sink& 始终有效）。bundle 为 composition root local，不 move。
struct HftLiveReportBundle {
    xt_report::XtCsvReportSink sink;
    xt_report::XtLiveReporter  reporter;
    HftLiveReportBundle(const xt_report::XtLiveReporter::Config& tracker_cfg,
                        int64_t control_start_ms)
        : reporter(sink, tracker_cfg, control_start_ms) {}
};


bool try_lock_single_instance(HftEngineRuntime& st, const XtLiveConfig& cfg) {
    std::string lock_holder;
    if (st.boot.instance_lock.acquire(cfg.parsed.cfg_path, &lock_holder)) return true;
    if (!lock_holder.empty()) {
        XtLogger::error_m("RT", "another XtRunner is running (pid={}) lock={}",
                          lock_holder, cfg.parsed.cfg_path + ".lock");
    } else {
        XtLogger::error_m("RT", "another XtRunner is running (lock={})",
                          cfg.parsed.cfg_path + ".lock");
    }
    return false;
}

bool try_create_strategy(HftEngineRuntime& st, const XtLiveConfig& cfg) {
    // 多策略加载 — 直接读 cfg.parsed.strategies[]。yaml parser 保证此 vector 至少
    // 1 element (`runtime.strategies` N 可为 1)。
    if (cfg.parsed.strategies.empty()) {
        XtLogger::error_m("RT",
            "cfg.parsed.strategies empty — yaml parser invariant violated "
            "(should always have at least 1 element from runtime.strategies)");
        return false;
    }
    const std::vector<LiveStrategyEntry>& entries = cfg.parsed.strategies;

    const std::size_t n = entries.size();
    st.boot.strategies.reserve(n);

    for (std::size_t i = 0; i < n; ++i) {
        const auto& entry = entries[i];
        const std::string so_path = PathUtil::resolve_relative(cfg.parsed.cfg_path, entry.factory_cfg.so_path);
        HftStrategyInstance inst;
        inst.plugin = std::make_unique<XtPluginHolder>();
        if (!inst.plugin->load(so_path)) {
            XtLogger::error_m("RT", "load strategy[{}] failed id={} so={} err={}",
                              i, entry.factory_cfg.id, so_path, inst.plugin->err);
            return false;
        }
        inst.id = entry.factory_cfg.id;
        XtLogger::info_m("RT", "strategy[{}] loaded id={}", i, inst.id);
        st.boot.strategies.push_back(std::move(inst));
    }
    return true;
}

bool try_build_runtime_and_params(HftEngineRuntime& st, const XtLiveConfig& cfg) {
    // ── 公共基础设施（account / catalog / param / latency）─────────────
    std::string boot_err;
    if (!st.boot.base.create(cfg, boot_err)) {
        XtLogger::error_m("RT", "{}", boot_err);
        return false;
    }

    // ── symbol + mark cache pre-warm + freeze ──────────────────────────────
    // config symbol 一次性注册、预热 mark cache 并发布 gen-0 FullSnapshot。
    // CTP 跨日新合约视为 yaml 订阅遗漏（config gap），由 stderr WARN 暴露。
    // 不在此过滤空订阅集：空集直达 configure_symbols 的 fail-fast，拒起而非
    // 让 store 停在「无 gen-0 帧」的降级态（读侧退化为直读 writer state）。
    if (st.boot.base.account_store) {
        st.boot.base.account_store->bind_worker_thread();
        st.boot.base.account_store->configure_symbols(cfg.parsed.md_cfg.subs);
        // closeable bootstrap 可信度按 venue trait 判定（binance 即时可信；
        // CTP/XTP fail-closed 等首次 position recovery，裁定见 VenueTraits.h）。
        if (!xt_live::common::provider_closeable_known_at_bootstrap(cfg.parsed.td_provider)) {
            st.boot.base.account_store->set_closeable_known(false);
            XtLogger::info_m("RT",
                "closeable_known=false until first position snapshot "
                "(provider={}, close orders reject as PositionStateUnknown during bootstrap gap)",
                cfg.parsed.td_provider);
        }
        XtLogger::info_m("RT",
            "account snapshots + mark_cache initialized with {} subscribed instrument(s)",
            cfg.parsed.md_cfg.subs.size());
    }

    // ── HFT-specific: XtRuntimeMarketCache + XtRuntimeView ─────────────
    st.boot.market_cache = std::make_unique<XtRuntimeMarketCache>();
    // off-worker 发布快照槽预注册（冷路径、worker 启动前）：让 control report emit / gRPC panel
    // 经 snapshot_last_tick 跨线程安全读到真值 bid/ask/L2；否则 published_tick_slots_ 恒空 →
    // off-worker 回填空值（panel bid/ask=0 / orderbook widget "no tick"）。
    st.boot.market_cache->register_published_symbols(cfg.parsed.md_cfg.subs);
    // tick/bar/orderflow caps 由组合根统一 configure()（本文件 engine 构造前，全覆盖三个
    // 字段）；bootstrap 窗口内无人读取 caps 或灌 tick，此处不再重复设置。
    {
        xt_strategy_engine::XtRuntimeView::Components comp;
        comp.market      = st.boot.market_cache.get();
        comp.account     = st.boot.base.account_view.get();
        comp.order_book  = &st.boot.order_book;
        comp.catalog     = &st.boot.base.catalog;
        st.boot.runtime_view = std::make_unique<xt_strategy_engine::XtRuntimeView>(comp);
    }
    return true;
}

// 创建 order gateway 并注入 Runner 持有的风控/账户依赖。
static void create_order_gateway_(HftEngineRuntime& st, const XtLiveConfig& cfg) {
    // gateway 先无 exec sink 构造；contexts 全部 add 完成后再一次性注入。
    st.run.order_gateway = std::make_unique<XtLiveOrderGateway>(
        cfg.bootstrap.order_gateway_cfg,
        nullptr,
        &st.shared.shared_state.trading_ready,
        &st.shared.shared_state.last_tick_action_time,
        &st.shared.shared_state.td_position_ready,
        &st.boot.base.param_domain,
        st.boot.base.latency_tracker.get(),
        &st.shared.shared_state.kill_switch,
        XtRiskEngine::InitDeps{&cfg.bootstrap.live_fees, &st.boot.base.catalog});
    st.run.order_gateway->inject_components(
        &st.boot.order_book,
        st.boot.base.account_store.get(),
        &st.shared.shared_state.latest_close_profit_fp,
        &st.boot.base.catalog,
        &st.shared.shared_state.last_position_snapshot_ms,
        cfg.bootstrap.runtime_state_cfg.enable_position_snapshot_stale_guard,
        cfg.bootstrap.runtime_state_cfg.position_snapshot_stale_ms);
    // STP 自成交防护上膛：以 subscribed symbols 预注册价格边界（否则未注册品种
    // would_self_trade 恒 false = 旁路）。运行期新合约由 submit 热路径 lazy 注册兜底。
    st.run.order_gateway->pre_warm_risk(cfg.parsed.md_cfg.subs);
    // 上膛度量（T1.159）：0 = 自成交防护旁路（含 stp_enabled=false）。装配期初值 set，
    // 之后 1Hz frame-tail 重发（F-D）。
    ct::metrics::XtMetricRegistry::instance().set_stp_registered_symbols(
        xt_util::canonical_venue_label(cfg.parsed.md_provider),
        static_cast<uint32_t>(st.run.order_gateway->stp_registered_gauge()));
}

static void apply_risk_hot_params_(XtLiveOrderGateway& gw,
                                   XtStrategyContext::IParamPort& param_port) {
    XtRiskCfg new_cfg = gw.risk_config();  // 保留未热更字段。

    auto get_double_or = [&](const char* key, double def) -> double {
        std::string buf;
        if (!param_port.get(key, buf)) return def;
        try { return std::stod(buf); } catch (...) { return def; }
    };
    auto get_bool_or = [&](const char* key, bool def) -> bool {
        std::string buf;
        if (!param_port.get(key, buf)) return def;
        std::string v;
        v.reserve(buf.size());
        for (char c : buf) v.push_back(static_cast<char>(::tolower(c)));
        if (v == "1" || v == "true" || v == "yes" || v == "on") return true;
        if (v == "0" || v == "false" || v == "no" || v == "off") return false;
        return def;
    };
    // 数量/频率字段用 uint64_t；负数字符串按缺省值处理。
    auto get_uint64_or = [&](const char* key, uint64_t def) -> uint64_t {
        std::string buf;
        if (!param_port.get(key, buf)) return def;
        try {
            const long long v = std::stoll(buf);
            return v < 0 ? def : static_cast<uint64_t>(v);
        } catch (...) { return def; }
    };

    const double   new_max_order_qty             = get_double_or("risk.max_order_qty",             new_cfg.max_order_qty);
    const double   new_max_order_notional        = get_double_or("risk.max_order_notional",        new_cfg.max_order_notional);
    const double   new_max_pos_qty               = get_double_or("risk.max_pos_qty",               new_cfg.max_pos_qty);
    const bool     new_allow_short               = get_bool_or  ("risk.allow_short",               new_cfg.allow_short);
    const uint64_t new_max_order_count_per_sym   = get_uint64_or("risk.max_order_count_per_symbol", new_cfg.max_order_count_per_symbol);
    const uint64_t new_max_order_rate_per_sym    = get_uint64_or("risk.max_order_rate_per_symbol",  new_cfg.max_order_rate_per_symbol);
    const double   new_max_daily_loss            = get_double_or("risk.max_daily_loss",            new_cfg.max_daily_loss);

    const bool changed =
        (new_max_order_qty             != new_cfg.max_order_qty) ||
        (new_max_order_notional        != new_cfg.max_order_notional) ||
        (new_max_pos_qty               != new_cfg.max_pos_qty) ||
        (new_allow_short               != new_cfg.allow_short) ||
        (new_max_order_count_per_sym   != new_cfg.max_order_count_per_symbol) ||
        (new_max_order_rate_per_sym    != new_cfg.max_order_rate_per_symbol) ||
        (new_max_daily_loss            != new_cfg.max_daily_loss);

    if (!changed) return;

    new_cfg.max_order_qty               = new_max_order_qty;
    new_cfg.max_order_notional          = new_max_order_notional;
    new_cfg.max_pos_qty                 = new_max_pos_qty;
    new_cfg.allow_short                 = new_allow_short;
    new_cfg.max_order_count_per_symbol  = new_max_order_count_per_sym;
    new_cfg.max_order_rate_per_symbol   = new_max_order_rate_per_sym;
    new_cfg.max_daily_loss              = new_max_daily_loss;
    gw.set_risk_config(new_cfg);
    XtLogger::info_m("RISK",
                      "hot reload applied: max_order_qty={} max_order_notional={} max_pos_qty={} "
                      "allow_short={} max_order_count_per_symbol={} max_order_rate_per_symbol={} "
                      "max_daily_loss={}",
                      new_max_order_qty, new_max_order_notional, new_max_pos_qty,
                      new_allow_short, new_max_order_count_per_sym, new_max_order_rate_per_sym,
                      new_max_daily_loss);
}

// 构造 XtHftEngine::Dependencies；调用方必须已完成 order gateway / TD /
// param / bootstrap 装配。assert 只保护装配顺序不变量，不参与 release 容错。
static XtHftEngine::Dependencies make_hft_engine_deps_(HftEngineRuntime& st,
                                                       const XtLiveConfig& cfg) {
    // helper 拆出后失去原 inline 上下文；这些依赖必须在 caller 侧先完成。
    assert(st.run.td             && "make_hft_engine_deps_: st.run.td 未 ready");
    assert(st.run.order_gateway  && "make_hft_engine_deps_: st.run.order_gateway 未 ready");
    XtHftEngine::Dependencies deps;
    deps.gateway             = st.run.order_gateway.get();
    deps.view                = st.boot.runtime_view.get();
    deps.account_store       = st.boot.base.account_store.get();
    deps.order_book          = &st.boot.order_book;
    deps.catalog             = &st.boot.base.catalog;
    deps.order_id_translator = &st.run.order_gateway->logical_order_tracker();
    // portfolio_router 启用时注入 sid fan-out 索引；关闭时保持 null，engine 走
    // broadcast 兼容路径。
    if (cfg.parsed.portfolio_router) {
        deps.declared_registry = st.run.declared_registry.get();
    }
    return deps;
}

// 构造 XtHftEngine::Options；unmatched policy 由 (是否多策略, 是否 portfolio_router) 派生。
static XtHftEngine::Options make_hft_engine_options_(const HftEngineRuntime& st,
                                                     const XtLiveConfig& cfg) {
    XtHftEngine::Options opts;
    if (cfg.bootstrap.has_roll_cfg) opts.roll = cfg.bootstrap.roll_cfg;
    opts.conditional_orders_enabled =
        cfg.bootstrap.runtime_state_cfg.conditional_orders_enabled;

    // 多策略路由找不到 sid 时必须 fail closed，避免静默丢事件导致分策略账本漂移：
    // portfolio_router 部署有控制面 kill-switch 兜底 → drop+alert 后由 operator 处置；
    // 无 router 时无此兜底 → fail-fast 立即暴露归因错误。
    // 单策略下 unmatched 分支不可达，取默认 warn+drop。
    if (st.boot.strategies.size() > 1) {
        opts.unmatched_routing = cfg.parsed.portfolio_router
            ? XtHftEngine::UnmatchedRoutingPolicy::DropAndAlert
            : XtHftEngine::UnmatchedRoutingPolicy::FailFast;
    }
    return opts;
}

// 填充 engine hook 中与 runner 控制面相关的两项；telemetry/observer 由 caller 续填。
static void fill_hft_engine_control_hooks_(XtHftEngine::Hooks& hooks,
                                           HftEngineRuntime& st) {
    // risk hot-reload 在 strategy on_params 后经 gateway.set_risk_config 应用。
    auto* gw_ptr  = st.run.order_gateway.get();
    auto* port_ptr = st.run.param_port.get();
    hooks.on_params_extra = [gw_ptr, port_ptr](uint64_t /*version*/) {
        if (!gw_ptr || !port_ptr) return;
        apply_risk_hot_params_(*gw_ptr, *port_ptr);
    };
    // 策略 on_init 失败 → fail-closed disable + per-sid kill latch（close bypass 保留）。
    // risk_monitor_state 已由 base.create() 构造（先于本装配），指针稳定。
    hooks.on_strategy_init_failure =
        xt_live::control::make_strategy_init_failure_handler(
            st.boot.base.risk_monitor_state.get());
}

// 填充 XtHftEventLoop::Deps；调用方必须已完成 engine 构造和 observer 注册。
static void fill_hft_event_loop_deps_(XtHftEventLoop::Deps& loop_deps,
                                      HftEngineRuntime& st,
                                      const XtLiveConfig& cfg,
                                      std::atomic<bool>& shutdown_flag) {
    assert(st.run.hft_engine && "fill_hft_event_loop_deps_: st.run.hft_engine 未 ready");
    assert(st.run.order_gateway && "fill_hft_event_loop_deps_: st.run.order_gateway 未 ready");
    loop_deps.engine                = st.run.hft_engine.get();
    loop_deps.shutdown_flag         = &shutdown_flag;
    loop_deps.last_tick_action_time = &st.shared.shared_state.last_tick_action_time;
    loop_deps.last_tick_steady_ms   = &st.shared.shared_state.last_tick_steady_ms;
    loop_deps.tick_proc_cnt         = &st.shared.shared_state.tick_proc_cnt;
    // session gate 由 EventLoop 每 tick 写 atomic，strategy context 只读该
    // atomic 短路 on_tick。未配置 sections 时保持 true，表示不拦截策略。
    loop_deps.in_trading_section    = &st.shared.shared_state.in_trading_section;
    loop_deps.session_sections      = cfg.bootstrap.order_gateway_cfg.session_gate_enabled
                                      ? &cfg.bootstrap.order_gateway_cfg.session_gate_sections
                                      : nullptr;
    // closeout_no_open window（如 14:55-14:59 / 02:20-02:30）未启用时
    // atomic 保持 false，表示允许开仓。
    loop_deps.closeout_no_open      = &st.shared.shared_state.closeout_no_open_window;
    loop_deps.closeout_windows      = cfg.bootstrap.order_gateway_cfg.closeout_enabled
                                      ? &cfg.bootstrap.order_gateway_cfg.closeout_windows
                                      : nullptr;
    // 成交成本归一化在 live 数据源层完成，engine 接收统一口径。
    loop_deps.live_cost             = &cfg.bootstrap.live_cost;
    loop_deps.live_fees             = &cfg.bootstrap.live_fees;
    loop_deps.catalog               = &st.boot.base.catalog;
    loop_deps.initial_param_version = st.boot.base.param_domain.version();
    // post-trade self-match 检测 + 风险出口；构造期一次性注入（无晚就绪约束）。
    loop_deps.self_match_detector   = st.boot.base.self_match_detector.get();
    loop_deps.blacklist_sink        = [gw = st.run.order_gateway.get()](const char* c, const char* r) {
        gw->insert_self_match_blacklist(c, r);
    };
    // off-worker 注册溢出侧道 worker 侧 drain：worker 每轮调用（侧道空时 O(1) 早退）。
    // order_book 地址稳定（st.boot 值成员），marshal 绑定与 drain 同锚 &st.boot.order_book。
    loop_deps.marshal_overflow_drain = [book = &st.boot.order_book]() {
        book->drain_marshal_side_channel();
    };
    // worker run() 首部 bind_producer_slot：worker 上的 latency tap 走确定性 SPSC slot。
    loop_deps.latency_tracker       = st.boot.base.latency_tracker.get();
}

// ─── try_init_and_start_strategy_runtime helpers ────────────────────────────
//
// 主装配按职责拆分；TdCleanupGuard 留在主函数覆盖跨阶段失败清理。

// 创建 TD gateway 并应用 recovery 配置；bind_td 延后到 channels start 阶段。
static bool create_td_gateway_(HftEngineRuntime& st, const XtLiveConfig& cfg) {
    std::string factory_err;
    st.run.td = gateway_factory::create_td_gateway(cfg.parsed.td_provider,
                                                   cfg.parsed.td_cfg,
                                                   cfg.parsed.td_provider_cfg,
                                                   &st.boot.base.catalog,
                                                   &factory_err);
    if (!st.run.td) {
        XtLogger::error_m("RT", "td gateway create failed: {}", factory_err);
        return false;
    }
    return true;
}

// 构造全部 strategy context（engine 构造前完成；ctx_cfg 不引用 engine）。
// 并发指标与运行信号在此一并注入：漏掉任一 context 会让 kill_switch / closeout /
// paused gate 对该策略失效。
static std::vector<std::unique_ptr<XtStrategyContext>> build_multi_contexts_(
    HftEngineRuntime& st, const XtLiveConfig& cfg) {
    assert(st.run.param_port && "build_multi_contexts_: st.run.param_port 未 ready");
    const std::size_t n = st.boot.strategies.size();
    std::vector<std::unique_ptr<XtStrategyContext>> contexts;
    contexts.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        XtStrategyBase* stra_i = st.boot.strategies[i].plugin->get();
        XtStrategyContext::Config ctx_cfg;
        ctx_cfg.strategy     = stra_i;
        ctx_cfg.exec_gateway = st.run.order_gateway.get();
        ctx_cfg.runtime_view = st.boot.runtime_view.get();
        ctx_cfg.strategy_id  = st.boot.strategies[i].id;
        ctx_cfg.param_port   = st.run.param_port.get();
        ctx_cfg.telemetry_sink = &xt_live::common::live_strategy_telemetry_sink();
        ctx_cfg.init_config  = (i < cfg.parsed.strategies.size())
                                   ? &cfg.parsed.strategies[i].init_cfg
                                   : &cfg.parsed.strategy_init_cfg;
        if (st.run.declared_registry && ctx_cfg.init_config) {
            const auto symbols =
                xt_live::control::collect_strategy_config_symbols(*ctx_cfg.init_config);
            for (const auto& code : symbols) {
                st.run.declared_registry->declare(ctx_cfg.strategy_id, code);
            }
        }
        // portfolio_router 启用时按 sid 过滤 get_open_orders；关闭时只读
        // runtime_view 聚合视图。
        if (cfg.parsed.portfolio_router) {
            ctx_cfg.route_by_sid = true;
        }
        ctx_cfg.declared_registry = st.run.declared_registry.get();
        // live 下把 signed in-flight 投影到 get_position_fp / plugin cur_pos，防止重单。
        ctx_cfg.project_inflight = cfg.parsed.portfolio_router;
        ctx_cfg.conditional_orders_enabled =
            cfg.bootstrap.runtime_state_cfg.conditional_orders_enabled;
        auto owned_ctx = std::make_unique<XtStrategyContext>(ctx_cfg);
        xt_live::common::apply_initial_control_enabled(
            *owned_ctx, st.boot.base.param_domain, st.boot.strategies[i].id, "RT");
        owned_ctx->enable_concurrent_metrics();
        owned_ctx->set_signals(&st.shared.shared_state);
        contexts.push_back(std::move(owned_ctx));
        XtLogger::info_m("RT", "context[{}] added strategy={}",
                         i, st.boot.strategies[i].id);
    }
    return contexts;
}

// 在 engine 接管所有权前从 context 集合生成 gateway 侧 sink view（context 即 exec
// sink，unique_ptr move 不动 pointee，构造前后指针等价）。
static void collect_exec_sink_view_(
    HftEngineRuntime& st,
    const std::vector<std::unique_ptr<XtStrategyContext>>& contexts) {
    st.shared.exec_sink_ptrs.clear();
    st.shared.exec_sink_ptrs.reserve(contexts.size());
    for (const auto& ctx : contexts) {
        st.shared.exec_sink_ptrs.push_back(
            ctx ? static_cast<xt_strategy_engine::IStrategyExecSink*>(ctx.get()) : nullptr);
    }
}

// 把已生成的 sink view 注入 gateway（gateway 侧协议不变）。
static void inject_gateway_exec_sinks_(HftEngineRuntime& st, const XtLiveConfig& cfg) {
    auto* fallback = st.shared.exec_sink_ptrs.empty() ? nullptr : st.shared.exec_sink_ptrs[0];
    if (!st.run.order_gateway) return;
    if (cfg.parsed.portfolio_router) {
        st.run.order_gateway->set_exec_sinks(
            fallback, st.shared.exec_sink_ptrs, st.run.sid_registry.get());
    } else {
        st.run.order_gateway->set_exec_sinks(fallback, {}, nullptr);
    }
}

// 校验 multi-mode 装配后的 strategy / sink / context 下标对齐。
static bool verify_multi_assembly_alignment_(const HftEngineRuntime& st,
                                              const XtLiveConfig& cfg) {
    std::vector<std::string> cfg_ids;
    cfg_ids.reserve(cfg.parsed.strategies.size());
    for (const auto& sc : cfg.parsed.strategies) {
        cfg_ids.push_back(sc.factory_cfg.id);
    }
    // non-router 路径保留 sid_id=0，不注册 per-strategy sid；只有 portfolio
    // routing 激活时才校验 registry 的 1-based sid slot 对齐。
    const xt_identity::StrategySidRegistry* sid_registry =
        cfg.parsed.portfolio_router ? st.run.sid_registry.get() : nullptr;
    const auto rep = xt_strategy_engine::check_multi_exec_sink_alignment(
        cfg_ids,
        st.boot.strategies,
        st.shared.exec_sink_ptrs,
        st.run.hft_engine.get(),
        sid_registry);
    if (!rep.aligned) {
        XtLogger::error_m("RT", "multi-assembly alignment: {}", rep.error);
        return false;
    }
    return true;
}

// 装配 HFT engine：contexts 与 hook owner 先建，engine 一次性原子构造。
// 单/多策略统一走 build_multi_contexts_，engine 不自建 default context。
static bool assemble_engine_and_contexts_(HftEngineRuntime& st, const XtLiveConfig& cfg) {
    assert(!st.boot.strategies.empty()
           && "engine ctor: strategies vector must have >=1 element");
    const std::size_t n = st.boot.strategies.size();
    const bool is_multi = (n > 1);

    if (is_multi && cfg.parsed.md_cfg.subs.size() <= 1) {
        // 多策略同 symbol 共享 account_store net position，反向信号会串扰策略内
        // counter；默认部署约束是每个策略使用不同 symbol。详
        // docs/architecture/hft_multi_strategy.md。
        XtLogger::warn_m("RT",
            "multi-strategy ({} strategies) on shared instrument: NET position "
            "fan-out may cause internal counter drift if same-symbol opposite "
            "direction. Use different symbols per strategy.", n);
    }

    // 单策略也通过 build_multi_contexts_，保证 sink/context 下标协议一致。
    auto contexts = build_multi_contexts_(st, cfg);

    // hook owner 先于 engine 建：observer 注册序 [hft_observer, fee_observer] 冻结；
    // telemetry 载入失败（PnL state 损坏且 per-strategy daily-loss 已启用）在 engine
    // 构造前走标准 fail_exit，不以损坏风控输入启动。
    XtHftEngine::Hooks hooks;
    fill_hft_engine_control_hooks_(hooks, st);
    st.run.hft_observer = std::make_unique<xt_live::XtHftLiveObserverCombo>(
        st.boot.base.latency_tracker.get(),
        &st.shared.shared_state,
        st.run.trade_drain_q.get(),
        st.run.order_update_drain_q.get());
    hooks.observers.push_back(st.run.hft_observer.get());
    if (!xt_live::hft::wire_hft_telemetry(st.run.strategy_pnl_aggr,
                                          st.run.strategy_pnl_baseline,
                                          st.run.fee_observer,
                                          hooks,
                                          cfg)) {
        return false;
    }

    // history caps 由组合根设置（engine 不再代持该配置）；须早于首笔行情写入。
    {
        xt_runtime::XtRuntimeMarketCacheConfig mc_cfg;
        mc_cfg.tick_history_cap      = cfg.bootstrap.strategy_history_ticks;
        mc_cfg.bar_history_cap       = cfg.bootstrap.strategy_history_bars;
        mc_cfg.orderflow_history_cap = cfg.bootstrap.strategy_history_ticks;
        st.boot.market_cache->configure(mc_cfg);
    }

    // sink view 在 engine 接管 contexts 前生成；构造失败时 context 随异常销毁，
    // 需把已收集的指针清掉（失败路径不注入 gateway，与旧行为一致）。
    collect_exec_sink_view_(st, contexts);

    // 装配契约违反抛 std::invalid_argument；转 bool 失败走 runner 标准 fail_exit
    // 路径（结构化日志 + exposer 收尾），不裸 terminate。
    try {
        st.run.hft_engine = std::make_unique<XtHftEngine>(make_hft_engine_deps_(st, cfg),
                                                          make_hft_engine_options_(st, cfg),
                                                          std::move(contexts),
                                                          std::move(hooks));
    } catch (const std::exception& e) {
        st.shared.exec_sink_ptrs.clear();
        XtLogger::error_m("RT", "engine assembly failed: {}", e.what());
        return false;
    }

    // gateway sink view 注入（engine 已封口，指针与构造前一致）。
    inject_gateway_exec_sinks_(st, cfg);

    // 多策略装配后必须 idx 对齐；单策略 N=1 时检查退化为恒真。
    if (!verify_multi_assembly_alignment_(st, cfg)) {
        return false;
    }
    return true;
}

// 逐个调用 strategy plugin init；异常只影响启动结果，不允许逃出 runner main。
static bool init_all_strategy_plugins_(HftEngineRuntime& st, const XtLiveConfig& cfg) {
    const std::size_t n = st.boot.strategies.size();
    try {
        for (std::size_t i = 0; i < n; ++i) {
            XtStrategyBase* stra_i = st.boot.strategies[i].plugin->get();
            const auto& init_cfg = (i < cfg.parsed.strategies.size())
                                       ? cfg.parsed.strategies[i].init_cfg
                                       : cfg.parsed.strategy_init_cfg;
            stra_i->init(init_cfg);
        }
        return true;
    } catch (const std::exception& e) {
        XtLogger::error_m("RT", "strategy init failed: {}", e.what());
    } catch (...) {
        XtLogger::error_m("RT", "strategy init failed");
    }
    return false;
}

// 启动 EventLoop（observer/telemetry hook 已在 engine 构造期封口）并绑定 worker 线程亲和。
static bool start_event_loop_(HftEngineRuntime& st,
                              const XtLiveConfig& cfg,
                              std::atomic<bool>& shutdown_flag) {
    XtHftEventLoop::Deps loop_deps;
    fill_hft_event_loop_deps_(loop_deps, st, cfg, shutdown_flag);
    st.run.event_loop = std::make_unique<XtHftEventLoop>(cfg.bootstrap.event_loop_cfg,
                                                          loop_deps);
    if (!st.run.event_loop->start()) {
        XtLogger::error_m("RT", "start strategy event loop failed");
        return false;
    }

    // off-worker submit（closeout/panel 控制线程）的 book/account 注册经 td 队列
    // 序列化到 worker：同队列 FIFO 保证注册先于该单首条回报被消费。
    XtHftEventLoop* loop_p = st.run.event_loop.get();
    st.boot.order_book.bind_worker_marshal(
        loop_p->worker_thread_id(),
        [loop_p](std::function<void()> fn) {
            return loop_p->push_td_event(XtHftRunnerEvent::make_worker_task(std::move(fn)));
        });
    // 行情直读闩权威绑定（策略 ctx get_* + engine roll 直读均在同一 worker；
    // off-worker 消费者走 snapshot_last_tick 不受影响）。
    st.boot.market_cache->bind_worker_thread(loop_p->worker_thread_id());
    // account 写闩重绑到真实写者：bootstrap 期 main 绑定（configure/pre-warm 写）
    // 到此结束，loop 起来后 worker 是唯一合法 apply 线程（off-worker submit 的
    // 账本注册已经上方 marshal 序列化到同一 worker）。
    if (st.boot.base.account_store) {
        st.boot.base.account_store->bind_worker_thread(loop_p->worker_thread_id());
    }
    return true;
}

// 按 gateway capability 可选启动 reconcile loop。
static void start_reconcile_loop_if_supported_(HftEngineRuntime& st, const XtLiveConfig& cfg) {
    auto* reconcilable = dynamic_cast<IReconcilableGateway*>(st.run.td.get());
    if (!reconcilable || !reconcilable->reconcile_supported() || !st.boot.base.account_store) {
        XtLogger::info_m("RT",
            "td gateway does not implement IReconcilableGateway or reconcile disabled, "
            "skipping reconcile loop construction");
        return;
    }
    // runtime.td.reconcile.*；缺节时为 XtHftReconcileLoopCfg 字段默认值。
    const xt_live::control::XtHftReconcileLoop::Config rc_cfg = cfg.bootstrap.reconcile_cfg;
    st.run.reconcile_loop = std::make_unique<xt_live::control::XtHftReconcileLoop>(
        *reconcilable,
        *st.boot.base.account_store.get(),
        st.shared.shared_state.kill_switch,
        &st.shared.shared_state.last_position_snapshot_ms,
        rc_cfg);
    // 注入 RESUME reset 请求 atomic source — control panel RESUME
    // 通过此机制清 reconcile 的 diverged_ counter, 避免 RESUME 失效死锁。
    st.run.reconcile_loop->set_reset_request_source(
        st.shared.shared_state.reconcile_reset_request);
    // divergence trip 打 source=ReconcileDivergence；重启后首轮重新对账一致时 loop 自清。
    st.run.reconcile_loop->set_kill_switch_source(st.shared.shared_state.kill_switch_source);
    // I1 divergence export (store vs exchange positionRisk) → xt_state_divergence
    // {field="reconcile_qty",symbol}. Singleton registry; live-only (loop not built
    // in backtest/unit-test → no-op there).
    st.run.reconcile_loop->set_metric_registry(&ct::metrics::XtMetricRegistry::instance());
    st.run.reconcile_loop->start();
}

// 创建 TD/order gateway、engine/context 和 EventLoop，并启动 strategy runtime。
// TdCleanupGuard 只 reset 未绑定的 td；bind_td 发生在 channels start 阶段。
bool try_init_and_start_strategy_runtime(HftEngineRuntime& st,
                                         const XtLiveConfig& cfg,
                                         std::atomic<bool>& shutdown_flag) {
    if (!create_td_gateway_(st, cfg)) return false;

    bool init_success = false;
    struct TdCleanupGuard {
        HftEngineRuntime& st;
        const bool& success;
        ~TdCleanupGuard() noexcept {
            if (!success && st.run.td) st.run.td.reset();
        }
    } td_guard{st, init_success};

    create_order_gateway_(st, cfg);
    st.run.param_port = std::make_unique<XtLiveParamPort>(&st.boot.base.param_domain);
    // DeclaredSymbolRegistry 很轻量，总是由 Runner 持有；portfolio_router 启用时
    // engine/context 仅保存 non-owning ref。
    st.run.declared_registry = std::make_unique<xt_strategy_engine::XtDeclaredSymbolRegistry>();

    // SPSC 诊断通道队列(observer 生产端 / HftRunnerControl 消费端)
    st.run.trade_drain_q        = std::make_unique<XtLockFreeQueue<XtTradeRecord>>(
        QueueMode::SPSC, 1024);
    st.run.order_update_drain_q = std::make_unique<XtLockFreeQueue<XtOrderExecUpdate>>(
        QueueMode::SPSC, 1024);

    if (!assemble_engine_and_contexts_(st, cfg))                       return false;
    if (!init_all_strategy_plugins_(st, cfg))                          return false;

    // 策略 init 在主线程同步完成，declare_position_interest 已全部落入 registry；
    // 此处不需要等待 worker。
    {
        std::vector<std::string> sid_list;
        sid_list.reserve(st.boot.strategies.size());
        for (const auto& sentry : st.boot.strategies) {
            sid_list.push_back(sentry.id);
        }
        const bool allow_overlap =
            cfg.bootstrap.order_gateway_cfg.risk.allow_strategy_symbol_overlap;
        if (!xt_live::control::run_overlap_check(
                st.run.declared_registry.get(), sid_list, allow_overlap,
                "HFT", /*wait_for_workers=*/false)) {
            XtLogger::error_m("RT",
                "cross-strategy symbol overlap detected — abort startup "
                "(set risk.allow_strategy_symbol_overlap=true to bypass)");
            return false;
        }
    }

    // userdata 回灌必须早于 event loop 启动：engine.start() 在 worker 线程 fire，
    // restore 在此（主线程、worker 未启动）满足 restore_userdata 契约。
    restore_all_strategy_userdata_(st, cfg);

    if (!start_event_loop_(st, cfg, shutdown_flag))                     return false;
    start_reconcile_loop_if_supported_(st, cfg);

    init_success = true;
    return true;
}

bool try_start_channels(HftEngineRuntime& st,
                        const XtLiveConfig& cfg,
                        std::atomic<bool>& shutdown_flag) {
    // 启动失败统一清理，保证不泄漏后台线程和网关连接
    auto cleanup_and_fail = [&]() -> bool {
        if (st.run.order_gateway) {
            st.run.order_gateway->bind_td(nullptr);
            st.run.order_gateway->bind_md(nullptr);
        }
        st.run.param_watch_stop.store(true, std::memory_order_release);
        if (st.run.param_watch_thread.joinable()) st.run.param_watch_thread.join();
        st.run.channels.reset();
        st.run.td.reset();
        st.run.md.reset();
        st.run.event_loop->stop();
        return false;
    };

    // 参数监控线程只发布版本事件；策略读参数仍通过 ParamDomain，避免复制配置快照。
    const int param_wait_timeout_ms = std::max(1, cfg.bootstrap.params_wait_timeout_ms);
    st.run.param_watch_stop.store(false, std::memory_order_release);
    st.run.param_watch_thread = std::thread([&st, &shutdown_flag, param_wait_timeout_ms]() {
        uint64_t seen = st.boot.base.param_domain.version();
        while (!st.run.param_watch_stop.load(std::memory_order_acquire) &&
               !shutdown_flag.load(std::memory_order_acquire)) {
            uint64_t cur = seen;
            if (st.boot.base.param_domain.wait_for_version_change(
                    seen, static_cast<uint32_t>(param_wait_timeout_ms), &cur) && cur > seen) {
                if (st.run.event_loop->push_flow_event(XtHftRunnerEvent::make_params_updated(cur)))
                    seen = cur;
            }
        }
    });

    std::string factory_err;
    st.run.md = gateway_factory::create_md_gateway(cfg.parsed.md_provider,
                                                   cfg.parsed.md_cfg,
                                                   cfg.parsed.md_provider_cfg,
                                                   &factory_err);
    if (!st.run.md) {
        XtLogger::error_m("RT", "md gateway create failed: {}", factory_err);
        return cleanup_and_fail();
    }

    // td gateway 已在 try_init_and_start_strategy_runtime 内创建；此处仅 bind 到 order_gateway。
    if (!st.run.td) {
        XtLogger::error_m("RT", "td gateway not initialized (logic error)");
        return cleanup_and_fail();
    }
    st.run.order_gateway->bind_td(st.run.td.get());
    st.run.order_gateway->bind_md(st.run.md.get());
    // channels 直接转发 raw transport ACK 给 latency tracker（区分 submit_to_ack 与
    // submit_to_trade_ack）；route_status resolver 由 order_gateway 解析（sid_id/strategy_id
    // 由 venue gateway 在回报边界 stamp）。
    {
        auto* gw = st.run.order_gateway.get();
        st.run.channels = std::make_unique<XtHftChannels>(
            st.run.md.get(),
            st.run.td.get(),
            st.run.event_loop.get(),
            &st.shared.shared_state,
            st.boot.base.latency_tracker.get(),
            [gw](uint64_t order_id, ITradeGateway::TdState td_state) -> const char* {
                return gw ? gw->resolve_route_status(order_id, td_state) : "unknown";
            });
    }
    // xt_risk_rejects_total 预注册：交易开闸前建 0 值 series，让 Prometheus 在首笔
    // reject 前先 scrape 到基线 0，rate() 不把启动后首窗 burst 当基线吞掉。启动到首个
    // scrape 之间的 reject 仍在采样盲区（counter 无 created-timestamp 的本质极限）。
    ct::metrics::XtMetricRegistry::instance().inc_risk_rejects_total(
        xt_util::canonical_venue_label(cfg.parsed.md_provider), 0);
    // 热路径 histogram emit 只在 HFT runner 打开；XT_DISABLE_LATENCY_METRIC_EMIT
    // 非空时跳过 registry 注入，tracker 保持 no-op，便于运行中回滚。
    // venue_label 用 canonical_venue_label 统一 CTP/binance_u/XTP 标签。
    if (st.boot.base.latency_tracker && !std::getenv("XT_DISABLE_LATENCY_METRIC_EMIT")) {
        st.boot.base.latency_tracker->set_metric_registry(
            &ct::metrics::XtMetricRegistry::instance(),
            xt_util::canonical_venue_label(cfg.parsed.md_provider));
        // emit meta 反查 fallback：submit 路径未带 strategy/code（recovery /
        // 跨重启在途单）时，从 order book hazard snapshot 反查 sid_id + code，
        // sid_id 经 global StrategySidRegistry 还原 strategy 名。
        st.boot.base.latency_tracker->set_meta_resolver(
            [book = &st.boot.order_book](uint64_t oid, std::string& strategy,
                                         std::string& code) {
                const auto snap = book->snapshot_order_by_id(oid);
                if (snap.id == 0 || snap.code[0] == '\0') return false;
                const char* sid =
                    xt_identity::StrategySidRegistry::resolve_sid_cstr_or_null(snap.sid_id);
                if (!sid) return false;
                strategy.assign(sid);
                code.assign(snap.code);
                return true;
            });
    }
    // account truth sink 必须在 channels->start()（SDK 回调线程启动）前注入：
    // 启动窗口内到达的 broker venue-truth overlay 否则会因 truth_sink==nullptr 被丢弃。
    // binance income provider（REST cashflow 拉取）在 caller 侧构造，factory 保持
    // venue-neutral、不引 gateway 依赖；非 binance venue 传空 provider。
    try {
        BinanceAccountTruthSource::IncomeProvider income_provider;
        if (const auto* bn = std::get_if<BinanceTdCfg>(&cfg.parsed.td_provider_cfg.extra)) {
            income_provider = binance_u::make_binance_income_provider(*bn);
        }
        auto truth = make_runner_account_truth_source(
            *st.boot.base.account_store,
            st.shared.shared_state,
            cfg,
            std::move(income_provider));
        st.run.account_truth_source = std::move(truth.source);
        st.run.account_truth_sink = truth.sink;
    } catch (const std::exception& e) {
        XtLogger::error_m("RT", "account truth source create failed: {}", e.what());
        return cleanup_and_fail();
    }
    st.run.channels->set_account_truth_sink(st.run.account_truth_sink);
    // H1（修 C2）+ K1（修 B-1）：STP reverse-sync re-arm 在回调点（SDK 线程）同步完成，早于
    // gateway 标 ReverseSync satisfied → trading_ready 放行；worker handler 不再 seed。cold
    // clear+seed / warm union-seed 分叉见 stp_rearm_reverse_sync（内部 gate stp_enabled）。
    {
        auto* gw = st.run.order_gateway.get();
        st.run.channels->set_stp_rearm_sink(
            [gw](const std::vector<AliveInflightOrder>& orders, bool is_cold_start) {
                if (gw) gw->stp_rearm_reverse_sync(orders, is_cold_start);
            });
    }
    st.run.channels->wire();
    return st.run.channels->start() ? true : cleanup_and_fail();
}

void stop_runtime_threads(HftEngineRuntime& st, std::atomic<bool>& shutdown_flag) {
    shutdown_flag.store(true, std::memory_order_release);
    st.run.param_watch_stop.store(true, std::memory_order_release);
    if (st.run.param_watch_thread.joinable()) st.run.param_watch_thread.join();
    // reconcile_loop 须早于 channels/td 关闭 (loop 还在跑时 td 析构会 UB)
    if (st.run.reconcile_loop) st.run.reconcile_loop->stop();
    // order_gateway->stop() 必须早于 channels->stop()，否则断开 td 后无法下发 cancel-all。
    if (st.run.order_gateway) st.run.order_gateway->stop();
    if (st.run.channels) st.run.channels->stop();
    if (st.run.event_loop) st.run.event_loop->stop();
    // worker 已 join：account_store 先置 quiescent（drain 出的 gateway 注册闭包会在非
    // worker 线程 apply(order_accepted)，置位后 guard 不再误记跨线程写），再由
    // order_book.shutdown_quiesce() 原子完成「置闩 + 最终 drain」收尾。
    if (st.boot.base.account_store) st.boot.base.account_store->enter_quiescent();
    st.boot.order_book.shutdown_quiesce();
    if (st.run.order_gateway) st.run.order_gateway->clear_exec_sinks();
}

// live_cfg 接收 mutable ref，用于写回 sid_id keyed risk 配置。
int run_hft_live_with_config(XtLiveConfig& live_cfg,
                             std::atomic<bool>& shutdown_flag,
                             std::atomic<int>& shutdown_sig) {
    HftEngineRuntime st;
    // RAII：作用域退出（含异常 unwind / fail_exit / 一切 return）时 stop() latency tracker，
    // 保证 assembler 停 + meta_resolver（捕获 &st.boot.order_book）清先于 st.boot 成员析构，
    // 消灭 teardown 期 resolver-after-order_book-death 的 UAF 窗口。tracker 在 boot 期才创建，
    // guard 持 unique_ptr 引用、析构时读当前值（幂等，正常关停已在 finalize 内 stop）。
    TrackerStopGuard latency_stop_guard{st.boot.base.latency_tracker};
    shutdown_flag.store(false, std::memory_order_release);
    shutdown_sig.store(0, std::memory_order_relaxed);

    // logger / signal handler / runtime tuning / metrics exposer 走三 runner
    // 共享 init（CTA/SEL 同源）。
    xt_live::common::runner_process_init(live_cfg, "RT", shutdown_flag, shutdown_sig);

    // metrics exposer 启动后再注册 reporter，确保 metric family 已存在。
    xt_live::common::register_position_aggregator_jump_reporter_();

    auto fail_exit = [&]() {
        // HFT 特有：先停 runtime 线程，再走共享 fail-exit（stop exposer + logger
        // shutdown，顺序与拆分前一致）。
        shutdown_flag.store(true, std::memory_order_release);
        stop_runtime_threads(st, shutdown_flag);
        return xt_live::common::runner_fail_exit(shutdown_flag);
    };

    if (!try_lock_single_instance(st, live_cfg)) return fail_exit();
    if (!try_create_strategy(st, live_cfg)) return fail_exit();
    if (!try_build_runtime_and_params(st, live_cfg)) return fail_exit();
    // sid registry boot + per_strategy_{max_pos_qty,daily_loss_pct} string→uint32 翻译。
    // 走三 runner 共用入口；注册顺序以 cfg.parsed.strategies 为准，st.boot.strategies
    // 是该 vector 的 1:1 有序副本，保证 sid_id 与策略配置顺序一致。
    // 必须在 try_init_and_start_strategy_runtime (内含 create_order_gateway_) 之前,
    // 让 XtLiveOrderGateway ctor 看到 uint32-keyed risk cfg (XtRiskEngine 直接读)。
    if (!xt_live::common::wire_sid_registry(st.run.sid_registry, live_cfg)) return fail_exit();
    // sid registry 收尾：发布 process-global + freeze（CTA/SEL 同一份
    // finalize_sid_registry——EventLoop tap 用 sid_id 反查 JSONL strat_id；boot 后
    // freeze，run-time 新 sid 注入 fail-soft 返 kNoSidId + stderr warn）。
    xt_live::common::finalize_sid_registry(st.run.sid_registry.get());
    // freeze 后按 slot 数 init per-sid kill_switch/daily_loss 槽位（init_per_sid_risk_state
    // 内 null-guard registry + risk_monitor_state）。必须在 try_build_runtime_and_params
    // 之后（risk_monitor_state 已由 base.create() 构造）；当前位置已满足，与 CTA/SEL 同序。
    xt_live::common::init_per_sid_risk_state(st.run.sid_registry.get(),
                                             st.boot.base.risk_monitor_state.get());
    if (!try_init_and_start_strategy_runtime(st, live_cfg, shutdown_flag)) return fail_exit();
    if (!try_start_channels(st, live_cfg, shutdown_flag)) return fail_exit();

    // 注入 per-strategy risk state + PnL 源（aggregator/baseline）并校验风控
    // 配置——setter 先于 validate 的顺序由共享入口保证（CTA 的 aggregator 在
    // runtime build 期直接注入 gateway、经共享入口传 nullptr 不覆盖；SEL 无源
    // 保持 bypass）。misconfig（如配了 daily-loss 却无 PnL 源）fail-exit。
    if (!xt_live::control::wire_risk_state_and_validate(
            st.boot.base.risk_monitor.get(),
            st.boot.base.account_store.get(),
            st.run.order_gateway.get(),
            st.boot.base.risk_monitor_state.get(),
            st.run.strategy_pnl_aggr.get(),
            st.run.strategy_pnl_baseline.get())) {
        return fail_exit();
    }

    // account truth source 已在 try_start_channels 内（channels->start 前）创建+注入，
    // 关闭 SDK 回调线程启动窗口的 broker overlay 丢弃 race。
    XtHftRunnerControlContext control_ctx = make_runner_control_context(st.run.td.get(),
                                                                   *st.run.event_loop,
                                                                   *st.run.order_gateway,
                                                                    st.shared.shared_state,
                                                                    *st.boot.base.account_store,
                                                                    st.boot.order_book,
                                                                    live_cfg.bootstrap.order_gateway_cfg,
                                                                   st.boot.base.latency_tracker.get(),
                                                                   &st.boot.base.param_domain,
                                                                   st.run.account_truth_source.get());
    // 注入 SPSC 诊断队列消费端
    control_ctx.trade_drain_q        = st.run.trade_drain_q.get();
    control_ctx.order_update_drain_q = st.run.order_update_drain_q.get();
    // 1Hz 帧尾 drain TD handoff 重投缓冲（补 S&F 后完全静默的尾巴；常态 O(1) hint 早退）
    // + STP 上膛度量 1Hz 重发（F-D：series-birth 免疫；disabled 诚实归 0）。
    control_ctx.frame_tail_hook =
        [ch = st.run.channels.get(),
         gw = st.run.order_gateway.get(),
         venue = std::string(xt_util::canonical_venue_label(live_cfg.parsed.md_provider))]() {
            ch->drain_td_handoff_buffer();
            ct::metrics::XtMetricRegistry::instance().set_stp_registered_symbols(
                venue, static_cast<uint32_t>(gw->stp_registered_gauge()));
        };
    // 兜底发布投递：控制线程不自己发布帧，只把「执行在册发布请求」闭包投给 event-loop
    // worker（= account 单写者），与 TD 回报同队列 FIFO。满队返 false → 计数 + 下帧重投。
    control_ctx.post_account_publish_request =
        [loop = st.run.event_loop.get(), store = st.boot.base.account_store.get()]() {
            return loop->push_td_event(XtHftRunnerEvent::make_worker_task(
                [store]() { store->run_publish_request(); }));
        };
    // Active risk monitor 由 HftRunnerControl frame 推进，gateway 读取广播状态执行 halt_open。
    control_ctx.risk_monitor = st.boot.base.risk_monitor.get();
    control_ctx.risk_monitor_state = st.boot.base.risk_monitor_state.get();
    control_ctx.md_gateway = st.run.md.get();  // gateway 自治健康检查
    // multibook shm reader 时派生 typed 指针供 frame-tail attach 观测（venue-direct 网关 → nullptr）；
    // dynamic_cast 只在此 composition root 一次，IMdGateway 接口不暴露 attach 计数（T1.141①）。
    control_ctx.shm_md_gateway = dynamic_cast<xt_ipc::XtShmMdGateway*>(st.run.md.get());
    control_ctx.market_cache = st.boot.market_cache.get();  // bid/ask 回填
    control_ctx.catalog = &st.boot.base.catalog;  // closeout aggressive 价 venue-legal tick snap
    control_ctx.strategy_pnl_aggr = st.run.strategy_pnl_aggr.get();  // per-strategy PnL emit
    control_ctx.declared_registry = st.run.declared_registry.get();  // per-strategy PnL 路由
    control_ctx.strategy_pnl_baseline = st.run.strategy_pnl_baseline.get();  // today PnL reset
    st.run.order_gateway->set_risk_monitor_state(st.boot.base.risk_monitor_state.get());
    const int64_t control_start_ms = XtSteadyClock::steady_now_ms();
    const auto& ctrl_cfg = live_cfg.runtime.control_cfg;
    const auto& grpc_cfg = live_cfg.runtime.control_grpc_cfg;
    xt_report::XtLiveReporter::Config tracker_cfg;
    tracker_cfg.flush_ms = ctrl_cfg.unified_report_flush_ms;
    HftLiveReportBundle report_bundle{tracker_cfg, control_start_ms};
    {
        xt_report::XtContractDims dims;
        dims.mode         = "live";
        dims.run_id       = ctrl_cfg.unified_run_id.empty()
                                ? "live_" + std::to_string(control_start_ms)
                                : ctrl_cfg.unified_run_id;
        dims.strategy_id  = ctrl_cfg.unified_strategy_id;
        dims.symbol       = ctrl_cfg.unified_symbol;
        dims.config_hash  = ctrl_cfg.unified_config_hash;
        dims.code_version = ctrl_cfg.unified_code_version;
        std::string csv_err;
        if (!report_bundle.sink.init(ctrl_cfg.unified_report_dir, dims, &csv_err, 50)) {
            XtLogger::error_m("RT", "init live csv report sink failed: {}", csv_err);
        }
    }
    // operator 控制只通过 gRPC mTLS 进入；热参数 schema 由 xt-panel 的
    // versioned protocol artifact 管理。
    HftRunnerControl::Deps control_deps{report_bundle.reporter};
    HftRunnerControl control_loop(live_cfg.runtime.control_cfg, control_ctx, control_deps);

    // per-sid 主动强平（auto-liquidation，HFT-only）：reject-only daily-loss 之上的平仓层。
    // liquidate_pct (uint32→double) 已由 RunnerBootCommon 翻译；此处转 fp 注入 control loop。
    // enabled=false（默认）时 step() 整段 no-op；CTA/SEL runner 不构造此 driver。
    {
        const auto& liq_risk = live_cfg.bootstrap.order_gateway_cfg.risk;
        xt_live::control::PerSidLiquidatorCfg liq_cfg;
        liq_cfg.enabled = liq_risk.per_strategy_auto_liquidate;
        liq_cfg.dry_run = liq_risk.per_strategy_auto_liquidate_dry_run;
        for (const auto& [sid_id, pct] : liq_risk.per_strategy_liquidate_pct) {
            if (pct > 0.0) liq_cfg.liquidate_pct_fp[sid_id] = xt_fp::to_fp(pct);
        }
        // 1:1 sid:symbol fail-closed：auto_liquidate 启用时，每个 registered sid 必须恰好声明
        // 1 个 symbol。liquidator 无法处理多 symbol sid（运行期只 warn+skip → 静默无保护），
        // 任一 sid declared_symbols!=1 即 fail boot（declared_symbols 来自 .so capability，
        // 只能在此 bootstrap 期校验，config-time validator 拿不到）。
        if (liq_cfg.enabled && st.run.declared_registry) {
            const auto* dreg = st.run.declared_registry.get();
            for (const std::string& sid_str : dreg->registered_sids()) {
                const std::size_t nsym = dreg->declared_symbols(sid_str).size();
                if (nsym != 1) {
                    XtLogger::error_m("RT",
                        "per_strategy_auto_liquidate=true 但 sid '{}' 声明 {} symbol "
                        "(要求恰好 1; 1:1 sid:symbol, 多 symbol sid 无法自动强平)",
                        sid_str, nsym);
                    return fail_exit();
                }
            }
        }
        control_loop.configure_auto_liquidator(std::move(liq_cfg));
    }

    // panel ForceExit CancelledOnly 续帧 latch（T1.155，HFT-only）：RPC 线程 arm、
    // control frame 每轮 step，直到 flat（Done）/ TTL 到期（Expired）。声明先于
    // grpc_server/hft_panel_control → 析构逆序保证 RPC handler 调 arm 期间对象
    // 仍 alive。enabled=false 时不装配（ctx 指针留 nullptr = 原一次性语义）。
    std::unique_ptr<xt_live::control::PanelForceExitLatch> force_exit_latch;
    if (live_cfg.bootstrap.order_gateway_cfg.closeout_force_exit_latch_enabled) {
        force_exit_latch = std::make_unique<xt_live::control::PanelForceExitLatch>(
            live_cfg.bootstrap.order_gateway_cfg.closeout_force_exit_latch_ttl_ms);
        control_ctx.force_exit_latch = force_exit_latch.get();
    }
    // force-flat submit 同步 fence（固定护栏，不 config）：liquidator / latch / RPC
    // 三路 force_exit 共用一实例，堵 genuine 标记 marshal 异步可见前的 over-close 缝。
    auto force_flat_fence = std::make_unique<xt_live::control::ForceFlatSubmitFence>();
    control_ctx.force_flat_fence = force_flat_fence.get();

    // gRPC 是唯一 control-plane path；dry-run/test profile 需显式关闭。
    // handler 复用现有 dispatch、risk gate 与 owner-published snapshot 协议。
#ifdef XT_HAVE_CONTROL_GRPC
    // panel 命令面（cancel/flatten/force_exit/manual）。声明在 grpc_server 之前；
    // 析构逆序保证 grpc handler lambda 调用期间本对象仍 alive。
    std::optional<xt_live::control::XtRunnerPanelControl> hft_panel_control;
    std::unique_ptr<xt_live::control::XtControlGrpcServer> grpc_server;
    std::unique_ptr<xt_live::control::XtControlAuditWriter> audit_writer;
    if (grpc_cfg.enabled) {
        grpc_server   = xt_live::control::make_control_grpc_server(grpc_cfg);
        audit_writer  = xt_live::control::make_control_audit_writer(grpc_cfg);

        // 绑定三 runner 共享 handler（rpc_complete_callback + kill_switch + get_trading_state）。
        xt_live::control::XtKillSwitchState* ks_state_ptr = nullptr;
        xt_live::control::CommonGrpcDeps common_deps;
        common_deps.kill_switch        = &st.shared.shared_state.kill_switch;
        common_deps.audit_writer       = audit_writer.get();
        common_deps.account_store      = st.boot.base.account_store.get();
        common_deps.risk_monitor_state = st.boot.base.risk_monitor_state.get();
        common_deps.sid_registry       = st.run.sid_registry.get();
        common_deps.kill_switch_source = &st.shared.shared_state.kill_switch_source;
        common_deps.kill_switch_state_out = &ks_state_ptr;
        common_deps.reconcile_reset_request = &st.shared.shared_state.reconcile_reset_request;
        common_deps.audit_dir = instance_audit_dir(
            live_cfg.parsed.md_provider, live_cfg.runtime.control_cfg.instance_id);
        xt_live::control::bind_common_grpc_handlers(*grpc_server, common_deps);

        // closeout auto-trip / auto-release 因果链写入 binder 持有的 XtKillSwitchState
        // （panel GetTradingState 可见 actor/reason）。closure 捕获静态 state 指针（进程生命期有效）。
        control_ctx.record_kill_switch_trip =
            [ks_state_ptr](bool active, const std::string& actor,
                           const std::string& reason, int64_t ts_ns) {
                if (ks_state_ptr) ks_state_ptr->update(active, actor, reason, ts_ns);
            };

        // panel_ctx 投影自 control_ctx，emplace 在 grpc bind 之前；
        // hft_panel_control 生命周期覆盖整个 grpc_server。
        xt_live::control::XtPanelContext hft_panel_ctx{};
        hft_panel_ctx.td_gateway           = control_ctx.td_gateway;
        hft_panel_ctx.order_book           = control_ctx.order_book;
        hft_panel_ctx.account_store        = control_ctx.account_store;
        hft_panel_ctx.live_order_gateway   = &control_ctx.live_order_gateway;
        hft_panel_ctx.runtime_shared_state = &control_ctx.runtime_shared_state;
        hft_panel_ctx.param_domain         = control_ctx.param_domain;
        hft_panel_ctx.catalog              = control_ctx.catalog;
        hft_panel_ctx.force_exit_latch     = control_ctx.force_exit_latch;
        hft_panel_ctx.force_flat_fence     = control_ctx.force_flat_fence;
        // attribution 路由模式（== portfolio_router，与 gateway sid_registry 注入同判据）：
        // manual order 空 strategy_id 时改写归属为保留名 "_manual"。
        hft_panel_ctx.attribution_router_mode = live_cfg.parsed.portfolio_router;
        // hosted 策略集合 → update_param 策略归属校验（T1.147）。
        hft_panel_ctx.hosted_strategies.reserve(live_cfg.parsed.strategies.size());
        for (const auto& entry : live_cfg.parsed.strategies) {
            hft_panel_ctx.hosted_strategies.push_back(entry.factory_cfg.id);
        }
        hft_panel_control.emplace(hft_panel_ctx);

        xt_live::control::HftControlBindCtx hft_ctx;
        hft_ctx.param_domain  = &st.boot.base.param_domain;
        hft_ctx.hft_engine    = st.run.hft_engine.get();
        hft_ctx.kill_switch   = &st.shared.shared_state.kill_switch;
        hft_ctx.control_loop  = &*hft_panel_control;
        hft_ctx.market_cache  = st.boot.market_cache.get();
        hft_ctx.fills_csv_path = ctrl_cfg.unified_report_dir + "/fills.csv";
        hft_ctx.venue          = live_cfg.parsed.md_provider;
        xt_live::control::bind_hft_control_handlers(*grpc_server, hft_ctx);

        if (!xt_live::control::start_control_grpc_or_log(*grpc_server, grpc_cfg, "RT")) {
            return fail_exit();
        }
    }
#else
    if (grpc_cfg.enabled) {
        XtLogger::error_m("RT",
            "control-plane gRPC requested but this binary was built without XT_HAVE_CONTROL_GRPC");
        return fail_exit();
    }
#endif

    control_loop.run(shutdown_flag);
#ifdef XT_HAVE_CONTROL_GRPC
    if (grpc_server) grpc_server->stop();
#endif

    const int sig = shutdown_sig.load(std::memory_order_relaxed);
    if (sig != 0) {
        XtLogger::warn_m("RT", "shutdown signal={} received", sig);
    }
    stop_runtime_threads(st, shutdown_flag);

    // 收尾报表 flush 移到 stop_runtime_threads 之后：tracker->stop() 的 final drain 才能
    // 纳入 event loop / gateway / channels 停机前产出的尾批 latency 样本（run() 内 stop
    // 会因 enabled=false 丢弃尾样）。finalize_report 内 stop 幂等（TrackerStopGuard 兜底）。
    control_loop.finalize_report();

    // 策略 userdata dump（event loop worker 已 join、主线程独占 ctx，满足
    // snapshot_userdata 契约；worker 内禁止的文件 IO 只在此 post-stop 点做）。
    dump_all_strategy_userdata_(st, live_cfg);

    // 账户停机对账必须在 event loop worker join 之后执行：frozen_audit_report
    // 直读 writer state，worker quiesce 后 account_store 才无写者。
    xt_live::control::run_account_reconcile_on_stop(live_cfg.runtime.control_cfg, control_ctx);

    // 与启动期 load_state 配对（共用 aggregator_state_path）；在 logger shutdown
    // 前保存累计 realized/fees，让 today_pnl_v2 跨重启连续。
    xt_live::hft::save_hft_telemetry(st.run.strategy_pnl_aggr.get(), live_cfg);

    // 显式停 Prometheus exposer：civetweb 工作线程在 main() 返回前
    // join 完成,避免 :9101 短暂 EADDRINUSE + 与 prometheus::Registry 析构竞争
    // (静态单例 dtor 仅在 __cxa_finalize 跑,即 XtLogger::shutdown 之后)。
    // 必须在 XtLogger::shutdown() 之前调,确保关闭路径异常仍可日志落盘。
#ifdef XT_HAVE_METRICS
    xt_live::common::stop_metrics_exposer_();
#endif
    XtLogger::shutdown();
    return 0;
}

} // namespace

int run_hft_live(int argc,
                 char** argv,
                 std::atomic<bool>& shutdown_flag,
                 std::atomic<int>& shutdown_sig) {
    XtLiveConfig live_cfg;
    std::string load_err;
    std::vector<std::string> validation_errors;
    // XtLogger 依赖配置才能 init，此时只能写 stderr
    if (!load_live_config(argc, argv, live_cfg, load_err, validation_errors)) {
        std::fprintf(stderr, "Runner: %s\n", load_err.c_str());
        return 1;
    }
    if (!validation_errors.empty()) {
        std::fprintf(stderr, "Runner: config validation failed (%zu)\n", validation_errors.size());
        for (size_t i = 0; i < validation_errors.size(); ++i) {
            std::fprintf(stderr, "  [%zu] %s\n", i + 1, validation_errors[i].c_str());
        }
        return 1;
    }
    return run_hft_live_with_config(live_cfg, shutdown_flag, shutdown_sig);
}
