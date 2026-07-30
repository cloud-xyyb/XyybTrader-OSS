#include <atomic>
#include "xt_engine/hft/XtHftEngine.h"
#include "xt_live/telemetry/observer/XtHftLiveObserverCombo.h"
#include "api/IHftEngineObserver.h"
#include "xt_live/telemetry/recording/XtLiveRecorder.h"

#include "XtMetricRegistry.h"
#include "xt_metrics/XtVenueLabel.h"
#include <cstdlib>
#include "xt_metrics/XtStrategyPnlAggregator.h"
#include "xt_strategy_engine/XtDeclaredSymbolRegistry.h"
#include "xt_metrics/XtStrategyPnlBaseline.h"

#ifdef XT_HAVE_CONTROL_GRPC
#include "XtControlGrpcServer.h"
#include "XtControlGrpcInterceptors.h"
#include "FillsCsvTail.h"
#include "OrderbookSnapshotBuilder.h"
#include "ControlGrpcBoot.h"
#include "ControlGrpcCommon.h"
#include "HftControlGrpcBinder.h"
#include "XtRunnerPanelControl.h"
#endif
#include "XtKillSwitchState.h"

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
#include "xt_ipc/XtShmMdGateway.h"
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
#include "xt_core/XtFixedPoint.h"
#include "xt_live/hft/control/HftRunnerControl.h"
#include "xt_live/hft/control/PanelForceExitLatch.h"
#include "xt_live/control/ForceFlatSubmitFence.h"
#include "xt_live/hft/reconcile/XtHftAccountReconcile.h"
#include "xt_live/common/AccountTruthSourceFactory.h"
#include "xt_live/common/StrategyUserdataStore.h"
#include "xt_live/common/VenueTraits.h"
#include "xt_live/common/XtLiveStrategyTelemetrySink.h"
#include "binance/rest/BinanceIncomeProvider.h"
#include "PanelHelpers.h"
#include "RunnerBootCommon.h"
#include "xt_util/runtime/XtProcessUtil.h"
#include "api/XtRunnerSignals.h"
#include "xt_live/hft/runtime/XtHftEventLoop.h"
#include "xt_live/hft/telemetry/XtHftTelemetryWiring.h"
#include "xt_live/hft/reconcile/XtHftReconcileLoop.h"
#include "api/IReconcilableGateway.h"

#include "XtPluginHolder.h"
#include "xt_strategy_engine/loader/StrategyInstance.h"
#include "xt_strategy_engine/loader/StrategyAssemblyContract.h"
#include "XtLiveBootstrap.h"
#include "RunnerCommon.h"

using xt_runtime::XtRuntimeMarketCache;

namespace {

using HftStrategyInstance = xt_strategy_engine::StrategyInstance;

struct HftEngineBootResources {
    XtProcessUtil::SingleInstanceLock instance_lock;

    std::vector<HftStrategyInstance> strategies;

    XtLiveBootstrap base;

    xt_runtime::order::XtOrderRuntimeBook       order_book;

    std::unique_ptr<XtRuntimeMarketCache>       market_cache;
    std::unique_ptr<xt_strategy_engine::XtRuntimeView>  runtime_view;
};

struct HftEngineSharedResources {
    XtRunnerSignals shared_state;

    std::vector<xt_strategy_engine::IStrategyExecSink*> exec_sink_ptrs;
};

struct HftEngineRunResources {
    std::unique_ptr<XtLiveOrderGateway> order_gateway;
    std::unique_ptr<XtStrategyContext::IParamPort> param_port;
    std::unique_ptr<xt_strategy_engine::XtDeclaredSymbolRegistry> declared_registry;

    std::unique_ptr<xt_identity::StrategySidRegistry> sid_registry;
    std::unique_ptr<XtHftEngine> hft_engine;
    std::unique_ptr<xt_live::XtHftLiveObserverCombo> hft_observer;

    std::unique_ptr<ct::metrics::XtStrategyPnlAggregator> strategy_pnl_aggr;
    std::unique_ptr<xt_runtime::IHftEngineObserver>       fee_observer;

    std::unique_ptr<ct::metrics::XtStrategyPnlBaseline>   strategy_pnl_baseline;
    std::unique_ptr<XtHftEventLoop> event_loop;
    std::unique_ptr<IAccountTruthSource> account_truth_source;
    IAccountTruthSink* account_truth_sink = nullptr;

    std::unique_ptr<xt_live::control::XtHftReconcileLoop> reconcile_loop;

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

void restore_all_strategy_userdata_(HftEngineRuntime& st, const XtLiveConfig& cfg) {
    if (!st.run.hft_engine) return;
    const std::string state_dir = xt_live::common::derive_userdata_state_dir(
        cfg.bootstrap.recording_cfg.file_path, cfg.parsed.cfg_path);

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

    std::string boot_err;
    if (!st.boot.base.create(cfg, boot_err)) {
        XtLogger::error_m("RT", "{}", boot_err);
        return false;
    }

    if (st.boot.base.account_store) {
        st.boot.base.account_store->bind_worker_thread();
        st.boot.base.account_store->configure_symbols(cfg.parsed.md_cfg.subs);

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

    st.boot.market_cache = std::make_unique<XtRuntimeMarketCache>();

    st.boot.market_cache->register_published_symbols(cfg.parsed.md_cfg.subs);

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

static void create_order_gateway_(HftEngineRuntime& st, const XtLiveConfig& cfg) {

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

    st.run.order_gateway->pre_warm_risk(cfg.parsed.md_cfg.subs);

    ct::metrics::XtMetricRegistry::instance().set_stp_registered_symbols(
        xt_util::canonical_venue_label(cfg.parsed.md_provider),
        static_cast<uint32_t>(st.run.order_gateway->stp_registered_gauge()));
}

static void apply_risk_hot_params_(XtLiveOrderGateway& gw,
                                   XtStrategyContext::IParamPort& param_port) {
    XtRiskCfg new_cfg = gw.risk_config();

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

static XtHftEngine::Dependencies make_hft_engine_deps_(HftEngineRuntime& st,
                                                       const XtLiveConfig& cfg) {

    assert(st.run.td             && "make_hft_engine_deps_: st.run.td 未 ready");
    assert(st.run.order_gateway  && "make_hft_engine_deps_: st.run.order_gateway 未 ready");
    XtHftEngine::Dependencies deps;
    deps.gateway             = st.run.order_gateway.get();
    deps.view                = st.boot.runtime_view.get();
    deps.account_store       = st.boot.base.account_store.get();
    deps.order_book          = &st.boot.order_book;
    deps.catalog             = &st.boot.base.catalog;
    deps.order_id_translator = &st.run.order_gateway->logical_order_tracker();

    if (cfg.parsed.portfolio_router) {
        deps.declared_registry = st.run.declared_registry.get();
    }
    return deps;
}

static XtHftEngine::Options make_hft_engine_options_(const HftEngineRuntime& st,
                                                     const XtLiveConfig& cfg) {
    XtHftEngine::Options opts;
    if (cfg.bootstrap.has_roll_cfg) opts.roll = cfg.bootstrap.roll_cfg;
    opts.conditional_orders_enabled =
        cfg.bootstrap.runtime_state_cfg.conditional_orders_enabled;

    if (st.boot.strategies.size() > 1) {
        opts.unmatched_routing = cfg.parsed.portfolio_router
            ? XtHftEngine::UnmatchedRoutingPolicy::DropAndAlert
            : XtHftEngine::UnmatchedRoutingPolicy::FailFast;
    }
    return opts;
}

static void fill_hft_engine_control_hooks_(XtHftEngine::Hooks& hooks,
                                           HftEngineRuntime& st) {

    auto* gw_ptr  = st.run.order_gateway.get();
    auto* port_ptr = st.run.param_port.get();
    hooks.on_params_extra = [gw_ptr, port_ptr](uint64_t  ) {
        if (!gw_ptr || !port_ptr) return;
        apply_risk_hot_params_(*gw_ptr, *port_ptr);
    };

    hooks.on_strategy_init_failure =
        xt_live::control::make_strategy_init_failure_handler(
            st.boot.base.risk_monitor_state.get());
}

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

    loop_deps.in_trading_section    = &st.shared.shared_state.in_trading_section;
    loop_deps.session_sections      = cfg.bootstrap.order_gateway_cfg.session_gate_enabled
                                      ? &cfg.bootstrap.order_gateway_cfg.session_gate_sections
                                      : nullptr;

    loop_deps.closeout_no_open      = &st.shared.shared_state.closeout_no_open_window;
    loop_deps.closeout_windows      = cfg.bootstrap.order_gateway_cfg.closeout_enabled
                                      ? &cfg.bootstrap.order_gateway_cfg.closeout_windows
                                      : nullptr;

    loop_deps.live_cost             = &cfg.bootstrap.live_cost;
    loop_deps.live_fees             = &cfg.bootstrap.live_fees;
    loop_deps.catalog               = &st.boot.base.catalog;
    loop_deps.initial_param_version = st.boot.base.param_domain.version();

    loop_deps.self_match_detector   = st.boot.base.self_match_detector.get();
    loop_deps.blacklist_sink        = [gw = st.run.order_gateway.get()](const char* c, const char* r) {
        gw->insert_self_match_blacklist(c, r);
    };

    loop_deps.marshal_overflow_drain = [book = &st.boot.order_book]() {
        book->drain_marshal_side_channel();
    };

    loop_deps.latency_tracker       = st.boot.base.latency_tracker.get();
}

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

        if (cfg.parsed.portfolio_router) {
            ctx_cfg.route_by_sid = true;
        }
        ctx_cfg.declared_registry = st.run.declared_registry.get();

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

static bool verify_multi_assembly_alignment_(const HftEngineRuntime& st,
                                              const XtLiveConfig& cfg) {
    std::vector<std::string> cfg_ids;
    cfg_ids.reserve(cfg.parsed.strategies.size());
    for (const auto& sc : cfg.parsed.strategies) {
        cfg_ids.push_back(sc.factory_cfg.id);
    }

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

static bool assemble_engine_and_contexts_(HftEngineRuntime& st, const XtLiveConfig& cfg) {
    assert(!st.boot.strategies.empty()
           && "engine ctor: strategies vector must have >=1 element");
    const std::size_t n = st.boot.strategies.size();
    const bool is_multi = (n > 1);

    if (is_multi && cfg.parsed.md_cfg.subs.size() <= 1) {

        XtLogger::warn_m("RT",
            "multi-strategy ({} strategies) on shared instrument: NET position "
            "fan-out may cause internal counter drift if same-symbol opposite "
            "direction. Use different symbols per strategy.", n);
    }

    auto contexts = build_multi_contexts_(st, cfg);

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

    {
        xt_runtime::XtRuntimeMarketCacheConfig mc_cfg;
        mc_cfg.tick_history_cap      = cfg.bootstrap.strategy_history_ticks;
        mc_cfg.bar_history_cap       = cfg.bootstrap.strategy_history_bars;
        mc_cfg.orderflow_history_cap = cfg.bootstrap.strategy_history_ticks;
        st.boot.market_cache->configure(mc_cfg);
    }

    collect_exec_sink_view_(st, contexts);

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

    inject_gateway_exec_sinks_(st, cfg);

    if (!verify_multi_assembly_alignment_(st, cfg)) {
        return false;
    }
    return true;
}

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

    XtHftEventLoop* loop_p = st.run.event_loop.get();
    st.boot.order_book.bind_worker_marshal(
        loop_p->worker_thread_id(),
        [loop_p](std::function<void()> fn) {
            return loop_p->push_td_event(XtHftRunnerEvent::make_worker_task(std::move(fn)));
        });

    st.boot.market_cache->bind_worker_thread(loop_p->worker_thread_id());

    if (st.boot.base.account_store) {
        st.boot.base.account_store->bind_worker_thread(loop_p->worker_thread_id());
    }
    return true;
}

static void start_reconcile_loop_if_supported_(HftEngineRuntime& st, const XtLiveConfig& cfg) {
    auto* reconcilable = dynamic_cast<IReconcilableGateway*>(st.run.td.get());
    if (!reconcilable || !reconcilable->reconcile_supported() || !st.boot.base.account_store) {
        XtLogger::info_m("RT",
            "td gateway does not implement IReconcilableGateway or reconcile disabled, "
            "skipping reconcile loop construction");
        return;
    }

    const xt_live::control::XtHftReconcileLoop::Config rc_cfg = cfg.bootstrap.reconcile_cfg;
    st.run.reconcile_loop = std::make_unique<xt_live::control::XtHftReconcileLoop>(
        *reconcilable,
        *st.boot.base.account_store.get(),
        st.shared.shared_state.kill_switch,
        &st.shared.shared_state.last_position_snapshot_ms,
        rc_cfg);

    st.run.reconcile_loop->set_reset_request_source(
        st.shared.shared_state.reconcile_reset_request);

    st.run.reconcile_loop->set_kill_switch_source(st.shared.shared_state.kill_switch_source);

    st.run.reconcile_loop->set_metric_registry(&ct::metrics::XtMetricRegistry::instance());
    st.run.reconcile_loop->start();
}

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

    st.run.declared_registry = std::make_unique<xt_strategy_engine::XtDeclaredSymbolRegistry>();

    st.run.trade_drain_q        = std::make_unique<XtLockFreeQueue<XtTradeRecord>>(
        QueueMode::SPSC, 1024);
    st.run.order_update_drain_q = std::make_unique<XtLockFreeQueue<XtOrderExecUpdate>>(
        QueueMode::SPSC, 1024);

    if (!assemble_engine_and_contexts_(st, cfg))                       return false;
    if (!init_all_strategy_plugins_(st, cfg))                          return false;

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
                "HFT",  false)) {
            XtLogger::error_m("RT",
                "cross-strategy symbol overlap detected — abort startup "
                "(set risk.allow_strategy_symbol_overlap=true to bypass)");
            return false;
        }
    }

    restore_all_strategy_userdata_(st, cfg);

    if (!start_event_loop_(st, cfg, shutdown_flag))                     return false;
    start_reconcile_loop_if_supported_(st, cfg);

    init_success = true;
    return true;
}

bool try_start_channels(HftEngineRuntime& st,
                        const XtLiveConfig& cfg,
                        std::atomic<bool>& shutdown_flag) {

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

    if (!st.run.td) {
        XtLogger::error_m("RT", "td gateway not initialized (logic error)");
        return cleanup_and_fail();
    }
    st.run.order_gateway->bind_td(st.run.td.get());
    st.run.order_gateway->bind_md(st.run.md.get());

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

    ct::metrics::XtMetricRegistry::instance().inc_risk_rejects_total(
        xt_util::canonical_venue_label(cfg.parsed.md_provider), 0);

    if (st.boot.base.latency_tracker && !std::getenv("XT_DISABLE_LATENCY_METRIC_EMIT")) {
        st.boot.base.latency_tracker->set_metric_registry(
            &ct::metrics::XtMetricRegistry::instance(),
            xt_util::canonical_venue_label(cfg.parsed.md_provider));

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

    if (st.run.reconcile_loop) st.run.reconcile_loop->stop();

    if (st.run.order_gateway) st.run.order_gateway->stop();
    if (st.run.channels) st.run.channels->stop();
    if (st.run.event_loop) st.run.event_loop->stop();

    if (st.boot.base.account_store) st.boot.base.account_store->enter_quiescent();
    st.boot.order_book.shutdown_quiesce();
    if (st.run.order_gateway) st.run.order_gateway->clear_exec_sinks();
}

int run_hft_live_with_config(XtLiveConfig& live_cfg,
                             std::atomic<bool>& shutdown_flag,
                             std::atomic<int>& shutdown_sig) {
    HftEngineRuntime st;

    TrackerStopGuard latency_stop_guard{st.boot.base.latency_tracker};
    shutdown_flag.store(false, std::memory_order_release);
    shutdown_sig.store(0, std::memory_order_relaxed);

    xt_live::common::runner_process_init(live_cfg, "RT", shutdown_flag, shutdown_sig);

    xt_live::common::register_position_aggregator_jump_reporter_();

    auto fail_exit = [&]() {

        shutdown_flag.store(true, std::memory_order_release);
        stop_runtime_threads(st, shutdown_flag);
        return xt_live::common::runner_fail_exit(shutdown_flag);
    };

    if (!try_lock_single_instance(st, live_cfg)) return fail_exit();
    if (!try_create_strategy(st, live_cfg)) return fail_exit();
    if (!try_build_runtime_and_params(st, live_cfg)) return fail_exit();

    if (!xt_live::common::wire_sid_registry(st.run.sid_registry, live_cfg)) return fail_exit();

    xt_live::common::finalize_sid_registry(st.run.sid_registry.get());

    xt_live::common::init_per_sid_risk_state(st.run.sid_registry.get(),
                                             st.boot.base.risk_monitor_state.get());
    if (!try_init_and_start_strategy_runtime(st, live_cfg, shutdown_flag)) return fail_exit();
    if (!try_start_channels(st, live_cfg, shutdown_flag)) return fail_exit();

    if (!xt_live::control::wire_risk_state_and_validate(
            st.boot.base.risk_monitor.get(),
            st.boot.base.account_store.get(),
            st.run.order_gateway.get(),
            st.boot.base.risk_monitor_state.get(),
            st.run.strategy_pnl_aggr.get(),
            st.run.strategy_pnl_baseline.get())) {
        return fail_exit();
    }

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

    control_ctx.trade_drain_q        = st.run.trade_drain_q.get();
    control_ctx.order_update_drain_q = st.run.order_update_drain_q.get();

    control_ctx.frame_tail_hook =
        [ch = st.run.channels.get(),
         gw = st.run.order_gateway.get(),
         venue = std::string(xt_util::canonical_venue_label(live_cfg.parsed.md_provider))]() {
            ch->drain_td_handoff_buffer();
            ct::metrics::XtMetricRegistry::instance().set_stp_registered_symbols(
                venue, static_cast<uint32_t>(gw->stp_registered_gauge()));
        };

    control_ctx.risk_monitor = st.boot.base.risk_monitor.get();
    control_ctx.risk_monitor_state = st.boot.base.risk_monitor_state.get();
    control_ctx.md_gateway = st.run.md.get();

    control_ctx.shm_md_gateway = dynamic_cast<xt_ipc::XtShmMdGateway*>(st.run.md.get());
    control_ctx.market_cache = st.boot.market_cache.get();
    control_ctx.catalog = &st.boot.base.catalog;
    control_ctx.strategy_pnl_aggr = st.run.strategy_pnl_aggr.get();
    control_ctx.declared_registry = st.run.declared_registry.get();
    control_ctx.strategy_pnl_baseline = st.run.strategy_pnl_baseline.get();
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

    HftRunnerControl::Deps control_deps{report_bundle.reporter};
    HftRunnerControl control_loop(live_cfg.runtime.control_cfg, control_ctx, control_deps);

    {
        const auto& liq_risk = live_cfg.bootstrap.order_gateway_cfg.risk;
        xt_live::control::PerSidLiquidatorCfg liq_cfg;
        liq_cfg.enabled = liq_risk.per_strategy_auto_liquidate;
        liq_cfg.dry_run = liq_risk.per_strategy_auto_liquidate_dry_run;
        for (const auto& [sid_id, pct] : liq_risk.per_strategy_liquidate_pct) {
            if (pct > 0.0) liq_cfg.liquidate_pct_fp[sid_id] = xt_fp::to_fp(pct);
        }

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

    std::unique_ptr<xt_live::control::PanelForceExitLatch> force_exit_latch;
    if (live_cfg.bootstrap.order_gateway_cfg.closeout_force_exit_latch_enabled) {
        force_exit_latch = std::make_unique<xt_live::control::PanelForceExitLatch>(
            live_cfg.bootstrap.order_gateway_cfg.closeout_force_exit_latch_ttl_ms);
        control_ctx.force_exit_latch = force_exit_latch.get();
    }

    auto force_flat_fence = std::make_unique<xt_live::control::ForceFlatSubmitFence>();
    control_ctx.force_flat_fence = force_flat_fence.get();

#ifdef XT_HAVE_CONTROL_GRPC

    std::optional<xt_live::control::XtRunnerPanelControl> hft_panel_control;
    std::unique_ptr<xt_live::control::XtControlGrpcServer> grpc_server;
    std::unique_ptr<xt_live::control::XtControlAuditWriter> audit_writer;
    if (grpc_cfg.enabled) {
        grpc_server   = xt_live::control::make_control_grpc_server(grpc_cfg);
        audit_writer  = xt_live::control::make_control_audit_writer(grpc_cfg);

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

        control_ctx.record_kill_switch_trip =
            [ks_state_ptr](bool active, const std::string& actor,
                           const std::string& reason, int64_t ts_ns) {
                if (ks_state_ptr) ks_state_ptr->update(active, actor, reason, ts_ns);
            };

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

        hft_panel_ctx.attribution_router_mode = live_cfg.parsed.portfolio_router;

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

    control_loop.finalize_report();

    dump_all_strategy_userdata_(st, live_cfg);

    xt_live::control::run_account_reconcile_on_stop(live_cfg.runtime.control_cfg, control_ctx);

    xt_live::hft::save_hft_telemetry(st.run.strategy_pnl_aggr.get(), live_cfg);

#ifdef XT_HAVE_METRICS
    xt_live::common::stop_metrics_exposer_();
#endif
    XtLogger::shutdown();
    return 0;
}

}

int run_hft_live(int argc,
                 char** argv,
                 std::atomic<bool>& shutdown_flag,
                 std::atomic<int>& shutdown_sig) {
    XtLiveConfig live_cfg;
    std::string load_err;
    std::vector<std::string> validation_errors;

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
