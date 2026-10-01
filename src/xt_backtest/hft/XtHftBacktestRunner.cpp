/// @file
/// @brief HFT backtest executable 的 runtime 装配与回放入口。
///
/// 本文件负责把已解析配置装配为 strategy、account、gateway、engine、reporter
/// 和 market-data source，并用 `XtReplayer` 驱动一次完整 HFT backtest。

#include "xt_backtest/infra/XtReplayer.h"
#include "xt_backtest/hft/XtHftBacktestOrderGateway.h"
#include "xt_backtest/data/source/XtMarketDataSource.h"
#include "xt_core/market/XtTickConvert.h"
#include "xt_backtest/core/BtRunnerUtil.h"
#include "xt_engine/hft/XtHftEngine.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_backtest/hft/XtLatencyModelFactory.h"
#include "xt_strategy_engine/IStrategyExecSink.h"
#include "xt_strategy_engine/XtStrategyContext.h"
#include "xt_runtime/order/IdentityOrderIdTranslator.h"
#include "xt_backtest/hft/XtHftBacktestDriver.h"
#include "xt_strategy_engine/view/XtRuntimeView.h"
#include "xt_runtime/market/XtRuntimeMarketCache.h"
#include "xt_account/state/AccountState.h"
#include "XtFixedPoint.h"
#include "XtStrategyBase.h"
#include "xt_strategy_engine/loader/XtPluginHolder.h"
#include "xt_strategy_engine/loader/StrategyInstance.h"
#include "xt_strategy_engine/loader/StrategyAssemblyContract.h"
#include "XtLogger.h"
#include "xt_backtest/reporter/XtBacktestReporter.h"
#include "xt_backtest/reporter/XtBacktestReporterObserver.h"
#include "xt_report/core/XtCsvReportSink.h"
#include "xt_backtest/reporter/XtReportCfg.h"
#include "xt_backtest/data/source/XtMarketDataSourceConfig.h"
#include "XtHftBacktestConfig.h"
#include "state/AccountStateStore.h"
#include "XtTradeExcursionTracker.h"
#include "xt_backtest/reporter/XtEventStreamSink.h"
#include "xt_backtest/core/IReplayReporter.h"
#include "xt_util/path/PathUtil.h"

#include <algorithm>
#include <cassert>
#include <exception>
#include <cstdio>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iomanip>

using xt_bt::emit_progress;
using xt_runtime::XtRuntimeMarketCache;

namespace {

// ── 装配元数据 ────────────────────────────────────────────────────────────────

/// @brief bootstrap 解析后的路径、日期窗和策略标识。
///
/// 只保存配置派生的元数据，不拥有策略、engine、account 或 reporter 等运行时资源。
struct BtMeta {
    std::string base_dir;
    std::string report_dir;
    std::string strategy_id;        ///< primary strategy id (= strategies[0].id)
    std::string strategy_symbol;    ///< primary strategy symbol
    std::vector<std::string> strategy_ids;
    uint32_t start_date = 0;        ///< config 回测窗口（YYYYMMDD），透传 daily_breakdown。
    uint32_t end_date = 0;
    int64_t clock_resolution_ns = 0; ///< replay clock grid 生效分辨率（parse 期已含 250ms 下限），
                                     ///< 透传 consumed_manifest 的交付量化披露。
};

// 本文件保留 BtStrategyInstance 名称，底层使用共享 StrategyInstance。
using BtStrategyInstance = xt_strategy_engine::StrategyInstance;

/// @brief 一次 HFT backtest 的 runtime 组件包。
///
/// 成员按 owner/consumer 生命周期拓扑声明；C++ 反向析构顺序保证 replayer/source
/// 先释放，随后 reporter/csv sink、engine/runtime view、gateway/market cache，
/// 最后才释放 strategy plugin 与 domain provider。
struct BtComponents {
    std::vector<BtStrategyInstance> strategies;

    // ── Domain 所有权（Runner 持所有 domain 组件） ──────────────────────
    // gw 持有下列 domain 组件的 raw ptr；声明顺序保证析构时 gw 先释放。
    // catalog 必须先于 account_store 声明：account_store 经 acfg.catalog 持 &catalog
    // （vol_scale 源）。C++ 析构反序 → account_store 先析构、catalog 后析构，避免悬空。
    XtCommodityCatalog                          catalog;
    std::unique_ptr<xt_account::AccountStateStore>          account_store;
    xt_runtime::order::XtOrderRuntimeBook       order_book;

    // ── 执行链 ────────────────────────────────────
    // market_cache 必须先于 gw 声明：gw 经 set_market_cache 持其 raw ptr（越板拒
    // 的 limit 源），反序析构下 gw 先释放、market_cache 后亡，避免悬垂窗口。
    std::unique_ptr<XtRuntimeMarketCache>  market_cache;
    XtHftBacktestOrderGateway              gw;
    std::unique_ptr<xt_strategy_engine::XtRuntimeView> runtime_view;
    std::unique_ptr<XtHftEngine>           engine;
    std::vector<xt_strategy_engine::IStrategyExecSink*> exec_sink_ptrs;
    xt_runtime::IdentityOrderIdTranslator  order_id_translator;
    std::unique_ptr<xt_backtest::hft::XtHftBacktestDriver> driver;

    // ── 报告链 ────────────────────────────────────────────────────────
    xt_report::XtCsvReportSink             csv_sink;
    std::unique_ptr<XtBacktestReporter>    reporter;
    std::unique_ptr<XtBacktestReporterObserver> reporter_observer;            ///< gateway 层：submit/update
    std::unique_ptr<XtBacktestReporterFillObserver> reporter_fill_observer;   ///< engine 层：fill（带 realized）
    // 逐笔 MAE/MFE：独立写 trade_excursions.csv（不碰 fills.csv → golden 不变）。
    xt_bt::hft::XtTradeExcursionTracker    excursion_tracker;

    // ── 数据回放 ─────────────────────────────────────────────────────────
    std::unique_ptr<XtMarketDataSource>    source;
    std::unique_ptr<XtReplayer>            replayer;

    // ── 配置快照 ───────────────────────────────────────────────────────
    /// bootstrap 后 cfg 会析构，run_replay 仍需 EOF grace 等收尾参数。
    XtHftBacktestEngineCfg                 engine_cfg_snapshot;

    /// 每次回测独占 sid registry；析构前清理 global，避免悬挂指针。
    std::unique_ptr<xt_identity::StrategySidRegistry>   sid_registry;
};

// ── 装配阶段函数 ──────────────────────────────────────────────────────────────

bool load_config(int argc, char** argv, XtHftBacktestConfig& cfg, std::string& err) {
    try {
        return load_backtest_config(argc, argv, cfg, err);
    } catch (const std::exception& ex) {
        err = std::string("config parse exception: ") + ex.what();
        return false;
    } catch (...) {
        err = "config parse exception";
        return false;
    }
}

// 加载 cfg.strategies[] 中的全部插件；任一失败则 bootstrap 失败。
// parser 保证至少有一个 strategy entry。
bool try_create_strategies(const XtHftBacktestConfig& cfg,
                           BtMeta& meta,
                           BtComponents& c,
                           std::string& err) {
    if (cfg.strategies.empty()) {
        err = "cfg.strategies empty — yaml parser invariant violated "
              "(should always have at least 1 element from backtest.strategies)";
        return false;
    }
    const std::size_t n = cfg.strategies.size();
    c.strategies.reserve(n);

    meta.strategy_ids.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        const auto& entry = cfg.strategies[i];
        const std::string so_path = PathUtil::resolve_relative(cfg.cfg_path, entry.factory_cfg.so_path);
        BtStrategyInstance inst;
        inst.plugin = std::make_unique<XtPluginHolder>();
        if (!inst.plugin->load(so_path)) {
            err = std::string("load strategy[" + std::to_string(i) + "] failed id=" +
                              entry.factory_cfg.id + " err=") + inst.plugin->err;
            return false;
        }
        inst.id = entry.factory_cfg.id;
        meta.strategy_ids.push_back(inst.id);
        XtLogger::info_m("BT", "strategy[{}] loaded id={}", i, inst.id);
        c.strategies.push_back(std::move(inst));
    }
    // sid registry 在 backtest run 内独占，供 Gateway 写 sid_id、Engine 反查
    // strategy_id；装配走共享 helper（set_global → 预注册 → freeze）。
    c.sid_registry = xt_bt::make_frozen_sid_registry(meta.strategy_ids);
    meta.strategy_id     = cfg.strategies[0].factory_cfg.id;
    meta.strategy_symbol = cfg.strategies[0].init_cfg.get_string("symbol", "");
    return true;
}

XtHftBacktestOrderGateway::InitParams make_gateway_params(const XtHftBacktestConfig& cfg) {
    XtHftBacktestOrderGateway::InitParams params;
    params.risk  = cfg.risk_cfg;
    params.match = cfg.match_engine_cfg;
    params.cost  = cfg.cost_cfg;
    params.fees  = cfg.fees_cfg;
    params.max_order_qty_fp = order_exec_to_fp(cfg.engine_cfg.max_order_qty);
    params.self_match_check = cfg.self_match_check;
    params.margin_call_enabled = cfg.engine_cfg.margin_call_enabled;
    params.maintenance_ratio   = cfg.engine_cfg.maintenance_ratio;
    return params;
}

void build_account_domain(const XtHftBacktestConfig& cfg,
                          BtComponents& c,
                          XtHftBacktestOrderGateway::InitParams& params) {
    // Runner 拥有 domain 组件；Gateway 保存 non-owning 指针。
    // catalog 必须先于 account_store 构建并在其生命周期内有效：期货 realized/unrealized
    // 经其 vol_scale 归一化（AccountState ctor 一次性绑、无 setter），且 AccountStateStore
    // 构造期强制 futures ⇒ catalog 非空。BtComponents 声明顺序保证 account_store 先析构。
    c.catalog.init(cfg.commodities_cfg);

    AccountState::Config acfg;
    acfg.initial_cash = cfg.engine_cfg.initial_cash;
    acfg.futures_mode = true;
    acfg.t1_enabled   = (cfg.engine_cfg.t1_mode == XtHftBacktestEngineCfg::T1Mode::Enabled);
    acfg.catalog      = &c.catalog;
    c.account_store   = std::make_unique<xt_account::AccountStateStore>(acfg);

    params.account    = c.account_store.get();
    params.catalog    = &c.catalog;
    params.order_book = &c.order_book;
}

void configure_gateway_timing_and_models(const XtHftBacktestConfig& cfg, BtComponents& c) {
    // match_delay_ticks 表示 submit 到可撮合的 tick 延迟。
    if (cfg.engine_cfg.match_delay_ticks >= 1)
        c.gw.set_order_delay_ticks(cfg.engine_cfg.match_delay_ticks);
    // fill 回程延迟：match -> strategy visible。
    c.gw.set_fill_latency_ns(
        static_cast<int64_t>(cfg.engine_cfg.fill_return_latency_ms) * 1'000'000LL);

    // latency model 负责 submit/match/fill 可见性延迟；未配置时按 0 延迟处理。
    if (cfg.engine_cfg.latency_model.has_value()) {
        c.gw.set_latency_model(xt_bt::hft::make_latency_model(
            cfg.engine_cfg.latency_model.value()));
    }

    // queue model 负责 queue_ahead 估算；未配置时按 0 queue_ahead 处理。
    if (cfg.engine_cfg.queue_model.has_value()) {
        c.gw.set_queue_model(xt_bt::hft::make_queue_model(
            cfg.engine_cfg.queue_model.value()));
    }
}

void build_runtime_view(BtComponents& c) {
    // RuntimeView 组合策略侧 market/account/order 只读视图。
    c.market_cache = std::make_unique<XtRuntimeMarketCache>();
    xt_strategy_engine::XtRuntimeView::Components comp;
    comp.market     = c.market_cache.get();
    comp.account    = c.gw.account_view();
    comp.order_book = &c.order_book;
    comp.catalog    = &c.catalog;
    c.runtime_view = std::make_unique<xt_strategy_engine::XtRuntimeView>(comp);
}

/// @brief 组装 backtest-only 回放扩展：交易日过滤与下一交易日推断。
XtHftEngine::ReplayExt make_replay_ext(const XtHftBacktestConfig& cfg) {
    XtHftEngine::ReplayExt ext;
    ext.session_offset_minutes = cfg.engine_cfg.session_offset_minutes;
    XtBacktestTradingCalendar cal(cfg.calendar_cfg);
    if (cfg.calendar_cfg.enabled) {
        ext.calendar_filter = [cal](uint32_t date) {
            return date != 0 && cal.is_trading_day(date);
        };
    }
    ext.next_trading_day = [cal](uint32_t date) {
        return cal.next_trading_day(date);
    };
    return ext;
}

XtHftEngine::Dependencies make_engine_deps(BtComponents& c) {
    XtHftEngine::Dependencies deps;
    deps.gateway             = &c.gw;
    deps.view                = c.runtime_view.get();
    deps.account_store       = c.account_store.get();
    deps.order_book          = &c.order_book;
    deps.catalog             = &c.catalog;
    deps.order_id_translator = &c.order_id_translator;
    // declared_registry 不 wire：BT per-strategy 归因链（tracker/risk）fail-safe
    // bypass 无消费者，declare_position_interest 契约 no-op。
    return deps;
}

XtHftEngine::Options make_engine_options(const XtHftBacktestConfig& cfg, bool is_multi) {
    XtHftEngine::Options opts;
    opts.session = cfg.session_cfg;
    opts.roll    = cfg.roll_cfg;
    opts.conditional_orders_enabled = cfg.engine_cfg.conditional_orders_enabled;
    opts.replay  = make_replay_ext(cfg);
    // backtest 对 unmatched routing 立即失败，避免 sid 配置错误污染 PnL；BT 无
    // kill-switch 入口故取 FailFast。单策略下 unmatched 分支不可达（旧 strict 标志
    // 同样从不生效），派生为 WarnAndDrop 与旧行为逐位等价。
    opts.unmatched_routing = is_multi
        ? XtHftEngine::UnmatchedRoutingPolicy::FailFast
        : XtHftEngine::UnmatchedRoutingPolicy::WarnAndDrop;
    return opts;
}

std::vector<std::unique_ptr<XtStrategyContext>> build_strategy_contexts(
    const XtHftBacktestConfig& cfg, BtComponents& c) {
    // 单策略时 vector size 为 1；顺序即 engine 路由下标顺序。
    const std::size_t n = c.strategies.size();
    const bool is_multi = (n > 1);

    std::vector<std::unique_ptr<XtStrategyContext>> contexts;
    contexts.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        XtStrategyContext::Config ctx_cfg;
        ctx_cfg.strategy     = c.strategies[i].plugin->get();
        ctx_cfg.exec_gateway = &c.gw;
        ctx_cfg.runtime_view = c.runtime_view.get();
        ctx_cfg.strategy_id  = c.strategies[i].id;
        // 对称 live portfolio_router：多策略下 get_open_orders 按 sid 隔离 +
        // get_position_fp 叠加本策略 in-flight（BT 撮合带 latency，在途窗口
        // 真实存在）——保证 BT 是 live 的忠实代理。registry 已 frozen（上方
        // make_frozen_sid_registry），gateway submit 期 lookup 同口径。
        // declared_registry 不 wire：BT per-strategy 归因链（tracker/risk）
        // fail-safe bypass 无消费者，declare_position_interest 契约 no-op。
        ctx_cfg.route_by_sid     = is_multi;
        ctx_cfg.project_inflight = is_multi;
        ctx_cfg.conditional_orders_enabled = cfg.engine_cfg.conditional_orders_enabled;
        ctx_cfg.init_config  = (i < cfg.strategies.size())
                                   ? &cfg.strategies[i].init_cfg
                                   : &cfg.strategy_init_cfg;
        contexts.push_back(std::make_unique<XtStrategyContext>(ctx_cfg));
        XtLogger::info_m("BT", "context[{}] added strategy={}", i, c.strategies[i].id);
    }
    return contexts;
}

// 在 engine 接管所有权前从 context 集合生成 sink view（context 即 exec sink，
// unique_ptr move 不动 pointee，构造前后指针等价）。
void collect_exec_sink_view(BtComponents& c,
                            const std::vector<std::unique_ptr<XtStrategyContext>>& contexts) {
    c.exec_sink_ptrs.clear();
    c.exec_sink_ptrs.reserve(contexts.size());
    for (const auto& ctx : contexts) {
        c.exec_sink_ptrs.push_back(
            ctx ? static_cast<xt_strategy_engine::IStrategyExecSink*>(ctx.get()) : nullptr);
    }
}

// 校验 cfg strategy、plugin、sink、engine context 四者按 index 一致。
bool verify_assembly_alignment(const XtHftBacktestConfig& cfg,
                               BtComponents& c,
                               std::string& err) {
    const std::size_t n = c.engine->context_count();

    std::vector<std::string> cfg_ids;
    cfg_ids.reserve(cfg.strategies.size());
    for (const auto& sc : cfg.strategies) {
        cfg_ids.push_back(sc.factory_cfg.id);
    }
    const auto rep = xt_strategy_engine::check_multi_exec_sink_alignment(
        cfg_ids,
        c.strategies,
        c.exec_sink_ptrs,
        c.engine.get(),
        c.sid_registry.get());
    if (!rep.aligned) {
        err = "multi-assembly alignment failed: " + rep.error;
        return false;
    }
    if (n > 1) {
        XtLogger::info_m(
            "BT",
            "multi-strategy backtest: {} strategies (same-symbol groups are rejected by config parser)",
            n);
    }
    return true;
}

// 组装 Gateway -> RuntimeView 执行链（engine 之前的 domain 部分）。
// Runner 持有 domain 组件所有权，Gateway 和 Engine 只保存运行期指针。
void build_engine_domain(const XtHftBacktestConfig& cfg, BtComponents& c) {
    auto params = make_gateway_params(cfg);
    build_account_domain(cfg, c, params);
    c.gw.init(std::move(params));
    configure_gateway_timing_and_models(cfg, c);
    build_runtime_view(c);
    // market cache 建于 runtime view，晚于 gw.init → 越板拒的 limit 源用 setter 后 wire。
    c.gw.set_market_cache(c.market_cache.get());
    // history caps 由组合根设置（engine 不再代持该配置）；须早于首笔行情写入。
    xt_runtime::XtRuntimeMarketCacheConfig mc_cfg;
    mc_cfg.tick_history_cap      = cfg.engine_cfg.history_ticks;
    mc_cfg.bar_history_cap       = cfg.engine_cfg.history_bars;
    mc_cfg.orderflow_history_cap = cfg.engine_cfg.history_ticks;
    c.market_cache->configure(mc_cfg);
}

// 原子装配 engine：contexts 与 hooks 就位后一次性构造，构造成功即完整可跑。
bool try_build_engine(const XtHftBacktestConfig& cfg,
                      BtComponents& c,
                      std::vector<std::unique_ptr<XtStrategyContext>> contexts,
                      XtHftEngine::Hooks hooks,
                      std::string& err) {
    assert(!c.strategies.empty()
           && "engine ctor: c.strategies vector must have >=1 element by now");

    const bool is_multi = (c.strategies.size() > 1);

    // sink view 在 engine 接管 contexts 前生成；构造失败时 context 随异常销毁，
    // 需把已收集的指针清掉（失败路径不进对齐校验，与旧行为一致）。
    collect_exec_sink_view(c, contexts);
    try {
        c.engine = std::make_unique<XtHftEngine>(make_engine_deps(c),
                                                 make_engine_options(cfg, is_multi),
                                                 std::move(contexts),
                                                 std::move(hooks));
    } catch (const std::exception& e) {
        // 装配契约违反抛 std::invalid_argument；转 bool 失败走 runner 标准失败路径，
        // 不裸 terminate。
        c.exec_sink_ptrs.clear();
        err = std::string("engine assembly failed: ") + e.what();
        return false;
    }

    if (!verify_assembly_alignment(cfg, c, err)) return false;

    // Replayer 通过 driver 同步驱动 engine 和 gateway。
    c.driver = std::make_unique<xt_backtest::hft::XtHftBacktestDriver>(*c.engine, c.gw);
    return true;
}

/// @brief 建报表链并产出 engine 侧 hook（observer 注册序 = 本函数 push 序，engine
///        构造期一次性接管）。gateway 侧 observer / reject sink 就地 wire。
bool try_build_reporter(const BtMeta& meta,
                        const XtHftBacktestConfig& cfg,
                        BtComponents& c,
                        XtHftEngine::Hooks& hooks,
                        std::string& err) {
    std::error_code ec;
    std::filesystem::create_directories(meta.report_dir, ec);
    {
        xt_report::XtContractDims dims;
        dims.mode = "backtest";
        std::string csv_err;
        if (!c.csv_sink.init(meta.report_dir, dims, &csv_err, 0, /*fills_only=*/true)) {
            err = "init csv sink failed: " + csv_err;
            return false;
        }
    }

    // base_dir 必须传空：report_dir 在上方已按 cfg_path 单点 resolve（含 unique 化），
    // 再传 base_dir 会让 sink 内 resolve_relative 二次前缀（相对 --config 路径下
    // events_full.csv 落到 configs/<report_dir>/ 而非报告目录——批十 e2e 实测抓出）。
    auto event_stream_sink = std::make_unique<XtEventStreamSink>(
        cfg.event_stream_cfg, /*base_dir=*/"", meta.report_dir);
    c.reporter = std::make_unique<XtBacktestReporter>();
    if (!c.reporter->init(c.csv_sink, std::move(event_stream_sink))) {
        err = "init report failed";
        return false;
    }

    // Gateway 只依赖 observer 和 reject sink 接口，reporter 细节留在本装配层。
    c.reporter_observer = std::make_unique<XtBacktestReporterObserver>(c.reporter.get());
    c.gw.add_observer(c.reporter_observer.get());
    c.gw.bind_reject_sink(c.reporter.get());
    // 日终账户探针：daily_breakdown 记录 eod equity/cash/frozen，让「资金/冻结类
    // 拒单」在按日产物里可归因（配合 rejects_by_reason 分桶）。
    c.reporter->set_account_probe([gw = &c.gw]() {
        return XtBacktestReporter::AccountEod{
            xt_fp::from_fp(gw->get_equity_fp()),
            xt_fp::from_fp(gw->get_cash_fp()),
            xt_fp::from_fp(gw->get_frozen_fp())};
    });
    // fill 走 engine fan-out：此时 domain 处理已完成，reporter 能拿到 realized PnL。
    c.reporter_fill_observer = std::make_unique<XtBacktestReporterFillObserver>(c.reporter.get());
    hooks.observers.push_back(c.reporter_fill_observer.get());
    return true;
}

bool try_build_data_replayer(const XtHftBacktestConfig& cfg,
                             std::unique_ptr<XtMarketDataSource>& out_source,
                             std::unique_ptr<XtReplayer>& out_replayer,
                             std::string& err) {
    auto source = std::make_unique<XtMarketDataSource>();
    if (!source->load(cfg.datasource_cfg)) {
        err = "load data failed";
        return false;
    }
    auto replayer = std::make_unique<XtReplayer>();
    replayer->set_config(cfg.replay_cfg);
    out_source   = std::move(source);
    out_replayer = std::move(replayer);
    return true;
}

// ── 顶层装配流程 ──────────────────────────────────────────────────────────────

// 完成 replay 前的一次性装配：加载策略、初始化 engine/reporter/replayer/callback，
// 并在 reporter 可观测后触发 strategy on_init。
// cfg 按值传入，允许本函数改写本次 run 的 report/event_stream 路径；成功后需要
// 跨 cfg 生命周期使用的收尾配置会写入 BtComponents。
bool bootstrap(XtHftBacktestConfig cfg, BtMeta& meta, BtComponents& c, std::string& err) {
    // 相对 report_dir 按 config 所在目录解析（仓库 convention，与插件 so_path 同规；
    // 绝对路径 passthrough），消除 CSV sink（按 CWD）与 event_stream sink
    // （config-relative）曾经的落盘分叉。
    cfg.report_cfg.report_dir =
        PathUtil::resolve_relative(cfg.cfg_path, cfg.report_cfg.report_dir);
    // report sink 与 event_stream 共享同一唯一目录（meta.report_dir 单点 resolve，
    // event_stream 经构造参数接收），避免同次 run 的 artifact 分散。
    const std::string unique_report_dir =
        xt_report::make_unique_report_dir(cfg.report_cfg.report_dir);
    cfg.report_cfg.report_dir = unique_report_dir;
    meta.base_dir   = cfg.cfg_path;
    meta.report_dir = unique_report_dir;
    meta.start_date = cfg.datasource_cfg.start_date;
    meta.end_date   = cfg.datasource_cfg.end_date;
    meta.clock_resolution_ns = cfg.replay_cfg.clock_resolution_ns;
    XtLogger::info_m("BT", "report_dir(unique)={}", unique_report_dir);

    if (!try_create_strategies(cfg, meta, c, err)) return false;

    // 策略 init 必须早于 engine/context 装配；异常在本层转成稳定错误文本。
    try {
        for (std::size_t i = 0; i < c.strategies.size(); ++i) {
            const auto& init_cfg = (i < cfg.strategies.size())
                                       ? cfg.strategies[i].init_cfg
                                       : cfg.strategy_init_cfg;
            c.strategies[i].plugin->get()->init(init_cfg);
        }
    } catch (const std::exception& e) {
        err = std::string("strategy init failed: ") + e.what();
        return false;
    } catch (...) {
        err = "strategy init failed";
        return false;
    }

    // 装配顺序（原子构造）：domain → contexts → 报表/回放/hook owner → engine。
    // engine 构造期一次性接管 contexts 与 hooks，构造成功即完整可跑。
    // latency/queue model factory 对非法配置 throw（schema enum 已前置校验，
    // 残余异常路径在此转干净报错退出，不让 uncaught throw terminate）。
    std::vector<std::unique_ptr<XtStrategyContext>> contexts;
    try {
        build_engine_domain(cfg, c);
        contexts = build_strategy_contexts(cfg, c);
    } catch (const std::exception& e) {
        err = std::string("build engine failed: ") + e.what();
        return false;
    } catch (...) {
        err = "build engine failed";
        return false;
    }

    XtHftEngine::Hooks hooks;
    // reporter 必须先于 engine 构造就位：on_init 可能触发下单或 reject，
    // 产物需要 observer / reject sink 已绑定。
    if (!try_build_reporter(meta, cfg, c, hooks, err)) return false;
    if (!try_build_data_replayer(cfg, c.source, c.replayer, err)) return false;

    // 逐笔 MAE/MFE 只写 trade_excursions.csv（FIFO 批次配对，净仓不回 flat 也逐笔出行）；
    // fills.csv 仍由 reporter 链独立维护。
    c.excursion_tracker.init(meta.report_dir, &c.catalog);
    hooks.mark_tick = [&c](const XtTick& t) {
        c.excursion_tracker.on_tick(t, *c.account_store);
    };
    hooks.trade_excursion =
        [&c](const XtTradeRecord& tr, int64_t bq, int64_t aq, int64_t aa, int64_t r) {
            c.excursion_tracker.on_fill(tr, bq, aq, aa, r);
        };

    try {
        if (!try_build_engine(cfg, c, std::move(contexts), std::move(hooks), err)) return false;
    } catch (const std::exception& e) {
        err = std::string("build engine failed: ") + e.what();
        return false;
    } catch (...) {
        err = "build engine failed";
        return false;
    }

    // engine 就绪后记录 resolved 装配摘要日志，便于报告追溯本次装配结果。
    XtLogger::info_m("BT",
                     "bootstrap summary: cfg={} stra_id={} symbol={} range=[{},{}] "
                     "latency_model_entry_us={} session={} roll={} logical={} trace_mode={}",
                     cfg.cfg_path,
                     meta.strategy_id,
                     meta.strategy_symbol,
                     cfg.datasource_cfg.start_date,
                     cfg.datasource_cfg.end_date,
                     cfg.engine_cfg.latency_model.has_value() ? cfg.engine_cfg.latency_model->entry_us : 0,
                     cfg.session_cfg.enabled ? "true" : "false",
                     cfg.roll_cfg.active ? "true" : "false",
                     cfg.roll_cfg.logical_symbol,
                     XtEventStreamSink::Config::mode_name(cfg.event_stream_cfg.trace_mode));

    c.engine_cfg_snapshot = cfg.engine_cfg;

    // start() 可能触发下单或 reject，reporter observer / reject sink 已在 engine
    // 构造前绑定。fail-closed：任一策略 on_init 失败 → 不产出报表产物，直接失败退出。
    const auto start_report = c.engine->start();
    if (!start_report.all_started) {
        err = "backtest strategy on_init failed (" +
              std::to_string(start_report.failures.size()) +
              " strategy disabled fail-closed) — aborting run";
        XtLogger::error_m("HFT_BT", "{}", err);
        return false;
    }
    return true;
}

// ── 运行与收尾 ────────────────────────────────────────────────────────────────

/// @brief replay 进度装饰器：转发 reporter 钩子，日界变化时把当前日期线性映射进
/// [10,90] 并 emit_progress（stdout 行协议，viewer run 面板消费）。
/// 同日路径只多一次 int 比较；不触碰任何报表文件。
class ProgressTickReporter final : public IReplayReporter {
public:
    /// @pre end_date > start_date（caller 用该条件决定是否安装本装饰器）。
    ProgressTickReporter(IReplayReporter& inner, uint32_t start_date, uint32_t end_date)
        : inner_(inner), start_ord_(date_ordinal(start_date)),
          span_ord_(std::max(1, date_ordinal(end_date) - date_ordinal(start_date))) {}

    void on_tick_pre(int64_t ts, const XtRawTick& t) override {
        const auto d = static_cast<uint32_t>(ts / 1000000000LL);
        if (d != cur_date_) {
            cur_date_ = d;
            const int off = std::clamp(date_ordinal(d) - start_ord_, 0, span_ord_);
            const int pct = 10 + 80 * off / span_ord_;
            if (pct != last_pct_) {
                last_pct_ = pct;
                emit_progress(pct, "replay", "session " + std::to_string(d));
            }
        }
        inner_.on_tick_pre(ts, t);
    }
    void on_tick_post(int64_t ts, size_t active_orders) override {
        inner_.on_tick_post(ts, active_orders);
    }

private:
    // YYYYMMDD → 单调序数（y*372 + m*31 + d 近似历法）。只用于进度插值，
    // 月界不均匀带来的 pct 偏差 < 一个百分点档，无需真实历法。
    static int date_ordinal(uint32_t ymd) {
        return static_cast<int>(ymd / 10000) * 372
             + static_cast<int>(ymd / 100 % 100) * 31
             + static_cast<int>(ymd % 100);
    }

    IReplayReporter& inner_;
    int start_ord_;
    int span_ord_;
    uint32_t cur_date_ = 0;
    int last_pct_ = -1;
};

bool run_replay(BtComponents& c, size_t& tick_count,
                const XtHftBacktestEngineCfg& engine_cfg,
                uint32_t start_date, uint32_t end_date,
                std::string& err) {
    XtLogger::info_m("BT", "==================== RUN =====================");
    // 多日窗口才装进度装饰器；单日/无日期窗跑得快，1→100 足够。
    if (start_date != 0 && end_date > start_date) {
        ProgressTickReporter progress(*c.reporter, start_date, end_date);
        tick_count = c.replayer->run(*c.source, *c.driver, progress);
    } else {
        tick_count = c.replayer->run(*c.source, *c.driver, *c.reporter);
    }
    // EOF 收尾语义由 driver 统一处理，grace_ms 只作为窗口参数传入。
    c.driver->flush_pending(engine_cfg.eof_grace_ms);
    if (c.source->has_io_error()) {
        err = "market data reader reported I/O error (corrupt/unreadable input), "
              "aborting run to avoid silent truncation";
        return false;
    }
    if (tick_count == 0) {
        err = "no ticks replayed, please check datasource path/range/mode";
        return false;
    }
    XtLogger::info_m("BT",
                     "replayer done, ticks={} trades={} eof_grace_ms={} lost_in_transit={}",
                     tick_count, c.reporter->trade_count(),
                     engine_cfg.eof_grace_ms,
                     c.gw.lost_in_transit_count());
    return true;
}

bool finalize(BtComponents& c, const BtMeta& meta, size_t tick_count,
              std::string& done_detail, std::string& err) {
    XtLogger::info_m("BT", "==================== END =====================");
    XtLogger::info_m("BT", "account cash={} equity={} unrealized={}",
                     xt_fp::from_fp(c.gw.get_cash_fp()),
                     xt_fp::from_fp(c.gw.get_equity_fp()),
                     xt_fp::from_fp(c.gw.get_unrealized_fp()));
    if (!c.reporter->flush()) {
        err = "flush report failed";
        return false;
    }
    c.excursion_tracker.flush();  // 落 trade_excursions.csv 尾 + 关闭文件（不影响 fills.csv）。
    // daily_breakdown.json 供策略验证流水线按交易日检查 tick/submit/fill/reject。
    const std::string daily_path =
        (std::filesystem::path(meta.report_dir) / "daily_breakdown.json").string();
    c.reporter->write_daily_breakdown(daily_path, meta.start_date, meta.end_date);

    // risk_observability.json：post-fill 风险观测（breach/峰值），供 viewer 读取
    // 展示；写失败不致命（观测面不升级停机）。
    {
        const auto& obs = c.gw.risk_observability();
        const std::string obs_path =
            (std::filesystem::path(meta.report_dir) / "risk_observability.json").string();
        std::ofstream ofs(obs_path);
        if (ofs) {
            // fp 为 1e6 定点 → fixed 6 位小数无损往返（防默认 6 位有效数字
            // 大值出科学计数法丢精度，T1.132 ① 同病类）。
            ofs << "{\n"
                << "  \"position_breach_count\": " << obs.position_breach_count << ",\n"
                << "  \"peak_abs_position\": " << std::fixed << std::setprecision(6)
                << xt_fp::from_fp(obs.peak_abs_position_fp) << ",\n"
                << "  \"peak_live_orders\": " << obs.peak_live_orders << "\n"
                << "}\n";
            if (!ofs.good()) {
                XtLogger::warn_m("BT", "risk_observability.json write failed: {}", obs_path);
            }
        } else {
            XtLogger::warn_m("BT", "risk_observability.json open failed: {}", obs_path);
        }
    }

    // consumed_manifest.json：本次 run 实际消费的数据输入 + 交付量化披露，供
    // viewer 以「实际消费」口径做指纹/对比（区别于 config 引用全集）；写失败
    // 不致命（观测面不升级停机）。
    {
        xt_bt::ConsumedManifest man;
        man.files               = c.source->consumed_tick_files();
        man.tick_source         = c.source->tick_source_name();
        man.rows_total          = tick_count;
        man.cache_enabled       = c.source->bin_cache_enabled();
        man.cache_hit           = c.source->bin_cache_hit();
        man.cache_path          = c.source->bin_cache_path();
        man.clock_resolution_ns = meta.clock_resolution_ns;

        // schema 2 载入→交付分解：HFT rows_total = replayer 交付（engine 前）。
        man.delivery_stage = "pre_engine";
        const auto& ds = c.source->drop_stats();
        if (ds.loaded_trackable && ds.rows_loaded >= 0) {
            man.rows_loaded            = ds.rows_loaded;
            man.exclusions.window      = ds.window;
            man.exclusions.session     = ds.session;
            man.exclusions.skip_last   = ds.skip_last;
            man.exclusions.symbol      = ds.symbol;
            man.exclusions.bad_row     = ds.bad_row;
            // roll/warmup 是 engine 级（post_delivery），不进 source exclusions。
            const int64_t classified =
                man.exclusions.window + man.exclusions.session + man.exclusions.skip_last +
                man.exclusions.symbol + man.exclusions.bad_row;
            const int64_t residual =
                ds.rows_loaded - static_cast<int64_t>(man.rows_total) - classified;
            if (residual >= 0) {
                man.exclusions.other = residual;   // 兜差（通常为 0）。
                man.reconciled = true;             // 每行都归因 → 守恒闭合。
            } else {
                man.reconciled = false;            // over-count（不应发生）不伪造负 other。
            }
        } else {
            man.rows_loaded = -1;                  // fbs/parquet 内部预筛不可归因。
            man.reconciled = false;
        }

        // D3：engine 级 roll/session/calendar 丢弃（交付之后）另立块，不进守恒。
        const auto& ec = c.engine->engine_drop_counters();
        man.has_post_delivery_drops = true;
        man.post_delivery_drops.roll     = static_cast<int64_t>(ec.roll);
        man.post_delivery_drops.session  = static_cast<int64_t>(ec.session);
        man.post_delivery_drops.calendar = static_cast<int64_t>(ec.calendar);

        const std::string man_path =
            (std::filesystem::path(meta.report_dir) / "consumed_manifest.json").string();
        if (!xt_bt::write_consumed_manifest(man_path, man)) {
            XtLogger::warn_m("BT", "consumed_manifest.json write failed: {}", man_path);
        }
    }

    // report_dir 带唯一后缀（时间戳），结束时必须告诉调用方产物真实落点。
    done_detail = "ticks=" + std::to_string(tick_count) +
                  " trades=" + std::to_string(c.reporter->trade_count()) +
                  " report_dir=" + meta.report_dir;
    return true;
}

/// @brief 装配 + 回放 + 出报告的主流程，返回进程 exit code。
///
/// 与 `main` 拆开，让顶层 try/catch 覆盖全部装配与回放路径：延迟模型校验
///（`validate_latency_ns`）、gateway 事件时钟溢出检查等 fail-fast 以 throw
/// 形态上报，无 handler 时会 std::terminate。
int run_backtest(int argc, char** argv) {
    XtHftBacktestConfig cfg;
    std::string err;
    if (!load_config(argc, argv, cfg, err)) {
        emit_progress(100, "error", err);
        std::fprintf(stderr, "BackTestRunner: %s\n", err.c_str());
        return 1;
    }

    xt_bt::LoggerGuard logger;
    if (!logger.init(cfg.log_cfg, err)) {
        emit_progress(100, "error", err);
        std::fprintf(stderr, "BackTestRunner: %s\n", err.c_str());
        return 1;
    }

    auto fail = [&](const char* stage, const std::string& detail) {
        emit_progress(100, "error", detail);
        XtLogger::error_m("BT", "[{}] {}", stage, detail);
        return 1;
    };

    emit_progress(1, "init", "config loaded");
    XtLogger::info_m("BT", "==================== INIT ====================");

    BtMeta meta;
    BtComponents components;
    if (!bootstrap(std::move(cfg), meta, components, err)) return fail("bootstrap", err);

    emit_progress(5, "engine", "bootstrap done");
    size_t tick_count = 0;
    if (!run_replay(components, tick_count, components.engine_cfg_snapshot,
                    meta.start_date, meta.end_date, err))
        return fail("run_replay", err);

    // 策略回调在 run 期间抛出的异常被 run_guarded 吞掉（仅 error log + engine 实例计数），
    // engine 层 throw 才穿透。据计数非零判 run 失败——否则 latency fail-fast
    //（validate_latency_ns / requires-ts）在策略 submit 回调内被吞后 run 仍 rc=0 完赛
    //（静默降级、产出看似成功的报告产物）。
    if (const uint64_t n = components.engine->strategy_callback_exception_count(); n > 0) {
        return fail("strategy_exception",
                    "backtest strategy callback threw " + std::to_string(n) +
                    " exception(s) during run (swallowed by guard; run marked failed)");
    }

    emit_progress(92, "report", "writing results");
    std::string done_detail;
    if (!finalize(components, meta, tick_count, done_detail, err)) return fail("finalize", err);

    emit_progress(100, "done", done_detail);
    return 0;
}

}  // namespace

// ── 入口 ──────────────────────────────────────────────────────────────────────

int main(int argc, char** argv) {
    try {
        return run_backtest(argc, argv);
    } catch (const std::exception& e) {
        const std::string detail = std::string("unhandled exception: ") + e.what();
        emit_progress(100, "error", detail);
        std::fprintf(stderr, "BackTestRunner: %s\n", detail.c_str());
        return 70;  // EX_SOFTWARE
    } catch (...) {
        emit_progress(100, "error", "unhandled unknown exception");
        std::fprintf(stderr, "BackTestRunner: unhandled unknown exception\n");
        return 71;
    }
}
