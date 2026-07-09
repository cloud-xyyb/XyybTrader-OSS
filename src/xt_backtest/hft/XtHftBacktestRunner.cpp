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
#include "xt_backtest/hft/XtHftBacktestExtConfig.h"
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

using xt_bt::emit_progress;
using xt_runtime::XtRuntimeMarketCache;

namespace {

/// @brief bootstrap 解析后的路径、日期窗和策略标识。
///
/// 只保存配置派生的元数据，不拥有策略、engine、account 或 reporter 等运行时资源。
struct BtMeta {
    std::string base_dir;
    std::string report_dir;
    std::string strategy_id;
    std::string strategy_symbol;
    std::vector<std::string> strategy_ids;
    uint32_t start_date = 0;
    uint32_t end_date = 0;
};

using BtStrategyInstance = xt_strategy_engine::StrategyInstance;

/// @brief 一次 HFT backtest 的 runtime 组件包。
///
/// 成员按 owner/consumer 生命周期拓扑声明；C++ 反向析构顺序保证 replayer/source
/// 先释放，随后 reporter/csv sink、engine/runtime view、gateway/market cache，
/// 最后才释放 strategy plugin 与 domain provider。
struct BtComponents {
    std::vector<BtStrategyInstance> strategies;

    XtCommodityCatalog                          catalog;
    std::unique_ptr<xt_account::AccountStateStore>          account_store;
    xt_runtime::order::XtOrderRuntimeBook       order_book;

    std::unique_ptr<XtRuntimeMarketCache>  market_cache;
    XtHftBacktestOrderGateway              gw;
    std::unique_ptr<xt_strategy_engine::XtRuntimeView> runtime_view;
    std::unique_ptr<XtHftEngine>           engine;
    std::vector<xt_strategy_engine::IStrategyExecSink*> exec_sink_ptrs;
    xt_runtime::IdentityOrderIdTranslator  order_id_translator;
    std::unique_ptr<xt_backtest::hft::XtHftBacktestDriver> driver;

    xt_report::XtCsvReportSink             csv_sink;
    std::unique_ptr<XtBacktestReporter>    reporter;
    std::unique_ptr<XtBacktestReporterObserver> reporter_observer;
    std::unique_ptr<XtBacktestReporterFillObserver> reporter_fill_observer;

    xt_bt::hft::XtTradeExcursionTracker    excursion_tracker;

    std::unique_ptr<XtMarketDataSource>    source;
    std::unique_ptr<XtReplayer>            replayer;

    XtHftBacktestEngineCfg                 engine_cfg_snapshot;

    std::unique_ptr<xt_identity::StrategySidRegistry>   sid_registry;
};

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

    c.catalog.init(cfg.commodities_cfg);

    AccountState::Config acfg;
    acfg.initial_cash = cfg.engine_cfg.initial_cash;
    acfg.futures_mode = true;
    acfg.t1_enabled   = (cfg.engine_cfg.t1_mode == XtHftBacktestEngineCfg::T1Mode::Enabled);
    acfg.catalog      = &c.catalog;
    c.account_store   = xt_account::make_account_state_store(acfg);

    params.account    = c.account_store.get();
    params.catalog    = &c.catalog;
    params.order_book = &c.order_book;
}

void configure_gateway_timing_and_models(const XtHftBacktestConfig& cfg, BtComponents& c) {

    if (cfg.engine_cfg.match_delay_ticks >= 1)
        c.gw.set_order_delay_ticks(cfg.engine_cfg.match_delay_ticks);

    c.gw.set_fill_latency_ns(
        static_cast<int64_t>(cfg.engine_cfg.fill_return_latency_ms) * 1'000'000LL);

    if (cfg.engine_cfg.latency_model.has_value()) {
        c.gw.set_latency_model(xt_bt::hft::make_latency_model(
            cfg.engine_cfg.latency_model.value()));
    }

    if (cfg.engine_cfg.queue_model.has_value()) {
        c.gw.set_queue_model(xt_bt::hft::make_queue_model(
            cfg.engine_cfg.queue_model.value()));
    }
}

void build_runtime_view(BtComponents& c) {

    c.market_cache = std::make_unique<XtRuntimeMarketCache>();
    xt_strategy_engine::XtRuntimeView::Components comp;
    comp.market     = c.market_cache.get();
    comp.account    = c.gw.account_view();
    comp.order_book = &c.order_book;
    comp.catalog    = &c.catalog;
    c.runtime_view = std::make_unique<xt_strategy_engine::XtRuntimeView>(comp);
}

XtHftEngine::Config make_engine_config(const BtMeta& meta,
                                       const XtHftBacktestConfig& cfg,
                                       BtComponents& c) {
    XtHftEngine::Config eng_cfg;
    eng_cfg.gateway                  = &c.gw;
    eng_cfg.view                     = c.runtime_view.get();
    eng_cfg.session                  = cfg.session_cfg;
    eng_cfg.roll                     = cfg.roll_cfg;
    eng_cfg.strategy_id              = meta.strategy_id;
    eng_cfg.history_ticks            = cfg.engine_cfg.history_ticks;
    eng_cfg.history_bars             = cfg.engine_cfg.history_bars;
    eng_cfg.init_config              = &cfg.strategy_init_cfg;
    eng_cfg.account_store            = c.account_store.get();
    eng_cfg.order_book               = &c.order_book;
    eng_cfg.catalog                  = &c.catalog;
    eng_cfg.order_id_translator      = &c.order_id_translator;
    eng_cfg.conditional_orders_enabled = cfg.engine_cfg.conditional_orders_enabled;

    eng_cfg.strict_unmatched_routing = true;
    return eng_cfg;
}

bool build_strategy_contexts(const XtHftBacktestConfig& cfg,
                             BtComponents& c,
                             XtHftEngine::Config& eng_cfg,
                             std::string& err) {

    const std::size_t n = c.strategies.size();
    const bool is_multi = (n > 1);
    XtStrategyBase* primary = c.strategies[0].plugin->get();

    c.engine = std::make_unique<XtHftEngine>( primary, eng_cfg);

    c.engine->reserve_contexts(n);
    for (std::size_t i = 0; i < n; ++i) {
        XtStrategyBase* stra_i = c.strategies[i].plugin->get();
        XtStrategyContext::Config ctx_cfg;
        ctx_cfg.strategy     = stra_i;
        ctx_cfg.exec_gateway = &c.gw;
        ctx_cfg.runtime_view = c.runtime_view.get();
        ctx_cfg.strategy_id  = c.strategies[i].id;

        ctx_cfg.route_by_sid     = is_multi;
        ctx_cfg.project_inflight = is_multi;
        ctx_cfg.conditional_orders_enabled = cfg.engine_cfg.conditional_orders_enabled;
        ctx_cfg.init_config  = (i < cfg.strategies.size())
                                   ? &cfg.strategies[i].init_cfg
                                   : &cfg.strategy_init_cfg;
        auto owned_ctx = std::make_unique<XtStrategyContext>(ctx_cfg);
        c.engine->add_context(std::move(owned_ctx));
        XtLogger::info_m("BT", "context[{}] added strategy={}", i, c.strategies[i].id);
    }

    c.exec_sink_ptrs.clear();
    c.exec_sink_ptrs.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        auto* ctx = c.engine->context(i);
        c.exec_sink_ptrs.push_back(
            ctx ? static_cast<xt_strategy_engine::IStrategyExecSink*>(ctx) : nullptr);
    }
    auto* fallback = c.exec_sink_ptrs.empty() ? nullptr : c.exec_sink_ptrs[0];

    try {
        c.engine->set_exec_sinks(fallback, c.exec_sink_ptrs);
    } catch (const std::exception& e) {
        XtLogger::error_m("BT", "engine exec sink assembly failed: {}", e.what());
        return false;
    }

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
    if (is_multi) {
        XtLogger::info_m(
            "BT",
            "multi-strategy backtest: {} strategies (same-symbol groups are rejected by config parser)",
            n);
    }
    return true;
}

void configure_backtest_ext(const XtHftBacktestConfig& cfg, BtComponents& c) {

    xt_backtest::hft::XtHftBacktestExtConfig ext;
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
    c.engine->configure_backtest_ext(ext);
}

bool try_build_engine(const BtMeta& meta,
                      const XtHftBacktestConfig& cfg,
                      BtComponents& c,
                      std::string& err) {
    assert(!c.strategies.empty()
           && "engine ctor: c.strategies vector must have >=1 element by now");

    auto params = make_gateway_params(cfg);
    build_account_domain(cfg, c, params);
    c.gw.init(std::move(params));
    configure_gateway_timing_and_models(cfg, c);
    build_runtime_view(c);

    c.gw.set_market_cache(c.market_cache.get());

    auto eng_cfg = make_engine_config(meta, cfg, c);
    if (!build_strategy_contexts(cfg, c, eng_cfg, err)) return false;
    configure_backtest_ext(cfg, c);

    c.driver = std::make_unique<xt_backtest::hft::XtHftBacktestDriver>(*c.engine, c.gw);
    return true;
}

bool try_build_reporter(const BtMeta& meta,
                        const XtHftBacktestConfig& cfg,
                        BtComponents& c,
                        std::string& err) {
    std::error_code ec;
    std::filesystem::create_directories(meta.report_dir, ec);
    {
        xt_report::XtContractDims dims;
        dims.mode = "backtest";
        std::string csv_err;
        if (!c.csv_sink.init(meta.report_dir, dims, &csv_err, 0,  true)) {
            err = "init csv sink failed: " + csv_err;
            return false;
        }
    }

    auto event_stream_sink = std::make_unique<XtEventStreamSink>(
        cfg.event_stream_cfg, meta.base_dir, meta.report_dir);
    c.reporter = std::make_unique<XtBacktestReporter>();
    if (!c.reporter->init(c.csv_sink, std::move(event_stream_sink))) {
        err = "init report failed";
        return false;
    }

    c.reporter_observer = std::make_unique<XtBacktestReporterObserver>(c.reporter.get());
    c.gw.add_observer(c.reporter_observer.get());
    c.gw.bind_reject_sink(c.reporter.get());

    c.reporter->set_account_probe([gw = &c.gw]() {
        return XtBacktestReporter::AccountEod{
            xt_fp::from_fp(gw->get_equity_fp()),
            xt_fp::from_fp(gw->get_cash_fp()),
            xt_fp::from_fp(gw->get_frozen_fp())};
    });

    c.reporter_fill_observer = std::make_unique<XtBacktestReporterFillObserver>(c.reporter.get());
    c.engine->add_observer(c.reporter_fill_observer.get());
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

bool bootstrap(XtHftBacktestConfig cfg, BtMeta& meta, BtComponents& c, std::string& err) {

    cfg.report_cfg.report_dir =
        PathUtil::resolve_relative(cfg.cfg_path, cfg.report_cfg.report_dir);

    const std::string unique_report_dir =
        xt_report::make_unique_report_dir(cfg.report_cfg.report_dir);
    cfg.report_cfg.report_dir = unique_report_dir;
    meta.base_dir   = cfg.cfg_path;
    meta.report_dir = unique_report_dir;
    meta.start_date = cfg.datasource_cfg.start_date;
    meta.end_date   = cfg.datasource_cfg.end_date;
    XtLogger::info_m("BT", "report_dir(unique)={}", unique_report_dir);

    if (!try_create_strategies(cfg, meta, c, err)) return false;

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

    try {
        if (!try_build_engine(meta, cfg, c, err)) return false;
    } catch (const std::exception& e) {
        err = std::string("build engine failed: ") + e.what();
        return false;
    } catch (...) {
        err = "build engine failed";
        return false;
    }

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

    if (!try_build_reporter(meta, cfg, c, err)) return false;
    if (!try_build_data_replayer(cfg, c.source, c.replayer, err)) return false;

    c.excursion_tracker.init(meta.report_dir);
    c.engine->set_mark_tick_callback(
        [&c](const XtTick& t) { c.excursion_tracker.on_tick(t, *c.account_store); });
    c.engine->set_trade_excursion_callback(
        [&c](const XtTradeRecord& tr, int64_t bq, int64_t aq, int64_t aa, int64_t r) {
            c.excursion_tracker.on_fill(tr, bq, aq, aa, r);
        });

    c.engine_cfg_snapshot = cfg.engine_cfg;

    c.engine->on_init();
    return true;
}

/// @brief replay 进度装饰器：转发 reporter 钩子，日界变化时把当前日期线性映射进
/// [10,90] 并 emit_progress（stdout 行协议，viewer run 面板消费）。
/// 同日路径只多一次 int 比较；不触碰任何报表文件。
class ProgressTickReporter final : public IReplayReporter {
public:

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

    if (start_date != 0 && end_date > start_date) {
        ProgressTickReporter progress(*c.reporter, start_date, end_date);
        tick_count = c.replayer->run(*c.source, *c.driver, progress);
    } else {
        tick_count = c.replayer->run(*c.source, *c.driver, *c.reporter);
    }

    c.driver->flush_pending(engine_cfg.eof_grace_ms);
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
    c.excursion_tracker.flush();

    const std::string daily_path =
        (std::filesystem::path(meta.report_dir) / "daily_breakdown.json").string();
    c.reporter->write_daily_breakdown(daily_path, meta.start_date, meta.end_date);

    done_detail = "ticks=" + std::to_string(tick_count) +
                  " trades=" + std::to_string(c.reporter->trade_count()) +
                  " report_dir=" + meta.report_dir;
    return true;
}

}

int main(int argc, char** argv) {
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

    emit_progress(92, "report", "writing results");
    std::string done_detail;
    if (!finalize(components, meta, tick_count, done_detail, err)) return fail("finalize", err);

    emit_progress(100, "done", done_detail);
    return 0;
}
