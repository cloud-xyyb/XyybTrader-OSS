// Inner latency benchmark：默认走完整生产路径，--mock 只测空壳 gateway id++。
// 用法: xt_inner_latency_bench [samples] [warmup] [mid_price] [qty] [symbol] [--mock] [--per-tick]

#include "XtStrategyContext.h"
#include "XtStrategyBase.h"
#include "xt_core/market/XtTickConvert.h"
#include "xt_core/market/XtRawTick.h"

#include "XtLiveOrderGateway.h"
#include "api/ITradeGateway.h"
#include "api/IMdGateway.h"
#include "xt_strategy_engine/IStrategyExecSink.h"
#include "xt_strategy_engine/view/XtRuntimeView.h"
#include "xt_runtime/market/XtRuntimeMarketCache.h"
#include "api/IAccountView.h"
#include "state/AccountEvent.h"
#include "ingest/AccountEventCodec.h"
#include "state/AccountStateStore.h"
#include "xt_account/AccountStoreView.h"
#include "xt_account/api/AccountSnapshot.h"
#include "xt_runtime/order/XtOrderRuntimeBook.h"
#include "xt_config/catalog/XtCommodityCatalog.h"
#include "xt_core/order/XtOrderLifecycle.h"
#include "xt_core/XtFixedPoint.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <utility>
#include <string>
#include <vector>

using xt_runtime::SubmitResult;
using xt_runtime::IExecutionGateway;
using xt_runtime::XtRuntimeMarketCache;

namespace {

class NoopExecSink final : public xt_strategy_engine::IStrategyExecSink {
public:
    void on_order(const XtOrderExecUpdate&) override {}
    void on_fill(const XtFillEvent&) override {}
    void on_account(const XtAccountUpdate&) override {}
};

uint64_t now_ns() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

uint64_t parse_u64(int argc, char** argv, int idx, uint64_t fallback) {
    if (idx >= argc) return fallback;
    char* end = nullptr;
    const unsigned long long v = std::strtoull(argv[idx], &end, 10);
    if (end == argv[idx] || (end && *end != '\0')) return fallback;
    return static_cast<uint64_t>(v);
}

double parse_double(int argc, char** argv, int idx, double fallback) {
    if (idx >= argc) return fallback;
    char* end = nullptr;
    const double v = std::strtod(argv[idx], &end);
    if (end == argv[idx] || (end && *end != '\0')) return fallback;
    return v;
}

bool has_flag(int argc, char** argv, const char* flag) {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], flag) == 0) return true;
    }
    return false;
}

struct BenchStats {
    uint64_t min_ns = 0;
    uint64_t p50_ns = 0;
    uint64_t p95_ns = 0;
    uint64_t p99_ns = 0;
    uint64_t p999_ns = 0;
    uint64_t max_ns = 0;
    double avg_ns = 0.0;
};

uint64_t percentile_value(const std::vector<uint64_t>& sorted, uint32_t p1000) {
    if (sorted.empty()) return 0;
    const uint64_t n = static_cast<uint64_t>(sorted.size());
    const uint64_t rank = (n * p1000 + 999u) / 1000u;
    const uint64_t idx = rank == 0 ? 0 : (rank - 1);
    return sorted[static_cast<size_t>(std::min<uint64_t>(idx, n - 1))];
}

BenchStats summarize(const std::vector<uint64_t>& samples) {
    BenchStats out;
    if (samples.empty()) return out;
    std::vector<uint64_t> sorted(samples.begin(), samples.end());
    std::sort(sorted.begin(), sorted.end());
    out.min_ns = sorted.front();
    out.max_ns = sorted.back();
    out.p50_ns = percentile_value(sorted, 500);
    out.p95_ns = percentile_value(sorted, 950);
    out.p99_ns = percentile_value(sorted, 990);
    out.p999_ns = percentile_value(sorted, 999);
    long double sum = 0.0;
    for (uint64_t v : sorted) sum += static_cast<long double>(v);
    out.avg_ns = static_cast<double>(sum / static_cast<long double>(sorted.size()));
    return out;
}

class InnerBenchStrategy final : public XtStrategyBase {
public:
    explicit InnerBenchStrategy(std::string symbol, double qty)
        : symbol_(std::move(symbol)), qty_(qty) {}

    void on_init(IStrategyContext* ctx) override {
        ctx->sub_tick(symbol_.c_str());
    }

    void on_tick(IStrategyContext* ctx, const XtTick& tick) override {
        const int64_t price_fp = tick.ask_price_fp[0] > 0 ? tick.ask_price_fp[0] : tick.price_fp;
        const int64_t qty_fp = static_cast<int64_t>(qty_ * 1000000);
        ctx->buy_fp(tick.code, price_fp, qty_fp, XtTimeInForce::GTC, "inner", XtOffset::Open);
    }

private:
    std::string symbol_;
    double qty_ = 1.0;
};

void init_tick(XtRawTick& tick, const std::string& symbol, double mid_price) {
    std::memset(&tick, 0, sizeof(tick));
    std::snprintf(tick.code, sizeof(tick.code), "%s", symbol.c_str());
    std::snprintf(tick.exchg, sizeof(tick.exchg), "SIM");
    tick.trading_date = 20260312;
    tick.action_date = 20260312;
    tick.action_time = 93000000;
    tick.price = mid_price;
    tick.bid_prices[0] = mid_price - 1.0;
    tick.ask_prices[0] = mid_price + 1.0;
    tick.bid_qty[0] = 10.0;
    tick.ask_qty[0] = 10.0;
}

struct MockBenchState {
    uint64_t next_order_id = 1;
    uint64_t submit_ok = 0;
    uint64_t submit_fail = 0;
};

class BenchExecutionGateway final : public IExecutionGateway {
public:
    explicit BenchExecutionGateway(MockBenchState* st) : st_(st) {}

    SubmitResult submit(const char*, bool, int64_t, int64_t, XtTimeInForce,
                        std::string_view, XtOffset, ExecFlags  ,
                        std::string_view  ) override {
        SubmitResult result;
        if (!st_) return result;
        result.order_id = st_->next_order_id++;
        ++st_->submit_ok;
        return result;
    }
    bool cancel(uint64_t) override { return true; }
    void stop(std::chrono::milliseconds timeout = std::chrono::seconds(5)) noexcept override {
        (void)timeout;
    }

private:
    MockBenchState* st_ = nullptr;
};

/// 空账户视图（mock 模式用，所有查询返回零值）
class NullAccountView final : public xt_runtime::IAccountView {
public:
    int64_t pos_qty_fp(const char*) const override { return 0; }
    int64_t pos_avg_fp(const char*) const override { return 0; }
    int64_t pos_unrealized_fp(const char*) const override { return 0; }
    bool pos_detail(const char*, xt_runtime::XtStraPositionDetail&) const override { return false; }
    int64_t cash_fp() const override { return 0; }
    int64_t equity_fp() const override { return 0; }
    int64_t unrealized_fp() const override { return 0; }
    xt_runtime::XtAccountSnapshotFp account_snapshot_fp() const override { return {}; }
};

class BenchTradeGateway final : public ITradeGateway {
public:
    bool init(const Config&) override { return true; }
    bool start() override { return true; }
    void stop() override {}

    uint64_t submit_order(const char*, bool, int64_t, int64_t,
                          XtTimeInForce, const char*, XtOffset,
                          ExecFlags  ) override {
        return ++next_id_;
    }
    bool cancel_order(uint64_t) override { return true; }

    void set_recovery_enabled(bool) {}
    bool is_recovering() const override { return false; }
    Capabilities capabilities() const override {
        return {true, false, true};
    }
    QueryControlStats query_control_stats() const override { return QueryControlStats{}; }
    void bind_callbacks(Callbacks) override {}
    bool check_health(uint32_t now_ms) override {
        (void)now_ms;
        return true;
    }

private:
    uint64_t next_id_ = 0;
};

class BenchMdGateway final : public IMdGateway {
public:
    bool init(const Config&) override { return true; }
    bool start() override { return true; }
    void stop() override {}
    Capabilities capabilities() const override { return {false}; }
    void bind_callbacks(Callbacks) override {}
    bool check_health(uint32_t now_ms) override {
        (void)now_ms;
        return true;
    }
};

struct BenchComponents {

    XtCommodityCatalog                                    catalog;
    std::unique_ptr<xt_account::AccountStateStore>        account_store;
    std::unique_ptr<xt_account::AccountStoreView>          account_view;

    xt_runtime::order::XtOrderRuntimeBook      order_book;

    void init(double initial_cash, bool futures_mode, double margin_rate,
              const std::string& symbol, double vol_scale) {
        XtCommodityCatalog::Config catalog_cfg;
        catalog_cfg.map = {{symbol, XtCommodityInfo{.vol_scale = vol_scale}}};
        catalog_cfg.default_margin_rate = margin_rate;
        catalog.init(std::move(catalog_cfg));

        AccountState::Config acfg;
        acfg.initial_cash = initial_cash;
        acfg.futures_mode = futures_mode;
        acfg.catalog      = &catalog;
        account_store = xt_account::make_account_state_store(acfg);
        account_view  = std::make_unique<xt_account::AccountStoreView>(*account_store);
    }

    void pre_warm(const std::string& sym) {

        order_book.pre_warm_instrument(sym.c_str());
        order_book.track_code(sym.c_str());
    }

    void on_order_update(const XtOrderExecUpdate& upd) {
        const bool final_status = order_status_is_final(upd.status);
        const int64_t eff_fp    = order_lifecycle::effective_remaining_fp(upd);
        account_store->apply(account_event_build::order_event(upd));
        order_book.on_order_update(upd, final_status, eff_fp);
    }

    void on_order_update_batch(const XtOrderExecUpdate* updates, size_t count) {
        for (size_t i = 0; i < count; ++i) {
            const XtOrderExecUpdate& upd = updates[i];
            const bool final_status = order_status_is_final(upd.status);
            const int64_t eff_fp    = order_lifecycle::effective_remaining_fp(upd);
            account_store->apply(account_event_build::order_event(upd));
            order_book.on_order_update(upd, final_status, eff_fp);
        }
    }
};

void run_ticks_totalN(XtStrategyContext& ctx, XtRawTick& tick, uint64_t loops) {
    for (uint64_t i = 0; i < loops; ++i) {
        ctx.dispatch_tick(xt_tick_convert::to_core(tick));
        tick.action_time += 1;
        tick.price += (i & 1u) ? 0.01 : -0.01;
        tick.bid_prices[0] = tick.price - 1.0;
        tick.ask_prices[0] = tick.price + 1.0;
    }
}

void run_ticks_totalN_drain(XtStrategyContext& ctx, XtRawTick& tick,
                            uint64_t loops, BenchComponents& comps,
                            uint64_t& next_drain_id,
                            uint64_t drain_interval,
                            bool batch_drain = false) {
    uint64_t pending_count = 0;

    std::vector<XtOrderExecUpdate> drain_buf;
    if (batch_drain) drain_buf.reserve(drain_interval);

    for (uint64_t i = 0; i < loops; ++i) {
        ctx.dispatch_tick(xt_tick_convert::to_core(tick));
        tick.action_time += 1;
        tick.price += (i & 1u) ? 0.01 : -0.01;
        tick.bid_prices[0] = tick.price - 1.0;
        tick.ask_prices[0] = tick.price + 1.0;
        ++pending_count;

        if (pending_count >= drain_interval) {
            if (batch_drain) {

                drain_buf.clear();
                for (uint64_t d = 0; d < pending_count; ++d) {
                    XtOrderExecUpdate fill{};
                    fill.id = next_drain_id++;
                    fill.status = XtOrderStatus::Filled;
                    fill.qty_fp = 1000000;
                    fill.filled_fp = 1000000;
                    fill.leaves_fp = 0;
                    fill.is_buy = true;
                    fill.close_offset = XtOffset::Open;
                    drain_buf.push_back(fill);
                }
                comps.on_order_update_batch(drain_buf.data(), drain_buf.size());
            } else {
                for (uint64_t d = 0; d < pending_count; ++d) {
                    XtOrderExecUpdate fill{};
                    fill.id = next_drain_id++;
                    fill.status = XtOrderStatus::Filled;
                    fill.qty_fp = 1000000;
                    fill.filled_fp = 1000000;
                    fill.leaves_fp = 0;
                    fill.is_buy = true;
                    fill.close_offset = XtOffset::Open;
                    comps.on_order_update(fill);
                }
            }
            pending_count = 0;
        }
    }
}

void print_results(const char* mode, const std::string& symbol,
                   uint64_t samples, uint64_t warmup,
                   uint64_t submit_ok, uint64_t submit_fail,
                   uint64_t total_ns) {
    const double avg_ns = samples > 0 ? static_cast<double>(total_ns) / static_cast<double>(samples) : 0.0;
    const double throughput =
        total_ns > 0 ? (static_cast<double>(samples) * 1e9) / static_cast<double>(total_ns) : 0.0;

    std::printf("========================================\n");
    std::printf("  XyybTrader Inner Latency Benchmark\n");
    std::printf("  (与 WonderTrader WtLatencyUFT 同维度)\n");
    std::printf("========================================\n");
    std::printf("mode=%s symbol=%s samples=%llu warmup=%llu\n",
                mode, symbol.c_str(),
                static_cast<unsigned long long>(samples),
                static_cast<unsigned long long>(warmup));
    std::printf("submit_ok=%llu submit_fail=%llu\n",
                static_cast<unsigned long long>(submit_ok),
                static_cast<unsigned long long>(submit_fail));
    std::printf("\n");
    std::printf("[total/N 均值] %llu ticks in %llu ns, Inner Latency: %.3f ns\n",
                static_cast<unsigned long long>(samples),
                static_cast<unsigned long long>(total_ns),
                avg_ns);
    std::printf("throughput: %.0f ticks/sec\n", throughput);
    std::printf("========================================\n");
}

void run_mock_bench(uint64_t samples, uint64_t warmup, double mid_price,
                    double qty, const std::string& symbol) {
    MockBenchState st;
    auto strategy = std::make_unique<InnerBenchStrategy>(symbol, qty);
    BenchExecutionGateway exec_gateway(&st);

    XtRuntimeMarketCache market_cache;
    NullAccountView null_account;
    xt_strategy_engine::XtRuntimeView::Components comp;
    comp.market  = &market_cache;
    comp.account = &null_account;
    xt_strategy_engine::XtRuntimeView runtime_view(comp);

    XtStrategyContext::Config cfg;
    cfg.strategy = strategy.get();
    cfg.exec_gateway = &exec_gateway;
    cfg.runtime_view = &runtime_view;
    XtStrategyContext ctx(cfg);
    ctx.enable_concurrent_metrics();
    ctx.on_init();

    XtRawTick tick;
    init_tick(tick, symbol, mid_price);

    if (warmup > 0) run_ticks_totalN(ctx, tick, warmup);
    st.submit_ok = 0;
    st.submit_fail = 0;

    const uint64_t t0 = now_ns();
    run_ticks_totalN(ctx, tick, samples);
    const uint64_t t1 = now_ns();

    print_results("hollow_mock", symbol, samples, warmup,
                  st.submit_ok, st.submit_fail, t1 - t0);
}

void run_realistic_bench(uint64_t samples, uint64_t warmup, double mid_price,
                         double qty, const std::string& symbol,
                         bool bypass_risk = false,
                         bool hft_mode = false) {

    const uint64_t drain_iv = []{ const char* e = std::getenv("XT_BENCH_DRAIN"); return e ? static_cast<uint64_t>(std::strtoull(e, nullptr, 10)) : 1000ull; }();

    BenchComponents comps;
    comps.init(1'000'000'000'000.0,  true,  0.10,
               symbol,  1.0);

    XtLiveOrderGateway::Config gw_cfg;
    gw_cfg.risk.max_order_qty = 0.0;
    gw_cfg.risk.max_order_notional = 0.0;
    gw_cfg.risk.max_pos_qty = 0.0;
    gw_cfg.risk.allow_short = true;
    gw_cfg.max_orders_per_sec = 0;
    gw_cfg.max_cancels_per_sec = 0;
    gw_cfg.max_rejects_per_min = 0;
    gw_cfg.cooldown_ms = 0;
    gw_cfg.require_l2_orderflow = false;
    gw_cfg.closeout_enabled = false;
    gw_cfg.session_gate_enabled = false;
    gw_cfg.bypass_risk_for_bench = bypass_risk;
    gw_cfg.venue_kind = xt_core::XtVenueKind::FuturesCN;
    gw_cfg.single_threaded_submit = hft_mode;

    std::atomic<bool> trading_ready{true};
    std::atomic<uint32_t> last_tick_action_time{93000000u};
    std::atomic<bool> td_position_ready{true};
    NoopExecSink exec_sink;

    XtLiveOrderGateway order_gw(gw_cfg, &exec_sink,
                                &trading_ready, &last_tick_action_time,
                                &td_position_ready,
                                nullptr,
                                nullptr);

    order_gw.inject_components(
        &comps.order_book,
        comps.account_store.get(),
        nullptr, &comps.catalog,
        nullptr,  false);

    BenchTradeGateway mock_td;
    BenchMdGateway mock_md;
    order_gw.bind_td(&mock_td);
    order_gw.bind_md(&mock_md);

    comps.pre_warm(symbol);
    XtRuntimeMarketCache market_cache;
    market_cache.pre_warm({symbol});
    XtRawTick seed_raw;
    init_tick(seed_raw, symbol, mid_price);
    market_cache.on_tick(xt_tick_convert::to_core(seed_raw));

    xt_strategy_engine::XtRuntimeView::Components comp;
    comp.market      = &market_cache;
    comp.account     = comps.account_view.get();
    comp.order_book  = &comps.order_book;
    comp.catalog     = &comps.catalog;
    xt_strategy_engine::XtRuntimeView runtime_view(comp);

    auto strategy = std::make_unique<InnerBenchStrategy>(symbol, qty);
    XtStrategyContext::Config ctx_cfg;
    ctx_cfg.strategy = strategy.get();
    ctx_cfg.exec_gateway = &order_gw;
    ctx_cfg.runtime_view = &runtime_view;
    XtStrategyContext ctx(ctx_cfg);
    ctx.enable_concurrent_metrics();
    ctx.on_init();

    XtRawTick tick;
    init_tick(tick, symbol, mid_price);

    uint64_t drain_id = 1;

    if (warmup > 0) {
        run_ticks_totalN_drain(ctx, tick, warmup, comps, drain_id, drain_iv, hft_mode);
    }

    const auto stats_before = order_gw.risk_stats_snapshot();
    const uint64_t t0 = now_ns();
    run_ticks_totalN_drain(ctx, tick, samples, comps, drain_id, drain_iv, hft_mode);
    const uint64_t t1 = now_ns();
    const auto stats_after = order_gw.risk_stats_snapshot();

    const uint64_t submit_ok = stats_after.submit_success - stats_before.submit_success;
    const uint64_t submit_fail = (stats_after.submit_attempts - stats_before.submit_attempts) - submit_ok;

    const char* mode_label = bypass_risk ? "L2_no_risk"
        : (hft_mode ? "L3_hft" : "L3_realistic");
    print_results(mode_label,
                  symbol, samples, warmup, submit_ok, submit_fail, t1 - t0);
    std::fprintf(stderr, "[diag] open_orders_after=%zu\n", comps.order_book.open_order_count());

    if (submit_fail > 0) {
        std::printf("\n[reject breakdown]\n");
        for (const auto& kv : stats_after.reject_by_reason) {
            if (kv.second > 0)
                std::printf("  %s: %llu\n", kv.first.c_str(),
                            static_cast<unsigned long long>(kv.second));
        }
    }
}

}

int main(int argc, char** argv) {
    const uint64_t samples = parse_u64(argc, argv, 1, 500000);
    const uint64_t warmup = parse_u64(argc, argv, 2, 20000);
    const double mid_price = parse_double(argc, argv, 3, 3500.0);
    const double qty = parse_double(argc, argv, 4, 1.0);
    const std::string symbol = (argc > 5 && argv[5][0] != '-') ? argv[5] : std::string("SHFE.rb2405");
    const bool use_mock = has_flag(argc, argv, "--mock");
    const bool no_risk = has_flag(argc, argv, "--no-risk");
    const bool hft_mode = has_flag(argc, argv, "--hft");

    if (use_mock) {
        run_mock_bench(samples, warmup, mid_price, qty, symbol);
    } else {
        run_realistic_bench(samples, warmup, mid_price, qty, symbol, no_risk, hft_mode);
    }
    return 0;
}
