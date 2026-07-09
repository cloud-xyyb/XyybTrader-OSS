#include "xt_live/hft/telemetry/XtHftTelemetryWiring.h"

#include <string>
#include <filesystem>
#include <system_error>
#include <utility>

#include "xt_engine/hft/XtHftEngine.h"
#include "xt_live/telemetry/recording/XtLiveRecorder.h"
#include "xt_metrics/XtVenueLabel.h"
#include "XtFixedPoint.h"
#include "XtMetricRegistry.h"
#include "XtLiveConfig.h"
#include "XtLogger.h"

namespace xt_live::hft {

namespace {

std::string aggregator_state_path(const XtLiveConfig& cfg) {
    return instance_state_dir(cfg.parsed.md_provider, cfg.runtime.control_cfg.instance_id) +
           "/aggregator_state.json";
}

std::string lifecycle_state_path(const XtLiveConfig& cfg) {

    if (cfg.bootstrap.recording_cfg.file_path.empty()) return {};
    const std::string& rec_path = cfg.bootstrap.recording_cfg.file_path;
    const std::size_t slash = rec_path.find_last_of('/');
    const std::string dir = (slash == std::string::npos)
                            ? "."
                            : rec_path.substr(0, slash);
    return dir + "/lifecycle_open_ts.json";
}

// engine 只保存 observer raw ptr；Runner 用 unique_ptr 持有到 stop 路径结束。
// engine 析构期不得再回调 observer。本 observer 只把 fee 计入 PnL aggregator。
class XtHftFeeObserver final : public xt_runtime::IHftEngineObserver {
public:
    explicit XtHftFeeObserver(ct::metrics::XtStrategyPnlAggregator* a)
        : aggr_(a) {}
    void on_trade(const XtTradeRecord& tr) override {
        if (aggr_) {
            aggr_->on_fill(tr.strategy_id, tr.code, tr.fee);
        }

    }
private:
    ct::metrics::XtStrategyPnlAggregator* aggr_;
};

}

void wire_hft_telemetry(
    std::unique_ptr<ct::metrics::XtStrategyPnlAggregator>& aggr_slot,
    std::unique_ptr<ct::metrics::XtStrategyPnlBaseline>& baseline_slot,
    std::unique_ptr<xt_runtime::IHftEngineObserver>& fee_observer_slot,
    XtHftEngine& engine,
    const XtLiveConfig& cfg) {

    aggr_slot = std::make_unique<ct::metrics::XtStrategyPnlAggregator>();

    {
        const std::string aggr_path = aggregator_state_path(cfg);
        const bool loaded_aggr = aggr_slot->load_state(aggr_path);
        if (loaded_aggr) {
            XtLogger::info_m("control", "PNL aggregator state reloaded from {}", aggr_path);
        } else {
            XtLogger::info_m("control", "PNL aggregator first-init (no prior state at {})", aggr_path);
        }
    }

    {

        const std::string instance_id =
            resolved_instance_id(cfg.parsed.md_provider, cfg.runtime.control_cfg.instance_id);
        const int tz_offset_h = 0;
        baseline_slot = std::make_unique<ct::metrics::XtStrategyPnlBaseline>(instance_id);
        const std::string base_dir   = state_root();
        const int trading_day_id     = ct::metrics::XtStrategyPnlBaseline::compute_trading_day_id(tz_offset_h);
        const bool loaded = baseline_slot->load(base_dir, trading_day_id);
        if (!loaded) {
            const auto status = baseline_slot->last_load_status();
            ct::metrics::XtMetricRegistry::instance().inc_strategy_baseline_load_failed_total(
                xt_util::canonical_venue_label(cfg.parsed.md_provider),
                ct::metrics::XtStrategyPnlBaseline::load_status_name(status));

            if (status == ct::metrics::XtStrategyPnlBaseline::LoadStatus::Missing &&
                !cfg.runtime.control_cfg.instance_id.empty()) {
                namespace fs = std::filesystem;
                const std::string fname =
                    "pnl_baseline_" + std::to_string(trading_day_id) + ".json";
                const fs::path inst_path =
                    fs::path(instance_state_dir(cfg.parsed.md_provider,
                                                cfg.runtime.control_cfg.instance_id)) / fname;
                const fs::path legacy_path =
                    fs::path(instance_state_dir(cfg.parsed.md_provider, std::string())) / fname;
                std::error_code ec;
                if (legacy_path != inst_path &&
                    fs::exists(legacy_path, ec) && !fs::exists(inst_path, ec)) {
                    fs::create_directories(inst_path.parent_path(), ec);
                    fs::copy_file(legacy_path, inst_path, fs::copy_options::skip_existing, ec);
                    if (!ec && baseline_slot->load(base_dir, trading_day_id)) {
                        XtLogger::info_m("control",
                            "PNL baseline recovered: migrated legacy {} -> per-instance {} and reloaded",
                            legacy_path.string(), inst_path.string());
                    } else {
                        XtLogger::info_m("control",
                            "PNL baseline legacy->per-instance recovery skipped/failed (ec={}): {}",
                            ec.message(), inst_path.string());
                    }
                }
            }
        }

        if (aggr_slot) {
            const auto aggr_rows = aggr_slot->snapshot();
            if (!aggr_rows.empty()) {
                auto base_rows = baseline_slot->snapshot();
                int seeded = 0;
                for (const auto& ar : aggr_rows) {
                    bool is_new = false;
                    (void)baseline_slot->get(ar.sid, ar.code, &is_new);
                    if (is_new) {
                        ct::metrics::XtStrategyPnlBaseline::BaselineRow br;
                        br.sid      = ar.sid;
                        br.code     = ar.code;
                        br.realized = ar.realized;
                        br.fees     = ar.fees;
                        br.fills    = ar.fills;
                        base_rows.push_back(br);
                        ++seeded;
                    }
                }
                if (seeded > 0) {
                    baseline_slot->rotate_day(base_rows);
                    baseline_slot->save(base_dir, trading_day_id);
                    XtLogger::info_m("control",
                        "PNL baseline backfilled {} missing strategy sid(s) from aggregator (today=0 for those)",
                        seeded);
                }
            }
        }
    }
    {
        fee_observer_slot = std::make_unique<XtHftFeeObserver>(aggr_slot.get());
        engine.add_observer(fee_observer_slot.get());
    }

    {
        XtHftEngine::LifecycleTelemetryInit init;
        init.state_path = lifecycle_state_path(cfg);

        init.lifecycle_sink = [](const XtHftEngine::PositionLifecycleEvent& ev) {
            auto* rec = XtLiveRecorder::global();
            if (!rec) return;
            XtLiveRecorder::PositionLifecycleParams p;
            p.ts_local_ns      = ev.ts_local_ns;
            p.ts_exch_ns       = ev.ts_exch_ns;
            p.strat_id         = ev.strategy_id.c_str();
            p.book_id          = ev.strategy_id.c_str();
            p.account_id       = nullptr;
            p.code             = ev.code.c_str();
            p.event_type       = ev.event_type;
            p.qty_fp           = ev.qty_fp;
            p.avg_price_fp     = ev.avg_price_fp;

            p.realized_pnl_fp  = ev.realized_pnl_fp;
            p.hold_duration_ms = ev.hold_duration_ms;
            p.cause_trade_id   = ev.cause_trade_id;
            rec->emit_position_lifecycle(p);
        };

        const std::string venue_label = cfg.parsed.md_provider;
        const std::string account_label = cfg.runtime.control_cfg.account_label.empty()
            ? "_default"
            : cfg.runtime.control_cfg.account_label;
        init.metric_sink =
            [venue_label, account_label](const std::string& sid, const char* code, int64_t qty_fp) {

                auto& reg = ct::metrics::XtMetricRegistry::instance();
                reg.set_position_qty_fp(sid, sid, venue_label, code ? code : "", qty_fp);
                reg.set_strategy_position_qty_fp(
                    venue_label, account_label, sid, sid, code ? code : "", qty_fp);
            };
        engine.init_lifecycle_telemetry(std::move(init));
    }

    {
        ct::metrics::XtStrategyPnlAggregator* aggr_raw = aggr_slot.get();
        engine.set_fill_realized_callback(
            [aggr_raw](const std::string& sid, const std::string& code, int64_t realized_fp) {
                if (aggr_raw) aggr_raw->on_close(sid, code, realized_fp);
            });
    }

    if (cfg.parsed.td_provider == "ctp") {
        const std::string venue_label{
            xt_util::canonical_venue_label(cfg.parsed.md_provider)};
        const std::string account_label = cfg.runtime.control_cfg.account_label.empty()
            ? "_default"
            : cfg.runtime.control_cfg.account_label;
        engine.set_cash_reanchor_probe(
            [venue_label, account_label](int64_t divergence_fp) {
                ct::metrics::XtMetricRegistry::instance().observe_cash_reanchor(
                    venue_label, account_label, xt_fp::from_fp(divergence_fp));
            });
    }
}

void save_hft_telemetry(const ct::metrics::XtStrategyPnlAggregator* aggr,
                        const XtLiveConfig& cfg) {

    if (!aggr) return;
    const std::string aggr_path = aggregator_state_path(cfg);
    const bool saved = aggr->save_state(aggr_path);
    XtLogger::info_m("control", "PNL aggregator state save ok={} path={}",
                      saved, aggr_path);
}

}
