#include "XtLiveBootstrap.h"

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>

#include "XtLiveConfig.h"
#include "XtLiveRuntimeStateCfg.h"
#include "XtRunnerControlCfg.h"
#include "XtLogger.h"

bool XtLiveBootstrap::create(const XtLiveConfig& cfg, std::string& err) {
    const XtLiveBootstrapPlan& bp = cfg.bootstrap;
    const XtLiveRuntimeStateCfg& rc = bp.runtime_state_cfg;

    XtCommodityCatalog::Config catalog_cfg;
    catalog_cfg.map = rc.commodity_map;
    catalog_cfg.margin_overrides = rc.margin_by_symbol;
    catalog_cfg.default_margin_rate = rc.margin_rate;
    catalog_cfg.default_offset_policy = rc.offset_policy_default;
    catalog.init(std::move(catalog_cfg));

    AccountState::Config account_cfg;
    account_cfg.initial_cash = rc.initial_cash;
    account_cfg.t1_enabled   = rc.t1_enabled;
    account_cfg.futures_mode = rc.futures_mode;
    account_cfg.catalog      = &catalog;
    account_store = std::make_unique<xt_account::AccountStateStore>(account_cfg);
    account_view = std::make_unique<xt_account::AccountStoreView>(*account_store);

    std::string backend_err;
    if (!param_domain.configure_backend(bp.param_backend_cfg, &backend_err)) {
        err = "params backend config failed: " + backend_err;
        return false;
    }
    XtLogger::info_m("RT", "params backend={}", param_domain.backend_name());

    std::string seed_err;
    if (!seed_param_domain(cfg.parsed.param_seed_values,
                           bp.latency_cfg,
                           cfg.runtime.control_cfg,
                           param_domain,
                           seed_err)) {
        err = "params seed failed: " + seed_err;
        return false;
    }

    latency_tracker = std::make_unique<XtLiveLatencyTracker>(
        resolve_latency_cfg(param_domain, bp.latency_cfg));

    if (bp.recording_cfg.enabled) {
        recorder = std::make_unique<XtLiveRecorder>(bp.recording_cfg);
        recorder->start();
        if (!recorder->enabled()) {

            XtLogger::warn_m("RT", "live recorder disabled: failed to open file_path={}",
                             bp.recording_cfg.file_path);
            recorder.reset();
        } else {

            if (bp.event_store_cfg.enabled && !bp.event_store_cfg.root_path.empty()) {

                try {
                    const std::filesystem::path events_root(bp.event_store_cfg.root_path);
                    if (std::filesystem::exists(events_root) &&
                        !std::filesystem::is_empty(events_root)) {
                        const auto sec = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::system_clock::now().time_since_epoch()).count();
                        std::filesystem::path rotated = events_root;
                        rotated += "." + std::to_string(sec);
                        std::filesystem::rename(events_root, rotated);
                        XtLogger::info_m("RT", "fbs events rotated: {} → {}",
                                         events_root.string(), rotated.string());
                    }
                } catch (const std::exception& e) {
                    XtLogger::warn_m("RT", "fbs events rotate failed: {} (continuing with append)",
                                     e.what());
                }
                event_store = std::make_unique<ct::data::LocalFsEventStore>(
                    bp.event_store_cfg.root_path);
                recorder->set_event_store(event_store.get());
                XtLogger::info_m("RT", "live fbs event store enabled root={}",
                                 bp.event_store_cfg.root_path);
            }

            int8_t vk = 0;
            if (cfg.parsed.md_provider == "ctp") vk = 1;
            else if (cfg.parsed.md_provider == "binance_u") vk = 4;
            else if (cfg.parsed.md_provider == "xtp") vk = 5;
            recorder->set_venue_kind(vk);

            XtLiveRecorder::set_global(recorder.get());
            XtLogger::info_m("RT", "live recorder started file={} queue_cap={}",
                             bp.recording_cfg.file_path,
                             static_cast<unsigned long long>(bp.recording_cfg.queue_capacity));
        }
    }

    self_match_detector = std::make_unique<xt_risk::SelfMatchDetector>();

    risk_monitor_state = std::make_unique<xt_risk::XtRiskMonitorState>();
    risk_monitor = std::make_unique<xt_risk::XtRiskMonitor>(
        bp.risk_monitor_cfg, risk_monitor_state.get());

    err.clear();
    return true;
}

void XtLiveBootstrap::shutdown_recorder() noexcept {

    XtLiveRecorder::set_global(nullptr);
    if (recorder) {
        recorder->set_event_store(nullptr);
        recorder->stop();
    }
    if (event_store && !event_store->flush()) {

        XtLogger::warn_m("RT", "fbs event store flush reported persistence failure on shutdown "
                               "(tail events may not be durable)");
    }
}

bool seed_param_domain(const std::vector<std::pair<std::string, std::string>>& param_seed_values,
                       const XtLiveLatencyTracker::Config& latency_cfg,
                       const XtRunnerControlConfig& control_cfg,
                       XtLiveParamDomain& out,
                       std::string& err) {
    auto set_required = [&](const std::string& key, const std::string& value) -> bool {
        std::string set_err;
        if (out.set_checked(key, value, nullptr, &set_err)) return true;
        err = "seed required param failed key=" + key + " err=" + set_err;
        return false;
    };

    if (!set_required("control.log_stats", control_cfg.log_stats ? "true" : "false")) return false;
    if (!set_required("trading.submit.enabled", "true")) return false;
    if (!set_required("trading.cancel.enabled", "true")) return false;
    if (!set_required("latency.enabled", latency_cfg.enabled ? "true" : "false")) return false;
    if (!set_required("latency.stale_ttl_us", std::to_string(latency_cfg.stale_ttl_us))) return false;
    if (!set_required("strategy_metrics.flush_sec",
                      std::to_string(control_cfg.strategy_metrics_flush_sec))) return false;

    for (const auto& kv : param_seed_values) {
        std::string set_err;
        if (out.set_checked(kv.first, kv.second, nullptr, &set_err)) continue;
        XtLogger::warn_m("RT", "param seed ignored key={} err={}", kv.first, set_err);
    }
    err.clear();
    return true;
}

XtLiveLatencyTracker::Config resolve_latency_cfg(const XtLiveParamDomain& params,
                                                  const XtLiveLatencyTracker::Config& defaults) {
    XtLiveLatencyTracker::Config cfg = defaults;
    cfg.enabled = params.get_bool_or("latency.enabled", defaults.enabled);
    const int64_t ttl_us = params.get_int_or(
        "latency.stale_ttl_us",
        static_cast<int64_t>(defaults.stale_ttl_us));
    cfg.stale_ttl_us = ttl_us > 0 ? static_cast<uint64_t>(ttl_us) : defaults.stale_ttl_us;
    return cfg;
}
