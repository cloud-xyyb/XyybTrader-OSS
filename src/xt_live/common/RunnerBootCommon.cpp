#include "RunnerBootCommon.h"

#include <cctype>
#include <cstdio>

#include "XtLiveConfig.h"
#include "xt_live/paramdomain/XtLiveParamDomain.h"
#include "xt_strategy_engine/XtStrategyContext.h"
#include "XtLogger.h"
#include "XtPluginHolder.h"
#include "xt_strategy_engine/loader/StrategyExecCaps.h"
#include "xt_risk/engine/XtRiskMonitorState.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_util/path/PathUtil.h"

#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif

namespace xt_live::common {

bool load_live_config_or_report(int argc, char** argv,
                                const char* runner_name, XtLiveConfig& out) {
    std::string load_err;
    std::vector<std::string> validation_errors;

    if (!load_live_config(argc, argv, out, load_err, validation_errors)) {
        std::fprintf(stderr, "%s: %s\n", runner_name, load_err.c_str());
        return false;
    }
    if (!validation_errors.empty()) {
        std::fprintf(stderr, "%s: config validation failed (%zu)\n",
                     runner_name, validation_errors.size());
        for (size_t i = 0; i < validation_errors.size(); ++i) {
            std::fprintf(stderr, "  [%zu] %s\n", i + 1, validation_errors[i].c_str());
        }
        return false;
    }
    return true;
}

void runner_process_init(const XtLiveConfig& cfg, const char* module_tag,
                         std::atomic<bool>& shutdown_flag,
                         std::atomic<int>& shutdown_sig) {
    XtLogger::init(cfg.parsed.log_cfg);
    XtLogger::info_m(module_tag, "init");
    XtProcessUtil::install_shutdown_signal_handlers(shutdown_flag, shutdown_sig);
    const auto tuning_result =
        XtProcessUtil::apply_runtime_tuning(cfg.bootstrap.process_tuning_cfg);
    if (tuning_result.mlockall_requested) {
        if (tuning_result.mlockall_succeeded) {
            XtLogger::info_m(module_tag, "mlockall enabled");
        } else {
            XtLogger::warn_m(module_tag,
                "mlockall failed (insufficient privilege or unsupported)");
        }
    }

#ifdef XT_HAVE_METRICS
    {
        const int mport = cfg.runtime.control_cfg.metrics_exposer_port;
        const std::string& mbind = cfg.runtime.control_cfg.metrics_exposer_bind_addr;
        if (mport > 0) {
            if (ct::metrics::XtMetricRegistry::instance().start_exposer(
                    static_cast<uint16_t>(mport), mbind)) {
                XtLogger::info_m(module_tag,
                    "Prometheus /metrics endpoint listening on {}:{}", mbind, mport);
            } else {
                XtLogger::warn_m(module_tag,
                    "Prometheus /metrics exposer failed to start (port {} in use?)",
                    mport);
            }
        }
    }
#endif
}

int runner_fail_exit(std::atomic<bool>& shutdown_flag) {
    shutdown_flag.store(true, std::memory_order_release);

#ifdef XT_HAVE_METRICS
    ct::metrics::XtMetricRegistry::instance().stop_exposer();
#endif
    XtLogger::shutdown();
    return 1;
}

bool lock_single_instance(XtProcessUtil::SingleInstanceLock& lock,
                          const std::string& cfg_path,
                          const char* module_tag, const char* runner_name) {
    std::string lock_holder;
    if (lock.acquire(cfg_path, &lock_holder)) return true;
    if (!lock_holder.empty()) {
        XtLogger::error_m(module_tag, "another {} is running (pid={}) lock={}",
                          runner_name, lock_holder, cfg_path + ".lock");
    } else {
        XtLogger::error_m(module_tag, "another {} is running (lock={})",
                          runner_name, cfg_path + ".lock");
    }
    return false;
}

bool load_strategy_plugins(std::vector<std::unique_ptr<XtPluginHolder>>& out,
                           const XtLiveConfig& cfg, const char* module_tag) {
    if (cfg.parsed.strategies.empty()) {
        XtLogger::error_m(module_tag,
            "no strategies configured (cfg.parsed.strategies empty)");
        return false;
    }
    out.reserve(cfg.parsed.strategies.size());
    for (size_t i = 0; i < cfg.parsed.strategies.size(); ++i) {
        const auto& entry = cfg.parsed.strategies[i];
        const std::string so_path = PathUtil::resolve_relative(
            cfg.parsed.cfg_path, entry.factory_cfg.so_path);
        auto holder = std::make_unique<XtPluginHolder>();
        if (!holder->load(so_path)) {
            XtLogger::error_m(module_tag, "load strategy[{}] failed id={} so={} err={}",
                              i, entry.factory_cfg.id, so_path, holder->err);
            return false;
        }

        if (auto* plugin = holder->get()) {
            plugin->init(entry.init_cfg);
        }
        XtLogger::info_m(module_tag, "strategy[{}] loaded id={}", i, entry.factory_cfg.id);
        out.push_back(std::move(holder));
    }
    return true;
}

bool validate_strategy_exec_sink_capabilities(
    const std::vector<std::unique_ptr<XtPluginHolder>>& plugins,
    const XtLiveConfig& cfg, const char* module_tag,
    bool gate_single_strategy, const char* deploy_desc) {
    const bool is_multi = cfg.parsed.strategies.size() > 1;
    if (!is_multi && !gate_single_strategy) return true;

    bool ok = true;
    for (size_t i = 0; i < plugins.size(); ++i) {
        auto* plugin = plugins[i]->get();
        if (!plugin) continue;
        const uint64_t caps = plugin->capabilities();
        const auto& sid = cfg.parsed.strategies[i].factory_cfg.id;
        if (exec_sink_gate_rejects(caps, is_multi, gate_single_strategy)) {
            XtLogger::error_m(module_tag, "{}",
                xt_strategy_engine::exec_sink_caps_error(sid, caps, deploy_desc));
            ok = false;
        } else {
            XtLogger::info_m(module_tag,
                "strategy[{}] id={} capabilities=0x{:x} (exec-sink gate OK)",
                i, sid, caps);
        }
    }
    return ok;
}

bool wire_sid_registry(std::unique_ptr<xt_identity::StrategySidRegistry>& out,
                       XtLiveConfig& cfg) {
    out = std::make_unique<xt_identity::StrategySidRegistry>();
    if (!cfg.parsed.portfolio_router) return true;

    for (const auto& entry : cfg.parsed.strategies) {
        const uint32_t sid_id = out->register_or_lookup(entry.factory_cfg.id);
        XtLogger::info_m("router", "sid_registry: '{}' -> sid_id={}",
                         entry.factory_cfg.id, sid_id);
    }

    auto& risk_cfg = cfg.bootstrap.order_gateway_cfg.risk;
    auto max_pos_by_sid = risk_cfg.per_strategy_max_pos_qty;
    for (const auto& [sid_str, qty] : risk_cfg.per_strategy_max_pos_qty_by_sid_string) {
        const uint32_t sid_id = out->lookup(sid_str);
        if (sid_id == xt_identity::StrategySidRegistry::kNoSidId) {
            XtLogger::error_m("F8",
                "per_strategy_max_pos_qty.{}: sid not in strategies[] after validation "
                "(qty={})", sid_str, qty);
            return false;
        }
        max_pos_by_sid[sid_id] = qty;
        XtLogger::info_m("F8",
            "per_strategy_max_pos_qty: '{}' (sid_id={}) -> {}", sid_str, sid_id, qty);
    }

    auto daily_loss_by_sid = risk_cfg.per_strategy_daily_loss_pct;
    for (const auto& [sid_str, pct] : risk_cfg.per_strategy_daily_loss_pct_by_sid_string) {
        const uint32_t sid_id = out->lookup(sid_str);
        if (sid_id == xt_identity::StrategySidRegistry::kNoSidId) {
            XtLogger::error_m("F8",
                "per_strategy_daily_loss_pct.{}: sid not in strategies[] after validation "
                "(pct={})", sid_str, pct);
            return false;
        }
        daily_loss_by_sid[sid_id] = pct;
        XtLogger::info_m("F8",
            "per_strategy_daily_loss_pct: '{}' (sid_id={}) -> {}", sid_str, sid_id, pct);
    }

    auto liquidate_by_sid = risk_cfg.per_strategy_liquidate_pct;
    for (const auto& [sid_str, pct] : risk_cfg.per_strategy_liquidate_pct_by_sid_string) {
        const uint32_t sid_id = out->lookup(sid_str);
        if (sid_id == xt_identity::StrategySidRegistry::kNoSidId) {
            XtLogger::error_m("F8",
                "per_strategy_liquidate_pct.{}: sid not in strategies[] after validation "
                "(pct={})", sid_str, pct);
            return false;
        }
        liquidate_by_sid[sid_id] = pct;
        XtLogger::info_m("F8",
            "per_strategy_liquidate_pct: '{}' (sid_id={}) -> {}", sid_str, sid_id, pct);
    }
    risk_cfg.per_strategy_max_pos_qty = max_pos_by_sid;
    risk_cfg.per_strategy_daily_loss_pct = daily_loss_by_sid;
    risk_cfg.per_strategy_liquidate_pct = liquidate_by_sid;
    return true;
}

void finalize_sid_registry(xt_identity::StrategySidRegistry* registry) {

    xt_identity::StrategySidRegistry::set_global(registry);

    registry->freeze();
}

void init_per_sid_risk_state(xt_identity::StrategySidRegistry* registry,
                             xt_risk::XtRiskMonitorState* risk_monitor_state) {
    if (registry && risk_monitor_state) {
        risk_monitor_state->per_strategy.init(registry->sid_slot_count());
    }
}

namespace {

bool parse_control_enabled(const std::string& value, bool fallback) {
    std::string v;
    v.reserve(value.size());
    for (char c : value) {
        v.push_back(static_cast<char>(::tolower(static_cast<unsigned char>(c))));
    }
    if (v == "true" || v == "1" || v == "yes" || v == "on") return true;
    if (v == "false" || v == "0" || v == "no" || v == "off") return false;
    return fallback;
}

}

void apply_initial_control_enabled(XtStrategyContext& ctx, XtLiveParamDomain& pd,
                                   const std::string& sid, const char* module_tag) {
    std::string value;
    if (!sid.empty() && pd.get("strategy." + sid + ".enabled", value)) {

        const bool enabled = parse_control_enabled(value,  false);
        ctx.set_control_enabled(enabled);
        XtLogger::info_m(module_tag,
                         "initial strategy enabled gate strategy={} value={} enabled={}",
                         sid, value, enabled);
    }
}

}
