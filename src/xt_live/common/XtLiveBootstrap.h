#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "state/AccountStateStore.h"
#include "xt_account/AccountStoreView.h"
#include "xt_config/catalog/XtCommodityCatalog.h"
#include "XtLiveParamDomain.h"
#include "latency/XtLiveLatencyTracker.h"
#include "recording/XtLiveRecorder.h"
#include "xt_data/LocalFsEventStore.h"
#include "xt_risk/live/SelfMatchDetector.h"
#include "xt_risk/engine/XtRiskMonitor.h"
#include "xt_risk/engine/XtRiskMonitorState.h"

struct XtLiveConfig;
struct XtRunnerControlConfig;

/// @brief 集中持有 HFT / CTA / SEL Runner 共用的实盘基础组件。
///
/// `create()` 按配置构造账户、参数、延迟、录制和 risk monitor 对象；
/// 成员声明顺序即析构安全顺序（约束见各成员注释）。
struct XtLiveBootstrap {

    XtCommodityCatalog catalog;

    std::unique_ptr<xt_account::AccountStateStore>  account_store;
    std::unique_ptr<xt_account::AccountStoreView>   account_view;

    XtLiveParamDomain param_domain;

    std::unique_ptr<XtLiveLatencyTracker> latency_tracker;

    std::unique_ptr<XtLiveRecorder> recorder;

    std::unique_ptr<ct::data::LocalFsEventStore> event_store;

    std::unique_ptr<xt_risk::SelfMatchDetector> self_match_detector;

    std::unique_ptr<xt_risk::XtRiskMonitorState> risk_monitor_state;

    std::unique_ptr<xt_risk::XtRiskMonitor>      risk_monitor;

    bool create(const XtLiveConfig& cfg, std::string& err);

    ~XtLiveBootstrap() { shutdown_recorder(); }

    XtLiveBootstrap() = default;
    XtLiveBootstrap(const XtLiveBootstrap&)            = delete;
    XtLiveBootstrap& operator=(const XtLiveBootstrap&) = delete;

    void shutdown_recorder() noexcept;
};

bool seed_param_domain(const std::vector<std::pair<std::string, std::string>>& param_seed_values,
                       const XtLiveLatencyTracker::Config& latency_cfg,
                       const XtRunnerControlConfig& control_cfg,
                       XtLiveParamDomain& out,
                       std::string& err);

XtLiveLatencyTracker::Config resolve_latency_cfg(const XtLiveParamDomain& params,
                                                 const XtLiveLatencyTracker::Config& defaults);
