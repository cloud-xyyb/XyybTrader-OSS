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
    // catalog 必须先于 account_store 声明：account_store 经 Config.catalog 持 &catalog
    // （vol_scale 源）。C++ 析构反序 → account_store 先析构、catalog 后析构，避免悬空。
    XtCommodityCatalog catalog;

    std::unique_ptr<xt_account::AccountStateStore>  account_store;
    std::unique_ptr<xt_account::AccountStoreView>   account_view;

    XtLiveParamDomain param_domain;

    std::unique_ptr<XtLiveLatencyTracker> latency_tracker;

    /// @note 创建后发布到 `XtLiveRecorder::global()`；销毁前必须先 shutdown。
    std::unique_ptr<XtLiveRecorder> recorder;

    /// @note recorder drain 线程写入 event_store；成员顺序保证 recorder 先析构。
    std::unique_ptr<ct::data::LocalFsEventStore> event_store;

    /// @brief Post-trade self-match 检测器。
    std::unique_ptr<xt_risk::SelfMatchDetector> self_match_detector;

    /// @brief Active risk monitor 广播状态：writer = 各 runner frame loop。
    std::unique_ptr<xt_risk::XtRiskMonitorState> risk_monitor_state;
    /// @brief Drawdown / stale 算法层；disabled 时 on_frame no-op。
    std::unique_ptr<xt_risk::XtRiskMonitor>      risk_monitor;

    /// @brief 按配置创建公共组件；失败时写入 `err`。
    bool create(const XtLiveConfig& cfg, std::string& err);

    /// @brief 自动关闭 recorder，避免 tap point 访问已析构对象。
    ~XtLiveBootstrap() { shutdown_recorder(); }

    XtLiveBootstrap() = default;
    XtLiveBootstrap(const XtLiveBootstrap&)            = delete;
    XtLiveBootstrap& operator=(const XtLiveBootstrap&) = delete;

    /// @brief 清空 recorder 全局指针并停止后台线程。
    void shutdown_recorder() noexcept;
};

/// @brief 向参数域写入必填系统参数和用户 seed 值。
bool seed_param_domain(const std::vector<std::pair<std::string, std::string>>& param_seed_values,
                       const XtLiveLatencyTracker::Config& latency_cfg,
                       const XtRunnerControlConfig& control_cfg,
                       XtLiveParamDomain& out,
                       std::string& err);

/// @brief 用运行时参数覆盖静态 latency 配置。
XtLiveLatencyTracker::Config resolve_latency_cfg(const XtLiveParamDomain& params,
                                                 const XtLiveLatencyTracker::Config& defaults);
