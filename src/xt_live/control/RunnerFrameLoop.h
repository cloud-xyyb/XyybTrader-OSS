#pragma once
/// @file
/// @brief CTA/SEL runner 共享的 1Hz 主等待循环骨架。
///
/// CTA/SEL 的 main wait loop 用它承担 1Hz risk/control 维护；
/// HFT 不接入（其帧循环在 `HftRunnerControl::run` 独立控制线程）。
/// 注：HFT不复用本文件是由于 HFT 每帧需要做更多的事情

#include <atomic>
#include <functional>
#include <string>

class XtLiveOrderGateway;
class IMdGateway;
class ITradeGateway;
namespace xt_account {
class AccountStateStore;
}
namespace xt_risk {
class XtRiskMonitor;
}
namespace xt_identity { class StrategySidRegistry; }
struct XtRunnerSignals;

namespace xt_live::control {

/// @brief 帧循环的非拥有依赖；全部指向 runner composition root 持有的对象。
struct RunnerFrameLoopCtx {
    const char* module_tag = nullptr;
    std::atomic<bool>* shutdown_flag = nullptr;
    xt_risk::XtRiskMonitor* risk_monitor = nullptr;
    xt_account::AccountStateStore* account_store = nullptr;
    XtRunnerSignals* signals = nullptr;
    XtLiveOrderGateway* order_gateway = nullptr;
    xt_identity::StrategySidRegistry* sid_registry = nullptr;
    IMdGateway* md = nullptr;
    ITradeGateway* td = nullptr;

    std::string venue_label;

    std::function<void()> frame_tail_hook;
};

void run_runner_frame_loop(const RunnerFrameLoopCtx& ctx);

}
