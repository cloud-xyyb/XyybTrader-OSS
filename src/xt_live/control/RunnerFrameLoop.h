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
    const char* module_tag = nullptr;                      ///< 日志 tag（"CTA"/"SEL"）
    std::atomic<bool>* shutdown_flag = nullptr;            ///< 置位即退出循环
    xt_risk::XtRiskMonitor* risk_monitor = nullptr;
    xt_account::AccountStateStore* account_store = nullptr;
    XtRunnerSignals* signals = nullptr;                    ///< last_tick_date / last_tick_steady_ms / cashflow 读侧
    XtLiveOrderGateway* order_gateway = nullptr;           ///< annual-loss latch arm 入口
    xt_identity::StrategySidRegistry* sid_registry = nullptr;        ///< integrity check 尺寸来源
    IMdGateway* md = nullptr;                              ///< 1Hz check_health
    ITradeGateway* td = nullptr;                           ///< 1Hz check_health
    /// session date 推导口径（advance_risk_frame）；取
    /// `runtime.control_cfg.venue_label`（= md_provider）。CTA/SEL 不写
    /// last_tick_date，venue 推导是其永久日期源（非临时兜底）。
    std::string venue_label;
    /// 每 1Hz 帧尾调用的 engine 特异 hook（kpi metrics emit、CTA volscale 推送）；
    /// 唯一的 per-engine 注入点。
    std::function<void()> frame_tail_hook;
    /// @brief 向 account worker 投递「执行在册发布请求」闭包（各 runner 用自己的
    ///        event-loop 控制队列）；返回 false = 队列满。见 RiskFrameDeps 同名字段。
    /// 未注入时兜底发布与 session-begin 同步一并跳过。
    std::function<bool()> post_publish_request;
};

/// @brief 阻塞运行 1Hz 帧循环直到 shutdown_flag 置位。
///
/// 每帧委托 advance_risk_frame：risk on_session_begin（按 trading_date dedup，
/// 无 tick 时 venue 日期兜底）+ annual-loss arm + on_frame + 10s 周期 per-sid
/// integrity check + MD/TD check_health；随后 frame_tail_hook。shutdown 触发后
/// ≤50ms 退出。
void run_runner_frame_loop(const RunnerFrameLoopCtx& ctx);

}  // namespace xt_live::control
