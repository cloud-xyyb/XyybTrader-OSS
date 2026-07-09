#pragma once
/// @file
/// @brief AdvanceRiskFrame.h 是 live runner 的“每帧风控维护”共享 helper
///
/// 把 HFT/CTA/SEL 每帧需要做的 risk 检查放在 advance_risk_frame() 统一处理

#include <string>

class IMdGateway;
class ITradeGateway;
class XtLiveOrderGateway;
namespace xt_account {
class AccountStateStore;
}
namespace xt_risk {
class XtRiskMonitor;
}
namespace xt_identity { class StrategySidRegistry; }
struct XtRunnerSignals;

namespace xt_live::control {

/// @brief advance_risk_frame 的非拥有依赖；全部指向 runner composition root 持有对象。
///
/// risk 块（session_begin / arm / on_frame / integrity）要求 `risk_monitor` 与
/// `account_store` 同时非空，否则整块跳过；health check 无条件执行（md/td 各自判空）。
struct RiskFrameDeps {
    xt_risk::XtRiskMonitor* risk_monitor = nullptr;
    const xt_account::AccountStateStore* account_store = nullptr;
    XtRunnerSignals* signals = nullptr;
    XtLiveOrderGateway* order_gateway = nullptr;
    const xt_identity::StrategySidRegistry* sid_registry = nullptr;
    IMdGateway* md = nullptr;
    ITradeGateway* td = nullptr;

    std::string venue_label;
};

/// @brief 跨帧维护状态；每个 frame loop 实例各持一份（可重入，无函数级 static）。
struct RiskFrameState {
    int integrity_tick = 0;
};

void advance_risk_frame(const RiskFrameDeps& deps, RiskFrameState& state);

}
