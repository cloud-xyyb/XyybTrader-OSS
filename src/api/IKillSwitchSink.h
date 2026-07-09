#pragma once

namespace xt_runtime {

/// @brief 紧急停止接收器（emergency kill-switch sink）。
///
/// @note Engine 只通过此接口触发/查询 kill-switch，不直接接触 xt_live 的
///   `XtRunnerSignals` 看板。Backtest 路径默认不注入（sink == nullptr），
///   trigger/is_active 分支退化为 no-op / false。
class IKillSwitchSink {
public:
    virtual ~IKillSwitchSink() = default;

    virtual void trigger_kill_switch(int reason_code) noexcept = 0;

    virtual bool is_kill_switch_active() const noexcept = 0;
};

}
