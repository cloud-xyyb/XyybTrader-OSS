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

    /// @brief 触发 kill-switch（trigger emergency stop）。
    /// @param reason_code 原因码（caller-defined，例如 SAME-symbol multi-strategy
    ///   reconnect = 1；保留扩展位给未来 reason 分类）。
    /// @note 实现需保证幂等（重复 trigger 只置 true 一次）。
    /// @note noexcept — 触发路径处于异常恢复语境，不允许再抛。
    virtual void trigger_kill_switch(int reason_code) noexcept = 0;

    /// @brief 查询 kill-switch 是否已激活（is kill-switch already on）。
    /// @return true = 已 trigger 过；false = 仍处于正常运行态。
    /// @note noexcept const — 仅原子读取，热路径风控可调。
    virtual bool is_kill_switch_active() const noexcept = 0;
};

} // namespace xt_runtime
