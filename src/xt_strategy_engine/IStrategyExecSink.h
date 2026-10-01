#pragma once

#include "xt_core/order/XtFillEvent.h"
#include "xt_core/order/XtOrderTypes.h"

namespace xt_strategy_engine {

/// @brief engine 向策略侧派发执行回报（订单/成交/账户）的回调接口。
///
/// 进程内 engine/gateway 的 wiring 契约，不属于策略 plugin ABI。
/// 调用方持 non-owning 指针，pointee 生命周期须覆盖所有回调；
/// 异常隔离由上游负责，实现只做事件转发。
class IStrategyExecSink {
public:
    virtual ~IStrategyExecSink() = default;

    virtual void on_order(const XtOrderExecUpdate& upd) = 0;
    virtual void on_fill(const XtFillEvent& fill) = 0;
    virtual void on_account(const XtAccountUpdate& upd) = 0;
};

}  // namespace xt_strategy_engine
