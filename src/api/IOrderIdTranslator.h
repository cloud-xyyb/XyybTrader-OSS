#pragma once

#include "xt_core/order/XtOrderTypes.h"  // XtOrderExecUpdate

namespace xt_runtime {

/// @brief 订单 ID 翻译器（order id translator）。
///
/// - Live 实现：LogicalOrderTracker（SHFE/INE 平今昨拆单聚合）
/// - Backtest 实现：IdentityOrderIdTranslator（pass-through）
class IOrderIdTranslator {
public:
    virtual ~IOrderIdTranslator() = default;

    /// @brief 将物理订单回报翻译为逻辑视图。
    ///
    /// @param in  物理订单回报（XtOrderExecUpdate.id = 物理切片 id 或单切片 id）
    /// @param out 输出：逻辑订单回报（id = clord_id 或 physical id，取决于实现）
    ///
    /// 安全性前提（Safety invariant）：out 可以等于 in（live 侧 LogicalOrderTracker
    /// 做了内部字段组装；bt 侧 identity 做整体赋值）。
    virtual void translate(const XtOrderExecUpdate& in, XtOrderExecUpdate& out) = 0;

    /// @brief 终态订单条目清理（cleanup of finished entries）— 默认空操作。
    ///
    /// Live LogicalOrderTracker 重写以 GC 聚合完成的多切片订单；bt identity
    /// 实现无内部状态，用默认空操作即可。
    virtual void cleanup_done() {}

    /// @brief 物理切片 id → 逻辑 clord_id 只读反查；未命中（单切片/直发单）返回 0。
    ///
    /// fill 事件派发用：strategy.on_fill 与 on_order 统一逻辑 id 口径
    /// （translate 会写聚合状态，fill 路径只需 id 映射）。默认 0 = 恒透传。
    virtual uint64_t lookup_clord(uint64_t /*phys_id*/) const { return 0; }
};

} // namespace xt_runtime
