#pragma once

#include "xt_core/order/XtOrderTypes.h"

namespace xt_runtime {

/// @brief 订单 ID 翻译器（order id translator）。
///
/// - Live 实现：LogicalOrderTracker（SHFE/INE 平今昨拆单聚合）
/// - Backtest 实现：IdentityOrderIdTranslator（pass-through）
class IOrderIdTranslator {
public:
    virtual ~IOrderIdTranslator() = default;

    virtual void translate(const XtOrderExecUpdate& in, XtOrderExecUpdate& out) = 0;

    virtual void cleanup_done() {}

    virtual uint64_t lookup_clord(uint64_t  ) const { return 0; }
};

}
