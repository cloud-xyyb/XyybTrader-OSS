#pragma once

#include "api/IOrderIdTranslator.h"

namespace xt_runtime {

/// @brief 回测/测试用的订单回报透传实现。
///
/// Engine 固定依赖 IOrderIdTranslator；回测产生的订单 ID 已经是策略可见 ID，
/// 所以 translate() 只把输入回报原样拷贝到输出回报。
class IdentityOrderIdTranslator : public IOrderIdTranslator {
public:
    void translate(const XtOrderExecUpdate& in, XtOrderExecUpdate& out) override {
        out = in;
    }
};

}  // namespace xt_runtime
