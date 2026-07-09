#pragma once

#include "XtRejectCode.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtOrderRequest.h"
#include "api/ExecFlags.h"

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

namespace xt_runtime {

/**
 * @brief 下单结果 DTO（submit result）。
 *
 * IExecutionGateway::submit() 的同步返回值，携带本地分配的
 * order_id（0 表示本地预检未通过）与拒单原因（reject_reason）。
 * 用于策略层在下单后立即判断是否受理，无需等待异步回调。
 */
struct SubmitResult {
    uint64_t order_id = 0;
    XtRejectCode reject_reason = XtRejectCode::NoError;
    [[nodiscard]] bool accepted() const { return order_id != 0; }
};

/**
 * @brief 统一执行网关接口（execution gateway contract）。
 *
 * @details
 * - 核心命令面仅保留：`submit` + `cancel(id)`。
 * - 条件撤单（如按 code/side/qty）由上层上下文/OMS 组装后下沉为 `cancel(id)`。
 */
class IExecutionGateway {
public:
    virtual ~IExecutionGateway() = default;

    [[nodiscard]] virtual SubmitResult submit(const char* code,
                                bool is_buy,
                                int64_t price_fp,
                                int64_t qty_fp,
                                XtTimeInForce tif,
                                std::string_view user_tag,
                                XtOffset offset,
                                ExecFlags exec_flags,
                                std::string_view strategy_id = "") = 0;

    virtual bool cancel(uint64_t id) = 0;

    [[nodiscard]] virtual SubmitResult submit(const xt_core::XtOrderRequest& req,
                                ExecFlags exec_flags) {

        return submit(req.order.code,
                      req.order.is_buy,
                      req.order.price_fp,
                      req.order.qty_fp,
                      req.order.tif,
                      req.order.tag,
                      req.order.close_offset,
                      exec_flags,
                      req.strategy_id);
    }

    [[nodiscard]] virtual std::string lookup_strategy_id(uint64_t  ) const {
        return std::string{};
    }

    [[nodiscard]] virtual size_t translate_open_orders(XtOrderExecUpdate*  ,
                                                       size_t n) const {
        return n;
    }

    virtual void stop(std::chrono::milliseconds timeout = std::chrono::seconds(5)) noexcept = 0;

    [[nodiscard]] virtual SubmitResult submit_with_intent_trace(const char* code,
                                bool is_buy,
                                int64_t price_fp,
                                int64_t qty_fp,
                                XtTimeInForce tif,
                                std::string_view user_tag,
                                XtOffset offset,
                                ExecFlags exec_flags,
                                std::string_view strategy_id,
                                uint64_t intent_trace_id) {
        (void)intent_trace_id;
        return submit(code,
                      is_buy,
                      price_fp,
                      qty_fp,
                      tif,
                      user_tag,
                      offset,
                      exec_flags,
                      strategy_id);
    }
};

}
