#pragma once

#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/order/XtOrderTypes.h"
#include "api/IntentResult.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

namespace xt_runtime {

/// @brief 策略仓位 intent 的执行接口。
///
/// `XtCtaEngine` 将策略 intent 归一为单合约 signed delta，并在 filter /
/// risk scaling 后交给 executer。实现负责把可接受的 delta 转成
/// `IExecutionGateway::submit()` 调用，并通过订单/成交回报维护自己的执行状态。
///
/// direct-order 路径直接持有 `IExecutionGateway`，不经过本接口；HFT live/backtest
/// 属于 direct-order 路径。
///
/// @note owning engine serializes calls；本接口不承诺跨线程安全。
class IExecuter {
public:
    virtual ~IExecuter() = default;

    virtual const char* name() const noexcept = 0;

    virtual void set_position(const std::unordered_map<std::string, int64_t>&  ) {}

    virtual void on_position_changed(std::string_view strategy_id,
                                     const char* code,
                                     int64_t     diff_pos_fp,
                                     const char* tag) = 0;

    virtual IntentResult on_position_changed_with_result(
        std::string_view strategy_id,
        const char* code,
        int64_t     diff_pos_fp,
        int64_t     price_fp,
        XtTimeInForce tif,
        const char* tag) {
        (void)price_fp;
        (void)tif;
        on_position_changed(strategy_id, code, diff_pos_fp, tag);
        IntentResult r;
        r.accepted_delta_fp = diff_pos_fp;
        r.reject_reason = XtRejectCode::NoError;
        return r;
    }

    virtual void on_tick(const char*  , const XtTick&  ) {}

    virtual void on_order_update(const XtOrderExecUpdate&  ) {}

    virtual void on_fill(const XtTradeRecord&  ) {}

    virtual std::string report() const { return {}; }
};

}
