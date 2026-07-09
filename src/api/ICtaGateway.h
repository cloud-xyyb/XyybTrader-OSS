#pragma once

#include "api/IExecutionGateway.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/order/XtOrderTypes.h"

#include <cstddef>
#include <vector>

namespace xt_runtime {

/// @brief CTA 执行网关扩展接口（CTA execution gateway extension）
///
/// 继承自 IExecutionGateway（submit + cancel），新增回测撮合驱动、日结与
/// 回报 drain / 终态通知方法。新增方法均有默认空操作实现，实盘 Gateway
/// 可直接继承按需重写。
class ICtaGateway : virtual public IExecutionGateway {
public:

    virtual int update_market_price(const char*  , int64_t  ,
                                    uint32_t  , uint32_t  ) {
        return 0;
    }

    virtual void advance_clock(uint32_t  , uint32_t  ) {}

    virtual int flush_session(uint32_t  , uint32_t  ) {
        return 0;
    }

    [[nodiscard]] virtual size_t active_orders() const { return 0; }

    virtual void notify_order_terminal(const char*  , bool  ) {}

    virtual void record_order_terminal(XtOrderStatus  ) {}

    virtual void erase_order_tracking(uint64_t  ) {}

    virtual size_t drain_fills(uint32_t  , uint32_t  ,
                               std::vector<XtTradeRecord>&  ) { return 0; }

    virtual size_t drain_all_fills(std::vector<XtTradeRecord>&  ) { return 0; }

    virtual size_t drain_order_updates(std::vector<XtOrderExecUpdate>&  ) { return 0; }
};

}
