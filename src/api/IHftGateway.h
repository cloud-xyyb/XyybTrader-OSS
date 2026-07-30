#pragma once

#include "api/IExecutionGateway.h"
#include "api/XtOrderTerminalRecord.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/order/XtOrderTypes.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace xt_runtime {

/// @brief HFT 执行网关扩展接口（HFT execution gateway extension）。
///
/// 继承自 IExecutionGateway（submit + cancel），新增回测撮合驱动方法。
/// 实盘 Gateway 可直接继承并接受默认空操作实现。
/// 回报链（fill/order 回调）由 Gateway 实现内部通过 exec sink 完成，不在此接口暴露。
class IHftGateway : virtual public IExecutionGateway {
public:

    virtual void advance_clock(uint32_t  , uint32_t  ,
                               uint32_t  ) {}

    virtual void process_tick(const XtTick&  ) {}

    virtual size_t drain_fills(uint32_t  , uint32_t  ,
                                std::vector<XtTradeRecord>&  ) { return 0; }

    virtual size_t drop_pending_fills() { return 0; }

    virtual size_t drain_order_updates(std::vector<XtOrderExecUpdate>&  ) { return 0; }

    virtual void set_dispatch_clock(uint32_t  , uint32_t  ) {}

    virtual void on_session_begin(uint32_t  ) {}

    virtual void notify_order_terminal(uint64_t  ) {}

    virtual void rearm_active_order(const std::string&  , uint64_t  ,
                                    bool  , int64_t  ) {}

    virtual void record_order_terminal(XtOrderStatus  ) {}

    virtual void erase_order_tracking(uint64_t  ) {}

    [[nodiscard]] virtual size_t live_orders() const = 0;

    [[nodiscard]] virtual bool is_order_terminal(uint64_t  ) const { return false; }

    [[nodiscard]] virtual bool lookup_terminal(uint64_t  ,
                                               XtOrderTerminalRecord&  ) const {
        return false;
    }
};

}
