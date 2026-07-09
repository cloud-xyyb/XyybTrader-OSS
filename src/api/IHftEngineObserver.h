#pragma once

#include <cstdint>
#include <string_view>

#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/order/XtOrderTypes.h"
#include "api/ITradeGateway.h"
#include "xt_account/api/AccountSnapshot.h"

namespace xt_runtime {

/// @brief HFT engine 执行事件观察者（HFT engine execution event observer）。
///
/// Engine 在 domain 操作（order_book / account / bridge）完成后，遍历 observer
/// 链扇出事件。Observer 不修改 domain 状态，仅用于 out-of-band 观测。
///
/// 默认所有方法为空实现 — 实现类只需 override 需要的事件。
class IHftEngineObserver {
public:
    virtual ~IHftEngineObserver() = default;

    virtual void on_trade(const XtTradeRecord&  ) {}

    virtual void on_order_update(const XtOrderExecUpdate&  ) {}

    virtual void on_order_submit(int64_t  ,
                                 uint64_t  ,
                                 const char*  ,
                                 bool  ,
                                 int64_t  ,
                                 int64_t  ,
                                 std::string_view  ,
                                 bool  ,
                                 bool  ) {}

    virtual void on_position_snapshot_begin(bool  ) {}

    virtual void on_position_snapshot_end(bool  , bool  ,
                                          uint32_t  ) {}

    virtual void on_channel_ready(const char*  ) {}

    virtual void on_channel_lost(const char*  , int  ) {}

    virtual void on_account_snapshot(const ITradeGateway::AccountSnapshot&  ,
                                      const xt_account::AccountSnapshot&  ) {}
};

}
