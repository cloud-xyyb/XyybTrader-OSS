#pragma once

#include "XtFixedPoint.h"
#include "state/AccountEvent.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"

/// @file
/// @brief TD 回报 → reducer 事件的唯一边界转换（gateway wire → fixed-point）。
///
/// 本文件把成交/订单回报压成 XtAccountEvent：数量沿用回报侧 fixed-point scale，
/// 价格和费用在此归一化为 reducer 使用的 fixed-point。返回事件里的 code 是
/// non-owning 指针，调用方必须在源回报对象存活期内同步 apply。
namespace account_event_build {

inline XtAccountEvent trade_event(const XtTradeRecord& tr) {
    return XtAccountEvent::trade_ex(tr.code.c_str(),
                                    tr.is_buy,
                                    tr.qty_fp,
                                    xt_fp::to_fp(tr.exec_price),
                                    xt_fp::to_fp(tr.fee),
                                    tr.is_close,
                                    tr.is_today);
}

inline XtAccountEvent order_event(const XtOrderExecUpdate& upd) {
    return XtAccountEvent::order_update(upd.id, upd.filled_fp,
                                        order_status_is_final(upd.status));
}

}
