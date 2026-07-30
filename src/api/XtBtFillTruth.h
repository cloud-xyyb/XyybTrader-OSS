#pragma once

#include <cstdint>
#include <functional>

#include "xt_core/order/XtTradeRecord.h"

/// @brief 已被账户吸收的成交视图（post-account fill truth）。
///
/// 回测报告行的唯一构造入口。字段全部取自「account 已 apply 完这笔成交」之后的同一
/// 调用帧：`equity_after_fp` 是 apply 后读到的账户权益，`realized_fp` 是 apply 前后的
/// 累计 realized 差值。构造点即真值点——报告层不再事后回填账户状态。
///
/// @pre 只在 account apply 完成后的同一调用帧内构造；`tr` 为引用，仅在该帧有效，
///      不得存活到帧外（消费者若需留存，必须拷贝出所需字段）。
struct XtBtFillTruth {
    const XtTradeRecord& tr;
    int64_t  equity_after_fp;
    int64_t  realized_fp;
    uint32_t trading_day;
    int64_t  visible_at_ns;
};

using XtBtFillSink = std::function<void(const XtBtFillTruth&)>;
