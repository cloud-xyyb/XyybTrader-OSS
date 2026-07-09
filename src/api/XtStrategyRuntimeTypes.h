#pragma once

#include "api/XtCommodityInfo.h"

/**
 * @brief 策略运行态共享类型（shared runtime DTOs）。
 */

struct XtRawTick;
struct XtTick;
struct XtBar;
struct XtOrderExecUpdate;
struct XtOrderQueue;
struct XtOrderDetail;
struct XtTransaction;

namespace xt_runtime {

using XtStraContractInfo = XtCommodityInfo;

/**
 * @brief 单侧仓位详情（one-side position detail，double，只读）。
 *
 * 字段语义：`position` 为该侧总仓位；`today` / `yesterday` 为今昨分拆；
 * `closeable_today` / `closeable_yesterday` 为可平今昨量；`open_no_trade`
 * 为当日未回报开仓量。策略通过 IStrategyContext::get_position_detail()
 * 读取，不可写入。
 */
struct XtStraPosSideDetail {
    double position = 0.0;
    double today = 0.0;
    double yesterday = 0.0;
    double closeable_today = 0.0;
    double closeable_yesterday = 0.0;
    double open_no_trade = 0.0;
    bool has_today_split = false;
};

/**
 * @brief 完整仓位详情（full position detail，多空双侧）。
 *
 * 字段语义：`long_side` 与 `short_side` 分别承载多头侧和空头侧的只读
 * 仓位详情。
 */
struct XtStraPositionDetail {
    XtStraPosSideDetail long_side;
    XtStraPosSideDetail short_side;
};

}

enum class TaskPeriod : uint8_t {
    None    = 0,
    Minute  = 4,
    Daily   = 8,
    Weekly  = 16,
    Monthly = 32,
    Yearly  = 64,
};
