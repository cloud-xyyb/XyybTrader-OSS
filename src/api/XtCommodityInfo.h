#pragma once

#include "xt_core/order/XtOffsetPolicy.h"

/**
 * @brief 合约品种规格（commodity / instrument spec）— 纯 DTO。
 *
 * 存储单个品种的静态交易参数：price tick、合约乘数、保证金率、滑点和
 * venue-neutral capability flags。默认值按期货品种语义设置；股票、加密等
 * 非期货品种应在 YAML 中显式覆盖能力字段。
 *
 * @note XtCommodityCatalog (xt_config) `#include` 此头并在其上提供归一化查询。
 */
struct XtCommodityInfo {
    double price_tick  = 0.0;
    double vol_scale   = 1.0;
    double margin_rate = 0.0;
    double slippage    = 0.0;
    bool   allow_short = true;

    bool has_open_interest = true;

    bool has_settle_price = true;

    bool is_t_plus_1 = false;

    int  min_lot = 1;

    bool has_broker_frozen_push = true;

    double daily_price_limit_pct = 0.0;

    XtOffsetPolicy offset_policy = XtOffsetPolicy::Inherit;
};
