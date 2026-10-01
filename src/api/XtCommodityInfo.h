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
    double price_tick  = 0.0;   ///< 最小变动价位（price tick）
    double vol_scale   = 1.0;   ///< 合约乘数（volume scale，股票=1，螺纹钢=10，沪深300=300）
    double margin_rate = 0.0;   ///< 保证金率（margin rate，0=股票模式全额，>0=期货保证金模式）
    double slippage    = 0.0;   ///< 每手滑点;0=未设,回退全局 trade_cost.slippage(无法表达强制 0)
    bool   allow_short = true;  ///< 是否允许卖空（allow short selling；A 股主板 false，融券除外）

    // ── venue-neutral capabilities ─────────────────────────────────────────
    /// 是否有持仓量字段；影响行情解析时 open_interest/pre_interest 的填充。
    bool has_open_interest = true;
    /// 是否有结算价字段；影响行情解析时 settle/pre_settle 的填充。
    bool has_settle_price = true;
    /// 是否按 T+1 卖出约束处理；A 股通常为 true。
    bool is_t_plus_1 = false;
    /// 最小报单数量；下单数量应为该值的整数倍。
    int  min_lot = 1;

    /// venue 是否推送 broker_frozen（in-flight close 平仓冻结量）。
    /// true 表示可将本地 outstanding 与 broker frozen 双源取严；false 表示只使用本地状态。
    bool has_broker_frozen_push = true;

    /// 日内涨跌停比例；0 表示 catalog 不提供固定涨跌停比例。
    double daily_price_limit_pct = 0.0;

    /// 品种级开平路由策略；Inherit = 未配置，查询侧经 venue 默认消解
    /// （XtCommodityCatalog::effective_offset_policy）。plugin 可见 DTO
    /// （XtStraContractInfo alias），扩展须尾部追加。
    XtOffsetPolicy offset_policy = XtOffsetPolicy::Inherit;
};
