#pragma once

/// @file XtFillCostModel.h
/// @brief 成交成本模型的无状态计算入口。
///
/// 声明 fill-time fee/slippage 计算、live 成交成本归一化和 pre-trade fee 估算helper。
/// 调用方提供成交记录、滑点配置、手续费规则和合约规格；本文件不持有状态。

#include "api/IFeeRuleProvider.h"

struct XtCommodityInfo;
struct XtTradeCostConfig;
struct XtTradeRecord;
class XtCommodityCatalog;

namespace xt_fill_cost {

/// @brief 单笔成交成本计算结果。
struct XtAppliedTradeCost {
    double exec_price = 0.0;  ///< 最终成交价；sim_slippage=true 时已含滑点调整
    double fee = 0.0;         ///< 手续费；负数表示 maker rebate 后净返佣
    double slippage = 0.0;    ///< 本次应用的绝对滑点
};

/// @brief 单笔手续费的科目拆分（A 股佣金 / 印花税 / 过户费）。
///
/// @note `total` 是权威值：与 `apply()` 写进 `XtTradeRecord::fee` 的数走同一算术
///       路径与同一 2dp 舍入，逐笔相等。三个分项不做 2dp 舍入，故其和与 `total`
///       可能相差一次舍入量级（<0.005），消费方展示分项、对账用 `total`。
/// @note `commission` 已含 A 股最低佣金抬升与 maker rebate 抵扣（rebate 只冲减佣金，
///       不冲减税费）。
struct XtFeeBreakdown {
    double commission = 0.0;
    double stamp_duty = 0.0;
    double transfer_fee = 0.0;
    double total = 0.0;
};

/// @brief 按科目拆分单笔成交手续费。
/// @param price 费用计算基准价，须与 `apply()` 实际使用的口径一致（含滑点后的
///        成交价）；传入回测已落账的 `XtTradeRecord::exec_price` 即可复现该笔 fee。
/// @param tr    成交记录；使用 qty_fp、买卖向、开平标志和 maker/taker 标志。
/// @param fees  费率规则提供者；nullptr 或空规则表示全零。
/// @param comm  品种信息；提供 vol_scale，nullptr 时按 1.0。
[[nodiscard]] XtFeeBreakdown breakdown_fee(double price,
                                           const XtTradeRecord& tr,
                                           const IFeeRuleProvider* fees,
                                           const XtCommodityInfo* comm);

/// @brief 计算单笔成交的手续费、滑点和最终成交价。
/// @param base_price    费用/滑点计算基准价；回测通常用委托成交价，实盘用 exec_price。
/// @param tr            原始成交记录；使用 qty_fp、买卖向、开平标志和 maker/taker 标志。
/// @param cost          synthetic slippage 全局配置；按买卖方向调整 exec_price/slippage。
/// @param fees          费率规则提供者；nullptr 或空规则表示 fee=0。
/// @param comm          品种信息；提供 vol_scale 与 per-symbol slippage 覆盖
///                      （comm->slippage != 0 时优先于 cost.slippage）；nullptr 时 vol_scale 按 1.0。
/// @param sim_slippage  true=按生效 slippage（per-symbol 覆盖或全局）调整 exec_price；false=只算 fee。
XtAppliedTradeCost apply(double base_price,
                         const XtTradeRecord& tr,
                         const XtTradeCostConfig& cost,
                         const IFeeRuleProvider* fees,
                         const XtCommodityInfo* comm,
                         bool sim_slippage);

/// @brief 归一化实盘成交成本。
/// CTP 若已带逐笔 fee/slippage，则保留 venue 值；缺失字段回落到统一成本模型。
/// 纯函数，无锁；live_cost 为 nullptr 时按零成本配置计算缺失字段。
XtTradeRecord normalize_trade_cost(const XtTradeRecord& tr,
                                   const XtTradeCostConfig* live_cost,
                                   const IFeeRuleProvider* live_fees,
                                   const XtCommodityCatalog* catalog);

/// @brief 估算 pre-trade 现金占用所需手续费。
/// 复用 fill-time fee 公式，包含 A 股 min_commission、stamp duty 和 transfer fee。
/// @param rule      品种费率规则。
/// @param price     估算单价，需与 fill-time fee 口径一致。
/// @param qty       数量。
/// @param is_open   true=开仓，false=平仓。
/// @param is_today  true=平今；非期货品种传 false。
/// @param is_buy    买卖向；A 股印花税仅卖向收取。
/// @param comm      品种信息；nullptr 时 vol_scale 按 1.0。
/// @return 估算手续费，已含佣金下限和附加税费。
double estimate_fee(const XtFeeRule& rule,
                    double price,
                    double qty,
                    bool is_open,
                    bool is_today,
                    bool is_buy,
                    const XtCommodityInfo* comm);

} // namespace xt_fill_cost
