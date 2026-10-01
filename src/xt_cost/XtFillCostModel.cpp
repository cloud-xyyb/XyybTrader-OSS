#include "xt_cost/XtFillCostModel.h"

#include <cmath>

#include "xt_config/catalog/XtCommodityCatalog.h"
#include "XtTradeCostConfig.h"
#include "xt_core/order/XtTradeRecord.h"

namespace {

double round_fee_2dp(double fee) {
    return std::round(fee * 100.0) / 100.0;
}

uint32_t fee_offset(const XtTradeRecord& tr) {
    if (!tr.is_close) return 0U;
    return tr.is_today ? 2U : 1U;
}

// 单笔手续费的科目分量。
//
// `total()` 的加法结合序是承重的：`base + (stamp + transfer)` 与历史实现逐位一致，
// 重排会在临界值上改变 2dp 舍入结果（回测 golden 逐笔比对不容忍）。
struct FeeParts {
    double base_commission = 0.0;  ///< 佣金（已含 min_commission 抬升），未扣 rebate
    double stamp_duty      = 0.0;
    double transfer_fee    = 0.0;
    double maker_rebate    = 0.0;  ///< 只冲减佣金，不冲减税费
    [[nodiscard]] double total() const {
        return (base_commission + (stamp_duty + transfer_fee)) - maker_rebate;
    }
};

// 标准费率（taker 口径），不含 maker_rebate。
FeeParts calc_fee_parts(const XtFeeRule& rule,
                        double price,
                        double qty,
                        uint32_t offset,
                        const XtCommodityInfo* comm,
                        bool is_buy) {
    double unit = 0.0;
    if (offset == 1U) unit = rule.close;
    else if (offset == 2U) unit = rule.close_today;
    else unit = rule.open;

    FeeParts parts;
    double base_fee = 0.0;
    if (unit > 0.0) {
        if (rule.by_volume) {
            base_fee = unit * qty;
        } else {
            const double vol_scale = comm ? comm->vol_scale : 1.0;
            base_fee = unit * price * qty * vol_scale;
        }
    }

    // A 股佣金下限只抬高 base fee，不影响 stamp_duty / transfer_fee。
    if (rule.min_commission > 0.0 && base_fee > 0.0 && base_fee < rule.min_commission) {
        base_fee = rule.min_commission;
    }
    parts.base_commission = base_fee;

    // A 股印花税仅卖向收取，过户费双向收取；其他品种默认 0。
    if (rule.stamp_duty_rate > 0.0 || rule.transfer_fee_rate > 0.0) {
        const double vol_scale = comm ? comm->vol_scale : 1.0;
        const double notional = price * qty * vol_scale;
        if (!is_buy && rule.stamp_duty_rate > 0.0)
            parts.stamp_duty = notional * rule.stamp_duty_rate;
        if (rule.transfer_fee_rate > 0.0)
            parts.transfer_fee = notional * rule.transfer_fee_rate;
    }
    return parts;
}

double calc_fee_from_rule(const XtFeeRule& rule,
                          double price,
                          double qty,
                          uint32_t offset,
                          const XtCommodityInfo* comm,
                          bool is_buy) {
    return calc_fee_parts(rule, price, qty, offset, comm, is_buy).total();
}

// maker_rebate 与标准费率使用相同 by_volume / vol_scale 口径。
double calc_maker_rebate(const XtFeeRule& rule,
                         double price,
                         double qty,
                         const XtCommodityInfo* comm) {
    if (rule.maker_rebate <= 0.0) return 0.0;
    if (rule.by_volume) return rule.maker_rebate * qty;
    const double vol_scale = comm ? comm->vol_scale : 1.0;
    return rule.maker_rebate * price * qty * vol_scale;
}

// 单笔成交的 fee 科目：apply() 与 breakdown_fee() 共用的唯一算术路径。
// price 必须传两者一致的基准价（apply 传滑点后 exec_price）。
FeeParts trade_fee_parts(double price,
                         const XtTradeRecord& tr,
                         const IFeeRuleProvider* fees,
                         const XtCommodityInfo* comm) {
    FeeParts parts;
    const double tr_qty = order_exec_from_fp(tr.qty_fp);
    if (tr.code.empty() || tr_qty <= 0.0) return parts;
    if (!fees || fees->empty_rules()) return parts;
    const XtFeeRule* rule = fees->lookup_fee_rule(tr.code.c_str());
    if (!rule) return parts;

    parts = calc_fee_parts(*rule, price, tr_qty, fee_offset(tr), comm, tr.is_buy);
    if (tr.is_passive && rule->maker_rebate > 0.0) {
        parts.maker_rebate = calc_maker_rebate(*rule, price, tr_qty, comm);
    }
    return parts;
}

} // namespace

namespace xt_fill_cost {

XtFeeBreakdown breakdown_fee(double price,
                             const XtTradeRecord& tr,
                             const IFeeRuleProvider* fees,
                             const XtCommodityInfo* comm) {
    const FeeParts parts = trade_fee_parts(price, tr, fees, comm);
    XtFeeBreakdown out;
    out.commission   = parts.base_commission - parts.maker_rebate;
    out.stamp_duty   = parts.stamp_duty;
    out.transfer_fee = parts.transfer_fee;
    out.total        = round_fee_2dp(parts.total());
    return out;
}

double estimate_fee(const XtFeeRule& rule,
                    double price,
                    double qty,
                    bool is_open,
                    bool is_today,
                    bool is_buy,
                    const XtCommodityInfo* comm) {
    const uint32_t offset = is_open ? 0U : (is_today ? 2U : 1U);
    return calc_fee_from_rule(rule, price, qty, offset, comm, is_buy);
}

XtAppliedTradeCost apply(double base_price,
                         const XtTradeRecord& tr,
                         const XtTradeCostConfig& cost,
                         const IFeeRuleProvider* fees,
                         const XtCommodityInfo* comm,
                         bool sim_slippage) {
    XtAppliedTradeCost out;
    out.exec_price = base_price;
    if (sim_slippage) {
        // per-symbol 滑点覆盖: comm->slippage != 0 时优先,否则回退全局 cost.slippage
        // (0 = 未设用全局,不表示强制零滑点)。
        const double slip = (comm && comm->slippage != 0.0) ? comm->slippage
                                                            : cost.slippage;
        out.slippage = slip;
        out.exec_price += tr.is_buy ? slip : -slip;
    }

    // fee 走与 breakdown_fee() 同一算术路径（含加法结合序），保证旁车产物的
    // total_fee 与本处写进 trades.csv 的 fee 逐笔相等。
    out.fee = round_fee_2dp(trade_fee_parts(out.exec_price, tr, fees, comm).total());
    return out;
}

XtTradeRecord normalize_trade_cost(const XtTradeRecord& tr,
                                   const XtTradeCostConfig* live_cost,
                                   const IFeeRuleProvider* live_fees,
                                   const XtCommodityCatalog* catalog) {
    XtTradeRecord out = tr;
    const XtCommodityInfo* comm = catalog ? catalog->find(tr.code.c_str()) : nullptr;
    const XtTradeCostConfig zero_cost;
    const XtTradeCostConfig& cost_cfg = live_cost ? *live_cost : zero_cost;
    const double base_price = tr.exec_price > 0.0 ? tr.exec_price : tr.price;
    const XtAppliedTradeCost model_cost =
        apply(base_price, tr, cost_cfg, live_fees, comm, true);

    // Venue 已给出的逐笔成本优先；缺失字段才由模型补齐。
    constexpr double kEps = 1e-12;
    const bool has_ctp_fee = std::isfinite(tr.fee) && std::fabs(tr.fee) > kEps;
    const bool has_ctp_slippage = std::isfinite(tr.slippage) && std::fabs(tr.slippage) > kEps;

    // Live 真实成交价 (tr.exec_price) 是权威；synthetic slippage 仅用于回测建模。
    // 当 venue 价有效时必须保留，model 结果只兜底缺价或缺 fee 的记录。
    if (!std::isfinite(out.exec_price) || out.exec_price <= 0.0) {
        out.exec_price = model_cost.exec_price;
    }
    out.fee = has_ctp_fee ? tr.fee : model_cost.fee;
    // venue 未报逐笔 slippage → 保留真实成交价、slippage 记 0（不报告 synthetic 值，与
    // out.exec_price=真实价 口径一致）；venue 报了则透传。
    out.slippage = has_ctp_slippage ? tr.slippage : 0.0;
    return out;
}

} // namespace xt_fill_cost
