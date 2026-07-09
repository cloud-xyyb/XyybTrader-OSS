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

double calc_fee_from_rule(const XtFeeRule& rule,
                          double price,
                          double qty,
                          uint32_t offset,
                          const XtCommodityInfo* comm,
                          bool is_buy) {
    double unit = 0.0;
    if (offset == 1U) unit = rule.close;
    else if (offset == 2U) unit = rule.close_today;
    else unit = rule.open;

    double base_fee = 0.0;
    if (unit > 0.0) {
        if (rule.by_volume) {
            base_fee = unit * qty;
        } else {
            const double vol_scale = comm ? comm->vol_scale : 1.0;
            base_fee = unit * price * qty * vol_scale;
        }
    }

    if (rule.min_commission > 0.0 && base_fee > 0.0 && base_fee < rule.min_commission) {
        base_fee = rule.min_commission;
    }

    double extra = 0.0;
    if (rule.stamp_duty_rate > 0.0 || rule.transfer_fee_rate > 0.0) {
        const double vol_scale = comm ? comm->vol_scale : 1.0;
        const double notional = price * qty * vol_scale;
        if (!is_buy && rule.stamp_duty_rate > 0.0) extra += notional * rule.stamp_duty_rate;
        if (rule.transfer_fee_rate > 0.0)          extra += notional * rule.transfer_fee_rate;
    }
    return base_fee + extra;
}

double calc_maker_rebate(const XtFeeRule& rule,
                         double price,
                         double qty,
                         const XtCommodityInfo* comm) {
    if (rule.maker_rebate <= 0.0) return 0.0;
    if (rule.by_volume) return rule.maker_rebate * qty;
    const double vol_scale = comm ? comm->vol_scale : 1.0;
    return rule.maker_rebate * price * qty * vol_scale;
}

}

namespace xt_fill_cost {

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
        out.slippage = cost.slippage;
        out.exec_price += tr.is_buy ? out.slippage : -out.slippage;
    }

    const double tr_qty = order_exec_from_fp(tr.qty_fp);
    if (!tr.code.empty() && tr_qty > 0.0) {
        if (fees && !fees->empty_rules()) {
            const XtFeeRule* rule = fees->lookup_fee_rule(tr.code.c_str());
            if (rule) {
                out.fee = calc_fee_from_rule(*rule, out.exec_price, tr_qty, fee_offset(tr), comm, tr.is_buy);
                if (tr.is_passive && rule->maker_rebate > 0.0) {
                    const double rebate = calc_maker_rebate(*rule, out.exec_price, tr_qty, comm);
                    out.fee -= rebate;
                }
            }
        }
    }
    out.fee = round_fee_2dp(out.fee);
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

    constexpr double kEps = 1e-12;
    const bool has_ctp_fee = std::isfinite(tr.fee) && std::fabs(tr.fee) > kEps;
    const bool has_ctp_slippage = std::isfinite(tr.slippage) && std::fabs(tr.slippage) > kEps;

    if (!std::isfinite(out.exec_price) || out.exec_price <= 0.0) {
        out.exec_price = model_cost.exec_price;
    }
    out.fee = has_ctp_fee ? tr.fee : model_cost.fee;

    out.slippage = has_ctp_slippage ? tr.slippage : 0.0;
    return out;
}

}
