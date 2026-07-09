#pragma once

/**
 * @brief 手续费规则（fee rule per instrument）— 纯 DTO。
 *
 * 定义单个合约的费率结构，区分开仓（open）、平仓（close）、
 * 平今仓（close_today）费率，支持按量（by_volume）或按面值计费，
 * 并可配置 maker 返佣（maker_rebate）。
 *
 * @note XtFeeRuleBook（xt_config）`#include` 此头并继续暴露 XtFeeRule。
 */
struct XtFeeRule {
    double open = 0.0;
    double close = 0.0;
    double close_today = 0.0;
    bool by_volume = false;

    double maker_rebate = 0.0;

    double stamp_duty_rate = 0.0;

    double transfer_fee_rate = 0.0;

    double min_commission = 0.0;

    double compute_fee(bool   is_open,
                       bool   is_today,
                       double qty,
                       double price,
                       double vol_scale  = 1.0,
                       bool   is_passive = false) const noexcept {
        const double rate = is_open ? open : (is_today ? close_today : close);

        double fee = by_volume ? qty * rate
                               : qty * price * vol_scale * rate;

        if (is_passive && maker_rebate > 0.0)
            fee -= by_volume ? qty * maker_rebate
                             : qty * price * vol_scale * maker_rebate;
        return fee;
    }
};

/**
 * @brief 手续费规则查询接口（fee rule provider）。
 *
 * 抽象费率查询接口，消费方（XtFillCostApplier / XtFillCostModel）
 * 通过此接口查询合约费率规则，无需依赖具体实现（XtFeeRuleBook）。
 *
 * 实现：`XtFeeRuleBook`（xt_config）。
 *
 * 调用频率：每笔成交（trade）至多 1 次 lookup_fee_rule()。
 * 路径：FillCostApplier::apply / xt_fill_cost::apply / normalize_trade_cost。
 *
 * 安全性前提（Safety invariant）：
 *   - 实现的 lookup_fee_rule 返回的指针生命周期与 provider 实例一致；
 *     调用方不可在 provider 销毁后继续访问返回值。
 *   - empty_rules() 用于快速短路（无规则时跳过 lookup）。
 */
class IFeeRuleProvider {
public:
    virtual ~IFeeRuleProvider() = default;

    [[nodiscard]] virtual bool empty_rules() const noexcept = 0;

    [[nodiscard]] virtual const XtFeeRule* lookup_fee_rule(const char* code) const noexcept = 0;
};
