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
    double open = 0.0;         ///< 开仓费率（open fee rate）
    double close = 0.0;        ///< 平仓费率（close fee rate）
    double close_today = 0.0;  ///< 平今仓费率（close-today fee rate）
    bool by_volume = false;    ///< 计费方式（true=按手数 per-volume，false=按金额 by-notional）

    // Maker 返佣（maker rebate）。
    // 当成交方向为被动单（XtTradeRecord::is_passive == true）时，
    // FillCostApplier 在标准费率基础上减去该值：
    //   最终手续费 = 标准费（open/close/close_today）- maker_rebate
    // 单位与 open/close 一致（by_volume=true 时：元/手；false 时：元/元面值）。
    // 国内期货交易所通常无 maker 返佣，默认为 0；
    // 部分期权或境外品种可设为正值以反映真实返佣。
    double maker_rebate = 0.0;

    // ── A 股专属费用项(venue-neutral，default 0 期货/加密兼容）──
    /// @brief 印花税率（仅卖向收，A 股 0.0005 = 0.05%）。期货/加密 = 0。
    /// 计算口径：rate × notional（同 by_notional），不依赖 by_volume flag。
    double stamp_duty_rate = 0.0;
    /// @brief 过户费率（沪市 A 股 0.00001 = 0.001%，深市 0）。
    /// 计算口径：rate × notional，买卖双向都收。
    double transfer_fee_rate = 0.0;
    /// @brief 单笔最低佣金（A 股券商通常 5 元；期货/加密 = 0 不生效）。
    /// 计算口径：commission < min_commission → fee 上调到 min_commission。
    /// 仅作用于"佣金"部分（open/close base fee），不影响 stamp_duty / transfer_fee。
    double min_commission = 0.0;

    // 计算单笔手续费：统一封装 by_volume / vol_scale / maker_rebate 逻辑，
    // 避免各使用方（XtFillCostModel / AccountReducer 等）各自重复实现。
    //   is_open   : true = 开仓，false = 平仓
    //   is_today  : true = 平今仓（适用 close_today 费率）
    //   qty       : 成交手数
    //   price     : 成交价
    //   vol_scale : 合约乘数（by_volume=false 时参与金额计算，期货按金额计费必须传入）
    //               默认 1.0，适用于股票或测试场景
    //   is_passive: true = maker 被动单（享受 maker_rebate 返佣）
    //
    // 精度边界（fp 约定）：返回 double 费率绝对值。项目货币口径为 int64 定点（×1e6），
    // double→fp 截断/舍入由调用方（AccountReducer / XtFillCostApplier）在折算入账时
    // 统一完成（单次跨越浮点边界）；A 股 min_commission/stamp_duty 等阈值比较应在
    // 调用方按定点口径裁定，避免在 double 层比 "fee >= 5.00 元" 受 epsilon 影响。
    double compute_fee(bool   is_open,
                       bool   is_today,
                       double qty,
                       double price,
                       double vol_scale  = 1.0,
                       bool   is_passive = false) const noexcept {
        const double rate = is_open ? open : (is_today ? close_today : close);
        // by_volume=true：按手数计费（元/手），与合约面值无关
        // by_volume=false：按金额比例计费，名义价值 = price × qty × vol_scale
        double fee = by_volume ? qty * rate
                               : qty * price * vol_scale * rate;
        // maker_rebate follows the production fill-time model: any passive fill
        // may receive a rebate, and net fee may be negative.
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

    /**
     * @brief 是否无任何规则（fast-path skip）。
     * @return true 表示 lookup_fee_rule 一定返回 nullptr，可短路。
     */
    [[nodiscard]] virtual bool empty_rules() const noexcept = 0;

    /**
     * @brief 按合约码查询费率规则（exact + symbol-key 双层查找）。
     *
     * @param code  合约码（C-string，例如 "IF2406"、"AU2406"），可为 nullptr。
     * @return      命中规则的指针（生命周期由 provider 持有），未命中返回 nullptr。
     *
     * 实现约定（XtFeeRuleBook）：
     *   1. 先按 normalize_key（exact match）查找；
     *   2. 未命中则按 extract_symbol_key（去除合约月份后缀）查找；
     *   3. 仍未命中返回 nullptr。
     */
    [[nodiscard]] virtual const XtFeeRule* lookup_fee_rule(const char* code) const noexcept = 0;
};
