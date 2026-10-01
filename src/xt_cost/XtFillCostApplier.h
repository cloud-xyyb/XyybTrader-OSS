#pragma once

#include <vector>

#include "XtTradeCostConfig.h"
#include "api/IFeeRuleProvider.h"

class XtCommodityCatalog;
struct XtTradeRecord;

namespace xt_fill_cost {

/// @brief 为成交记录补齐执行成本字段。
///
/// 调用方先用 `init()` 注入滑点配置、费率规则和合约规格；之后通过 `apply()`
/// 对单笔或批量 `XtTradeRecord` 原地写入 `exec_price`、`fee` 和 `slippage`。
/// 费率与合约规格来源为 non-owning，可为空；为空或未命中时按零手续费 / `vol_scale=1.0` 处理。
class FillCostApplier {
public:
    FillCostApplier() = default;

    /// @brief 初始化成本模型依赖。
    /// @param cost         synthetic slippage 配置快照；按买卖方向调整 exec_price/slippage。
    /// @param fees         手续费规则来源；non-owning，按成交合约查询费率，nullptr 表示 fee=0。
    /// @param commodities  合约规格来源；non-owning，提供 vol_scale 与 per-symbol slippage
    ///                     覆盖（非零时优先于 cost.slippage），nullptr 或未命中按 vol_scale=1.0、全局滑点。
    void init(XtTradeCostConfig cost, const IFeeRuleProvider* fees, const XtCommodityCatalog* commodities);

    /// @brief 对成交列表原地应用 fee/slippage。
    void apply(std::vector<XtTradeRecord>& trades) const;

    /// @brief 对单笔成交原地应用 fee/slippage。
    void apply(XtTradeRecord& trade) const;

private:
    XtTradeCostConfig cost_; ///< 全局成交滑点模型；slippage 为绝对价格偏移，买单加、卖单减；可被品种级 slippage 覆盖。
    const IFeeRuleProvider* fees_ = nullptr; ///< 成交手续费规则入口；查 open/close/close_today 与 maker_rebate，空时 fee=0。
    const XtCommodityCatalog* commodities_ = nullptr; ///< 合约规格入口；查 vol_scale 计算 by-notional fee/rebate，空或未命中按 1.0。
};

} // namespace xt_fill_cost
