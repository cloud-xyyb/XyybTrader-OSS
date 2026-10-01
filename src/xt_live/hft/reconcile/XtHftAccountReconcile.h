#pragma once

/// @file
/// @brief 停机账户对账入口。
///
/// runner 停机后，run_account_reconcile_on_stop() 拿本地账本快照跟 broker
/// 最后账户快照直比，顺带审计冻结资金与在途单是否对得上，输出一份报告。
/// 只报告差异，不重放、不自动修复。
/// pick_broker_balance() 是 broker 侧余额来源选择的纯决策，供单测。

#include <cstddef>
#include <cstdint>

class ITradeGateway;
struct XtRunnerControlConfig;
struct XtHftRunnerControlContext;

namespace xt_live::control {

/// @brief 停机对账 broker 侧余额来源选择结果（纯决策，供单测）。
struct BrokerBalancePick {
    bool     has = false;       ///< false = 双源皆无 → broker 面默认通过
    bool     fresh = false;     ///< true = 停机时刻 fresh REST；false = 最后 anchor 缓存
    double   balance = 0.0;
    uint64_t snapshot_ms = 0;
};

/// @brief fresh-first：gateway 支持且拉取成功用停机时刻 fresh 余额，
///        否则回退最后 anchor 缓存（消除 stale-anchor 尾部 fill 结构性假 FAIL）。
/// @param gw nullable；null 或 query_wallet_balance_sync 失败时走缓存回退。
BrokerBalancePick pick_broker_balance(ITradeGateway* gw,
                                      bool cached_present,
                                      double cached_balance,
                                      uint64_t cached_ms,
                                      uint64_t now_ms);

/// @brief 「gateway 交付投影 open 集合 vs 账户订单冻结 ledger」差集观测结果
///        （observe-only，不影响对账 PASS/FAIL；纯决策，供单测）。
struct LedgerOpenObservation {
    bool present = false;        ///< gateway 是否提供交付投影 open 视图
    std::size_t open_count = 0;  ///< 已登记未终态订单数
    /// open 集为空而账户仍持订单冻结 → trade-only 终态漏收尾的 stale frozen
    /// 候选（反向「open 有单而冻结为 0」是平仓单不冻结的正常形态，不标记）。
    bool stale_frozen_suspect = false;
};

/// @brief 差集观测纯决策（输入 = gateway open 视图 + frozen 审计聚合量）。
LedgerOpenObservation make_ledger_open_observation(bool supported,
                                                   std::size_t open_count,
                                                   double frozen_cash_by_orders);

/// @brief 停机时 runtime 账本快照 vs broker 最后快照直比 + frozen 审计，输出报告。
/// @pre event loop worker 已 join（调用方在 stop_runtime_threads 之后调用）；
///      frozen_audit_report 直读 writer state，仅在 account_store 无写者后安全。
void run_account_reconcile_on_stop(const XtRunnerControlConfig& cfg,
                                    const XtHftRunnerControlContext& ctx);

}  // namespace xt_live::control
