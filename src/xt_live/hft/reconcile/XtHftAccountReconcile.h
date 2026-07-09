#pragma once

/// @file
/// @brief 停机账户对账入口。
///
/// runner 停机后，run_account_reconcile_on_stop() 拿本地账本快照跟 broker
/// 最后账户快照直比，顺带审计冻结资金与在途单是否对得上，输出一份报告。
/// 只报告差异，不重放、不自动修复。
/// pick_broker_balance() 是 broker 侧余额来源选择的纯决策，供单测。

#include <cstdint>

class ITradeGateway;
struct XtRunnerControlConfig;
struct XtHftRunnerControlContext;

namespace xt_live::control {

/// @brief 停机对账 broker 侧余额来源选择结果（纯决策，供单测）。
struct BrokerBalancePick {
    bool     has = false;
    bool     fresh = false;
    double   balance = 0.0;
    uint64_t snapshot_ms = 0;
};

BrokerBalancePick pick_broker_balance(ITradeGateway* gw,
                                      bool cached_present,
                                      double cached_balance,
                                      uint64_t cached_ms,
                                      uint64_t now_ms);

void run_account_reconcile_on_stop(const XtRunnerControlConfig& cfg,
                                    const XtHftRunnerControlContext& ctx);

}
