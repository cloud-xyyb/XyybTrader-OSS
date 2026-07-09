#pragma once

#include <cstdint>
#include <functional>
#include <mutex>

#include "xt_account/api/IAccountTruthSource.h"
#include "xt_core/session/XtTradingDay.h"

/// @brief AccountStateStore-backed account truth 的通用读侧 source。
///
/// 面向没有专用 broker day-policy 的 venue，输出由 store 聚合出的 AccountTruth。
/// Source 只维护 broker overlay 和 trading-day fallback policy。fetch_snapshot()
/// 按值返回快照，不写本地 reducer 状态；内部 overlay 状态由 mutex 保护。
class StoreBackedAccountTruthSource final : public IAccountTruthSource {
public:
    using SnapshotProvider = std::function<AccountTruth()>;

    explicit StoreBackedAccountTruthSource(
        SnapshotProvider provider = SnapshotProvider{},
        xt_core::TimezonePolicy trading_day_policy =
            xt_core::TimezonePolicy::AsiaShanghaiNaturalDay);

    AccountTruth fetch_snapshot() override;
    void on_trading_day_rollover(int64_t, int64_t) override {}
    void update_broker_overlay(const BrokerVenueTruth& bt) override;

private:
    int64_t derive_trading_day_id(uint64_t ts_ns) const;

    mutable std::mutex mu_;
    BrokerVenueTruth overlay_;
    SnapshotProvider provider_;
    xt_core::TimezonePolicy trading_day_policy_;
};
