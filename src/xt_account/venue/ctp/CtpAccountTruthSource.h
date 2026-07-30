#pragma once

#include <cstdint>
#include <functional>
#include <mutex>

#include "xt_account/api/IAccountTruthSink.h"
#include "xt_account/api/IAccountTruthSource.h"

/// @brief CTP account truth 的 source（读）+ sink（写）实现。
///
/// 面向 live control、report 和 metrics 输出 CTP 口径的 AccountTruth。
/// Source 以 AccountStateStore provider 的聚合账本为主体，并维护 broker overlay
/// 中的 available / total margin balance。fetch_snapshot() 按值返回快照，
/// 不写本地 reducer 状态；内部 overlay 状态由 mutex 保护。
class CtpAccountTruthSource final : public IAccountTruthSource, public IAccountTruthSink {
public:

    using SnapshotProvider = std::function<AccountTruth()>;

    explicit CtpAccountTruthSource(SnapshotProvider provider = SnapshotProvider{});

    AccountTruth fetch_snapshot() override;

    void on_trading_day_rollover(int64_t prev_day, int64_t new_day) override {}

    void update_broker_overlay(const BrokerVenueTruth& bt) override;

private:
    static int64_t derive_trading_day_id(uint64_t ts_ns);

    mutable std::mutex mu_;
    int64_t overlay_available_fp_ = 0;
    int64_t overlay_total_fp_ = 0;
    SnapshotProvider provider_;
};
