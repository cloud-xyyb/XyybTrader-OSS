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
    /// @brief 返回已聚合 AccountTruth 的回调；trading_day_id<=0 时由本类补齐。
    using SnapshotProvider = std::function<AccountTruth()>;

    /// @brief 创建 CTP truth source。
    /// @param provider 聚合真值回调；调用方保证回调自身线程安全。
    explicit CtpAccountTruthSource(SnapshotProvider provider = SnapshotProvider{});

    /// @brief 读取账户真值：provider() 聚合 + broker overlay merge。
    /// @return AccountTruth；缺 trading_day_id 时按 ts_ns 推导。
    AccountTruth fetch_snapshot() override;
    /// @brief CTP day-scoped 累加（fees/cashflow）由 provider(account_store) 维护，
    ///        本 source 无 day-scoped 状态可重置。
    void on_trading_day_rollover(int64_t prev_day, int64_t new_day) override {}
    /// @brief 写入 broker venue-truth overlay。
    /// @note 只消费 available + total（broker Balance 动态权益）；CTP TD 不提供
    ///       maint_margin 与 per_symbol，两者被丢弃。
    void update_broker_overlay(const BrokerVenueTruth& bt) override;
    // note_fill_fee_fp: CTP fees 以 provider(account_store)为真值，沿用基类 no-op。

private:
    static int64_t derive_trading_day_id(uint64_t ts_ns);

    mutable std::mutex mu_;
    int64_t overlay_available_fp_ = 0;  ///< broker Available；由 update_broker_overlay 写，mu_ 保护。
    int64_t overlay_total_fp_ = 0;      ///< broker Balance 动态权益；fetch 透传作 raw equity。
    SnapshotProvider provider_;         ///< 聚合真值回调；fetch_snapshot() 在锁外调用。
};
