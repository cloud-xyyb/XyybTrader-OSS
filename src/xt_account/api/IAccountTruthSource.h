#pragma once

/// @file
/// @brief 账户真值（account truth）读侧接口和跨 venue 通用 DTO。
///
/// Source 负责把 venue、AccountStateStore provider 或测试数据转换为 AccountTruth；
/// 调用方只消费快照字段，不通过该接口修改 reducer 状态。
/// venue 回报灌入（broker overlay / 成交手续费）走写侧 IAccountTruthSink。

#include <cstdint>
#include <string>
#include <vector>

#include "api/BrokerVenueTruth.h"

/// @brief 跨 venue 的账户真值快照。
///
/// 由 IAccountTruthSource 按值返回，供 live control、report 和 metrics 消费。
/// 金额字段均为 fixed-point(1e6)，ts_ns 为 epoch nanoseconds，
/// trading_day_id 为 YYYYMMDD。venue 无法提供的字段保持 0；调用方不能仅凭
/// 0 区分真实零值和 unknown/unavailable。
struct AccountTruth {
    int64_t  wallet_balance_fp = 0;
    int64_t  position_value_fp = 0;
    int64_t  unrealized_pnl_fp = 0;
    int64_t  fees_today_fp = 0;
    int64_t  external_cashflow_today_fp = 0;
    uint64_t ts_ns = 0;
    int64_t  trading_day_id = 0;
    int64_t  available_balance_fp = 0;
    int64_t  maint_margin_fp = 0;
    int64_t  total_margin_balance_fp = 0;
    std::vector<PerSymbolTruth> per_symbol;
    bool per_symbol_is_broker_truth = false;

    std::vector<std::pair<std::string, int64_t>> runtime_per_symbol_marks_fp;
};

/// @brief account truth 的读侧 source 接口。
///
/// 实现把 venue、AccountStateStore provider 或测试数据转换成 AccountTruth。
/// 调用方通过 fetch_snapshot() 按值读取，并用 rollover 入口同步 source 自己维护的
/// day-scoped 状态；本接口不修改本地 reducer 状态，也不接受 venue 回报灌入
/// （broker overlay / 成交手续费在写侧 IAccountTruthSink）。
class IAccountTruthSource {
public:
    virtual ~IAccountTruthSource() = default;

    virtual void poll() {}

    [[nodiscard]] virtual AccountTruth fetch_snapshot() = 0;

    virtual void on_trading_day_rollover(int64_t prev_day, int64_t new_day) = 0;
};

inline int64_t account_truth_equity_fp(const AccountTruth& truth) noexcept {
    return truth.wallet_balance_fp + truth.unrealized_pnl_fp;
}

inline int64_t account_truth_today_pnl_fp(const AccountTruth& current,
                                          int64_t open_equity_fp,
                                          int64_t open_external_cashflow_fp = 0) noexcept {
    return account_truth_equity_fp(current) - open_equity_fp -
           (current.external_cashflow_today_fp - open_external_cashflow_fp);
}
