#pragma once

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "xt_account/api/IAccountTruthSink.h"
#include "xt_account/api/IAccountTruthSource.h"

/// @brief Binance account truth 的 source（读）+ sink（写）实现。
///
/// 面向 live control、report 和 metrics 输出 Binance 口径的 AccountTruth。
/// 账本字段来自注入的 SnapshotProvider（AccountStateStore 聚合）；Source 自身只维护
/// Binance broker overlay、成交手续费和 income cashflow 的 day-scoped 缓存，
/// fetch_snapshot() 把两者合并后按值返回，不写本地 reducer 状态。
/// 内部可变状态由 mutex 保护。
class BinanceAccountTruthSource final : public IAccountTruthSource, public IAccountTruthSink {
public:

    using SnapshotProvider = std::function<AccountTruth()>;

    /// @brief Binance income row；用于识别当日外部现金流。
    struct IncomeRecord {
        std::string income_type;
        std::string asset;
        int64_t income_fp = 0;
        uint64_t time_ms = 0;
    };

    using IncomeProvider = std::function<std::vector<IncomeRecord>(
        int64_t trading_day_id,
        uint64_t start_ms,
        uint64_t end_ms)>;

    /// @brief Binance income provider 轮询节流配置。
    struct IncomePollConfig {
        uint64_t poll_interval_ms = 60'000;
    };

    explicit BinanceAccountTruthSource(SnapshotProvider provider = SnapshotProvider{});

    BinanceAccountTruthSource(SnapshotProvider provider, IncomeProvider income_provider);

    BinanceAccountTruthSource(SnapshotProvider provider,
                              IncomeProvider income_provider,
                              IncomePollConfig income_poll);

    void set_income_provider(IncomeProvider provider);

    void poll() override;

    AccountTruth fetch_snapshot() override;

    void on_trading_day_rollover(int64_t prev_day, int64_t new_day) override;

    void update_broker_overlay(const BrokerVenueTruth& bt) override;

    void note_fill_fee_fp(int64_t fee_fp, uint64_t ts_ns) override;

    static bool is_external_cashflow_income_type(const std::string& income_type);

private:
    static int64_t derive_trading_day_id(uint64_t ts_ns);
    static uint64_t trading_day_start_ms(int64_t trading_day_id);
    static uint64_t now_ms();

    void maybe_refresh_external_cashflow(int64_t trading_day_id, uint64_t ts_ns);

    mutable std::mutex mu_;
    BrokerVenueTruth overlay_;
    SnapshotProvider provider_;
    IncomeProvider income_provider_;
    IncomePollConfig income_poll_;
    int64_t fees_today_fp_ = 0;
    int64_t fees_trading_day_id_ = 0;
    int64_t external_cashflow_today_fp_ = 0;
    int64_t external_cashflow_trading_day_id_ = 0;
    uint64_t last_income_poll_ms_ = 0;
};
