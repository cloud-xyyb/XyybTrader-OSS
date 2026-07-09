#pragma once

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "xt_account/api/IAccountTruthSource.h"

/// @brief Binance account truth 的读侧 source。
///
/// 面向 live control、report 和 metrics 输出 Binance 口径的 AccountTruth。
/// Source 维护 Binance broker overlay、成交手续费和 income cashflow 的 day-scoped 缓存；
/// fetch_snapshot() 按值返回快照，不写本地 reducer 状态。
/// 内部可变状态由 mutex 保护。
class BinanceAccountTruthSource final : public IAccountTruthSource {
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

    /// @brief Binance per-symbol position truth。
    struct Position {
        std::string symbol;
        int64_t qty_fp = 0;
        int64_t mark_price_fp = 0;
        int64_t avg_price_fp = 0;
        int64_t unrealized_pnl_fp = 0;
        int64_t margin_fp = 0;
    };

    /// @brief Binance account/detail 缓存快照。
    struct Snapshot {
        int64_t wallet_balance_fp = 0;
        int64_t unrealized_pnl_fp = 0;
        int64_t external_cashflow_today_fp = 0;
        uint64_t ts_ns = 0;
        int64_t trading_day_id = 0;
        int64_t available_balance_fp = 0;
        int64_t maint_margin_fp = 0;
        int64_t total_margin_balance_fp = 0;
        std::vector<Position> positions;
    };

    BinanceAccountTruthSource();

    explicit BinanceAccountTruthSource(SnapshotProvider provider);

    BinanceAccountTruthSource(SnapshotProvider provider, IncomeProvider income_provider);

    BinanceAccountTruthSource(SnapshotProvider provider,
                              IncomeProvider income_provider,
                              IncomePollConfig income_poll);

    void update_snapshot(const Snapshot& snapshot);

    void record_commission_fp(int64_t commission_fp, uint64_t ts_ns);

    void set_income_provider(IncomeProvider provider);

    AccountTruth fetch_snapshot() override;

    void on_trading_day_rollover(int64_t prev_day, int64_t new_day) override;

    void update_broker_overlay(const BrokerVenueTruth& bt) override;

    void note_fill_fee_fp(int64_t fee_fp, uint64_t ts_ns) override { record_commission_fp(fee_fp, ts_ns); }

    static bool is_external_cashflow_income_type(const std::string& income_type);

private:
    static int64_t derive_trading_day_id(uint64_t ts_ns);
    static int64_t position_value_fp(const std::vector<Position>& positions);
    static uint64_t trading_day_start_ms(int64_t trading_day_id);
    static uint64_t now_ms();

    int64_t cached_external_cashflow_fp(int64_t trading_day_id) const;
    void maybe_refresh_external_cashflow(int64_t trading_day_id, uint64_t ts_ns);

    mutable std::mutex mu_;
    Snapshot snapshot_;
    SnapshotProvider provider_;
    IncomeProvider income_provider_;
    IncomePollConfig income_poll_;
    int64_t fees_today_fp_ = 0;
    int64_t fees_trading_day_id_ = 0;
    int64_t external_cashflow_today_fp_ = 0;
    int64_t external_cashflow_trading_day_id_ = 0;
    uint64_t last_income_poll_ms_ = 0;
};
