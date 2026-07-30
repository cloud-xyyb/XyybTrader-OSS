#include "xt_account/venue/binance/BinanceAccountTruthSource.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <set>
#include <utility>

#include "xt_core/session/XtTradingDay.h"
#include "xt_util/time/XtTime.h"

BinanceAccountTruthSource::BinanceAccountTruthSource(SnapshotProvider provider)
    : BinanceAccountTruthSource(std::move(provider), IncomeProvider{}, IncomePollConfig{}) {}

BinanceAccountTruthSource::BinanceAccountTruthSource(SnapshotProvider provider,
                                                     IncomeProvider income_provider)
    : BinanceAccountTruthSource(std::move(provider), std::move(income_provider), IncomePollConfig{}) {}

BinanceAccountTruthSource::BinanceAccountTruthSource(SnapshotProvider provider,
                                                     IncomeProvider income_provider,
                                                     IncomePollConfig income_poll)
    : provider_(std::move(provider)),
      income_provider_(std::move(income_provider)),
      income_poll_(income_poll) {}

void BinanceAccountTruthSource::update_broker_overlay(const BrokerVenueTruth& bt) {

    std::lock_guard<std::mutex> lock(mu_);
    overlay_ = bt;
}

void BinanceAccountTruthSource::note_fill_fee_fp(int64_t commission_fp, uint64_t ts_ns) {
    std::lock_guard<std::mutex> lock(mu_);
    const int64_t day = derive_trading_day_id(ts_ns);
    if (day > 0 && day != fees_trading_day_id_) {
        fees_today_fp_ = 0;
        fees_trading_day_id_ = day;
    }
    fees_today_fp_ += commission_fp;
}

void BinanceAccountTruthSource::set_income_provider(IncomeProvider provider) {
    std::lock_guard<std::mutex> lock(mu_);
    income_provider_ = std::move(provider);
    last_income_poll_ms_ = 0;
}

void BinanceAccountTruthSource::poll() {
    {

        std::lock_guard<std::mutex> lock(mu_);
        if (!income_provider_) return;
    }

    const AccountTruth base = provider_ ? provider_() : AccountTruth{};
    const int64_t trading_day =
        base.trading_day_id > 0 ? base.trading_day_id : derive_trading_day_id(base.ts_ns);
    maybe_refresh_external_cashflow(trading_day, base.ts_ns);
}

AccountTruth BinanceAccountTruthSource::fetch_snapshot() {
    AccountTruth out = provider_ ? provider_() : AccountTruth{};
    if (out.trading_day_id <= 0) out.trading_day_id = derive_trading_day_id(out.ts_ns);

    std::lock_guard<std::mutex> lock(mu_);
    out.available_balance_fp = overlay_.available_balance_fp;
    out.maint_margin_fp = overlay_.maint_margin_fp;
    out.total_margin_balance_fp = overlay_.total_margin_balance_fp;
    out.per_symbol.reserve(overlay_.per_symbol.size());
    for (const auto& pos : overlay_.per_symbol) {
        if (pos.symbol.empty()) continue;

        if (pos.qty_fp == 0 && pos.unrealized_pnl_fp == 0 && pos.margin_fp == 0) continue;
        out.per_symbol.push_back(pos);
    }
    out.per_symbol_is_broker_truth = true;

    out.external_cashflow_today_fp =
        external_cashflow_trading_day_id_ > 0 ? external_cashflow_today_fp_ : 0;

    out.fees_today_fp = (fees_trading_day_id_ == 0 || fees_trading_day_id_ == out.trading_day_id)
        ? fees_today_fp_ : 0;
    return out;
}

int64_t BinanceAccountTruthSource::derive_trading_day_id(uint64_t ts_ns) {
    if (ts_ns == 0) return 0;
    const int64_t epoch_ms = static_cast<int64_t>(ts_ns / 1'000'000ULL);
    return static_cast<int64_t>(
        xt_core::derive_trading_day(epoch_ms, xt_core::TimezonePolicy::UtcNaturalDay));
}

void BinanceAccountTruthSource::on_trading_day_rollover(int64_t, int64_t new_day) {
    std::lock_guard<std::mutex> lock(mu_);
    fees_today_fp_ = 0;
    fees_trading_day_id_ = new_day;
    external_cashflow_today_fp_ = 0;
    external_cashflow_trading_day_id_ = new_day;
    last_income_poll_ms_ = 0;
}

bool BinanceAccountTruthSource::is_external_cashflow_income_type(const std::string& income_type) {
    static const std::set<std::string> kTypes = {
        "TRANSFER",
        "INTERNAL_TRANSFER",
        "WELCOME_BONUS",
        "CROSS_COLLATERAL_TRANSFER",
        "COIN_SWAP_DEPOSIT",
        "COIN_SWAP_WITHDRAW",
        "STRATEGY_UMFUTURES_TRANSFER",
        "TEST_NET_FUND",
        "CONTEST_REWARD",
        "BFUSD_REWARD",
    };
    return kTypes.find(income_type) != kTypes.end();
}

uint64_t BinanceAccountTruthSource::trading_day_start_ms(int64_t trading_day) {
    if (trading_day <= 0) return 0;
    const int y = static_cast<int>(trading_day / 10000);
    const unsigned m = static_cast<unsigned>((trading_day / 100) % 100);
    const unsigned d = static_cast<unsigned>(trading_day % 100);
    if (y <= 1970 || m < 1 || m > 12 || d < 1 || d > 31) return 0;
    return static_cast<uint64_t>(xt_time::days_from_civil(y, m, d)) * 86'400'000ULL;
}

uint64_t BinanceAccountTruthSource::now_ms() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
}

void BinanceAccountTruthSource::maybe_refresh_external_cashflow(int64_t trading_day, uint64_t ts_ns) {
    IncomeProvider provider;
    IncomePollConfig poll;
    const uint64_t snapshot_ms = ts_ns > 0 ? static_cast<uint64_t>(ts_ns / 1'000'000ULL) : now_ms();
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!income_provider_ || trading_day <= 0) return;
        if (external_cashflow_trading_day_id_ != trading_day) {
            external_cashflow_today_fp_ = 0;
            external_cashflow_trading_day_id_ = trading_day;
            last_income_poll_ms_ = 0;
        }
        const uint64_t interval = income_poll_.poll_interval_ms;
        if (last_income_poll_ms_ != 0 &&
            snapshot_ms >= last_income_poll_ms_ &&
            snapshot_ms - last_income_poll_ms_ < interval) {
            return;
        }
        last_income_poll_ms_ = snapshot_ms;

        provider = income_provider_;
        poll = income_poll_;
    }

    (void)poll;
    const uint64_t start_ms = trading_day_start_ms(trading_day);
    if (start_ms == 0) return;

    int64_t sum_fp = 0;
    try {
        const auto rows = provider(trading_day, start_ms, std::max(start_ms, snapshot_ms));
        for (const auto& row : rows) {
            if (!row.asset.empty() && row.asset != "USDT") continue;
            if (!is_external_cashflow_income_type(row.income_type)) continue;
            sum_fp += row.income_fp;
        }
    } catch (const std::exception&) {

        return;
    } catch (...) {

        return;
    }

    std::lock_guard<std::mutex> lock(mu_);
    if (external_cashflow_trading_day_id_ != trading_day) return;
    external_cashflow_today_fp_ = sum_fp;
}
