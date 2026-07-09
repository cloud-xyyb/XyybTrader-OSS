#include "xt_account/venue/generic/StoreBackedAccountTruthSource.h"

#include <utility>

StoreBackedAccountTruthSource::StoreBackedAccountTruthSource(
    SnapshotProvider provider,
    xt_core::TimezonePolicy trading_day_policy)
    : provider_(std::move(provider)),
      trading_day_policy_(trading_day_policy) {}

AccountTruth StoreBackedAccountTruthSource::fetch_snapshot() {
    AccountTruth out = provider_ ? provider_() : AccountTruth{};
    if (out.trading_day_id <= 0) out.trading_day_id = derive_trading_day_id(out.ts_ns);

    std::lock_guard<std::mutex> lock(mu_);
    out.available_balance_fp = overlay_.available_balance_fp;
    out.maint_margin_fp = overlay_.maint_margin_fp;
    out.total_margin_balance_fp = overlay_.total_margin_balance_fp;
    if (overlay_.per_symbol_is_broker_truth) {
        out.per_symbol = overlay_.per_symbol;
        out.per_symbol_is_broker_truth = true;
    }
    return out;
}

int64_t StoreBackedAccountTruthSource::derive_trading_day_id(uint64_t ts_ns) const {
    if (ts_ns == 0) return 0;
    const int64_t epoch_ms = static_cast<int64_t>(ts_ns / 1'000'000ULL);
    return static_cast<int64_t>(xt_core::derive_trading_day(epoch_ms, trading_day_policy_));
}

void StoreBackedAccountTruthSource::update_broker_overlay(const BrokerVenueTruth& bt) {
    std::lock_guard<std::mutex> lock(mu_);
    overlay_ = bt;
}
