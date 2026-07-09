#include "xt_account/venue/ctp/CtpAccountTruthSource.h"

#include <utility>

#include "xt_core/session/XtTradingDay.h"

CtpAccountTruthSource::CtpAccountTruthSource(SnapshotProvider provider)
    : provider_(std::move(provider)) {}

AccountTruth CtpAccountTruthSource::fetch_snapshot() {
    AccountTruth out = provider_ ? provider_() : AccountTruth{};
    if (out.trading_day_id <= 0) out.trading_day_id = derive_trading_day_id(out.ts_ns);

    std::lock_guard<std::mutex> lock(mu_);
    out.available_balance_fp = overlay_available_fp_;
    out.total_margin_balance_fp = overlay_total_fp_;
    return out;
}

int64_t CtpAccountTruthSource::derive_trading_day_id(uint64_t ts_ns) {
    if (ts_ns == 0) return 0;
    const int64_t epoch_ms = static_cast<int64_t>(ts_ns / 1'000'000ULL);
    return static_cast<int64_t>(
        xt_core::derive_trading_day(epoch_ms, xt_core::TimezonePolicy::ChinaFuturesNightSession));
}

void CtpAccountTruthSource::update_broker_overlay(const BrokerVenueTruth& bt) {

    std::lock_guard<std::mutex> lock(mu_);
    overlay_available_fp_ = bt.available_balance_fp;
    overlay_maint_fp_ = bt.maint_margin_fp;
    overlay_total_fp_ = bt.total_margin_balance_fp;
}
