#include "xt_account/venue/ctp/CtpAccountTruthSource.h"

#include <utility>

#include "xt_core/session/XtTradingDay.h"

CtpAccountTruthSource::CtpAccountTruthSource(SnapshotProvider provider)
    : provider_(std::move(provider)) {}

AccountTruth CtpAccountTruthSource::fetch_snapshot() {
    AccountTruth out = provider_ ? provider_() : AccountTruth{};
    if (out.trading_day_id <= 0) out.trading_day_id = derive_trading_day_id(out.ts_ns);
    // provider(account_store 聚合)不含 CTP broker Available/Balance(网关
    // OnRspQryTradingAccount 读到的字段)；从 update_broker_overlay 写入的 overlay 补
    // available_balance_fp + total_margin_balance_fp(broker 动态权益，供
    // xt_venue_equity_raw cross-check)。锁下:overlay 写走 CTP SPI 线程, fetch 走 1Hz
    // 控制线程。per_symbol 已由 provider 用 account_store 活值填(store-mirror,
    // per_symbol_is_broker_truth=false)，此处不动。
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
    // CTP 网关 OnRspQryTradingAccount 经 on_broker_account_truth callback 转发到此，
    // 补 provider(account_store 聚合)不含的 broker Available + Balance(total)。
    // 锁下:转发走 CTP SPI 线程，fetch_snapshot 走 1Hz 控制线程。
    std::lock_guard<std::mutex> lock(mu_);
    overlay_available_fp_ = bt.available_balance_fp;
    overlay_total_fp_ = bt.total_margin_balance_fp;
}
