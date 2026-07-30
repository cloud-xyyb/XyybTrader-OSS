#include "xt_live/common/AccountTruthSourceFactory.h"

#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "XtLiveConfig.h"
#include "xt_account/venue/AccountTruthStoreProvider.h"
#include "xt_account/venue/ctp/CtpAccountTruthSource.h"
#include "xt_account/venue/generic/StoreBackedAccountTruthSource.h"
#include "xt_core/session/XtTradingDay.h"

#include "xt_live/common/VenueTraits.h"

namespace {

template <typename T>
RunnerAccountTruth make_ports(std::unique_ptr<T> impl) {
    RunnerAccountTruth out;
    out.sink = static_cast<IAccountTruthSink*>(impl.get());
    out.source = std::move(impl);
    return out;
}

}

RunnerAccountTruth make_runner_account_truth_source(
    const xt_account::AccountStateStore& account_store,
    XtRunnerSignals& signals,
    const XtLiveConfig& cfg,
    BinanceAccountTruthSource::IncomeProvider income_provider) {
    const bool is_binance = xt_live::common::provider_is_binance(cfg.parsed.td_provider);
    const bool is_ctp = xt_live::common::provider_is_ctp(cfg.parsed.td_provider);
    const bool is_store_backed = xt_live::common::provider_is_xtp(cfg.parsed.td_provider);
    if (!is_binance && !is_ctp && !is_store_backed) {
        throw std::invalid_argument(
            "unsupported td_provider for account truth source: " + cfg.parsed.td_provider);
    }

    const xt_core::TimezonePolicy trading_day_policy =
        is_binance
            ? xt_core::TimezonePolicy::UtcNaturalDay
            : (is_ctp ? xt_core::TimezonePolicy::ChinaFuturesNightSession
                      : xt_core::TimezonePolicy::AsiaShanghaiNaturalDay);
    const bool fill_store_per_symbol = !is_binance;
    auto provider = [&account_store, &signals, trading_day_policy, fill_store_per_symbol]() {
        return xt_account::read_store_account_truth(
            account_store, signals, trading_day_policy, fill_store_per_symbol);
    };
    if (is_binance) {

        return make_ports(std::make_unique<BinanceAccountTruthSource>(
            std::move(provider),
            std::move(income_provider)));
    }
    if (is_ctp) {
        return make_ports(std::make_unique<CtpAccountTruthSource>(std::move(provider)));
    }
    return make_ports(std::make_unique<StoreBackedAccountTruthSource>(
        std::move(provider),
        trading_day_policy));
}
