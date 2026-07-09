#include "BinanceIncomeProvider.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "BinanceRestClient.h"
#include "td/BinanceTdCfg.h"
#include "XtFixedPoint.h"
#include "XtLogger.h"

namespace {

const std::vector<std::string>& external_income_query_types() {

    static const std::vector<std::string> kTypes = {
        "TRANSFER",
        "INTERNAL_TRANSFER",
        "WELCOME_BONUS",
        "CROSS_COLLATERAL_TRANSFER",
        "COIN_SWAP_DEPOSIT",
        "COIN_SWAP_WITHDRAW",
        "STRATEGY_UMFUTURES_TRANSFER",
        "CONTEST_REWARD",
        "BFUSD_REWARD",
    };
    return kTypes;
}

}

namespace binance_u {

BinanceAccountTruthSource::IncomeProvider make_binance_income_provider(const BinanceTdCfg& cfg) {
    if (cfg.api_key.empty() || cfg.api_secret.empty() || cfg.base_rest_url.empty()) {
        return {};
    }

    BinanceRestClient::Config rcfg;
    rcfg.base_url = cfg.base_rest_url;
    rcfg.api_key = cfg.api_key;
    rcfg.api_secret = cfg.api_secret;
    rcfg.recv_window_ms = cfg.recv_window_ms;

    rcfg.timeout_sec = 3;
    auto rest = std::make_shared<BinanceRestClient>(std::move(rcfg));
    (void)rest->sync_server_time();

    return [rest](int64_t, uint64_t start_ms, uint64_t end_ms) {
        std::vector<BinanceAccountTruthSource::IncomeRecord> out;
        constexpr int kLimit = 1000;
        constexpr int kMaxPagesPerType = 10;

        constexpr int64_t kRefreshBudgetMs = 5000;
        const auto t_start = std::chrono::steady_clock::now();
        const auto over_budget = [&t_start, kRefreshBudgetMs]() {
            return std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - t_start).count() >= kRefreshBudgetMs;
        };

        for (const std::string& income_type : external_income_query_types()) {
            if (over_budget()) {
                XtLogger::warn_m("RT",
                                 "binance income refresh hit {}ms budget; returning partial "
                                 "(guards watchdog)", kRefreshBudgetMs);
                break;
            }
            uint64_t page_start = start_ms;
            for (int page = 0; page < kMaxPagesPerType && page_start <= end_ms; ++page) {
                if (over_budget()) break;
                std::map<std::string, std::string> params;
                params["incomeType"] = income_type;
                params["startTime"] = std::to_string(page_start);
                params["endTime"] = std::to_string(end_ms);
                params["limit"] = std::to_string(kLimit);

                const auto resp = rest->get_signed("/fapi/v1/income", std::move(params));
                if (!resp.ok()) {
                    XtLogger::warn_m("RT",
                                     "binance income poll failed type={} http_status={} err={}",
                                     income_type, resp.http_status, resp.curl_err);
                    break;
                }

                nlohmann::json rows;
                try {
                    rows = nlohmann::json::parse(resp.body);
                } catch (const std::exception& e) {
                    XtLogger::warn_m("RT", "binance income parse failed type={} err={}",
                                     income_type, e.what());
                    break;
                }
                if (!rows.is_array()) break;

                uint64_t max_seen_ms = 0;
                for (const auto& row : rows) {

                    BinanceAccountTruthSource::IncomeRecord record;
                    record.income_type = row.value("incomeType", income_type);
                    record.asset = row.value("asset", "USDT");
                    const std::string income = row.value("income", "0");
                    try {
                        record.income_fp = xt_fp::to_fp(std::stod(income));
                    } catch (const std::exception&) {
                        record.income_fp = 0;
                    }
                    record.time_ms = row.value("time", 0ULL);
                    max_seen_ms = std::max(max_seen_ms, record.time_ms);
                    out.push_back(std::move(record));
                }

                if (rows.size() < static_cast<size_t>(kLimit) || max_seen_ms + 1 <= page_start) {
                    break;
                }

                page_start = max_seen_ms + 1;
            }
        }
        return out;
    };
}

}
