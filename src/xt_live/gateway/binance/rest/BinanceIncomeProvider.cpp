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
    // 只查询外部资金流类型；交易盈亏/手续费等由订单和账户链路自身归因。
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

}  // namespace

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
    // income provider 在主控制循环里同步刷新；短超时配合下方总预算，保证失败的
    // REST 调用不会长期占用 watchdog 主帧。
    rcfg.timeout_sec = 3;
    auto rest = std::make_shared<BinanceRestClient>(std::move(rcfg));
    (void)rest->sync_server_time();

    return [rest](int64_t, uint64_t start_ms, uint64_t end_ms) {
        std::vector<BinanceAccountTruthSource::IncomeRecord> out;
        constexpr int kLimit = 1000;
        constexpr int kMaxPagesPerType = 10;
        // 总壁钟预算：预算耗尽时返回已取到的部分结果。income 是外部资金流遥测，
        // best-effort 即可；下一轮刷新会继续补齐。
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
                    // Binance income 金额以字符串返回；解析失败时按 0 记录，保留时间和类型用于排查。
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
                // 下一页从已见最大时间 + 1ms 开始，避免重复消费同一批 income row。
                page_start = max_seen_ms + 1;
            }
        }
        return out;
    };
}

}  // namespace binance_u
