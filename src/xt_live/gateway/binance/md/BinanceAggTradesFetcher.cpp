#include "BinanceAggTradesFetcher.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <sstream>

#include <nlohmann/json.hpp>

#include "rest/BinancePublicHttp.h"

namespace binance_u {

namespace {

std::string to_upper(std::string_view s) {
    std::string out(s);
    std::transform(out.begin(), out.end(), out.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return out;
}

}  // namespace

bool parse_agg_trades_response(const std::string& body,
                               std::vector<AggTrade>& out,
                               std::string* err) {
    auto j = nlohmann::json::parse(body, nullptr, /*allow_exceptions=*/false);
    if (j.is_discarded()) {
        if (err) *err = "binance aggTrades response: invalid json";
        return false;
    }
    if (!j.is_array()) {
        // binance 错误体是 object {"code":..,"msg":..}；非数组即失败。
        if (err) *err = "binance aggTrades response: not a json array: " + body;
        return false;
    }
    out.clear();
    out.reserve(j.size());
    for (const auto& e : j) {
        if (!e.is_object() || !e.contains("a")) {
            if (err) *err = "binance aggTrades response: element missing 'a'";
            return false;
        }
        try {
            AggTrade t;
            t.a = e["a"].get<int64_t>();
            t.q = std::stod(e.at("q").get<std::string>());
            t.p = std::stod(e.at("p").get<std::string>());
            // P15：非有限价/量 = 坏页，整页按解析失败（caller 已按失败退避）。
            if (!std::isfinite(t.p) || !std::isfinite(t.q)) {
                if (err) *err = "binance aggTrades response: non-finite p/q";
                return false;
            }
            out.push_back(t);
        } catch (const std::exception& ex) {
            if (err) *err = std::string("binance aggTrades response: parse failed: ") + ex.what();
            return false;
        }
    }
    return true;
}

bool BinanceAggTradesFetcher::fetch_sync(std::string_view symbol, int64_t from_id, int limit,
                                         std::atomic<bool>* cancel,
                                         std::vector<AggTrade>* out, std::string* err) {
    std::ostringstream path;
    path << "/fapi/v1/aggTrades?symbol=" << to_upper(symbol)
         << "&fromId=" << from_id << "&limit=" << limit;

    std::string body;
    if (!binance_public_http_get(cfg_.host, cfg_.port, path.str(),
                                 static_cast<long>(cfg_.timeout_sec),
                                 cancel, &body, /*out_http=*/nullptr, err)) {
        return false;
    }
    std::vector<AggTrade> parsed;
    if (!parse_agg_trades_response(body, parsed, err)) return false;
    if (out) *out = std::move(parsed);
    return true;
}

}  // namespace binance_u
