#include "BinanceSnapshotFetcher.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cmath>
#include <sstream>
#include <string>
#include <utility>

#include <nlohmann/json.hpp>

#include "rest/BinancePublicHttp.h"

namespace binance_u {

BinanceSnapshotFetcher::BinanceSnapshotFetcher(Config cfg)
    : cfg_(std::move(cfg)) {}

namespace {

std::string to_upper(std::string_view s) {
    std::string out(s);
    std::transform(out.begin(), out.end(), out.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return out;
}

bool parse_depth_response(const std::string& body,
                          DepthSnapshot& out,
                          std::string* err) {
    auto j = nlohmann::json::parse(body, nullptr,  false);
    if (j.is_discarded()) {
        if (err) *err = "binance depth response: invalid json";
        return false;
    }
    if (!j.contains("lastUpdateId") || !j.contains("bids") || !j.contains("asks")) {
        if (err) *err = "binance depth response: missing lastUpdateId/bids/asks";
        return false;
    }
    out.last_update_id = j["lastUpdateId"].get<uint64_t>();

    auto parse_side = [&err](const nlohmann::json& arr,
                             std::vector<std::pair<double, double>>& dst,
                             const char* side) -> bool {

        dst.reserve(arr.size());
        for (const auto& lv : arr) {
            if (!lv.is_array() || lv.size() < 2) {
                if (err) *err = std::string("binance depth response: bad ") + side + " level";
                return false;
            }
            try {
                const double price = std::stod(lv[0].get<std::string>());
                const double qty   = std::stod(lv[1].get<std::string>());

                if (!std::isfinite(price) || !std::isfinite(qty)) {
                    if (err) *err = std::string("binance depth response: non-finite ") + side + " level";
                    return false;
                }
                dst.emplace_back(price, qty);
            } catch (const std::exception& e) {
                if (err) *err = std::string("binance depth response: stod failed on ") + side
                              + ": " + e.what();
                return false;
            }
        }
        return true;
    };

    out.bids.clear();
    out.asks.clear();
    if (!parse_side(j["bids"], out.bids, "bid")) return false;
    if (!parse_side(j["asks"], out.asks, "ask")) return false;
    return true;
}

}

std::optional<DepthSnapshot> BinanceSnapshotFetcher::fetch_sync(std::string_view symbol,
                                                                int limit,
                                                                std::string* err,
                                                                std::atomic<bool>* cancel) {
    std::ostringstream path;
    path << "/fapi/v1/depth?symbol=" << to_upper(symbol) << "&limit=" << limit;

    std::string body;
    if (!binance_public_http_get(cfg_.host, cfg_.port, path.str(),
                                 static_cast<long>(cfg_.timeout_sec),
                                 cancel, &body,  nullptr, err)) {
        return std::nullopt;
    }
    DepthSnapshot snap;
    if (!parse_depth_response(body, snap, err)) {
        return std::nullopt;
    }
    return snap;
}

}
