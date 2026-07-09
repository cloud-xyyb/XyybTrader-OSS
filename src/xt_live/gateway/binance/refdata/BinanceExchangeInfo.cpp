#include "BinanceExchangeInfo.h"

#include <cmath>
#include <exception>
#include <utility>

#include <nlohmann/json.hpp>

namespace binance_u {

namespace {
constexpr double kFpScale = 1.0e6;
}

bool parse_exchange_info(const std::string& body,
                         std::unordered_map<std::string, SymbolFilter>& out,
                         std::string* err) {
    auto j = nlohmann::json::parse(body, nullptr,  false);
    if (j.is_discarded()) {
        if (err) *err = "exchangeInfo: invalid json";
        return false;
    }
    if (!j.contains("symbols") || !j["symbols"].is_array()) {
        if (err) *err = "exchangeInfo: missing symbols[]";
        return false;
    }

    out.clear();
    for (const auto& sym : j["symbols"]) {
        if (!sym.is_object() || !sym.contains("symbol") || !sym.contains("filters")
            || !sym["filters"].is_array()) {
            continue;
        }
        SymbolFilter f;
        for (const auto& flt : sym["filters"]) {
            if (!flt.is_object()) continue;
            const std::string ft = flt.value("filterType", std::string());
            try {
                if (ft == "PRICE_FILTER" && flt.contains("tickSize")) {
                    f.tick = std::stod(flt["tickSize"].get<std::string>());
                } else if (ft == "LOT_SIZE" && flt.contains("stepSize")) {
                    f.step = std::stod(flt["stepSize"].get<std::string>());
                }
            } catch (const std::exception&) {

            }
        }

        if (f.tick <= 0.0) continue;
        f.tick_fp = static_cast<int64_t>(std::llround(f.tick * kFpScale));
        f.step_fp = static_cast<int64_t>(std::llround(f.step * kFpScale));
        out.emplace(sym["symbol"].get<std::string>(), f);
    }
    return true;
}

}
