#include "td/BinanceTdParse.h"

#include <cmath>
#include <exception>
#include <initializer_list>

#include <nlohmann/json.hpp>

#include "transport/BinanceTdWireUtil.h"
#include "XtFixedPoint.h"
#include "xt_identity/OrderAttributionStore.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_logger/XtLogger.h"

namespace {

bool all_finite(std::initializer_list<double> vs) {
    for (double v : vs) {
        if (!std::isfinite(v)) return false;
    }
    return true;
}

}

namespace binance_u {

std::optional<AccountTruthDetail>
parse_account_detail_body(const std::string& body) {
    auto j = nlohmann::json::parse(body, nullptr,  false);
    if (!j.is_object()) {
        XtLogger::warn_m("BinanceTD", "parse_account_detail_body: body not a JSON object");
        return std::nullopt;
    }
    AccountTruthDetail det{};
    try {

        det.acct.balance = std::stod(j.value("totalWalletBalance", "0"));
        det.acct.available = std::stod(j.value("availableBalance", "0"));
        det.acct.margin = std::stod(j.value("totalInitialMargin", "0"));
        det.acct.position_profit = std::stod(j.value("totalUnrealizedProfit", "0"));
        const double maint_margin = std::stod(j.value("totalMaintMargin", "0"));
        const double total_margin_balance = std::stod(j.value("totalMarginBalance", "0"));

        if (!all_finite({det.acct.balance, det.acct.available, det.acct.margin,
                         det.acct.position_profit, maint_margin, total_margin_balance})) {
            XtLogger::warn_m("BinanceTD",
                             "parse_account_detail_body: non-finite top-level fund field, reject snapshot");
            return std::nullopt;
        }
        det.maint_margin_fp = xt_fp::to_fp(maint_margin);
        det.available_balance_fp = xt_fp::to_fp(det.acct.available);
        det.total_margin_balance_fp = xt_fp::to_fp(total_margin_balance);
        if (j.contains("positions") && j["positions"].is_array()) {
            det.per_symbol.reserve(j["positions"].size());
            for (const auto& p : j["positions"]) {
                BinanceAccountTruthSource::Position pos{};
                try {
                    pos.symbol = p.value("symbol", "");
                    const double entry = std::stod(p.value("entryPrice", "0"));
                    const double amt = std::stod(p.value("positionAmt", "0"));
                    const double upnl = std::stod(p.value("unrealizedProfit", "0"));
                    const double margin = std::stod(p.value("positionInitialMargin", "0"));

                    if (!all_finite({entry, amt, upnl, margin})) continue;
                    pos.avg_price_fp = xt_fp::to_fp(entry);
                    pos.qty_fp = xt_fp::to_fp(amt);
                    pos.unrealized_pnl_fp = xt_fp::to_fp(upnl);
                    pos.margin_fp = xt_fp::to_fp(margin);
                } catch (...) { continue; }
                if (pos.symbol.empty()) continue;
                if (pos.qty_fp == 0 && pos.unrealized_pnl_fp == 0 && pos.margin_fp == 0) continue;
                det.per_symbol.push_back(std::move(pos));
            }
        }
    } catch (const std::exception& e) {
        XtLogger::warn_m("BinanceTD", "parse_account_detail_body parse error: {}", e.what());
        return std::nullopt;
    }
    return det;
}

std::optional<double> parse_wallet_balance_body(const std::string& body) {
    auto j = nlohmann::json::parse(body, nullptr,  false);
    if (!j.is_object() || !j.contains("totalWalletBalance")) return std::nullopt;
    const auto& v = j["totalWalletBalance"];
    try {
        double out = 0.0;
        if (v.is_string()) out = std::stod(v.get<std::string>());
        else if (v.is_number()) out = v.get<double>();
        else return std::nullopt;
        if (!std::isfinite(out)) return std::nullopt;
        return out;
    } catch (...) {
    }
    return std::nullopt;
}

std::unordered_map<std::string, int64_t>
parse_position_marks_body(const std::string& body) {
    std::unordered_map<std::string, int64_t> marks;
    auto j = nlohmann::json::parse(body, nullptr,  false);
    if (!j.is_array()) return marks;
    marks.reserve(j.size());
    for (const auto& p : j) {
        try {
            const std::string sym = p.value("symbol", "");
            const double mp = std::stod(p.value("markPrice", "0"));

            if (!sym.empty() && std::isfinite(mp) && mp > 0) marks[sym] = xt_fp::to_fp(mp);
        } catch (...) {}
    }
    return marks;
}

void populate_gap_fill_strategy_identity(GapFillOrderAttribution& attr) {
    attr.strategy_sid = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(attr.local_id);
    if (const char* sid = xt_identity::StrategySidRegistry::resolve_sid_cstr_or_null(
            attr.strategy_sid.raw())) {
        attr.strategy_id = sid;
    }
}

std::optional<GapFillOrderAttribution>
parse_gap_fill_order_attribution_body(const std::string& body) {
    auto j = nlohmann::json::parse(body, nullptr,  false);
    if (!j.is_object()) return std::nullopt;
    const std::string client_order_id = j.value("clientOrderId", std::string{});
    const uint64_t local_id = parse_local_id(client_order_id);
    if (local_id == 0) return std::nullopt;

    GapFillOrderAttribution attr;
    attr.local_id = local_id;
    attr.is_close = j.value("reduceOnly", false);
    populate_gap_fill_strategy_identity(attr);
    return attr;
}

}
