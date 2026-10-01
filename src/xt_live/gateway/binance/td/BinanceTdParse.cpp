#include "td/BinanceTdParse.h"

#include <cmath>
#include <exception>
#include <initializer_list>

#include <nlohmann/json.hpp>

#include "transport/BinanceTdWireUtil.h"  // parse_local_id。
#include "XtFixedPoint.h"                  // Binance decimal → fixed-point。
#include "xt_identity/OrderAttributionStore.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_logger/XtLogger.h"

namespace {

// P15 NaN/Inf ingress 防线：std::stod("nan"/"inf") 会成功解析出非有限值（不抛），
// try-catch 接不住；非有限资金/仓位数据进 to_fp 会产生垃圾定点值污染钱路。
bool all_finite(std::initializer_list<double> vs) {
    for (double v : vs) {
        if (!std::isfinite(v)) return false;
    }
    return true;
}

}  // namespace

namespace binance_u {

// /fapi/v2/account 是 cash / margin / per-symbol broker truth 的单次快照。
// 顶层资金字段严格解析，失败返回 nullopt；per-symbol 行 best-effort 解析，
// 纯零行过滤掉，避免 truth source 随 Binance 全合约列表抖动。
std::optional<AccountTruthDetail>
parse_account_detail_body(const std::string& body) {
    auto j = nlohmann::json::parse(body, nullptr, /*allow_exceptions=*/false);
    if (!j.is_object()) {
        XtLogger::warn_m("BinanceTD", "parse_account_detail_body: body not a JSON object");
        return std::nullopt;
    }
    AccountTruthDetail det{};
    try {
        // AccountSnapshot.balance = totalWalletBalance（wallet 口径，不含 uPnL）：
        // 与 UDS `wb` 锚同口径喂 set_cash，cash 账本不被 uPnL 污染（equity =
        // cash + unrealized 单计）。truth overlay 的 total_margin_balance_fp
        // 独立 parse totalMarginBalance（venue equity/margin_ratio 源），两口径
        // 刻意分开——勿改回从 acct.balance 派生。
        det.acct.balance = std::stod(j.value("totalWalletBalance", "0"));
        det.acct.available = std::stod(j.value("availableBalance", "0"));
        det.acct.margin = std::stod(j.value("totalInitialMargin", "0"));
        det.acct.position_profit = std::stod(j.value("totalUnrealizedProfit", "0"));
        const double maint_margin = std::stod(j.value("totalMaintMargin", "0"));
        const double total_margin_balance = std::stod(j.value("totalMarginBalance", "0"));
        // 顶层资金字段严格解析口径延伸：非有限 = 整份快照不可信，拒收（caller 保留
        // 上一份 truth）。检查先于 to_fp：非有限值不进定点转换。
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
                PerSymbolTruth pos{};
                try {
                    pos.symbol = p.value("symbol", "");
                    const double entry = std::stod(p.value("entryPrice", "0"));
                    const double amt = std::stod(p.value("positionAmt", "0"));
                    const double upnl = std::stod(p.value("unrealizedProfit", "0"));
                    const double margin = std::stod(p.value("positionInitialMargin", "0"));
                    // per-symbol 行 best-effort：非有限单行跳过（同 parse 异常 continue 语义）。
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

// wallet 口径（不含 uPnL）直比 runtime cash；不复用 parse_account_detail_body，
// 避免停机路径为单字段承担 per-symbol 全量解析面。Binance 现发 string，
// number 形态一并接受（schema 漂移时不静默降级回 anchor 缓存）。
std::optional<double> parse_wallet_balance_body(const std::string& body) {
    auto j = nlohmann::json::parse(body, nullptr, /*allow_exceptions=*/false);
    if (!j.is_object() || !j.contains("totalWalletBalance")) return std::nullopt;
    const auto& v = j["totalWalletBalance"];
    try {
        double out = 0.0;
        if (v.is_string()) out = std::stod(v.get<std::string>());
        else if (v.is_number()) out = v.get<double>();
        else return std::nullopt;
        if (!std::isfinite(out)) return std::nullopt;  // "NaN"/"inf" 文本能过 stod，拒收
        return out;
    } catch (...) {
    }
    return std::nullopt;
}

// markPrice 只用于补 account.positions 缺失的字段；解析失败返回空 map，调用方保留 0。
std::unordered_map<std::string, int64_t>
parse_position_marks_body(const std::string& body) {
    std::unordered_map<std::string, int64_t> marks;
    auto j = nlohmann::json::parse(body, nullptr, /*allow_exceptions=*/false);
    if (!j.is_array()) return marks;
    marks.reserve(j.size());
    for (const auto& p : j) {
        try {
            const std::string sym = p.value("symbol", "");
            const double mp = std::stod(p.value("markPrice", "0"));
            // mp > 0 已滤 NaN（NaN>0 为 false）；isfinite 再拦 +Inf。
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
    auto j = nlohmann::json::parse(body, nullptr, /*allow_exceptions=*/false);
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

}  // namespace binance_u
