#include "td/BinanceReconciler.h"

#include <algorithm>
#include <cmath>
#include <map>
#include <stdexcept>
#include <utility>

#include <nlohmann/json.hpp>

#include "td/BinanceOrderLedger.h"
#include "transport/BinanceTdWireUtil.h"
#include "rest/BinanceRestClient.h"
#include "rest/BinanceSign.h"
#include "transport/BinanceUserDataStream.h"
#include "xt_live/gateway/common/ReverseSyncHelper.h"
#include "xt_core/session/XtTradingDay.h"
#include "xt_identity/OrderAttributionStore.h"
#include "xt_logger/XtLogger.h"

#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif

namespace binance_u {

namespace {

[[nodiscard]] double stod_finite(const std::string& s) {
    const double v = std::stod(s);
    if (!std::isfinite(v)) throw std::invalid_argument("non-finite numeric field");
    return v;
}

int parse_venue_err_code(const std::string& body) {
    auto j = nlohmann::json::parse(body, nullptr,  false);
    return j.is_object() ? j.value("code", 0) : 0;
}

constexpr int kErrOrderNotExist = -2013;

}

std::optional<std::vector<BinanceReconciler::VenueOpenOrderBrief>>
BinanceReconciler::fetch_open_orders_brief() {
    auto arr = get_open_orders_array_("fetch_open_orders_brief");

    if (!arr) return std::nullopt;
    const nlohmann::json& j = *arr;
    std::vector<VenueOpenOrderBrief> rows;
    rows.reserve(j.size());
    for (const auto& o : j) {
        VenueOpenOrderBrief row;
        row.sys_id = std::to_string(o.value("orderId", 0LL));
        row.client_order_id = o.value("clientOrderId", std::string{});
        row.symbol = o.value("symbol", std::string{});
        row.order_time_ms = static_cast<uint64_t>(o.value("time", 0LL));
        rows.push_back(std::move(row));
    }
    return rows;
}

std::size_t BinanceReconciler::cancel_leaked_venue_orders(
    const std::vector<VenueOpenOrderBrief>& venue_open, uint32_t min_age_ms) {

    constexpr std::size_t kMaxCancelsPerCycle = 10;

    const int64_t age_floor_ms =
        std::max<int64_t>(static_cast<int64_t>(min_age_ms), 5000);
    const int64_t now_ms = static_cast<int64_t>(sign::now_ms());
    std::size_t canceled = 0;
    for (const auto& row : venue_open) {
        if (canceled >= kMaxCancelsPerCycle) break;

        const uint64_t local_id = parse_local_id(row.client_order_id);
        if (local_id == 0) continue;
        if (ledger_.is_inflight(local_id)) continue;
        if (row.order_time_ms == 0 ||
            now_ms - static_cast<int64_t>(row.order_time_ms) < age_floor_ms) {
            continue;
        }
        auto resp = rest_->delete_signed("/fapi/v1/order",
                                         {{"symbol", row.symbol},
                                          {"origClientOrderId", row.client_order_id}});
        if (resp.ok()) {
            ++canceled;
            XtLogger::error_m("BinanceTD",
                "leaked venue order canceled: local_id={} coid={} sys_id={} age_ms={} "
                "(venue-alive but untracked locally — tracking was wrongly terminalized)",
                local_id, row.client_order_id, row.sys_id,
                now_ms - static_cast<int64_t>(row.order_time_ms));
        } else {

            XtLogger::warn_m("BinanceTD",
                "leaked venue order cancel failed: local_id={} coid={} http={} body='{}'",
                local_id, row.client_order_id, resp.http_status, resp.body);
        }
    }
    return canceled;
}

std::optional<XtOrderExecUpdate>
BinanceReconciler::fetch_order_status(const std::string& sys_id,
                                      const std::string& symbol,
                                      uint64_t local_id,
                                      XtLatencyEligibility& out_elig) {
    out_elig = XtLatencyEligibility::kEligible;
    auto resp = rest_->get_signed("/fapi/v1/order",
                                  {{"symbol", symbol}, {"orderId", sys_id}});
    if (!resp.ok()) {

        if (parse_venue_err_code(resp.body) == kErrOrderNotExist) {
            out_elig = XtLatencyEligibility::kIneligible;
            return synthesize_not_exist_terminal_(local_id, sys_id);
        }
        XtLogger::warn_m("BinanceTD",
            "GET /fapi/v1/order failed: orderId={} symbol={} http={} err='{}'",
            sys_id, symbol, resp.http_status, resp.curl_err);
        return std::nullopt;
    }
    auto j = nlohmann::json::parse(resp.body, nullptr,  false);
    if (!j.is_object()) return std::nullopt;

    XtOrderExecUpdate upd;
    upd.id = local_id;
    upd.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(local_id).raw();
    upd.is_buy = (j.value("side", std::string("BUY")) == "BUY");
    upd.close_offset = j.value("reduceOnly", false) ? XtOffset::Close : XtOffset::Open;
    const std::string status_str = j.value("status", std::string("CANCELED"));
    const auto mapped = parse_binance_status_opt(status_str);
    if (!mapped) {

        XtLogger::warn_m("BinanceTD",
            "fetch_order_status unmapped status='{}': orderId={} symbol={} (kept alive)",
            status_str, sys_id, symbol);
        return std::nullopt;
    }
    if (*mapped == XtOrderStatus::Accepted || *mapped == XtOrderStatus::PartiallyFilled) {
        return std::nullopt;
    }
    upd.status = *mapped;
    try {
        upd.qty_fp    = order_exec_to_fp(stod_finite(j.value("origQty",     "0")));
        upd.filled_fp = order_exec_to_fp(stod_finite(j.value("executedQty", "0")));
        upd.leaves_fp = upd.qty_fp - upd.filled_fp;
        upd.price_fp  = order_exec_to_fp(stod_finite(j.value("price",       "0")));
    } catch (...) { return std::nullopt; }
    upd.local_cb_us = lat_now_us();
    ledger_.remember_terminal(upd.id);
    return upd;
}

std::optional<XtOrderExecUpdate>
BinanceReconciler::synthesize_not_exist_terminal_(uint64_t local_id,
                                                  const std::string& sys_id) {
    XtOrderExecUpdate upd;
    upd.id = local_id;
    upd.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(local_id).raw();
    upd.status = XtOrderStatus::Canceled;
    upd.local_cb_us = lat_now_us();

    const auto decision = ledger_.on_reconcile_terminal(
        local_id, BinanceOrderLedger::ReconcileTerminal::kNotExistCanceled, upd);
    if (decision.action != BinanceOrderLedger::TerminalDecision::Action::kEmit) {
        return std::nullopt;
    }
    XtLogger::warn_m("BinanceTD",
        "ghost confirmed not-exist by venue (-2013) → synthesize CANCELED: "
        "local_id={} sys_id={}", local_id, sys_id);
    return upd;
}

void BinanceReconciler::note_ghost_fetch_failure_(
    uint64_t local_id, const std::string& sys_id,
    std::unordered_map<uint64_t, uint32_t>& next_streak) {
    const auto prev = ghost_fetch_fail_streak_.find(local_id);
    const uint32_t streak =
        prev == ghost_fetch_fail_streak_.end() ? 1u : prev->second + 1u;
    next_streak[local_id] = streak;
    XtLogger::warn_m("BinanceTD",
        "ghost detected but fetch_order_status failed: local_id={} sys_id={} "
        "consecutive_cycles={}", local_id, sys_id, streak);

    if (streak >= kGhostFetchFailEscalateCycles &&
        streak % kGhostFetchFailEscalateCycles == 0) {
        XtLogger::error_m("BinanceTD",
            "zombie order not converging: {} consecutive fetch failures, "
            "local_id={} sys_id={} (kept alive by design)",
            streak, local_id, sys_id);
    }
}

BinanceReconciler::ClientOrderLookup
BinanceReconciler::find_open_order_by_client_id(
    const std::string& symbol, const std::string& client_order_id) {
    ClientOrderLookup out;
    if (symbol.empty() || client_order_id.empty()) return out;
    auto resp = rest_->get_signed("/fapi/v1/openOrders", {{"symbol", symbol}});
    if (!resp.ok()) {
        XtLogger::warn_m("BinanceTD",
            "GET openOrders by symbol failed: symbol={} http={} err='{}' body='{}'",
            symbol, resp.http_status, resp.curl_err, resp.body);
        return out;
    }
    out.ok = true;
    auto j = nlohmann::json::parse(resp.body, nullptr,  false);
    if (!j.is_array()) return out;
    for (const auto& o : j) {
        if (o.value("clientOrderId", std::string()) != client_order_id) continue;
        out.found = true;
        out.sys_id = std::to_string(o.value("orderId", 0LL));
        return out;
    }
    return out;
}

bool BinanceReconciler::synthesize_blank_sys_id_reject(uint64_t local_id) {
    XtOrderExecUpdate upd;
    upd.id = local_id;
    upd.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(local_id).raw();
    upd.status = XtOrderStatus::Rejected;
    upd.local_cb_us = lat_now_us();

    const auto decision = ledger_.on_reconcile_terminal(
        local_id, BinanceOrderLedger::ReconcileTerminal::kBlankSysIdReject, upd);
    const bool erased =
        decision.action == BinanceOrderLedger::TerminalDecision::Action::kEmit;
    if (erased) {

        emit_reconcile_terminal_update(upd, XtLatencyEligibility::kIneligible);
    }
    return erased;
}

bool BinanceReconciler::resolve_blank_sys_id_for_reconcile(
    uint64_t local_id, const std::string& symbol,
    uint64_t age_ms, uint32_t min_age_ms) {
    const std::string client_order_id = ledger_.client_order_id_for(local_id);
    const auto lookup = find_open_order_by_client_id(symbol, client_order_id);
    if (lookup.ok && lookup.found) {
        ledger_.bind_sys_id_if_blank(local_id, lookup.sys_id);
        return false;
    }
    if (!lookup.ok || age_ms <= static_cast<uint64_t>(min_age_ms) * 2) return false;
    return synthesize_blank_sys_id_reject(local_id);
}

std::optional<std::size_t>
BinanceReconciler::detect_and_resolve_ghosts(uint32_t min_age_ms) {
    auto venue_open = fetch_open_orders_brief();
    if (!venue_open.has_value()) return std::nullopt;

    std::unordered_set<std::string> exchange_open;
    exchange_open.reserve(venue_open->size());
    for (const auto& row : *venue_open) exchange_open.insert(row.sys_id);

    const auto snapshot = ledger_.inflight_snapshot();
    const uint64_t now_ms = static_cast<uint64_t>(lat_now_us() / 1000);

    std::size_t resolved_count = 0;
    std::unordered_map<uint64_t, uint32_t> next_fetch_fail_streak;
    for (const auto& inf : snapshot) {
        const uint64_t age_ms = now_ms - inf.submit_ts_ms;
        if (age_ms < min_age_ms) continue;
        if (inf.sys_id.empty()) {
            if (resolve_blank_sys_id_for_reconcile(inf.local_id, inf.symbol,
                                                   age_ms, min_age_ms)) {
                ++resolved_count;
            }
            continue;
        }
        if (exchange_open.count(inf.sys_id) > 0) continue;

        XtLatencyEligibility term_elig = XtLatencyEligibility::kEligible;
        auto term = fetch_order_status(inf.sys_id, inf.symbol, inf.local_id, term_elig);
        if (!term.has_value()) {
            note_ghost_fetch_failure_(inf.local_id, inf.sys_id, next_fetch_fail_streak);
            continue;
        }
        XtLogger::warn_m("BinanceTD",
            "ghost order resolved: local_id={} sys_id={} status={} age_ms={}",
            inf.local_id, inf.sys_id, static_cast<int>(term->status), age_ms);

        const bool has_fills =
            term->filled_fp > 0 || term->status == XtOrderStatus::Filled;
        if (has_fills) {
            resync_user_trades_for_order(inf.symbol, inf.sys_id);
        }

        if (ledger_.on_reconcile_terminal(
                inf.local_id, BinanceOrderLedger::ReconcileTerminal::kVenueTerminal,
                *term).action == BinanceOrderLedger::TerminalDecision::Action::kEmit) {
            emit_reconcile_terminal_update(*term, term_elig);
        }
        ++resolved_count;

        if (has_fills) {
            XtLogger::warn_m("BinanceTD",
                "ghost with fills (filled_fp={}) → queue position resync (gap-fill + "
                "write-through, no reconnect) for local_id={}",
                term->filled_fp, inf.local_id);
            hooks_.queue_reconcile_resync();
        }
    }

    ghost_fetch_fail_streak_ = std::move(next_fetch_fail_streak);

    resolved_count += cancel_leaked_venue_orders(*venue_open, min_age_ms);
    return resolved_count;
}

void BinanceReconciler::resync_user_trades_for_symbol(const std::string& symbol) {
    if (!uds_) return;
    const int64_t last_seen = uds_->last_seen_trade_id(symbol);

    const int64_t from_id = last_seen > 0 ? last_seen + 1 : 1;

    int64_t cursor = from_id;
    size_t emitted = 0, deduped = 0, skipped = 0;
    int rounds = 0;
    for (;;) {
        ++rounds;
        std::map<std::string, std::string> qs = {
            {"symbol", symbol},
            {"fromId", std::to_string(cursor)},
            {"limit",  std::to_string(kUserTradesPageLimit)},
        };
        auto resp = rest_->get_signed("/fapi/v1/userTrades", qs);
        if (!resp.ok()) {
            XtLogger::warn_m("BinanceTD",
                "resync_user_trades GET failed sym={} round={} http={} err='{}'",
                symbol, rounds, resp.http_status, resp.curl_err);
            return;
        }
        auto j = nlohmann::json::parse(resp.body, nullptr,  false);
        if (!j.is_array()) return;
        if (j.empty()) {
            if (rounds == 1) {
                XtLogger::info_m("BinanceTD",
                    "resync_user_trades sym={} from_id={} no missed fills (UDS healthy)",
                    symbol, from_id);
                return;
            }
            break;
        }
        int64_t max_tid = 0;
        replay_user_trades_json_(symbol, j, emitted, deduped, skipped, max_tid);
        if (j.size() < kUserTradesPageLimit) break;
        if (max_tid < cursor) {

            XtLogger::warn_m("BinanceTD",
                "resync_user_trades sym={} round={} cursor not advancing (max_tid={} cursor={})",
                symbol, rounds, max_tid, cursor);
            break;
        }
        cursor = max_tid + 1;
        if (rounds >= kUserTradesPageRoundsMax) {
            user_trades_pages_capped_total_.fetch_add(1, std::memory_order_relaxed);
            XtLogger::error_m("BinanceTD",
                "resync_user_trades sym={} page rounds capped at {} (still full page); "
                "residual left to next reconcile cycle",
                symbol, kUserTradesPageRoundsMax);
            break;
        }
    }
    XtLogger::warn_m("BinanceTD",
        "resync_user_trades sym={} from_id={} rounds={} replayed={} deduped={} skipped={} (UDS already had it)",
        symbol, from_id, rounds, emitted, deduped, skipped);
}

void BinanceReconciler::resync_user_trades_for_order(const std::string& symbol,
                                                     const std::string& sys_id) {
    if (!uds_ || !rest_ || symbol.empty() || sys_id.empty()) return;

    int64_t cursor = 0;
    size_t emitted = 0, deduped = 0, skipped = 0;
    int rounds = 0;
    for (;;) {
        ++rounds;
        std::map<std::string, std::string> qs = {
            {"symbol", symbol},
            {"orderId", sys_id},
            {"limit", std::to_string(kUserTradesPageLimit)},
        };
        if (cursor > 0) qs.emplace("fromId", std::to_string(cursor));
        auto resp = rest_->get_signed("/fapi/v1/userTrades", qs);
        if (!resp.ok()) {
            XtLogger::warn_m("BinanceTD",
                "resync_user_trades(orderId) GET failed sym={} orderId={} round={} http={} err='{}'",
                symbol, sys_id, rounds, resp.http_status, resp.curl_err);
            return;
        }
        auto j = nlohmann::json::parse(resp.body, nullptr,  false);
        if (!j.is_array()) return;
        if (j.empty()) {
            if (rounds == 1) {

                XtLogger::warn_m("BinanceTD",
                    "resync_user_trades(orderId) sym={} orderId={} returned no trades",
                    symbol, sys_id);
                return;
            }
            break;
        }
        int64_t max_tid = 0;
        replay_user_trades_json_(symbol, j, emitted, deduped, skipped, max_tid);
        if (j.size() < kUserTradesPageLimit) break;
        if (max_tid < cursor) {
            XtLogger::warn_m("BinanceTD",
                "resync_user_trades(orderId) sym={} orderId={} round={} cursor not advancing",
                symbol, sys_id, rounds);
            break;
        }
        cursor = max_tid + 1;
        if (rounds >= kUserTradesPageRoundsMax) {
            user_trades_pages_capped_total_.fetch_add(1, std::memory_order_relaxed);
            XtLogger::error_m("BinanceTD",
                "resync_user_trades(orderId) sym={} orderId={} page rounds capped at {}; "
                "residual left to next reconcile cycle",
                symbol, sys_id, kUserTradesPageRoundsMax);
            break;
        }
    }
    XtLogger::warn_m("BinanceTD",
        "resync_user_trades(orderId) sym={} orderId={} rounds={} replayed={} deduped={} skipped={}",
        symbol, sys_id, rounds, emitted, deduped, skipped);
}

void BinanceReconciler::replay_user_trades_json_(const std::string& symbol,
                                                 const nlohmann::json& trades,
                                                 size_t& emitted, size_t& deduped,
                                                 size_t& out_skipped,
                                                 int64_t& out_max_trade_id) {

    for (const auto& t : trades) {
        try {
            XtTradeRecord tr;
            const int64_t binance_trade_id = t.value("id", 0LL);
            if (binance_trade_id <= 0) { ++out_skipped; continue; }
            out_max_trade_id = std::max(out_max_trade_id, binance_trade_id);
            const int64_t exchange_order_id = t.value("orderId", 0LL);
            tr.trade_id   = std::to_string(binance_trade_id);
            tr.order_sys_id = std::to_string(exchange_order_id);
            tr.code       = symbol;
            const std::string side = t.value("side", std::string{});
            tr.is_buy     = (side == "BUY");
            tr.price      = stod_finite(t.value("price", "0"));
            tr.exec_price = tr.price;
            const double qty = stod_finite(t.value("qty", "0"));
            tr.qty_fp     = order_exec_to_fp(qty);
            tr.fee        = stod_finite(t.value("commission", "0"));
            tr.is_passive = t.value("maker", false);
            tr.is_close   = false;
            tr.is_today   = false;
            const int64_t event_ms = t.value("time", 0LL);
            tr.fill_ts_us = static_cast<uint64_t>(event_ms) * 1000ULL;
            tr.local_cb_us = lat_now_us();

            tr.trade_date = xt_core::derive_trading_day(
                event_ms, xt_core::TimezonePolicy::UtcNaturalDay);
            tr.trade_time = xt_core::derive_action_time(
                event_ms, xt_core::TimezonePolicy::UtcNaturalDay);
            tr.trading_day = xt_core::derive_trading_day(
                event_ms, xt_core::TimezonePolicy::UtcNaturalDay);
            if (auto attr = resolve_gap_fill_order_attribution(symbol, exchange_order_id)) {
                tr.order_id = attr->local_id;
                tr.is_close = attr->is_close;
                tr.close_offset = attr->is_close ? XtOffset::Close : XtOffset::Open;
                tr.strategy_id = attr->strategy_id;
                tr.sid_id = attr->strategy_sid.raw();
            } else {
                tr.order_id = 0;
            }
            if (uds_->inject_gap_fill_trade(tr)) {
                ++emitted;
            } else {
                ++deduped;
            }
        } catch (...) { ++out_skipped; continue; }
    }
}

std::optional<GapFillOrderAttribution>
BinanceReconciler::resolve_gap_fill_order_attribution(
    const std::string& symbol, int64_t exchange_order_id) {
    if (symbol.empty() || exchange_order_id <= 0 || !rest_) return std::nullopt;
    const std::string order_sys_id = std::to_string(exchange_order_id);
    if (const auto hit = ledger_.find_inflight_by_sys_id(order_sys_id)) {
        GapFillOrderAttribution attr;
        attr.local_id = hit->local_id;
        attr.is_close = hit->close_offset != XtOffset::Open;
        populate_gap_fill_strategy_identity(attr);
        return attr;
    }

    auto resp = rest_->get_signed("/fapi/v1/order",
                                  {{"symbol", symbol}, {"orderId", order_sys_id}});
    if (!resp.ok()) {
        XtLogger::warn_m("BinanceTD",
            "gap-fill attribution GET /order failed sym={} orderId={} http={} err='{}'",
            symbol, order_sys_id, resp.http_status, resp.curl_err);
        return std::nullopt;
    }
    auto attr = parse_gap_fill_order_attribution_body(resp.body);
    if (!attr) {
        XtLogger::warn_m("BinanceTD",
            "gap-fill attribution skipped sym={} orderId={} body missing local clientOrderId",
            symbol, order_sys_id);
    }
    return attr;
}

bool BinanceReconciler::rebuild_from_open_orders_body(
    const std::string& body, bool is_cold_start) {
    auto j = nlohmann::json::parse(body, nullptr,  false);
    if (!j.is_array()) {
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::OpenOrders,
                             "binance openOrders response was not an array");
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::ReverseSync,
                             "binance openOrders response was not an array");
#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance()
            .inc_recovery_rebuild_alive_total("binance_u", "failure");
#endif
        XtLogger::warn_m("BinanceTD",
            "rebuild_alive_inflight: openOrders body not array, skipping");
        return false;
    }

    std::vector<AliveInflightOrder> orders;
    std::vector<uint64_t> ids_to_clear;
    orders.reserve(static_cast<size_t>(j.size()));
    ids_to_clear.reserve(static_cast<size_t>(j.size()));

    for (const auto& o : j) {
        const std::string client_order_id = o.value("clientOrderId", std::string{});
        if (client_order_id.empty()) {
#ifdef XT_HAVE_METRICS
            ct::metrics::XtMetricRegistry::instance()
                .inc_recovery_rebuild_alive_skipped_foreign_total("binance_u");
#endif
            continue;
        }

        const uint64_t local_id = parse_local_id(client_order_id);
        if (local_id == 0) {
#ifdef XT_HAVE_METRICS
            ct::metrics::XtMetricRegistry::instance()
                .inc_recovery_rebuild_alive_skipped_foreign_total("binance_u");
#endif
            continue;
        }

        AliveInflightOrder a;
        a.local_id = local_id;
        a.code = o.value("symbol", std::string{});
        a.is_buy = (o.value("side", std::string("BUY")) == "BUY");
        try {
            a.total_qty_fp =
                order_exec_to_fp(stod_finite(o.value("origQty",     std::string("0"))));
            a.filled_qty_fp =
                order_exec_to_fp(stod_finite(o.value("executedQty", std::string("0"))));
            a.price_fp =
                order_exec_to_fp(stod_finite(o.value("price",       std::string("0"))));
        } catch (...) {

#ifdef XT_HAVE_METRICS
            ct::metrics::XtMetricRegistry::instance()
                .inc_recovery_rebuild_alive_skipped_foreign_total("binance_u");
#endif
            continue;
        }
        a.close_offset = o.value("reduceOnly", false) ? XtOffset::Close : XtOffset::Open;
        a.strategy_sid = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(local_id);

        {
            BinanceOrderLedger::OrderRecord rec;
            rec.client_order_id = client_order_id;
            rec.symbol = a.code;
            rec.sys_id = std::to_string(o.value("orderId", 0LL));
            rec.status = XtOrderStatus::Accepted;
            rec.is_buy = a.is_buy;
            rec.price_fp = a.price_fp;
            rec.qty_fp = a.total_qty_fp;
            rec.close_offset = a.close_offset;
            rec.submit_ts_ms = static_cast<uint64_t>(o.value("time", 0LL));
            ledger_.upsert_recovered(local_id, std::move(rec));
        }
        tracked_symbols_.note(a.code);

        ids_to_clear.push_back(local_id);
        orders.push_back(std::move(a));
    }

    if (!is_cold_start && !ids_to_clear.empty()) {
        hooks_.erase_dedup_for_local_ids(ids_to_clear);
    }

    readiness_gate_.satisfy(xt_live::gateway::TdReadinessStage::OpenOrders,
                            "binance openOrders snapshot parsed");
    if (!cb_.on_reverse_sync_batch) {
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::ReverseSync,
                             "binance reverse-sync handler not bound");
        return false;
    }

    xyybtrader::recovery::emit_reverse_sync_batch_with_metric(
        std::move(orders), is_cold_start, "binance_u", cb_.on_reverse_sync_batch);
    readiness_gate_.satisfy(xt_live::gateway::TdReadinessStage::ReverseSync,
                            "binance reverse-sync batch emitted");
    return true;
}

void BinanceReconciler::rebuild_alive_inflight_and_emit(bool is_cold_start) {

    constexpr int kMaxAttempts = 2;
    constexpr int kRetryDelayMs = 1000;
    for (int attempt = 1; attempt <= kMaxAttempts; ++attempt) {
        auto resp = rest_->get_signed("/fapi/v1/openOrders", {});
        if (resp.ok()) {
            if (attempt > 1) {
                XtLogger::info_m("BinanceTD",
                    "rebuild_alive_inflight GET openOrders succeeded after retry attempt={}",
                    attempt);
            }
            (void)rebuild_from_open_orders_body(resp.body, is_cold_start);
            return;
        }

        if (attempt < kMaxAttempts) {
            XtLogger::warn_m("BinanceTD",
                "rebuild_alive_inflight GET openOrders failed (attempt {}): http={} err='{}', retrying after {}ms",
                attempt, resp.http_status, resp.curl_err, kRetryDelayMs);

            if (hooks_.wait_shutdown(kRetryDelayMs)) {
                XtLogger::info_m("BinanceTD",
                    "rebuild_alive_inflight retry interrupted by shutdown (attempt {})", attempt);
                return;
            }
            continue;
        }

#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance()
            .inc_recovery_rebuild_alive_total("binance_u", "failure");
#endif
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::OpenOrders,
                             "binance openOrders request failed");
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::ReverseSync,
                             "binance openOrders request failed");
        XtLogger::warn_m("BinanceTD",
            "rebuild_alive_inflight GET openOrders failed after {} attempts: http={} err='{}',"
            " 30s 后 reconcile 周期重试",
            kMaxAttempts, resp.http_status, resp.curl_err);
        return;
    }
}

}
