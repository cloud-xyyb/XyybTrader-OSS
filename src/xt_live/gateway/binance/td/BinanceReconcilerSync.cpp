#include "td/BinanceReconciler.h"

#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <utility>

#include <nlohmann/json.hpp>

#include "rest/BinanceRestClient.h"
#include "rest/BinanceSign.h"
#include "td/ResyncFillReplayer.h"
#include "transport/BinanceUserDataStream.h"
#include "xt_logger/XtLogger.h"

namespace binance_u {

namespace {

constexpr uint64_t kAccountAnchorMinIntervalMs = 10'000;

[[nodiscard]] bool account_anchor_due(uint64_t now_ms,
                                      uint64_t last_fetch_ms,
                                      uint64_t min_interval_ms) noexcept {
    if (last_fetch_ms == 0) return true;
    return now_ms >= last_fetch_ms && now_ms - last_fetch_ms >= min_interval_ms;
}

[[nodiscard]] std::string defensive_symbol(const nlohmann::json& p) {
    if (p.is_object()) {
        auto it = p.find("symbol");
        if (it != p.end() && it->is_string()) return it->get<std::string>();
    }
    return {};
}

[[nodiscard]] double stod_finite(const std::string& s) {
    const double v = std::stod(s);
    if (!std::isfinite(v)) throw std::invalid_argument("non-finite numeric field");
    return v;
}

}

BinanceReconciler::BinanceReconciler(std::unique_ptr<BinanceRestClient>& rest,
                                     std::unique_ptr<BinanceUserDataStream>& uds,
                                     BinanceOrderLedger& ledger,
                                     ITradeGateway::Callbacks& cb,
                                     xt_live::gateway::TdReadinessGate& readiness_gate,
                                     TrackedSymbolSet& tracked_symbols,
                                     ImplHooks hooks)
    : rest_(rest),
      uds_(uds),
      ledger_(ledger),
      cb_(cb),
      readiness_gate_(readiness_gate),
      tracked_symbols_(tracked_symbols),
      hooks_(std::move(hooks)) {}

std::optional<nlohmann::json>
BinanceReconciler::get_position_risk_array_(const char* tag) {
    const auto symbols = tracked_symbols_.snapshot();
    if (!symbols.empty() && symbols.size() <= kSymbolFilterMax) {
        nlohmann::json merged = nlohmann::json::array();
        bool all_ok = true;
        for (const auto& sym : symbols) {
            auto pr = rest_->get_signed("/fapi/v2/positionRisk", {{"symbol", sym}});
            if (!pr.ok()) { all_ok = false; break; }
            auto j = nlohmann::json::parse(pr.body, nullptr,  false);
            if (!j.is_array()) { all_ok = false; break; }
            for (auto& row : j) merged.push_back(std::move(row));
        }
        if (all_ok) return merged;
        XtLogger::warn_m("BinanceTD",
            "{}: per-symbol positionRisk failed, fallback to full fetch", tag);
    }
    auto pr = rest_->get_signed("/fapi/v2/positionRisk", {});
    if (!pr.ok()) {
        XtLogger::warn_m("BinanceTD",
            "{}: GET positionRisk failed: http={} err='{}'",
            tag, pr.http_status, pr.curl_err);
        return std::nullopt;
    }
    auto j = nlohmann::json::parse(pr.body, nullptr,  false);
    if (!j.is_array()) {
        XtLogger::warn_m("BinanceTD", "{}: positionRisk body not array", tag);
        return std::nullopt;
    }
    return j;
}

std::optional<nlohmann::json>
BinanceReconciler::get_open_orders_array_(const char* tag) {
    const auto symbols = tracked_symbols_.snapshot();
    if (!symbols.empty() && symbols.size() <= kSymbolFilterMax) {
        nlohmann::json merged = nlohmann::json::array();
        bool all_ok = true;
        for (const auto& sym : symbols) {
            auto resp = rest_->get_signed("/fapi/v1/openOrders", {{"symbol", sym}});
            if (!resp.ok()) { all_ok = false; break; }
            auto j = nlohmann::json::parse(resp.body, nullptr,  false);
            if (!j.is_array()) { all_ok = false; break; }
            for (auto& row : j) merged.push_back(std::move(row));
        }
        if (all_ok) return merged;
        XtLogger::warn_m("BinanceTD",
            "{}: per-symbol openOrders failed, fallback to full fetch", tag);
    }
    auto resp = rest_->get_signed("/fapi/v1/openOrders", {});
    if (!resp.ok()) {
        XtLogger::warn_m("BinanceTD",
            "{}: GET openOrders failed: http={} err='{}'",
            tag, resp.http_status, resp.curl_err);
        return std::nullopt;
    }
    auto j = nlohmann::json::parse(resp.body, nullptr,  false);
    if (!j.is_array()) {
        XtLogger::warn_m("BinanceTD", "{}: openOrders body not array", tag);
        return std::nullopt;
    }
    return j;
}

std::optional<std::vector<XtPositionSnapshot>>
BinanceReconciler::fetch_positions() {
    auto arr = get_position_risk_array_("fetch_positions");
    if (!arr) return std::nullopt;
    const nlohmann::json& j = *arr;
    std::vector<XtPositionSnapshot> positions;
    std::size_t dropped_tracked = 0;
    std::size_t dropped_other = 0;
    if (j.is_array()) {
        for (const auto& p : j) {
            const std::string symbol = defensive_symbol(p);
            if (symbol.empty()) {
                ++dropped_tracked;
                XtLogger::warn_m("BinanceTD",
                    "fetch_positions: row without parsable symbol dropped (fail-closed)");
                continue;
            }
            try {
                const double amt = stod_finite(p.value("positionAmt", "0"));
                if (amt == 0.0) continue;
                XtPositionSnapshot ps;
                ps.code = symbol;
                const double entry = stod_finite(p.value("entryPrice", "0"));
                if (amt > 0) { ps.long_qty = amt; ps.long_avg = entry; }
                else         { ps.short_qty = -amt; ps.short_avg = entry; }
                positions.push_back(std::move(ps));
            } catch (...) {
                if (tracked_symbols_.contains(symbol)) ++dropped_tracked;
                else                                   ++dropped_other;
                XtLogger::warn_m("BinanceTD",
                    "fetch_positions: unparsable positionRisk row dropped symbol={}",
                    symbol);
            }
        }
    }
    if (dropped_tracked > 0) {
        XtLogger::warn_m("BinanceTD",
            "fetch_positions: {} tracked/unknown unparsable row(s) → treat whole fetch "
            "as failed (fail-closed, no absent=flat inference this cycle)",
            dropped_tracked);
        return std::nullopt;
    }
    if (dropped_other > 0) {
        XtLogger::warn_m("BinanceTD",
            "fetch_positions: {} untracked unparsable row(s) skipped", dropped_other);
    }
    return positions;
}

void BinanceReconciler::forward_uds_trade(const XtTradeRecord& tr,
                                          XtLatencyEligibility elig) {
    {
        std::lock_guard<std::mutex> lk(resync_gate_mu_);
        if (resync_buffering_) {

            resync_fill_buffer_.push_back(tr);
            return;
        }
    }
    if (cb_.on_trade) cb_.on_trade(tr, elig);
}

void BinanceReconciler::forward_order_update_gated(
    const XtOrderExecUpdate& upd, EffectiveSignalDedup::Source src,
    XtLatencyEligibility elig) {

    if (order_status_is_final(upd.status)) {
        std::lock_guard<std::mutex> lk(resync_gate_mu_);
        if (resync_buffering_) {
            resync_terminal_buffer_.push_back(ResyncTerminal{upd, src, elig});
            return;
        }
    }
    hooks_.emit_through_dedup(upd, src, elig);
}

void BinanceReconciler::forward_uds_position(
    const std::vector<XtPositionSnapshot>& positions) {

    for (const auto& ps : positions) {
        if (ps.long_qty != 0.0 || ps.short_qty != 0.0) tracked_symbols_.note(ps.code);
    }
    {
        std::lock_guard<std::mutex> lk(resync_gate_mu_);
        if (resync_buffering_) {
            ++resync_pos_dropped_total_;
            XtLogger::warn_m("BinanceTD",
                "UDS absolute position snapshot dropped during resync gate "
                "(superseded by incoming recovery snapshot), total={}",
                resync_pos_dropped_total_);
            return;
        }
    }
    if (cb_.on_position) cb_.on_position(positions);
}

void BinanceReconciler::emit_reconcile_terminal_update(const XtOrderExecUpdate& upd,
                                                       XtLatencyEligibility elig) {
    forward_order_update_gated(upd, EffectiveSignalDedup::Source::kAck, elig);
}

BinanceReconciler::ResyncFillGate::ResyncFillGate(BinanceReconciler& reconciler)
    : reconciler_(reconciler) {
    std::lock_guard<std::mutex> lk(reconciler_.resync_gate_mu_);
    reconciler_.resync_buffering_ = true;
}

BinanceReconciler::ResyncFillGate::~ResyncFillGate() {
    if (!finished_) {
        reconciler_.finish_resync_fill_gate_(nullptr, {}, 0,  false);
    }
}

void BinanceReconciler::ResyncFillGate::finish_with_snapshot(
    const std::vector<XtPositionSnapshot>& positions,
    const std::unordered_map<std::string, int64_t>& fence_update_ms,
    int64_t fence_floor_ms, bool mark_recovering) {
    finished_ = true;
    reconciler_.finish_resync_fill_gate_(&positions, fence_update_ms, fence_floor_ms,
                                         mark_recovering);
}

void BinanceReconciler::finish_resync_fill_gate_(
    const std::vector<XtPositionSnapshot>* positions,
    const std::unordered_map<std::string, int64_t>& fence_update_ms,
    int64_t fence_floor_ms, bool mark_recovering) {

    std::lock_guard<std::mutex> lk(resync_gate_mu_);
    std::vector<XtTradeRecord> buffered;
    buffered.swap(resync_fill_buffer_);
    std::vector<ResyncTerminal> terminals;
    terminals.swap(resync_terminal_buffer_);
    resync_buffering_ = false;

    const std::size_t buffered_total = buffered.size();
    ReplayPlan plan = partition_resync_replay(
        std::move(buffered), std::move(terminals), fence_update_ms, fence_floor_ms,
         positions != nullptr);

    const auto drain_terminals = [&] {
        for (const auto& t : plan.terminals) {
            hooks_.emit_through_dedup(t.upd, t.src, t.elig);
        }
        if (!plan.terminals.empty()) {
            XtLogger::warn_m("BinanceTD",
                "resync gate drained {} buffered terminal order update(s) after fills",
                plan.terminals.size());
        }
    };

    for (const auto& tr : plan.pre_fills) {
        if (cb_.on_trade) cb_.on_trade(tr, XtLatencyEligibility::kIneligible);
    }

    if (!plan.emit_position) {

        drain_terminals();
        return;
    }

    if (mark_recovering) {
        const bool prev = reconcile_in_progress_.exchange(true, std::memory_order_release);
        if (cb_.on_position) cb_.on_position(*positions);
        reconcile_in_progress_.store(prev, std::memory_order_release);
    } else if (cb_.on_position) {
        cb_.on_position(*positions);
    }

    for (const auto& tr : plan.post_fills) {
        if (cb_.on_trade) cb_.on_trade(tr, XtLatencyEligibility::kIneligible);
    }

    drain_terminals();

    if (buffered_total > 0) {
        XtLogger::warn_m("BinanceTD",
            "resync fill gate drained: buffered={} pre_snapshot={} post_snapshot={} "
            "fence_floor_ms={}",
            buffered_total, plan.pre_fills.size(), plan.post_fills.size(), fence_floor_ms);
    }
}

int64_t BinanceReconciler::sample_position_fence_floor_ms_() {
    auto t = rest_->get_public("/fapi/v1/time");
    if (t.ok()) {
        auto tj = nlohmann::json::parse(t.body, nullptr,  false);
        if (tj.is_object()) {
            const int64_t server_ms = tj.value("serverTime", 0LL);
            if (server_ms > 0) return server_ms;
        }
    }

    return static_cast<int64_t>(sign::now_ms()) + rest_->server_time_offset_ms() - 1000;
}

void BinanceReconciler::reconcile_resync() {

    (void)hooks_.wait_shutdown(500);

    ResyncFillGate fill_gate(*this);

    if (uds_) {
        const auto symbols = uds_->tracked_trade_symbols();
        for (const auto& sym : symbols) {
            resync_user_trades_for_symbol(sym);
        }
    }

    const int64_t fence_floor_ms = sample_position_fence_floor_ms_();

    auto arr = get_position_risk_array_("reconcile_resync");
    if (!arr) {
        return;
    }
    const nlohmann::json& j = *arr;

    std::vector<XtPositionSnapshot> positions;
    std::unordered_map<std::string, int64_t> fence_update_ms;
    positions.reserve(static_cast<size_t>(j.size()));
    fence_update_ms.reserve(static_cast<size_t>(j.size()));

    std::size_t dropped_tracked = 0;
    std::size_t dropped_other = 0;
    for (const auto& p : j) {
        const std::string symbol = defensive_symbol(p);
        if (symbol.empty()) {
            ++dropped_tracked;
            XtLogger::warn_m("BinanceTD",
                "reconcile_resync: row without parsable symbol dropped (fail-closed)");
            continue;
        }
        try {

            fence_update_ms[symbol] = p.value("updateTime", 0LL);
            const double amt = stod_finite(p.value("positionAmt", "0"));

            if (amt == 0.0) continue;
            XtPositionSnapshot ps;
            ps.code = symbol;
            if (amt > 0) {
                ps.long_qty   = amt;
                ps.long_today = amt;
                ps.long_avg   = stod_finite(p.value("entryPrice", "0"));
            } else if (amt < 0) {
                ps.short_qty   = -amt;
                ps.short_today = -amt;
                ps.short_avg   = stod_finite(p.value("entryPrice", "0"));
            }

            tracked_symbols_.note(symbol);
            positions.push_back(std::move(ps));
        } catch (...) {
            if (tracked_symbols_.contains(symbol)) ++dropped_tracked;
            else                                   ++dropped_other;
            XtLogger::warn_m("BinanceTD",
                "reconcile_resync: unparsable positionRisk row dropped symbol={}",
                symbol);
        }
    }
    if (dropped_tracked > 0) {
        XtLogger::warn_m("BinanceTD",
            "reconcile_resync: {} tracked/unknown unparsable row(s) → abort snapshot "
            "(reset+set would wipe symbols missing from a partial snapshot)",
            dropped_tracked);
        return;
    }
    if (dropped_other > 0) {
        XtLogger::warn_m("BinanceTD",
            "reconcile_resync: {} untracked unparsable row(s) skipped", dropped_other);
    }

    fill_gate.finish_with_snapshot(positions, fence_update_ms, fence_floor_ms,
                                    true);

    XtLogger::warn_m("BinanceTD",
        "reconcile_resync emitted {} position(s) is_recovery=true (overwrites local)",
        positions.size());

    const bool cold = is_first_recovery_.exchange(false);
    rebuild_alive_inflight_and_emit( cold);

    anchor_account_truth_after_resync_();
}

void BinanceReconciler::anchor_account_truth_after_resync_() {
    const uint64_t now_ms = lat_now_us() / 1000;
    if (!account_anchor_due(now_ms,
                            last_account_fetch_ms_.load(std::memory_order_relaxed),
                            kAccountAnchorMinIntervalMs)) {
        return;
    }
    auto det = fetch_account_detail();
    if (!det) return;
    if (cb_.on_account) cb_.on_account(det->acct);
    XtLogger::info_m("BinanceTD",
        "reconcile_resync account re-anchor balance={} available={}",
        det->acct.balance, det->acct.available);
}

std::optional<AccountTruthDetail> BinanceReconciler::fetch_account_detail() {
    auto ar = rest_->get_signed("/fapi/v2/account", {});
    if (!ar.ok()) {
        XtLogger::warn_m("BinanceTD",
            "GET account failed: http={} err='{}' body='{}'",
            ar.http_status, ar.curl_err, ar.body);
        return std::nullopt;
    }
    last_account_fetch_ms_.store(lat_now_us() / 1000, std::memory_order_relaxed);
    return parse_account_detail_body(ar.body);
}

std::unordered_map<std::string, int64_t> BinanceReconciler::fetch_position_marks() {
    auto arr = get_position_risk_array_("fetch_position_marks");
    if (!arr) return {};

    return parse_position_marks_body(arr->dump());
}

void BinanceReconciler::refresh_snapshot() {

    const auto t0 = sign::now_ms();
    auto det = fetch_account_detail();
    if (!det) {
        XtLogger::warn_m("BinanceTD",
            "refresh_snapshot done acct=FAIL elapsed_ms={}",
            sign::now_ms() - t0);
        return;
    }
    if (cb_.on_account) cb_.on_account(det->acct);

    auto marks = fetch_position_marks();
    for (auto& pos : det->per_symbol) {
        auto it = marks.find(pos.symbol);
        if (it != marks.end()) pos.mark_price_fp = it->second;
    }

    if (cb_.on_broker_account_truth) {
        BrokerVenueTruth bt;
        bt.available_balance_fp = det->available_balance_fp;
        bt.maint_margin_fp = det->maint_margin_fp;
        bt.total_margin_balance_fp = det->total_margin_balance_fp;
        bt.per_symbol = std::move(det->per_symbol);
        bt.per_symbol_is_broker_truth = true;
        cb_.on_broker_account_truth(bt);
    }

    XtLogger::info_m("BinanceTD",
        "refresh_snapshot done acct=ok elapsed_ms={}",
        sign::now_ms() - t0);
}

void BinanceReconciler::bootstrap_snapshot() {

    {
        const uint32_t attempt = bootstrap_attempts_.fetch_add(1, std::memory_order_relaxed);
        int64_t backoff_ms = 30000LL << (attempt < 4u ? attempt : 4u);
        if (backoff_ms > 300000LL) backoff_ms = 300000LL;
        bootstrap_next_retry_ms_.store(static_cast<int64_t>(lat_now_us() / 1000) + backoff_ms,
                                       std::memory_order_relaxed);
    }

    ResyncFillGate fill_gate(*this);
    const int64_t fence_floor_ms = sample_position_fence_floor_ms_();
    std::unordered_map<std::string, int64_t> fence_update_ms;
    std::vector<XtPositionSnapshot> positions;
    auto pr = rest_->get_signed("/fapi/v2/positionRisk", {});
    if (pr.ok()) {
        auto j = nlohmann::json::parse(pr.body, nullptr,  false);
        if (j.is_array()) {
            fence_update_ms.reserve(static_cast<size_t>(j.size()));
            for (const auto& p : j) {
                try {
                    const std::string symbol = p.value("symbol", std::string{});
                    if (symbol.empty()) continue;

                    fence_update_ms[symbol] = p.value("updateTime", 0LL);
                    const double amt = stod_finite(p.value("positionAmt", "0"));
                    if (amt == 0.0) continue;
                    XtPositionSnapshot ps;
                    ps.code = symbol;
                    const double entry = stod_finite(p.value("entryPrice", "0"));
                    if (amt > 0) { ps.long_qty = amt; ps.long_avg = entry; }
                    else         { ps.short_qty = -amt; ps.short_avg = entry; }

                    XtLogger::warn_m("BinanceTD",
                        "bootstrap position detail symbol={} positionAmt={} positionSide='{}' entry={}",
                        ps.code, amt, p.value("positionSide", std::string("?")), entry);
                    tracked_symbols_.note(ps.code);
                    positions.push_back(std::move(ps));
                } catch (...) {

                    const std::string symbol = defensive_symbol(p);
                    if (!symbol.empty()) tracked_symbols_.note(symbol);

                    bool provable_nonzero_position = false;
                    try {

                        const auto it_amt = p.find("positionAmt");
                        if (it_amt != p.end() && it_amt->is_number()) {
                            provable_nonzero_position = it_amt->get<double>() != 0.0;
                        } else {
                            provable_nonzero_position =
                                stod_finite(p.value("positionAmt", "0")) != 0.0;
                        }
                    } catch (...) {

                    }
                    if (provable_nonzero_position) {
                        XtLogger::error_m("BinanceTD",
                            "bootstrap: unparsable non-zero positionRisk row symbol='{}'; "
                            "hold Position readiness fail-closed", symbol);
                        readiness_gate_.fail(
                            xt_live::gateway::TdReadinessStage::Position,
                            "unparsable non-zero positionRisk row symbol=" + symbol);
                        return;
                    }
                    XtLogger::warn_m("BinanceTD",
                        "bootstrap: unparsable positionRisk row dropped symbol='{}' "
                        "(seeded to tracked set)", symbol);
                }
            }
            readiness_gate_.satisfy(xt_live::gateway::TdReadinessStage::Position,
                                    "binance positionRisk snapshot parsed");
        } else {
            readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Position,
                                 "binance positionRisk response was not an array");
            XtLogger::error_m("BinanceTD",
                "bootstrap positionRisk body not array; hold TD readiness fail-closed");
            return;
        }
        XtLogger::info_m("BinanceTD",
            "bootstrap positions: {} non-zero detected",
            positions.size());
        if (!positions.empty()) {
            XtLogger::warn_m("BinanceTD",
                "bootstrap importing {} inherited position(s) via position snapshot recovery",
                positions.size());
        }
    } else {
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Position,
                             "binance positionRisk request failed");
        XtLogger::warn_m("BinanceTD",
            "bootstrap positionRisk failed: http={} err='{}' body='{}'",
            pr.http_status, pr.curl_err, pr.body);
        return;
    }

    ITradeGateway::AccountSnapshot acct{};
    auto ar = rest_->get_signed("/fapi/v2/account", {});
    bool account_truth_ok = false;
    if (ar.ok()) {
        auto j = nlohmann::json::parse(ar.body, nullptr,  false);
        if (j.is_object()) {
            try {

                acct.balance = stod_finite(j.value("totalWalletBalance", "0"));
                acct.available = stod_finite(j.value("availableBalance", "0"));
                acct.margin = stod_finite(j.value("totalInitialMargin", "0"));
                acct.position_profit = stod_finite(j.value("totalUnrealizedProfit", "0"));
                XtLogger::info_m("BinanceTD",
                    "bootstrap account: wallet={:.2f} available={:.2f} margin={:.2f} unrealized={:.2f}",
                    acct.balance, acct.available, acct.margin, acct.position_profit);
                account_truth_ok = true;
            } catch (...) {
                readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Account,
                                     "binance bootstrap account parse error");
                XtLogger::warn_m("BinanceTD", "bootstrap account parse error: {}", ar.body);
            }
        } else {
            readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Account,
                                 "binance bootstrap account response was not an object");
        }
    } else {
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Account,
                             "binance bootstrap account request failed");
        XtLogger::warn_m("BinanceTD",
            "bootstrap account failed: http={} err='{}' body='{}'",
            ar.http_status, ar.curl_err, ar.body);
    }
    if (!account_truth_ok) return;
    readiness_gate_.satisfy(xt_live::gateway::TdReadinessStage::Account,
                            "binance account snapshot parsed");
    if (cb_.on_account) cb_.on_account(acct);

    fill_gate.finish_with_snapshot(positions, fence_update_ms, fence_floor_ms,
                                    false);

    const bool cold = is_first_recovery_.exchange(false);
    rebuild_alive_inflight_and_emit( cold);
    hooks_.emit_ready_if_td_readiness_satisfied();
}

}
