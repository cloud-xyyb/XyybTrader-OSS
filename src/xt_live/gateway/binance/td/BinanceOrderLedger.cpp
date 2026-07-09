#include "td/BinanceOrderLedger.h"

#include <algorithm>
#include <utility>

#include "transport/BinanceTdWireUtil.h"
#include "transport/ITradeTransport.h"

namespace binance_u {

void BinanceOrderLedger::register_submit(uint64_t local_id, OrderRecord rec) {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    rec.has_pending = true;
    rec.has_inflight = true;
    client_to_local_[rec.client_order_id] = local_id;
    records_[local_id] = std::move(rec);
}

void BinanceOrderLedger::upsert_recovered(uint64_t local_id, OrderRecord venue_rec) {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    if (it == records_.end()) {
        venue_rec.has_pending = false;
        venue_rec.has_inflight = true;
        records_.emplace(local_id, std::move(venue_rec));
        return;
    }
    OrderRecord& rec = it->second;

    if (rec.client_order_id.empty()) rec.client_order_id = std::move(venue_rec.client_order_id);
    if (rec.symbol.empty()) rec.symbol = std::move(venue_rec.symbol);
    if (!rec.has_inflight) {

        rec.sys_id = std::move(venue_rec.sys_id);
        rec.status = venue_rec.status;
        rec.is_buy = venue_rec.is_buy;
        rec.price_fp = venue_rec.price_fp;
        rec.qty_fp = venue_rec.qty_fp;
        rec.close_offset = venue_rec.close_offset;
        rec.submit_ts_ms = venue_rec.submit_ts_ms;
        rec.has_inflight = true;
    }
}

void BinanceOrderLedger::bind_sys_id_if_blank(uint64_t local_id, const std::string& sys_id) {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    if (it == records_.end() || !it->second.has_inflight) return;
    if (!it->second.sys_id.empty()) return;
    it->second.sys_id = sys_id;
    it->second.status = XtOrderStatus::Accepted;
}

void BinanceOrderLedger::apply_record_fields_(const OrderRecord& rec,
                                              XtOrderExecUpdate& upd) {
    upd.is_buy = rec.is_buy;
    upd.close_offset = rec.close_offset;
    upd.price_fp = rec.price_fp;
    upd.qty_fp = rec.qty_fp;
    if (upd.filled_fp <= 0) upd.filled_fp = 0;
    if (upd.leaves_fp <= 0) upd.leaves_fp = std::max<int64_t>(0, upd.qty_fp - upd.filled_fp);
}

void BinanceOrderLedger::erase_inflight_unlocked_(OrderRecord& rec) {
    rec.has_inflight = false;
    rec.trace_id = 0;
}

void BinanceOrderLedger::erase_client_route_unlocked_(OrderRecord& rec) {
    if (rec.client_order_id.empty()) return;
    client_to_local_.erase(rec.client_order_id);
    rec.client_order_id.clear();
}

BinanceOrderLedger::TerminalDecision
BinanceOrderLedger::on_ack(const TransportAck& ack, uint64_t local_cb_us) {
    TerminalDecision d;

    if (ack.req_kind == TransportAck::ReqKind::kCancel &&
        ack.kind != TransportAck::Kind::kCanceled) {
        d.action = TerminalDecision::Action::kKeepInflight;
        return d;
    }

    d.action = TerminalDecision::Action::kEmit;
    XtOrderExecUpdate& upd = d.update;
    upd.id = ack.local_id;
    upd.local_cb_us = local_cb_us;

    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(ack.local_id);

    if (it != records_.end() && it->second.has_pending) {
        it->second.has_pending = false;
        const OrderRecord& rec = it->second;
        upd.is_buy = rec.is_buy;
        upd.close_offset = rec.close_offset;
        upd.price_fp = rec.price_fp;
        upd.qty_fp = rec.qty_fp;
        upd.filled_fp = ack.filled_fp;
        upd.leaves_fp = rec.qty_fp - ack.filled_fp;
        upd.last_fill_fp = ack.filled_fp;
    } else if (it != records_.end() && it->second.has_inflight) {
        upd.filled_fp = ack.filled_fp;
        upd.last_fill_fp = ack.filled_fp;
        apply_record_fields_(it->second, upd);
    } else {
        upd.filled_fp = 0;
        upd.last_fill_fp = 0;
    }

    d.terminal_seen_before =
        terminal_orders_.find(ack.local_id) != terminal_orders_.end();

    switch (ack.kind) {
    case TransportAck::Kind::kAccepted:
        upd.status = parse_binance_status(ack.raw_status);

        if (!ack.sys_id.empty() && !d.terminal_seen_before &&
            it != records_.end() && it->second.has_inflight) {
            it->second.sys_id = ack.sys_id;
            it->second.status = upd.status;
        }
        if (order_status_is_final(upd.status)) {
            upd.leaves_fp = 0;
            remember_terminal_unlocked_(ack.local_id);
            if (it != records_.end()) erase_inflight_unlocked_(it->second);
        }
        break;
    case TransportAck::Kind::kCanceled:

        upd.status = XtOrderStatus::Canceled;
        remember_terminal_unlocked_(ack.local_id);
        if (it != records_.end()) erase_inflight_unlocked_(it->second);
        break;
    case TransportAck::Kind::kRejected:
        upd.status = XtOrderStatus::Rejected;
        upd.leaves_fp = 0;
        remember_terminal_unlocked_(ack.local_id);
        if (it != records_.end()) erase_inflight_unlocked_(it->second);
        break;
    case TransportAck::Kind::kTransportError_Stale:
    case TransportAck::Kind::kTransportError_TimeoutBeforeSend:
    case TransportAck::Kind::kTransportError_QueueOverflow:
    case TransportAck::Kind::kTransportError_Internal:

        upd.status = XtOrderStatus::Rejected;
        upd.leaves_fp = 0;
        remember_terminal_unlocked_(ack.local_id);
        if (it != records_.end()) erase_inflight_unlocked_(it->second);
        break;
    case TransportAck::Kind::kTransportError_TimeoutAfterSend:
    case TransportAck::Kind::kTransportError_Disconnect:

        upd.status = XtOrderStatus::Submitted;
        break;
    default:
        upd.status = XtOrderStatus::Rejected;
        upd.leaves_fp = 0;
        remember_terminal_unlocked_(ack.local_id);
        if (it != records_.end()) erase_inflight_unlocked_(it->second);
        break;
    }
    return d;
}

BinanceOrderLedger::TerminalDecision
BinanceOrderLedger::on_uds_terminal(const XtOrderExecUpdate& upd) {
    TerminalDecision d;
    d.action = TerminalDecision::Action::kEmit;
    if (!order_status_is_final(upd.status)) return d;
    std::lock_guard<std::mutex> lk(ledger_mu_);
    remember_terminal_unlocked_(upd.id);
    auto it = records_.find(upd.id);
    if (it == records_.end()) return d;
    erase_inflight_unlocked_(it->second);
    erase_client_route_unlocked_(it->second);
    return d;
}

BinanceOrderLedger::TerminalDecision
BinanceOrderLedger::on_reconcile_terminal(uint64_t local_id, ReconcileTerminal kind,
                                          XtOrderExecUpdate& upd) {
    TerminalDecision d;
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    switch (kind) {
    case ReconcileTerminal::kNotExistCanceled:

        if (it == records_.end() || !it->second.has_inflight) return d;
        apply_record_fields_(it->second, upd);
        remember_terminal_unlocked_(local_id);
        break;
    case ReconcileTerminal::kBlankSysIdReject:

        if (it == records_.end() || !it->second.has_inflight) return d;
        if (!it->second.sys_id.empty()) return d;
        apply_record_fields_(it->second, upd);
        remember_terminal_unlocked_(local_id);
        erase_inflight_unlocked_(it->second);
        break;
    case ReconcileTerminal::kVenueTerminal:

        remember_terminal_unlocked_(local_id);
        if (it != records_.end()) erase_inflight_unlocked_(it->second);
        break;
    }
    d.action = TerminalDecision::Action::kEmit;
    return d;
}

bool BinanceOrderLedger::terminal_seen(uint64_t local_id) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    return terminal_orders_.find(local_id) != terminal_orders_.end();
}

void BinanceOrderLedger::remember_terminal_unlocked_(uint64_t local_id) {
    if (local_id == 0) return;
    if (terminal_orders_.insert(local_id).second) {
        terminal_order_fifo_.push_back(local_id);
    }
    while (terminal_orders_.size() > kTerminalOrderCacheCap && !terminal_order_fifo_.empty()) {
        const uint64_t old = terminal_order_fifo_.front();
        terminal_order_fifo_.pop_front();
        terminal_orders_.erase(old);
    }
}

void BinanceOrderLedger::remember_terminal(uint64_t local_id) {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    remember_terminal_unlocked_(local_id);
}

bool BinanceOrderLedger::is_inflight(uint64_t local_id) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    return it != records_.end() && it->second.has_inflight;
}

std::string BinanceOrderLedger::sys_id_for(uint64_t local_id) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    if (it == records_.end() || !it->second.has_inflight) return std::string();
    return it->second.sys_id;
}

std::string BinanceOrderLedger::client_order_id_for(uint64_t local_id) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    return it == records_.end() ? std::string() : it->second.client_order_id;
}

std::optional<uint64_t>
BinanceOrderLedger::local_for_client(std::string_view client_order_id) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    const auto it = client_to_local_.find(std::string(client_order_id));
    if (it == client_to_local_.end()) return std::nullopt;
    return it->second;
}

uint64_t BinanceOrderLedger::trace_id_for(uint64_t local_id) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    return it == records_.end() ? 0 : it->second.trace_id;
}

std::string BinanceOrderLedger::symbol_for(uint64_t local_id) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    return it == records_.end() ? std::string() : it->second.symbol;
}

std::vector<BinanceOrderLedger::InflightBrief>
BinanceOrderLedger::inflight_snapshot() const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    std::vector<InflightBrief> out;
    out.reserve(records_.size());
    for (const auto& [local_id, rec] : records_) {
        if (!rec.has_inflight) continue;
        InflightBrief b;
        b.local_id = local_id;
        b.sys_id = rec.sys_id;
        b.symbol = rec.symbol;
        b.submit_ts_ms = rec.submit_ts_ms;
        b.close_offset = rec.close_offset;
        out.push_back(std::move(b));
    }
    return out;
}

std::optional<BinanceOrderLedger::InflightBrief>
BinanceOrderLedger::find_inflight_by_sys_id(std::string_view sys_id) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    for (const auto& [local_id, rec] : records_) {
        if (!rec.has_inflight || rec.sys_id != sys_id) continue;
        InflightBrief b;
        b.local_id = local_id;
        b.sys_id = rec.sys_id;
        b.symbol = rec.symbol;
        b.submit_ts_ms = rec.submit_ts_ms;
        b.close_offset = rec.close_offset;
        return b;
    }
    return std::nullopt;
}

}
