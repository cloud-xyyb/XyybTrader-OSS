#include "ctp/recovery/CTPStateTracker.h"
#include "XtFixedPoint.h"
#include "ctp/common/CTPUtil.h"

#include "xt_identity/OrderAttributionStore.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_util/time/XtTime.h"
#include "xt_util/time/XtTimeParse.h"

#include <algorithm>
#include <cmath>

namespace {

using ctp_util::parse_local_id;

XtOrderStatus map_ctp_order_status(const CThostFtdcOrderField* pOrder) {
    if (!pOrder) return XtOrderStatus::Submitted;
    if (pOrder->OrderSubmitStatus == THOST_FTDC_OSS_InsertRejected) {
        return XtOrderStatus::Rejected;
    }
    return ctp_util::map_order_status(pOrder->OrderStatus);
}

}

void CtpStateTracker::register_order(uint64_t local_id,
                                     const char* order_ref,
                                     const char* code,
                                     bool is_buy,
                                     double price,
                                     int64_t qty_fp,
                                     const char* user_tag,
                                     bool is_close,
                                     bool is_today,
                                     uint64_t submit_begin_us,
                                     uint64_t submit_end_us) {
    LocalOrderState state;
    state.ref = order_ref ? order_ref : "";
    state.code = code ? code : "";
    state.is_buy = is_buy;
    state.price = price;
    state.qty_fp = qty_fp;
    state.user_tag = user_tag ? user_tag : "";
    state.is_close = is_close;
    state.is_today = is_today;
    state.submit_begin_us = submit_begin_us;
    state.submit_end_us = submit_end_us;
    std::lock_guard<std::mutex> guard(mu_);

    deduper_.forget(local_id);
    clear_terminal_local(local_id);
    local_[local_id] = state;
}

bool CtpStateTracker::get_order_ref(uint64_t local_id, std::string& out_ref) const {
    std::lock_guard<std::mutex> guard(mu_);
    auto it = local_.find(local_id);
    if (it == local_.end()) return false;
    out_ref = it->second.ref;
    return !out_ref.empty();
}

bool CtpStateTracker::has_local_order_ref(const char* order_ref) const {
    const uint64_t local_id = parse_local_id(order_ref);
    if (local_id == 0) return false;
    std::lock_guard<std::mutex> guard(mu_);
    return local_.find(local_id) != local_.end();
}

bool CtpStateTracker::get_cancel_target(uint64_t local_id, CancelTarget& out) const {
    std::lock_guard<std::mutex> guard(mu_);

    if (terminal_local_ids_.find(local_id) != terminal_local_ids_.end()) {
        return false;
    }
    auto it = local_.find(local_id);
    if (it == local_.end()) return false;
    const LocalOrderState& state = it->second;
    const std::string key = build_terminal_order_key(state.exchange_id, state.order_sys_id);
    if (!key.empty() && terminal_order_keys_.find(key) != terminal_order_keys_.end()) {
        return false;
    }
    out.ref = state.ref;
    out.code = state.code;
    out.exchange_id = state.exchange_id;
    out.order_sys_id = state.order_sys_id;
    out.front_id = state.front_id;
    out.session_id = state.session_id;
    return !out.ref.empty();
}

bool CtpStateTracker::is_local_terminal(uint64_t local_id) const {
    if (local_id == 0) return false;
    std::lock_guard<std::mutex> guard(mu_);
    return terminal_local_ids_.find(local_id) != terminal_local_ids_.end();
}

void CtpStateTracker::on_recovery_begin() {
    std::lock_guard<std::mutex> guard(mu_);

    trade_seen_.clear();
    trade_seen_fifo_.clear();
}

bool CtpStateTracker::on_order_insert_reject(const CThostFtdcInputOrderField* pInputOrder,
                                             const CThostFtdcRspInfoField* pRspInfo,
                                             uint64_t local_cb_us,
                                             XtOrderExecUpdate& out) {
    if (!pInputOrder) return false;
    const uint64_t id = parse_local_id(pInputOrder->OrderRef);
    LocalOrderState state;
    const bool has_state = try_get_local(id, state);
    XtOrderExecUpdate upd;
    upd.id = id;
    upd.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(id).raw();
    upd.is_buy = (pInputOrder->Direction == THOST_FTDC_D_Buy);
    upd.status = XtOrderStatus::Rejected;
    upd.price_fp = order_exec_to_fp(pInputOrder->LimitPrice);
    upd.qty_fp = order_exec_to_fp(pInputOrder->VolumeTotalOriginal);
    upd.filled_fp = 0;
    upd.leaves_fp = upd.qty_fp;
    upd.last_fill_fp = 0;
    const char offset = pInputOrder->CombOffsetFlag[0];
    upd.close_offset = (offset == THOST_FTDC_OF_Open) ? XtOffset::Open
                                                      : ctp_util::to_xt_close_offset(offset);

    (void)pRspInfo;
    upd.local_cb_us = local_cb_us;
    if (has_state) {
        apply_local_to_update(state, upd);
    }
    mark_terminal(id, has_state ? &state : nullptr);
    if (!should_emit(id, upd)) return false;
    out = std::move(upd);
    return true;
}

bool CtpStateTracker::on_order(const CThostFtdcOrderField* pOrder,
                               uint64_t local_cb_us,
                               XtOrderExecUpdate& out,
                               bool* out_suppressed_terminal) {
    if (!pOrder) return false;
    const uint64_t id = parse_local_id(pOrder->OrderRef);
    if (id != 0) {
        update_local_cancel_fields(id, pOrder);
    }
    LocalOrderState state;
    const bool has_state = try_get_local(id, state);
    XtOrderExecUpdate upd;
    upd.id = id;
    upd.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(id).raw();
    upd.is_buy = (pOrder->Direction == THOST_FTDC_D_Buy);
    upd.status = map_ctp_order_status(pOrder);
    upd.price_fp = order_exec_to_fp(pOrder->LimitPrice);
    upd.qty_fp = order_exec_to_fp(pOrder->VolumeTotalOriginal);
    upd.filled_fp = order_exec_to_fp(pOrder->VolumeTraded);
    upd.leaves_fp = order_exec_to_fp(pOrder->VolumeTotal);
    upd.last_fill_fp = 0;
    const char offset = pOrder->CombOffsetFlag[0];
    upd.close_offset = (offset == THOST_FTDC_OF_Open) ? XtOffset::Open
                                                      : ctp_util::to_xt_close_offset(offset);
    upd.local_cb_us = local_cb_us;
    if (has_state) {
        apply_local_to_update(state, upd);
    }

    if (id != 0 && !order_status_is_final(upd.status) &&
        order_lifecycle::is_final_update(upd)) {
        upd.status = (upd.filled_fp >= upd.qty_fp && upd.qty_fp > 0)
                         ? XtOrderStatus::Filled
                         : XtOrderStatus::Canceled;
    }
    bool newly_terminal = false;
    if (order_status_is_final(upd.status)) {
        newly_terminal = mark_terminal(id, has_state ? &state : nullptr);
    }
    if (!should_emit(id, upd)) {

        if (out_suppressed_terminal && newly_terminal && id != 0) {
            out = std::move(upd);
            *out_suppressed_terminal = true;
        }
        return false;
    }
    out = std::move(upd);
    return true;
}

bool CtpStateTracker::on_trade(const CThostFtdcTradeField* pTrade,
                               uint64_t local_cb_us,
                               XtTradeRecord& out) {
    if (!pTrade) return false;
    const uint64_t id = parse_local_id(pTrade->OrderRef);
    XtTradeRecord tr;
    tr.order_id = id;

    tr.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(id).raw();
    if (const char* sid = xt_identity::StrategySidRegistry::resolve_sid_cstr_or_null(tr.sid_id)) {
        tr.strategy_id = sid;
    }
    tr.code = pTrade->InstrumentID;
    tr.exchange_id = pTrade->ExchangeID;
    tr.trade_id = pTrade->TradeID;
    tr.order_ref = pTrade->OrderRef;
    tr.order_sys_id = pTrade->OrderSysID;
    tr.is_buy = (pTrade->Direction == THOST_FTDC_D_Buy);
    tr.is_close = (pTrade->OffsetFlag != THOST_FTDC_OF_Open);
    tr.is_today = (pTrade->OffsetFlag == THOST_FTDC_OF_CloseToday);
    tr.close_offset = ctp_util::to_xt_close_offset(pTrade->OffsetFlag);
    tr.qty_fp = order_exec_to_fp(pTrade->Volume);
    tr.price = pTrade->Price;
    tr.exec_price = pTrade->Price;
    tr.trade_date = XtTimeParse::parse_fixed_date_yyyymmdd(pTrade->TradeDate);
    tr.trade_time = XtTimeParse::parse_fixed_time_hms_ms(pTrade->TradeTime, 0);

    tr.trading_day = XtTimeParse::parse_fixed_date_yyyymmdd(pTrade->TradingDay);
    tr.local_cb_us = local_cb_us;

    tr.fill_ts_us = xt_time::cst_wall_to_epoch_us(tr.trade_date, tr.trade_time);

    std::string tkey;
    tkey.reserve(80);
    tkey.append(pTrade->TradingDay);
    tkey.push_back('|');
    tkey.append(pTrade->ExchangeID);
    tkey.push_back('|');
    tkey.append(pTrade->TradeID);
    tkey.push_back('|');
    tkey.append(pTrade->OrderSysID);
    tkey.push_back('|');
    tkey.append(pTrade->OrderRef);
    {
        std::lock_guard<std::mutex> guard(mu_);
        if (trade_seen_.find(tkey) != trade_seen_.end()) {
            return false;
        }
        trade_seen_.insert(tkey);
        trade_seen_fifo_.push_back(tkey);

        constexpr size_t kTradeSeenCap = 200000;
        while (trade_seen_fifo_.size() > kTradeSeenCap) {
            const std::string& old = trade_seen_fifo_.front();
            trade_seen_.erase(old);
            trade_seen_fifo_.pop_front();
        }
    }
    out = std::move(tr);
    accumulate_local_trade_qty(id, order_exec_from_fp(out.qty_fp));
    LocalOrderState state;
    if (try_get_local(id, state)) apply_local_to_trade(state, out);
    return true;
}

bool CtpStateTracker::on_cancel_action_error(const char* order_ref,
                                             int error_id,
                                             uint64_t local_cb_us,
                                             XtOrderExecUpdate& out) {

    if (error_id != 26) return false;
    const uint64_t id = parse_local_id(order_ref);
    if (id == 0) return false;

    LocalOrderState state;
    if (!try_get_local(id, state)) return false;

    XtOrderExecUpdate upd;
    upd.id = id;
    upd.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(id).raw();
    upd.is_buy = state.is_buy;
    upd.price_fp = order_exec_to_fp(state.price);
    upd.qty_fp = state.qty_fp;
    const double qty = xt_fp::from_fp(state.qty_fp);
    const double filled = std::min(std::max(0.0, state.traded_qty), qty);
    const double remaining = std::max(0.0, qty - filled);
    upd.filled_fp = order_exec_to_fp(filled);
    upd.leaves_fp = order_exec_to_fp(remaining);
    upd.last_fill_fp = 0;
    upd.close_offset = state.is_close ? (state.is_today ? XtOffset::CloseToday : XtOffset::CloseYesterday)
                                      : XtOffset::Open;
    upd.local_cb_us = local_cb_us;
    upd.status = (remaining <= 1e-8) ? XtOrderStatus::Filled : XtOrderStatus::Canceled;
    apply_local_to_update(state, upd);

    mark_terminal(id, &state);
    if (!should_emit(id, upd)) return false;
    out = std::move(upd);
    return true;
}

bool CtpStateTracker::try_get_local(uint64_t local_id, LocalOrderState& out) const {
    std::lock_guard<std::mutex> guard(mu_);
    auto it = local_.find(local_id);
    if (it == local_.end()) return false;
    out = it->second;
    return true;
}

void CtpStateTracker::apply_local_to_update(const LocalOrderState& state, XtOrderExecUpdate& upd) {
    if (state.is_close) {
        upd.close_offset = state.is_today ? XtOffset::CloseToday : XtOffset::CloseYesterday;
    } else {
        upd.close_offset = XtOffset::Open;
    }
}

void CtpStateTracker::apply_local_to_trade(const LocalOrderState& state, XtTradeRecord& tr) {
    tr.user_tag = state.user_tag;

    tr.local_submit_begin_us = state.submit_begin_us;
    tr.local_submit_end_us = state.submit_end_us;
    if (tr.close_offset == XtOffset::Close && state.is_close) {
        tr.close_offset = state.is_today ? XtOffset::CloseToday : XtOffset::CloseYesterday;
    }
}

void CtpStateTracker::update_local_cancel_fields(uint64_t local_id, const CThostFtdcOrderField* pOrder) {
    if (local_id == 0 || !pOrder) return;
    std::lock_guard<std::mutex> guard(mu_);
    auto it = local_.find(local_id);
    if (it == local_.end()) return;
    LocalOrderState& state = it->second;
    if (pOrder->OrderRef[0] != '\0') state.ref = pOrder->OrderRef;
    if (pOrder->InstrumentID[0] != '\0') state.code = pOrder->InstrumentID;
    if (pOrder->ExchangeID[0] != '\0') state.exchange_id = pOrder->ExchangeID;
    if (pOrder->OrderSysID[0] != '\0') state.order_sys_id = pOrder->OrderSysID;

    if (pOrder->FrontID != 0) state.front_id = pOrder->FrontID;
    if (pOrder->SessionID != 0) state.session_id = pOrder->SessionID;
}

void CtpStateTracker::backfill_cancel_routing(uint64_t local_id,
                                              const std::string& exchange_id,
                                              const std::string& order_sys_id,
                                              int front_id,
                                              int session_id) {
    if (local_id == 0) return;
    std::lock_guard<std::mutex> guard(mu_);
    auto it = local_.find(local_id);
    if (it == local_.end()) return;
    LocalOrderState& state = it->second;
    if (!exchange_id.empty()) state.exchange_id = exchange_id;
    if (!order_sys_id.empty()) state.order_sys_id = order_sys_id;
    if (front_id != 0) state.front_id = front_id;
    if (session_id != 0) state.session_id = session_id;
}

void CtpStateTracker::accumulate_local_trade_qty(uint64_t local_id, double qty) {
    if (local_id == 0 || !std::isfinite(qty) || qty <= 0.0) return;
    std::lock_guard<std::mutex> guard(mu_);
    auto it = local_.find(local_id);
    if (it == local_.end()) return;
    it->second.traded_qty += qty;
}

bool CtpStateTracker::should_emit(uint64_t local_id, const XtOrderExecUpdate& upd) {
    if (local_id == 0) return true;
    std::lock_guard<std::mutex> guard(mu_);

    if (!deduper_.should_emit(local_id, upd, ctp_util::status_rank)) {
        return false;
    }
    if (order_lifecycle::is_final_update(upd)) {
        auto lit = local_.find(local_id);
        if (lit != local_.end()) {
            local_.erase(lit);
        }
    }
    return true;
}

std::string CtpStateTracker::build_terminal_order_key(const std::string& exchange_id,
                                                      const std::string& order_sys_id) {
    if (exchange_id.empty() || order_sys_id.empty()) return "";
    std::string key;
    key.reserve(exchange_id.size() + order_sys_id.size() + 1);
    key.append(exchange_id);
    key.push_back('|');
    key.append(order_sys_id);
    return key;
}

bool CtpStateTracker::mark_terminal(uint64_t local_id, const LocalOrderState* state) {
    if (local_id == 0) return false;
    std::lock_guard<std::mutex> guard(mu_);
    const bool newly_terminal = terminal_local_ids_.insert(local_id).second;
    if (newly_terminal) {
        terminal_local_fifo_.push_back(local_id);
        constexpr size_t kTerminalLocalCap = 200000;
        while (terminal_local_fifo_.size() > kTerminalLocalCap) {
            const uint64_t old = terminal_local_fifo_.front();
            terminal_local_ids_.erase(old);
            terminal_local_fifo_.pop_front();
        }
    }

    if (state) {
        const std::string key = build_terminal_order_key(state->exchange_id, state->order_sys_id);
        remember_terminal_key_unlocked(key);
    }

    auto it = local_.find(local_id);
    if (it != local_.end()) {
        const std::string key = build_terminal_order_key(it->second.exchange_id, it->second.order_sys_id);
        remember_terminal_key_unlocked(key);
        local_.erase(it);
    }
    return newly_terminal;
}

void CtpStateTracker::clear_terminal_local(uint64_t local_id) {
    if (local_id == 0) return;
    if (terminal_local_ids_.erase(local_id) == 0) return;
    for (auto it = terminal_local_fifo_.begin(); it != terminal_local_fifo_.end(); ++it) {
        if (*it != local_id) continue;
        terminal_local_fifo_.erase(it);
        break;
    }
}

void CtpStateTracker::remember_terminal_key_unlocked(const std::string& key) {
    if (key.empty()) return;
    if (!terminal_order_keys_.insert(key).second) return;
    terminal_order_key_fifo_.push_back(key);
    constexpr size_t kTerminalOrderKeyCap = 200000;
    while (terminal_order_key_fifo_.size() > kTerminalOrderKeyCap) {
        const std::string& old = terminal_order_key_fifo_.front();
        terminal_order_keys_.erase(old);
        terminal_order_key_fifo_.pop_front();
    }
}
