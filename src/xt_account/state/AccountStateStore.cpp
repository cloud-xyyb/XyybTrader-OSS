#include "state/AccountStateStore.h"

#include "XtFixedPoint.h"
#include "state/AccountState.h"
#include "state/AccountReducer.h"
#include "xt_account/api/AccountSnapshot.h"
#include "state/MarkPriceTable.h"
#include "xt_account/api/AccountMarking.h"
#include "xt_config/catalog/XtCommodityCatalog.h"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <sstream>
#include <stdexcept>
#include <string_view>

namespace xt_account_store_drops {
std::atomic<uint64_t>& cross_thread_write() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}
}

namespace xt_account {

namespace {

using PositionPool = xt_util::sync::XtSnapshotPool<PositionSnapshot, kMaxSymbols * 4>;
using RiskPool = xt_util::sync::XtSnapshotPool<RiskSnapshot, kMaxSymbols * 4>;
using FullPool = xt_util::sync::XtSnapshotPool<FullSnapshot, 64>;

template <typename Pool>
auto acquire_after_reclaim_scan(Pool& pool, xt_util::sync::HazardDomain& domain) noexcept
    -> decltype(pool.acquire()) {
    auto* fresh = pool.acquire();
    if (!fresh) {
        domain.try_scan();
        fresh = pool.acquire();
    }
    return fresh;
}

void release_position_snapshot(void* ctx, const void* ptr) noexcept {
    static_cast<PositionPool*>(ctx)->release(const_cast<PositionSnapshot*>(
        static_cast<const PositionSnapshot*>(ptr)));
}

void release_risk_snapshot(void* ctx, const void* ptr) noexcept {
    static_cast<RiskPool*>(ctx)->release(const_cast<RiskSnapshot*>(
        static_cast<const RiskSnapshot*>(ptr)));
}

void release_full_snapshot(void* ctx, const void* ptr) noexcept {
    static_cast<FullPool*>(ctx)->release(const_cast<FullSnapshot*>(
        static_cast<const FullSnapshot*>(ptr)));
}

void audit_frozen_direct(int64_t frozen_cash_fp,
                         const std::unordered_map<uint64_t, AccountState::FrozenOrderSlot>& slots,
                         account_audit::XtFrozenAuditReport& out) {
    out.ok = true;
    out.reason.clear();
    out.frozen_cash = xt_fp::from_fp(frozen_cash_fp);

    int64_t total_fp = 0;
    for (const auto& kv : slots) {
        total_fp += kv.second.frozen;
    }
    out.frozen_cash_by_orders = xt_fp::from_fp(total_fp);

    if (frozen_cash_fp < 0) {
        out.ok = false;
        out.reason = "frozen_cash negative";
        return;
    }
    if (total_fp == frozen_cash_fp) return;

    out.ok = false;
    std::ostringstream oss;
    oss << "frozen_cash mismatch runtime=" << out.frozen_cash
        << " tracked=" << out.frozen_cash_by_orders
        << " slot_count=" << slots.size()
        << " slots=[";
    constexpr std::size_t kMaxDumpSlots = 64;
    std::size_t dumped = 0;
    for (const auto& kv : slots) {
        if (dumped >= kMaxDumpSlots) break;
        if (dumped > 0) oss << ",";
        oss << "{oid=" << kv.first
            << " frozen=" << xt_fp::from_fp(kv.second.frozen)
            << " filled=" << xt_fp::from_fp(kv.second.filled)
            << " per_unit=" << xt_fp::from_fp(kv.second.per_unit)
            << "}";
        ++dumped;
    }
    if (slots.size() > kMaxDumpSlots) {
        oss << ",... omitted=" << (slots.size() - kMaxDumpSlots);
    }
    oss << "]";
    out.reason = oss.str();
}

}

AccountStateStore::AccountStateStore(const AccountState::Config& cfg)
    : account_(cfg, AccountState::CtorToken{}), mark_cache_() {

    if (cfg.futures_mode && cfg.catalog == nullptr)
        throw std::invalid_argument(
            "AccountStateStore: futures_mode requires a non-null commodity catalog (vol_scale source)");
}

void AccountStateStore::apply(const XtAccountEvent& ev) {
    guard_worker_thread_();
    on_event_before_apply(ev);
    apply_event(account_, ev);

    if (current_.load(std::memory_order_acquire)) {
        publish_partial_(ev);
        hp_domain_.try_scan_if_due();
    }
}

void AccountStateStore::on_position_snapshot(
    const std::vector<XtPositionSnapshot>& positions,
    bool is_recovery,
    const XtCommodityCatalog& catalog) {
    guard_worker_thread_();
    if (is_recovery) {

        apply(XtAccountEvent::reset());
    }
    for (const auto& p : positions) {
        if (p.code.empty()) continue;
        const XtCommodityInfo* cinfo = catalog.find(p.code.c_str());
        const double vol_scale =
            (cinfo && std::isfinite(cinfo->vol_scale) && cinfo->vol_scale > 0.0)
                ? cinfo->vol_scale : 1.0;

        apply(XtAccountEvent::set_position(
            p.code.c_str(),
            xt_fp::to_fp(p.long_qty),  xt_fp::to_fp(p.long_avg  / vol_scale),
            xt_fp::to_fp(p.short_qty), xt_fp::to_fp(p.short_avg / vol_scale),
            xt_fp::to_fp(0.0),
            xt_fp::to_fp(p.long_today), xt_fp::to_fp(p.short_today),
            xt_fp::to_fp(p.long_frozen), xt_fp::to_fp(p.short_frozen),
            static_cast<int64_t>(p.broker_frozen_last_update_ms)));
    }
    if (is_recovery) {

        set_closeable_known(true);
    }
    if (current_.load(std::memory_order_acquire)) {

        publish_all_snapshots_();
        hp_domain_.try_scan_if_due();
    }
}

void AccountStateStore::bind_worker_thread() noexcept {
    bind_worker_thread(std::this_thread::get_id());
}

void AccountStateStore::bind_worker_thread(std::thread::id tid) noexcept {

    worker_tid_ = tid;
    worker_bound_.store(true, std::memory_order_release);
}

void AccountStateStore::on_session_begin_hook(int64_t equity_fp) noexcept {

    starting_balance_fp_.store(equity_fp, std::memory_order_release);
}

int64_t AccountStateStore::starting_balance_fp() const noexcept {
    return starting_balance_fp_.load(std::memory_order_acquire);
}

void AccountStateStore::set_closeable_known(bool known) noexcept {
    closeable_known_.store(known, std::memory_order_release);
}

void AccountStateStore::unbind_worker_thread() noexcept {

    worker_bound_.store(false, std::memory_order_release);
    write_first_touch_tid_.store(std::thread::id{}, std::memory_order_release);
}

void AccountStateStore::register_symbol(std::string_view code) {
    assert(!symbols_frozen_.load(std::memory_order_acquire));
    if (code.empty() || symbols_frozen_.load(std::memory_order_acquire)) {
        return;
    }
    symbols_sorted_.emplace_back(code);
    std::sort(symbols_sorted_.begin(), symbols_sorted_.end());
    symbols_sorted_.erase(std::unique(symbols_sorted_.begin(), symbols_sorted_.end()), symbols_sorted_.end());
    assert(symbols_sorted_.size() <= kMaxSymbols);

    mark_cache_.pre_warm_one(code);
}

void AccountStateStore::freeze_symbols() noexcept {

    if (symbols_frozen_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    mark_cache_.freeze();
    if (symbols_sorted_.size() > kMaxSymbols) {
        std::fprintf(stderr, "AccountStateStore freeze_symbols overflow: %zu > %zu\n",
                     symbols_sorted_.size(), kMaxSymbols);
        symbols_sorted_.resize(kMaxSymbols);
    }

    auto* fresh = acquire_after_reclaim_scan(full_pool_, hp_domain_);
    if (!fresh) {
        std::fprintf(stderr, "AccountStateStore full snapshot pool exhausted during freeze\n");
        return;
    }
    fresh->generation = 0;
    fresh->account = build_account_snapshot_();
    fresh->tracked_codes = symbols_sorted_;
    fresh->reset_slots();

    for (std::size_t i = 0; i < fresh->tracked_codes.size(); ++i) {
        const auto& code = fresh->tracked_codes[i];
        if (auto* pos = acquire_after_reclaim_scan(position_pool_, hp_domain_)) {
            *pos = build_position_snapshot_(code);
            fresh->position_slots[i].store(pos, std::memory_order_release);
        }
        if (auto* risk = acquire_after_reclaim_scan(risk_pool_, hp_domain_)) {
            *risk = build_risk_snapshot_(code);
            fresh->risk_slots[i].store(risk, std::memory_order_release);
        }
    }

    const auto* old = current_.exchange(fresh, std::memory_order_acq_rel);
    if (old) {
        hp_domain_.retire_raw(old, &full_pool_, release_full_snapshot);
    }
}

bool AccountStateStore::symbols_frozen() const noexcept {
    return symbols_frozen_.load(std::memory_order_acquire);
}

bool AccountStateStore::futures_mode() const noexcept {

    return account_.futures_mode();
}

bool AccountStateStore::is_registered_symbol(std::string_view code) const noexcept {
    if (code.empty()) {
        return false;
    }

    auto guard = hp_domain_.protect(current_);
    return guard && guard->find_symbol_idx(code) != kInvalidSymbolIndex;
}

AccountSnapshot AccountStateStore::snapshot_account() const noexcept {
    auto guard = hp_domain_.protect(current_);
    return guard ? guard->account : build_account_snapshot_();
}

int64_t AccountStateStore::realized_pnl() const noexcept {

    return account_.realized_pnl();
}

PositionSnapshot AccountStateStore::snapshot_position(std::string_view code) const noexcept {
    PositionSnapshot out;
    for (;;) {
        auto guard = hp_domain_.protect(current_);
        if (!guard) {
            out = build_position_snapshot_(code);
            break;
        }
        const auto idx = guard->find_symbol_idx(code);
        if (idx == kInvalidSymbolIndex) {
            out = symbols_frozen_.load(std::memory_order_acquire)
                      ? PositionSnapshot{}
                      : build_position_snapshot_(code);
            break;
        }

        auto pguard = hp_domain_.protect(guard->position_slots[idx]);
        if (current_.load(std::memory_order_acquire) != guard.get()) {
            continue;
        }
        out = pguard ? *pguard : PositionSnapshot{};
        break;
    }

    out.closeable_known = closeable_known_.load(std::memory_order_acquire);
    return out;
}

RiskSnapshot AccountStateStore::snapshot_risk(std::string_view code) const noexcept {
    for (;;) {
        auto guard = hp_domain_.protect(current_);
        if (!guard) {
            return build_risk_snapshot_(code);
        }
        const auto idx = guard->find_symbol_idx(code);
        if (idx == kInvalidSymbolIndex) {
            return symbols_frozen_.load(std::memory_order_acquire) ? RiskSnapshot{}
                                                                   : build_risk_snapshot_(code);
        }

        auto rguard = hp_domain_.protect(guard->risk_slots[idx]);
        if (current_.load(std::memory_order_acquire) != guard.get()) {
            continue;
        }
        return rguard ? *rguard : RiskSnapshot{};
    }
}

std::vector<std::string> AccountStateStore::snapshot_tracked_codes() const {
    auto guard = hp_domain_.protect(current_);
    return guard ? guard->tracked_codes : mark_cache_.tracked_codes_sorted();
}

void AccountStateStore::frozen_audit_report(account_audit::XtFrozenAuditReport& out) const {
    audit_frozen_direct(account_.frozen_cash(), account_.order_slots(), out);
}

void AccountStateStore::frozen_audit_from_snapshot(account_audit::XtFrozenAuditReport& out) const {
    out = account_audit::XtFrozenAuditReport{};

    auto guard = hp_domain_.protect(current_);
    if (!guard) {
        out.reason = "no published snapshot (pre-freeze)";
        return;
    }
    const AccountSnapshot& a = guard->account;
    out.frozen_cash = xt_fp::from_fp(a.frozen_cash);
    out.frozen_cash_by_orders = xt_fp::from_fp(a.frozen_by_orders);
    if (a.frozen_cash < 0) {
        out.ok = false;
        out.reason = "frozen_cash negative";
        return;
    }
    if (a.frozen_cash == a.frozen_by_orders) return;

    out.ok = false;
    std::ostringstream oss;
    oss << "frozen_cash mismatch runtime=" << out.frozen_cash
        << " tracked=" << out.frozen_cash_by_orders
        << " (publish-boundary aggregate; per-slot dump only in on-stop audit)";
    out.reason = oss.str();
}

void AccountStateStore::guard_worker_thread_() const noexcept {
    const std::thread::id self = std::this_thread::get_id();
    if (worker_bound_.load(std::memory_order_acquire)) {
        if (self == worker_tid_) return;
    } else {

        std::thread::id seen = write_first_touch_tid_.load(std::memory_order_acquire);
        if (seen == std::thread::id{}) {
            if (write_first_touch_tid_.compare_exchange_strong(
                    seen, self, std::memory_order_acq_rel, std::memory_order_acquire)) {
                return;
            }

        }
        if (seen == self) return;
    }
    on_cross_thread_write_();
}

void AccountStateStore::on_cross_thread_write_() const noexcept {

    xt_account_store_drops::cross_thread_write().fetch_add(1, std::memory_order_relaxed);
    static std::atomic<bool> warned{false};
    if (!warned.exchange(true)) {
        std::fprintf(stderr,
                     "[AccountStore][error] cross-thread write detected (fail-open) — "
                     "apply()/on_position_snapshot() must run on the bound worker "
                     "thread; event still applied, check caller thread model\n");
    }
}

void AccountStateStore::publish_partial_(const XtAccountEvent& ev) noexcept {
    using T = XtAccountEvent::Type;
    switch (ev.type) {
    case T::Invalid:
        break;
    case T::TradeEx:
    case T::SetPosition:
        publish_account_snapshot_();
        publish_position_for_(ev.code ? std::string_view(ev.code) : std::string_view{});
        publish_risk_for_(ev.code ? std::string_view(ev.code) : std::string_view{});
        break;
    case T::MarketTick:
        publish_account_snapshot_();
        publish_position_for_(ev.code ? std::string_view(ev.code) : std::string_view{});
        break;
    case T::Reset:
    case T::SessionBegin:
        publish_all_snapshots_();
        break;
    case T::OrderAccepted:
    case T::OrderExecUpdate:
    case T::OrderFilled:
    case T::OrderCanceled:
    case T::OrderRejected:
    case T::SetCash:
    case T::DailySettlement:
    case T::CashTransfer:
        publish_account_snapshot_();
        break;
    }
}

void AccountStateStore::publish_all_snapshots_() noexcept {
    publish_account_snapshot_();
    for (const auto& code : symbols_sorted_) {
        publish_position_for_(code);
        publish_risk_for_(code);
    }
}

void AccountStateStore::publish_account_snapshot_() noexcept {
    const auto* old = current_.load(std::memory_order_acquire);
    if (!old) {
        return;
    }
    auto* fresh = acquire_after_reclaim_scan(full_pool_, hp_domain_);
    if (!fresh) {
        std::fprintf(stderr, "AccountStateStore full snapshot pool exhausted\n");
        return;
    }
    fresh->generation = next_generation_++;
    fresh->account = build_account_snapshot_();
    fresh->tracked_codes = old->tracked_codes;
    fresh->reset_slots();
    for (std::size_t i = 0; i < fresh->tracked_codes.size() && i < kMaxSymbols; ++i) {
        fresh->position_slots[i].store(old->position_slots[i].load(std::memory_order_acquire),
                                       std::memory_order_release);
        fresh->risk_slots[i].store(old->risk_slots[i].load(std::memory_order_acquire),
                                   std::memory_order_release);
    }
    const auto* retired = current_.exchange(fresh, std::memory_order_acq_rel);
    if (retired) {
        hp_domain_.retire_raw(retired, &full_pool_, release_full_snapshot);
    }
}

void AccountStateStore::publish_position_for_(std::string_view code) noexcept {
    if (code.empty()) {
        return;
    }
    const auto* full = current_.load(std::memory_order_acquire);
    if (!full) {
        return;
    }
    const auto idx = full->find_symbol_idx(code);
    if (idx == kInvalidSymbolIndex) {
        return;
    }
    auto* fresh = acquire_after_reclaim_scan(position_pool_, hp_domain_);
    if (!fresh) {
        std::fprintf(stderr, "AccountStateStore position snapshot pool exhausted\n");
        return;
    }
    *fresh = build_position_snapshot_(code);
    const auto* old = full->position_slots[idx].exchange(fresh, std::memory_order_acq_rel);
    if (old) {
        hp_domain_.retire_raw(old, &position_pool_, release_position_snapshot);
    }
}

void AccountStateStore::publish_risk_for_(std::string_view code) noexcept {
    if (code.empty()) {
        return;
    }
    const auto* full = current_.load(std::memory_order_acquire);
    if (!full) {
        return;
    }
    const auto idx = full->find_symbol_idx(code);
    if (idx == kInvalidSymbolIndex) {
        return;
    }
    auto* fresh = acquire_after_reclaim_scan(risk_pool_, hp_domain_);
    if (!fresh) {
        std::fprintf(stderr, "AccountStateStore risk snapshot pool exhausted\n");
        return;
    }
    *fresh = build_risk_snapshot_(code);
    const auto* old = full->risk_slots[idx].exchange(fresh, std::memory_order_acq_rel);
    if (old) {
        hp_domain_.retire_raw(old, &risk_pool_, release_risk_snapshot);
    }
}

AccountSnapshot AccountStateStore::build_account_snapshot_() const noexcept {
    AccountSnapshot out;
    out.cash = account_.cash();
    out.frozen_cash = account_.frozen_cash();
    out.available_cash = account_.available_cash();
    int64_t total_unrealized = 0;
    int64_t total_market_value = 0;
    for (const auto& code : mark_cache_.tracked_codes_sorted()) {
        if (code.empty()) continue;
        const auto mark_input = make_mark_input(code.c_str());
        total_unrealized += xt_account::position_unrealized(mark_input);
        total_market_value += xt_account::position_market_value(mark_input);
    }
    out.unrealized_pnl = total_unrealized;
    out.equity = xt_account::equity(
        out.cash, total_unrealized, total_market_value, account_.futures_mode());
    out.realized_pnl = account_.realized_pnl();
    out.fees = account_.fees();
    out.market_value = total_market_value;
    out.frozen_by_orders = account_.frozen_by_orders();
    return out;
}

PositionSnapshot
AccountStateStore::build_position_snapshot_(std::string_view code) const noexcept {
    char code_buf[128] = {};
    const auto len = std::min(code.size(), sizeof(code_buf) - 1);
    std::copy_n(code.data(), len, code_buf);
    const char* c = code_buf;

    const auto r = account_.position_readout(c);
    PositionSnapshot out;
    out.qty = r.qty;
    out.long_qty = r.long_qty;
    out.short_qty = r.short_qty;
    out.long_today_qty = r.long_today_qty;
    out.short_today_qty = r.short_today_qty;
    out.avg_price = r.avg_price;
    out.mark_price = mark_cache_.mark_price(c);
    out.unrealized_pnl = xt_account::position_unrealized(make_mark_input(c, r));
    out.available_sell_today_qty = r.available_sell_today_qty;
    out.available_sell_total_qty = r.available_sell_total_qty;
    out.available_cover_today_qty = r.available_cover_today_qty;
    out.available_cover_total_qty = r.available_cover_total_qty;

    out.broker_long_frozen_fp  = r.broker_long_frozen_fp;
    out.broker_short_frozen_fp = r.broker_short_frozen_fp;

    out.broker_frozen_last_update_ms = r.broker_frozen_last_update_ms;
    out.has_today_split = account_.has_today_split();
    out.closeable_known = closeable_known_.load(std::memory_order_acquire);
    return out;
}

RiskSnapshot
AccountStateStore::build_risk_snapshot_(std::string_view code) const noexcept {
    const auto pos = build_position_snapshot_(code);
    const auto account = build_account_snapshot_();

    RiskSnapshot out;
    out.cash_available_fp = account.available_cash;
    out.equity_fp = account.equity;

    out.process_net_pnl_fp = account.realized_pnl - account.fees + account.unrealized_pnl;
    out.position_qty_fp = pos.qty;
    out.available_close_qty_fp = pos.available_sell_total_qty;
    out.yesterday_long_qty_fp =
        std::max<int64_t>(0, pos.available_sell_total_qty - pos.available_sell_today_qty);
    out.today_pnl_fp = 0;
    out.limit_up_fp = 0;
    out.limit_down_fp = 0;

    return out;
}

xt_account::XtPositionMarkInput AccountStateStore::make_mark_input(const char* code) const {
    return make_mark_input(code, account_.position_readout(code));
}

xt_account::XtPositionMarkInput AccountStateStore::make_mark_input(
    const char* code, const AccountState::PositionReadout& readout) const {
    xt_account::XtPositionMarkInput mark_input;
    mark_input.long_qty = readout.long_qty;
    mark_input.long_avg_price = readout.long_avg_price;
    mark_input.short_qty = readout.short_qty;
    mark_input.short_avg_price = readout.short_avg_price;
    mark_input.mark_price = mark_cache_.mark_price(code);

    if (account_.futures_mode()) {
        if (const XtCommodityCatalog* cat = account_.commodity_catalog()) {
            const XtCommodityInfo* info = cat->find(code);
            if (info && info->vol_scale > 0.0)
                mark_input.vol_scale_fp = xt_fp::to_fp(info->vol_scale);
        }
    }
    return mark_input;
}

void AccountStateStore::on_event_before_apply(const XtAccountEvent& ev) {
    switch (ev.type) {
    case XtAccountEvent::Type::Reset:

        return;
    case XtAccountEvent::Type::MarketTick:
        mark_cache_.on_market_tick(ev.code, ev.payload.tick.price);
        return;
    case XtAccountEvent::Type::SetPosition:
        mark_cache_.on_set_position(ev.code, ev.payload.set_position.last_price);
        return;
    case XtAccountEvent::Type::TradeEx:
        mark_cache_.on_trade(ev.code);
        return;
    default:
        return;
    }
}

std::unique_ptr<AccountStateStore> make_account_state_store(const AccountState::Config& cfg) {
    return std::make_unique<AccountStateStore>(cfg);
}

}
