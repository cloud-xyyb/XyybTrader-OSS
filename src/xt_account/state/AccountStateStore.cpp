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
#include <string>
#include <string_view>

namespace xt_account_store_drops {
std::atomic<uint64_t>& cross_thread_write() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

std::atomic<uint64_t>& snapshot_publish_skipped() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}
}

namespace xt_account {

namespace {

using PositionPool = xt_util::sync::XtSnapshotPool<PositionSnapshot, kMaxSymbols * 4>;
using FullPool = xt_util::sync::XtSnapshotPool<FullSnapshot, 64>;

/// @brief 批次发布抑制 RAII；异常路径也保证复位（apply 链可抛）。
///
/// 析构硬置 false 而不恢复旧值：抑制闩只服务 on_position_snapshot 的单层批次，
/// 嵌套会让「批尾统一发布一次」失去唯一收敛点，构造期直接拒绝。
struct PublishSuppressScope {
    explicit PublishSuppressScope(bool& flag) noexcept : flag_(flag) {
        assert(!flag_ && "PublishSuppressScope must not nest");
        flag_ = true;
    }
    ~PublishSuppressScope() { flag_ = false; }
    PublishSuppressScope(const PublishSuppressScope&) = delete;
    PublishSuppressScope& operator=(const PublishSuppressScope&) = delete;

    bool& flag_;
};

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

void release_full_snapshot(void* ctx, const void* ptr) noexcept {
    static_cast<FullPool*>(ctx)->release(const_cast<FullSnapshot*>(
        static_cast<const FullSnapshot*>(ptr)));
}

void note_publish_skipped(const char* what) noexcept {
    xt_account_store_drops::snapshot_publish_skipped().fetch_add(1, std::memory_order_relaxed);
    static std::atomic<bool> warned{false};
    if (!warned.exchange(true)) {
        std::fprintf(stderr,
                     "AccountStateStore %s snapshot pool exhausted; publish skipped "
                     "(further skips counted, not logged)\n",
                     what);
    }
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
    : account_(cfg), mark_cache_() {}

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
    {

        PublishSuppressScope suppress(suppress_partial_publish_);
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
    }
    if (current_.load(std::memory_order_acquire)) {

        publish_all_snapshots_();
        hp_domain_.try_scan_if_due();
    }
    if (is_recovery) {

        set_closeable_known(true);
    }
}

void AccountStateStore::bind_worker_thread() noexcept {
    bind_worker_thread(std::this_thread::get_id());
}

void AccountStateStore::bind_worker_thread(std::thread::id tid) noexcept {

    worker_tid_ = tid;
    worker_quiescent_.store(false, std::memory_order_release);
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
    worker_quiescent_.store(false, std::memory_order_release);
}

void AccountStateStore::enter_quiescent() noexcept {
    worker_quiescent_.store(true, std::memory_order_release);
}

bool AccountStateStore::insert_symbol_sorted_(std::string_view code) {
    const auto it = std::lower_bound(symbols_sorted_.begin(), symbols_sorted_.end(), code,
                                     [](const std::string& lhs, std::string_view rhs) {
                                         return std::string_view(lhs) < rhs;
                                     });
    if (it != symbols_sorted_.end() && std::string_view(*it) == code) {
        return false;
    }
    symbols_sorted_.emplace(it, code);
    return true;
}

void AccountStateStore::register_symbol_(std::string_view code) {

    if (symbols_frozen_.load(std::memory_order_acquire)) {
        throw std::logic_error(
            "AccountStateStore::register_symbol_: symbol set already frozen, code=" +
            std::string(code));
    }
    if (code.empty()) {
        return;
    }

    if (symbols_sorted_.size() >= kMaxSymbols && !is_in_universe_(code)) {
        throw std::invalid_argument(
            "AccountStateStore::register_symbol_: symbol count would exceed kMaxSymbols " +
            std::to_string(kMaxSymbols) + ", code=" + std::string(code));
    }
    insert_symbol_sorted_(code);

    mark_cache_.pre_warm_one(code);
}

void AccountStateStore::freeze_symbols_() {

    if (symbols_frozen_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    mark_cache_.freeze();

    if (symbols_sorted_.size() > kMaxSymbols) {
        throw std::invalid_argument(
            "AccountStateStore::freeze_symbols_: symbol count " +
            std::to_string(symbols_sorted_.size()) + " exceeds kMaxSymbols " +
            std::to_string(kMaxSymbols));
    }

    auto* fresh = acquire_after_reclaim_scan(full_pool_, hp_domain_);
    if (!fresh) {

        throw std::logic_error(
            "AccountStateStore::freeze_symbols_: full snapshot pool exhausted at gen-0");
    }
    fresh->account = build_account_snapshot_();

    fresh->universe = &symbols_sorted_;
    fresh->reset_slots();

    for (std::size_t i = 0; i < symbols_sorted_.size(); ++i) {
        const auto& code = symbols_sorted_[i];
        if (auto* pos = acquire_after_reclaim_scan(position_pool_, hp_domain_)) {
            *pos = build_position_snapshot_(code);
            fresh->position_slots[i].store(pos, std::memory_order_release);
        } else {

            publish_dirty_ = true;
            note_publish_skipped("position");
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

AccountStateStore::AccountPositionFrame
AccountStateStore::snapshot_account_and_position(std::string_view code) const noexcept {
    AccountPositionFrame out;
    for (;;) {
        auto guard = hp_domain_.protect(current_);
        if (!guard) {
            out.account = build_account_snapshot_();
            out.position = build_position_snapshot_(code);
            break;
        }
        const auto idx = guard->find_symbol_idx(code);
        if (idx == kInvalidSymbolIndex) {
            if (symbols_frozen_.load(std::memory_order_acquire)) {

                out.account = guard->account;
                out.position = PositionSnapshot{};
            } else {
                out.account = build_account_snapshot_();
                out.position = build_position_snapshot_(code);
            }
            break;
        }

        auto pguard = hp_domain_.protect(guard->position_slots[idx]);
        if (current_.load(std::memory_order_acquire) != guard.get()) {
            continue;
        }
        out.account = guard->account;
        out.position = pguard ? *pguard : PositionSnapshot{};
        break;
    }

    out.position.closeable_known = closeable_known_.load(std::memory_order_acquire);
    return out;
}

std::vector<std::string> AccountStateStore::snapshot_tracked_codes() const {
    auto guard = hp_domain_.protect(current_);

    if (guard && guard->universe) return *guard->universe;
    return symbols_sorted_;
}

std::size_t AccountStateStore::quarantined_symbol_count() const noexcept {
    return quarantined_count_.load(std::memory_order_acquire);
}

uint64_t AccountStateStore::quarantined_overflow_count() const noexcept {
    return quarantined_overflow_.load(std::memory_order_relaxed);
}

std::vector<std::string> AccountStateStore::snapshot_quarantined_codes() const {
    std::lock_guard<std::mutex> lk(quarantine_mu_);
    return quarantined_codes_;
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

    if (worker_quiescent_.load(std::memory_order_acquire)) return;
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
    if (suppress_partial_publish_) {

        return;
    }
    if (publish_dirty_) {

        publish_all_snapshots_();
        return;
    }
    using T = XtAccountEvent::Type;
    switch (ev.type) {
    case T::Invalid:
        break;
    case T::TradeEx:
    case T::SetPosition:
    case T::MarketTick:
        publish_event_snapshot_(ev.code ? std::string_view(ev.code) : std::string_view{});
        break;
    case T::Reset:
    case T::SessionBegin:
        publish_all_snapshots_();
        break;
    case T::OrderAccepted:
    case T::OrderExecUpdate:
    case T::SetCash:
    case T::DailySettlement:
    case T::CashTransfer:
        publish_account_snapshot_();
        break;
    }
}

void AccountStateStore::publish_all_snapshots_() noexcept {
    const auto* old = current_.load(std::memory_order_acquire);
    if (!old) {
        return;
    }
    auto* fresh = acquire_after_reclaim_scan(full_pool_, hp_domain_);
    if (!fresh) {
        publish_dirty_ = true;
        note_publish_skipped("full");
        return;
    }
    fresh->account = build_account_snapshot_();
    fresh->universe = old->universe;
    fresh->reset_slots();
    const std::size_t n =
        std::min(fresh->universe ? fresh->universe->size() : std::size_t{0}, kMaxSymbols);

    for (std::size_t i = 0; i < n; ++i) {
        auto* pos = acquire_after_reclaim_scan(position_pool_, hp_domain_);
        if (!pos) {
            publish_dirty_ = true;
            note_publish_skipped("position");
            for (std::size_t j = 0; j < i; ++j) {
                position_pool_.release(const_cast<PositionSnapshot*>(
                    fresh->position_slots[j].load(std::memory_order_relaxed)));
            }
            full_pool_.release(fresh);
            return;
        }
        *pos = build_position_snapshot_((*fresh->universe)[i]);
        fresh->position_slots[i].store(pos, std::memory_order_relaxed);
    }
    const auto* retired = current_.exchange(fresh, std::memory_order_acq_rel);
    if (retired) {

        for (std::size_t i = 0; i < n; ++i) {
            if (const auto* p = retired->position_slots[i].load(std::memory_order_acquire)) {
                hp_domain_.retire_raw(p, &position_pool_, release_position_snapshot);
            }
        }
        hp_domain_.retire_raw(retired, &full_pool_, release_full_snapshot);
    }

    publish_dirty_ = false;
}

void AccountStateStore::publish_account_snapshot_() noexcept {
    const auto* old = current_.load(std::memory_order_acquire);
    if (!old) {
        return;
    }
    auto* fresh = acquire_after_reclaim_scan(full_pool_, hp_domain_);
    if (!fresh) {
        note_publish_skipped("full");
        return;
    }
    fresh->account = build_account_snapshot_();
    fresh->universe = old->universe;
    fresh->reset_slots();
    const std::size_t n = fresh->universe ? fresh->universe->size() : 0;
    for (std::size_t i = 0; i < n && i < kMaxSymbols; ++i) {
        fresh->position_slots[i].store(old->position_slots[i].load(std::memory_order_acquire),
                                       std::memory_order_relaxed);
    }
    const auto* retired = current_.exchange(fresh, std::memory_order_acq_rel);
    if (retired) {
        hp_domain_.retire_raw(retired, &full_pool_, release_full_snapshot);
    }
}

void AccountStateStore::publish_event_snapshot_(std::string_view code) noexcept {
    const auto* old = current_.load(std::memory_order_acquire);
    if (!old) {
        return;
    }
    auto* fresh = acquire_after_reclaim_scan(full_pool_, hp_domain_);
    if (!fresh) {
        publish_dirty_ = true;
        note_publish_skipped("full");
        return;
    }
    fresh->account = build_account_snapshot_();
    fresh->universe = old->universe;
    fresh->reset_slots();
    const std::size_t n =
        std::min(fresh->universe ? fresh->universe->size() : std::size_t{0}, kMaxSymbols);
    for (std::size_t i = 0; i < n; ++i) {
        fresh->position_slots[i].store(old->position_slots[i].load(std::memory_order_acquire),
                                       std::memory_order_relaxed);
    }

    const PositionSnapshot* replaced = nullptr;
    if (!code.empty()) {
        const auto idx = fresh->find_symbol_idx(code);
        if (idx != kInvalidSymbolIndex) {
            auto* pos = acquire_after_reclaim_scan(position_pool_, hp_domain_);
            if (!pos) {

                publish_dirty_ = true;
                note_publish_skipped("position");
                full_pool_.release(fresh);
                return;
            }
            *pos = build_position_snapshot_(code);
            replaced = fresh->position_slots[idx].exchange(pos, std::memory_order_relaxed);
        }
    }
    const auto* retired = current_.exchange(fresh, std::memory_order_acq_rel);
    if (retired) {
        hp_domain_.retire_raw(retired, &full_pool_, release_full_snapshot);
    }
    if (replaced) {

        hp_domain_.retire_raw(replaced, &position_pool_, release_position_snapshot);
    }
}

AccountSnapshot AccountStateStore::build_account_snapshot_() const noexcept {
    AccountSnapshot out;
    out.cash = account_.cash();
    out.frozen_cash = account_.frozen_cash();
    out.available_cash = account_.available_cash();
    int64_t total_unrealized = 0;
    int64_t total_market_value = 0;
    for (const auto& code : symbols_sorted_) {
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
        note_symbol_if_open_(ev.code,
                             mark_cache_.on_market_tick(ev.code, ev.payload.tick.price));
        return;
    case XtAccountEvent::Type::SetPosition:
        note_symbol_if_open_(
            ev.code, mark_cache_.on_set_position(ev.code, ev.payload.set_position.last_price));
        quarantine_if_unknown_(ev.code, "SetPosition");
        return;
    case XtAccountEvent::Type::TradeEx:
        note_symbol_if_open_(ev.code, mark_cache_.on_trade(ev.code));
        quarantine_if_unknown_(ev.code, "TradeEx");
        return;
    default:
        return;
    }
}

bool AccountStateStore::is_in_universe_(std::string_view code) const noexcept {
    const auto it = std::lower_bound(symbols_sorted_.begin(), symbols_sorted_.end(), code,
                                     [](const std::string& lhs, std::string_view rhs) {
                                         return std::string_view(lhs) < rhs;
                                     });
    return it != symbols_sorted_.end() && std::string_view(*it) == code;
}

void AccountStateStore::quarantine_if_unknown_(const char* code, const char* event_tag) {
    if (!code || code[0] == '\0') return;
    if (!symbols_frozen_.load(std::memory_order_acquire)) return;

    const std::string_view sv(code);
    if (is_in_universe_(sv)) return;

    std::lock_guard<std::mutex> lk(quarantine_mu_);
    for (const auto& known : quarantined_codes_) {
        if (known == sv) return;
    }
    if (quarantined_codes_.size() >= kMaxQuarantinedCodes) {
        quarantined_overflow_.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    quarantined_codes_.emplace_back(sv);
    quarantined_count_.store(quarantined_codes_.size(), std::memory_order_release);
    std::fprintf(stderr,
                 "[xt_account][error] symbol '%s' outside frozen universe (event=%s); "
                 "accounted at account level but invisible per-symbol "
                 "(repeats for this code suppressed)\n",
                 code, event_tag);
}

void AccountStateStore::note_symbol_if_open_(const char* code, bool tracked) {

    if (!tracked || symbols_frozen_.load(std::memory_order_acquire)) {
        return;
    }
    insert_symbol_sorted_(code);
}

}
