#include "xt_identity/OrderAttributionStore.h"
#include "order/XtOrderRuntimeBook.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "api/AliveInflightOrder.h"
#include "xt_core/order/XtTradeRecord.h"
#include "XtFixedPoint.h"
#include "xt_account/api/AccountSnapshot.h"
#include "xt_util/sync/XtAllocGuard.h"

#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif

namespace xt_runtime_book_drops {

std::atomic<uint64_t>& slot_pool_full_drops() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

std::atomic<uint64_t>& orphan_update_dropped() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

std::atomic<uint64_t>& orphan_reap_dropped() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

std::atomic<uint64_t>& snapshot_publish_skipped() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

std::atomic<uint64_t>& cross_thread_direct_read_blocked() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

std::atomic<uint64_t>& tracked_codes_truncated() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}
}

namespace xt_runtime {
namespace order {

namespace {
using OrderBookSnapshotPool = xt_util::sync::XtSnapshotPool<OrderBookSnapshot, 8>;

OrderBookSnapshot* acquire_order_book_snapshot(OrderBookSnapshotPool& pool,
                                               xt_util::sync::HazardDomain& domain) noexcept {
    auto* fresh = pool.acquire();
    if (!fresh) {
        domain.try_scan();
        fresh = pool.acquire();
    }
    return fresh;
}

void release_order_book_snapshot(void* ctx, const void* ptr) noexcept {
    auto* pool = static_cast<OrderBookSnapshotPool*>(ctx);
    pool->release(const_cast<OrderBookSnapshot*>(static_cast<const OrderBookSnapshot*>(ptr)));
}
}

XtOrderRuntimeBook::XtOrderRuntimeBook() {
    init_slot_pool();
    publish_snapshot_();
}

XtOrderRuntimeBook::XtOrderRuntimeBook(XtOrderRuntimeBook&& other) noexcept
    : slot_pool_(std::move(other.slot_pool_)),
      free_stack_(other.free_stack_),
      free_top_(other.free_top_),
      id_to_slot_idx_(std::move(other.id_to_slot_idx_)),
      close_outstanding_(std::move(other.close_outstanding_)),
      open_order_seq_(other.open_order_seq_),
      order_final_before_submit_(std::move(other.order_final_before_submit_)),
      order_terminal_seen_(std::move(other.order_terminal_seen_)),
      order_terminal_fifo_(std::move(other.order_terminal_fifo_)),
      tracked_codes_(std::move(other.tracked_codes_)),
      instrument_registry_(std::move(other.instrument_registry_)),
      next_generation_(other.next_generation_) {

    assert(!other.marshal_bound_.load(std::memory_order_relaxed) &&
           "XtOrderRuntimeBook moved after bind_worker_marshal (marshal binding not transferred)");
    publish_snapshot_();
}

XtOrderRuntimeBook& XtOrderRuntimeBook::operator=(XtOrderRuntimeBook&& other) noexcept {
    if (this != &other) {
        assert(!other.marshal_bound_.load(std::memory_order_relaxed) &&
               "XtOrderRuntimeBook move-assigned after bind_worker_marshal");
        slot_pool_ = std::move(other.slot_pool_);
        free_stack_ = other.free_stack_;
        free_top_ = other.free_top_;
        id_to_slot_idx_ = std::move(other.id_to_slot_idx_);
        close_outstanding_ = std::move(other.close_outstanding_);
        open_order_seq_ = other.open_order_seq_;
        order_final_before_submit_ = std::move(other.order_final_before_submit_);
        order_terminal_seen_ = std::move(other.order_terminal_seen_);
        order_terminal_fifo_ = std::move(other.order_terminal_fifo_);
        tracked_codes_ = std::move(other.tracked_codes_);
        instrument_registry_ = std::move(other.instrument_registry_);
        next_generation_ = other.next_generation_;
        publish_snapshot_();
    }
    return *this;
}

void XtOrderRuntimeBook::bind_worker_marshal(std::thread::id worker_tid, WorkerPostFn post) {
    marshal_worker_tid_ = worker_tid;
    marshal_post_ = std::move(post);

    marshal_bound_.store(static_cast<bool>(marshal_post_), std::memory_order_release);
}

void XtOrderRuntimeBook::unbind_worker_marshal() {

    marshal_bound_.store(false, std::memory_order_release);
    marshal_post_ = nullptr;
    marshal_worker_tid_ = std::thread::id{};
    collect_first_touch_tid_.store(std::thread::id{}, std::memory_order_release);
}

bool XtOrderRuntimeBook::is_bound_worker_thread() const noexcept {
    if (!marshal_bound_.load(std::memory_order_acquire)) return true;
    return std::this_thread::get_id() == marshal_worker_tid_;
}

bool XtOrderRuntimeBook::post_to_worker(std::function<void()> fn) const {
    if (!fn || !marshal_bound_.load(std::memory_order_acquire)) return false;
    return marshal_post_(std::move(fn));
}

void XtOrderRuntimeBook::init_slot_pool() {
    slot_pool_.fill(OrderLifecycleSlot{});
    free_top_ = 0;
    for (uint16_t i = 0; i < kMaxInflightOrders; ++i) {
        free_stack_[free_top_++] = i;
    }

    id_to_slot_idx_.clear();
    id_to_slot_idx_.reserve(kMaxInflightOrders);
}

uint16_t XtOrderRuntimeBook::alloc_slot() {
    if (free_top_ == 0) {

        xt_runtime_book_drops::slot_pool_full_drops().fetch_add(1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance().inc_runtime_book_drop_total("slot_pool_full");
#endif
        return kInvalidSlotIdx;
    }
    return free_stack_[--free_top_];
}

void XtOrderRuntimeBook::free_slot_by_idx(uint16_t idx) {
    assert(idx < kMaxInflightOrders);
    slot_pool_[idx] = OrderLifecycleSlot{};
    free_stack_[free_top_++] = idx;
}

XtOrderRuntimeBook::OrderLifecycleSlot* XtOrderRuntimeBook::find_slot(uint64_t id) {
    auto it = id_to_slot_idx_.find(id);
    if (it == id_to_slot_idx_.end()) return nullptr;
    return &slot_pool_[it->second];
}

const XtOrderRuntimeBook::OrderLifecycleSlot* XtOrderRuntimeBook::find_slot(uint64_t id) const {
    auto it = id_to_slot_idx_.find(id);
    if (it == id_to_slot_idx_.end()) return nullptr;
    return &slot_pool_[it->second];
}

XtOrderRuntimeBook::OrderLifecycleSlot* XtOrderRuntimeBook::upsert_slot(uint64_t id, bool& inserted) {
    auto it = id_to_slot_idx_.find(id);
    if (it != id_to_slot_idx_.end()) {
        inserted = false;
        return &slot_pool_[it->second];
    }
    const uint16_t idx = alloc_slot();
    if (idx == kInvalidSlotIdx) { inserted = false; return nullptr; }
    id_to_slot_idx_[id] = idx;
    inserted = true;
    return &slot_pool_[idx];
}

void XtOrderRuntimeBook::erase_slot(uint64_t id) {
    auto it = id_to_slot_idx_.find(id);
    if (it == id_to_slot_idx_.end()) return;

    xt_identity::OrderAttributionStore::forget_order_strategy_sid(id);
    free_slot_by_idx(it->second);
    id_to_slot_idx_.erase(it);
}

bool XtOrderRuntimeBook::terminal_order_seen_(uint64_t id) const noexcept {
    return id != 0 && order_terminal_seen_.find(id) != order_terminal_seen_.end();
}

void XtOrderRuntimeBook::remember_terminal_order_(uint64_t id) {
    if (id == 0) return;
    if (order_terminal_seen_.insert(id).second) {
        order_terminal_fifo_.push_back(id);
    }
    while (order_terminal_seen_.size() > kTerminalOrderCacheCap &&
           !order_terminal_fifo_.empty()) {
        const uint64_t old = order_terminal_fifo_.front();
        order_terminal_fifo_.pop_front();
        order_terminal_seen_.erase(old);

        order_final_before_submit_.erase(old);
    }
}

int64_t XtOrderRuntimeBook::unfilled_today_fp(const OrderLifecycleSlot& slot) {
    return xt_fp::clamp_nonneg(slot.locked_today_fp - slot.filled_today_fp);
}

int64_t XtOrderRuntimeBook::unfilled_yesterday_fp(const OrderLifecycleSlot& slot) {
    return xt_fp::clamp_nonneg(slot.locked_yesterday_fp - slot.filled_yesterday_fp);
}

bool XtOrderRuntimeBook::has_close_contribution(const OrderLifecycleSlot& slot) {

    return slot.is_close && (slot.instrument_id != kInvalidInstrumentId);
}

void XtOrderRuntimeBook::add_close_outstanding_delta(XtInstrumentId id,
                                                      bool is_buy,
                                                      int64_t delta_today_fp,
                                                      int64_t delta_yesterday_fp) {
    if (id == kInvalidInstrumentId) return;
    if (delta_today_fp == 0 && delta_yesterday_fp == 0) return;

    const CloseOutstandingKey key{id, is_buy};
    auto it = close_outstanding_.find(key);
    if (it == close_outstanding_.end()) {
        const int64_t today_fp = xt_fp::clamp_nonneg(delta_today_fp);
        const int64_t yesterday_fp = xt_fp::clamp_nonneg(delta_yesterday_fp);
        if (today_fp <= 0 && yesterday_fp <= 0) return;
        close_outstanding_.emplace(key, CloseOutstandingAgg{today_fp, yesterday_fp});
        return;
    }

    it->second.today_fp = xt_fp::clamp_nonneg(it->second.today_fp + delta_today_fp);
    it->second.yesterday_fp = xt_fp::clamp_nonneg(it->second.yesterday_fp + delta_yesterday_fp);
    if (it->second.today_fp <= 0 && it->second.yesterday_fp <= 0) {
        close_outstanding_.erase(it);
    }
}

void XtOrderRuntimeBook::add_slot_close_contribution(const OrderLifecycleSlot& slot) {
    if (!has_close_contribution(slot)) return;
    add_close_outstanding_delta(slot.instrument_id,
                                slot.is_buy,
                                unfilled_today_fp(slot),
                                unfilled_yesterday_fp(slot));
}

void XtOrderRuntimeBook::remove_slot_close_contribution(const OrderLifecycleSlot& slot) {
    if (!has_close_contribution(slot)) return;
    add_close_outstanding_delta(slot.instrument_id,
                                slot.is_buy,
                                -unfilled_today_fp(slot),
                                -unfilled_yesterday_fp(slot));
}

OrderSnapshot
XtOrderRuntimeBook::build_order_snapshot_(const OrderLifecycleSlot& slot) const noexcept {
    OrderSnapshot out;
    out.id = slot.id;
    out.instrument_id = slot.instrument_id;
    copy_snapshot_code(out.code, instrument_registry_.code_of(slot.instrument_id));
    out.is_buy = slot.is_buy;
    out.is_close = slot.is_close;
    out.close_offset = slot.close_offset;
    out.price_fp = slot.price_fp;
    out.leaves_qty_fp = slot.leaves_qty_fp;
    out.locked_today_fp = slot.locked_today_fp;
    out.locked_yesterday_fp = slot.locked_yesterday_fp;
    out.filled_today_fp = slot.filled_today_fp;
    out.filled_yesterday_fp = slot.filled_yesterday_fp;
    out.seq = slot.seq;
    out.sid_id = slot.sid_id;
    return out;
}

void XtOrderRuntimeBook::publish_snapshot_() noexcept {
    auto* fresh = acquire_order_book_snapshot(snapshot_pool_, hp_domain_);
    if (!fresh) {

        xt_runtime_book_drops::snapshot_publish_skipped().fetch_add(1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance().inc_runtime_book_drop_total(
            "snapshot_publish_skipped");
#endif
        std::fprintf(stderr, "XtOrderRuntimeBook snapshot pool exhausted\n");
        return;
    }

    fresh->order_count = 0;
    fresh->tracked_code_count = 0;
    fresh->force_flat_pending_count = 0;
    fresh->close_outstanding_count = 0;
    fresh->generation = next_generation_++;

    for (const auto& kv : id_to_slot_idx_) {
        if (fresh->order_count >= fresh->orders.size()) break;
        const auto idx = kv.second;
        if (idx >= kMaxInflightOrders) continue;
        const auto& slot = slot_pool_[idx];
        if (slot.id == 0) continue;
        fresh->orders[fresh->order_count++] = build_order_snapshot_(slot);
    }

    bool tracked_codes_hit_truncation = false;
    for (const XtInstrumentId iid : tracked_codes_) {
        if (fresh->tracked_code_count >= fresh->tracked_codes.size()) {
            tracked_codes_hit_truncation = true;
            break;
        }
        const char* code = instrument_registry_.code_of(iid);
        if (!code || code[0] == '\0') continue;
        auto& out = fresh->tracked_codes[fresh->tracked_code_count++];
        out.instrument_id = iid;
        copy_snapshot_code(out.code, code);

        out.force_flat_sid_id = 0;
    }
    if (tracked_codes_hit_truncation) {
        xt_runtime_book_drops::tracked_codes_truncated().fetch_add(1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance().inc_runtime_book_drop_total(
            "tracked_codes_truncated");
#endif
        static std::atomic<bool> warned{false};
        if (!warned.exchange(true)) {
            std::fprintf(stderr,
                         "[OrderBook][critical] tracked codes exceed snapshot capacity "
                         "(%zu > %zu) — cross-thread readers blind to truncated codes\n",
                         tracked_codes_.size(), fresh->tracked_codes.size());
        }
    }

    publish_force_flat_keys_.clear();

    for (const auto& kv : id_to_slot_idx_) {
        const auto slot_idx = kv.second;
        if (slot_idx >= kMaxInflightOrders) continue;
        const auto& slot = slot_pool_[slot_idx];
        if (slot.id == 0 || !slot.is_close || slot.leaves_qty_fp <= 0) continue;
        const XtInstrumentId iid = slot.instrument_id;
        if (iid == kInvalidInstrumentId) continue;
        const uint32_t sid_id = slot.sid_id;
        const uint64_t key = (static_cast<uint64_t>(iid) << 32) ^ sid_id;
        auto seen = publish_force_flat_keys_.find(key);
        if (seen != publish_force_flat_keys_.end()) {

            if (slot.is_force_flat) {
                fresh->force_flat_pending_codes[seen->second].is_genuine_force_flat = true;
            }
            continue;
        }
        if (fresh->force_flat_pending_count >= fresh->force_flat_pending_codes.size()) continue;
        const char* code = instrument_registry_.code_of(iid);
        if (!code || code[0] == '\0') continue;
        const std::size_t out_idx = fresh->force_flat_pending_count++;
        publish_force_flat_keys_.emplace(key, out_idx);
        auto& out = fresh->force_flat_pending_codes[out_idx];
        out.instrument_id = iid;
        copy_snapshot_code(out.code, code);
        out.force_flat_sid_id = sid_id;
        out.is_genuine_force_flat = slot.is_force_flat;
    }

    for (const auto& kv : close_outstanding_) {
        if (fresh->close_outstanding_count >= fresh->close_outstanding.size()) break;
        const char* code = instrument_registry_.code_of(kv.first.instrument_id);
        if (!code || code[0] == '\0') continue;
        auto& out = fresh->close_outstanding[fresh->close_outstanding_count++];
        out.instrument_id = kv.first.instrument_id;
        copy_snapshot_code(out.code, code);
        out.is_buy = kv.first.is_buy;
        out.today_fp = kv.second.today_fp;
        out.yesterday_fp = kv.second.yesterday_fp;
    }

    const auto* retired = current_.exchange(fresh, std::memory_order_acq_rel);
    if (retired) {
        hp_domain_.retire_raw(retired, &snapshot_pool_, release_order_book_snapshot);
    }
    hp_domain_.try_scan_if_due();
}

void XtOrderRuntimeBook::populate_submit_slot_(OrderLifecycleSlot& atom,
                                               uint64_t id,
                                               XtInstrumentId iid,
                                               bool is_buy,
                                               int64_t qty_fp,
                                               int64_t price_fp,
                                               XtOffset close_offset,
                                               bool is_close,
                                               StrategySid sid) {
    atom.id = id;
    atom.instrument_id = iid;
    atom.is_buy = is_buy;
    atom.is_close = is_close;
    atom.close_offset = close_offset;
    atom.price_fp = price_fp;
    atom.leaves_qty_fp = qty_fp > 0 ? qty_fp : 0;
    atom.qty_fp = qty_fp > 0 ? qty_fp : 0;
    atom.filled_upd_fp = 0;
    atom.filled_trd_fp = 0;
    atom.seq = ++open_order_seq_;
    atom.locked_today_fp = 0;
    atom.locked_yesterday_fp = 0;

    atom.sid_id = sid.raw();
    xt_identity::OrderAttributionStore::remember_order_strategy_sid(id, sid);
    atom.filled_today_fp = 0;
    atom.filled_yesterday_fp = 0;
    atom.status = XtOrderStatus::Submitted;
    atom.recovered_unowned = false;
    atom.orphan_reap_attempted = false;
    atom.is_force_flat = false;
}

bool XtOrderRuntimeBook::should_skip_submit(uint64_t id) {
    assert(id != 0);
    if (id == 0) std::abort();

    return order_final_before_submit_.erase(id) > 0 || terminal_order_seen_(id);
}

void XtOrderRuntimeBook::on_order_submitted(uint64_t id,
                                            const char* code,
                                            bool is_buy,
                                            int64_t qty_fp,
                                            int64_t price_fp,
                                            XtOffset close_offset,
                                            const xt_account::CloseableView& closeable_view,
                                            StrategySid sid) {
    assert(id != 0);
    if (id == 0) std::abort();

    if (!is_bound_worker_thread()) {
        std::string code_copy(code ? code : "");
        if (post_to_worker([this, id, code_copy = std::move(code_copy), is_buy, qty_fp,
                            price_fp, close_offset, closeable_view, sid] {
                on_order_submitted(id, code_copy.c_str(), is_buy, qty_fp, price_fp,
                                   close_offset, closeable_view, sid);
            })) {
            return;
        }
    }

    const XtInstrumentId iid = (code && code[0] != '\0')
                                   ? instrument_registry_.register_or_get(code)
                                   : kInvalidInstrumentId;

    XtOrderRuntimeBook::OrderLifecycleSlot* old_slot = find_slot(id);
    if (old_slot) {
        remove_slot_close_contribution(*old_slot);
    }

    const bool is_close = (close_offset != XtOffset::Open);
    bool inserted = false;
    XtOrderRuntimeBook::OrderLifecycleSlot* atom = upsert_slot(id, inserted);
    if (!atom) return;

    populate_submit_slot_(*atom, id, iid, is_buy, qty_fp, price_fp, close_offset, is_close, sid);

    if (is_close && qty_fp > 0) {
        init_close_locks_on_submit(iid, is_buy, qty_fp, close_offset, closeable_view, *atom);
    }

    add_slot_close_contribution(*atom);

    if (iid != kInvalidInstrumentId) {
        tracked_codes_.insert(iid);
    }
    publish_snapshot_();
}

void XtOrderRuntimeBook::on_order_submitted(uint64_t id,
                                            const char* code,
                                            bool is_buy,
                                            int64_t qty_fp,
                                            int64_t price_fp,
                                            XtOffset close_offset,
                                            const xt_account::CloseableView& closeable_view) {
    on_order_submitted(id, code, is_buy, qty_fp, price_fp, close_offset, closeable_view,
                       StrategySid::legacy());
}

void XtOrderRuntimeBook::init_close_locks_on_submit(XtInstrumentId instrument_id,
                                                    bool is_buy,
                                                    int64_t qty_fp,
                                                    XtOffset close_offset,
                                                    const xt_account::CloseableView& closeable_view,
                                                    OrderLifecycleSlot& slot) const {
    slot.locked_today_fp = 0;
    slot.locked_yesterday_fp = 0;
    slot.filled_today_fp = 0;
    slot.filled_yesterday_fp = 0;
    if (qty_fp <= 0) return;

    if (close_offset == XtOffset::CloseToday) {
        slot.locked_today_fp = qty_fp;
        return;
    }
    if (close_offset == XtOffset::CloseYesterday) {
        slot.locked_yesterday_fp = qty_fp;
        return;
    }

    int64_t outstanding_today_fp = 0;
    int64_t outstanding_yesterday_fp = 0;

    const CloseOutstandingKey key{instrument_id, is_buy};
    auto agg_it = close_outstanding_.find(key);
    if (agg_it != close_outstanding_.end()) {
        outstanding_today_fp = agg_it->second.today_fp;
        outstanding_yesterday_fp = agg_it->second.yesterday_fp;
    }

    const int64_t available_today_fp =
        closeable_view.today_fp > outstanding_today_fp
            ? (closeable_view.today_fp - outstanding_today_fp)
            : 0;
    const int64_t available_yesterday_fp =
        closeable_view.yesterday_fp > outstanding_yesterday_fp
            ? (closeable_view.yesterday_fp - outstanding_yesterday_fp)
            : 0;

    int64_t left_fp = qty_fp;
    const int64_t alloc_yesterday_fp = std::min(left_fp, available_yesterday_fp);
    left_fp -= alloc_yesterday_fp;
    const int64_t alloc_today_fp = std::min(left_fp, available_today_fp);
    left_fp -= alloc_today_fp;

    slot.locked_yesterday_fp = alloc_yesterday_fp;
    slot.locked_today_fp = alloc_today_fp;
    if (left_fp > 0) {

        slot.locked_yesterday_fp += left_fp;
#ifndef NDEBUG
        fprintf(stderr, "[xt_runtime] WARN: close_outstanding fallback — "
                "left_fp=%lld归入昨仓 (instrument_id=%u, is_buy=%d)\n",
                (long long)left_fp, instrument_id, (int)is_buy);
#endif
    }
}

void XtOrderRuntimeBook::accumulate_close_fills_on_trade(OrderLifecycleSlot& slot,
                                                         int64_t fill_qty_fp,
                                                         bool fill_today) {
    if (!slot.is_close || fill_qty_fp <= 0) return;

    auto consume = [&](int64_t& filled_fp, int64_t locked_fp, int64_t& left_fp) {
        if (left_fp <= 0) return;
        const int64_t room_fp = locked_fp > filled_fp ? (locked_fp - filled_fp) : 0;
        if (room_fp <= 0) return;
        const int64_t use_fp = std::min(left_fp, room_fp);
        filled_fp += use_fp;
        left_fp -= use_fp;
    };

    int64_t left_fp = fill_qty_fp;
    if (fill_today) {
        consume(slot.filled_today_fp, slot.locked_today_fp, left_fp);
        consume(slot.filled_yesterday_fp, slot.locked_yesterday_fp, left_fp);
        if (left_fp > 0) slot.filled_today_fp += left_fp;
    } else {
        consume(slot.filled_yesterday_fp, slot.locked_yesterday_fp, left_fp);
        consume(slot.filled_today_fp, slot.locked_today_fp, left_fp);
        if (left_fp > 0) slot.filled_yesterday_fp += left_fp;
    }
}

int64_t XtOrderRuntimeBook::slot_leaves_from_filled(const OrderLifecycleSlot& s) {
    const int64_t filled = std::max(s.filled_upd_fp, s.filled_trd_fp);
    return s.qty_fp > filled ? (s.qty_fp - filled) : 0;
}

void XtOrderRuntimeBook::on_trade(const XtTradeRecord& tr) {

    XT_ALLOC_GUARD();
    const int64_t tr_qty_fp = tr.qty_fp;
    if (tr.order_id == 0 || tr_qty_fp <= 0) return;

    XtOrderRuntimeBook::OrderLifecycleSlot* atom = find_slot(tr.order_id);
    if (!atom) return;

    remove_slot_close_contribution(*atom);
    if (atom->qty_fp > 0) {
        atom->filled_trd_fp =
            std::min(atom->qty_fp, atom->filled_trd_fp + tr_qty_fp);
        atom->leaves_qty_fp = slot_leaves_from_filled(*atom);
    } else {

        atom->leaves_qty_fp = std::max<int64_t>(0, atom->leaves_qty_fp - tr_qty_fp);
    }
    accumulate_close_fills_on_trade(*atom, tr_qty_fp, tr.is_today);

    if (atom->leaves_qty_fp <= 0) {
        remember_terminal_order_(tr.order_id);
        erase_slot(tr.order_id);
    } else {
        atom->status = XtOrderStatus::PartiallyFilled;
        add_slot_close_contribution(*atom);
    }
    publish_snapshot_();
}

std::string XtOrderRuntimeBook::resolve_order_code_on_update(const XtOrderExecUpdate& upd) const {
    assert(upd.id != 0);
    if (upd.id == 0) std::abort();
    const XtOrderRuntimeBook::OrderLifecycleSlot* slot = find_slot(upd.id);
    if (!slot) return "";

    return instrument_registry_.code_of(slot->instrument_id);
}

XtInstrumentId XtOrderRuntimeBook::resolve_instrument_id_on_update(const XtOrderExecUpdate& upd) const {
    assert(upd.id != 0);
    if (upd.id == 0) std::abort();
    const XtOrderRuntimeBook::OrderLifecycleSlot* slot = find_slot(upd.id);
    if (!slot) return kInvalidInstrumentId;
    return slot->instrument_id;
}

void XtOrderRuntimeBook::upsert_order_atom_on_update(const XtOrderExecUpdate& upd,
                                                      XtInstrumentId upd_instrument_id,
                                                      int64_t effective_remaining_fp) {
    assert(upd.id != 0);
    if (upd.id == 0) std::abort();

    XtOrderRuntimeBook::OrderLifecycleSlot* atom = find_slot(upd.id);
    if (!atom) return;

    if (upd_instrument_id != kInvalidInstrumentId) atom->instrument_id = upd_instrument_id;
    atom->is_buy = upd.is_buy;
    atom->status = upd.status;
    atom->is_close = atom->is_close || upd.is_close();
    if (upd.close_offset != XtOffset::Open) {
        atom->close_offset = upd.close_offset;
    }
    if (atom->sid_id == 0 && upd.sid_id != 0) {
        atom->sid_id = upd.sid_id;
    }
    if (atom->sid_id != 0) {
        xt_identity::OrderAttributionStore::remember_order_strategy_sid(
            upd.id, StrategySid::from_raw(atom->sid_id));
    }

    if (upd.qty_fp > 0) atom->qty_fp = upd.qty_fp;
    if (atom->qty_fp > 0) {

        const int64_t remaining =
            effective_remaining_fp > 0 ? effective_remaining_fp : 0;
        const int64_t filled_from_upd =
            atom->qty_fp > remaining ? (atom->qty_fp - remaining) : 0;
        if (filled_from_upd > atom->filled_upd_fp) {
            atom->filled_upd_fp = filled_from_upd;
        }
        atom->leaves_qty_fp = slot_leaves_from_filled(*atom);
    } else {

        atom->leaves_qty_fp = effective_remaining_fp > 0 ? effective_remaining_fp : 0;
    }
}

void XtOrderRuntimeBook::on_order_update(const XtOrderExecUpdate& upd,
                                         bool final_status,
                                         int64_t effective_remaining_fp) {

    const bool was_locally_submitted = (upd.id != 0) && (find_slot(upd.id) != nullptr);
    if (upd.id != 0 && final_status) {
        remember_terminal_order_(upd.id);
    }
    if (upd.id != 0 && final_status && !was_locally_submitted) {

        order_final_before_submit_.insert(upd.id);
    }

    if (!final_status && upd.id != 0 && !was_locally_submitted &&
        terminal_order_seen_(upd.id)) {

        return;
    }

    if (!final_status && upd.id != 0 && !was_locally_submitted) {
        xt_runtime_book_drops::orphan_update_dropped().fetch_add(
            1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance().inc_runtime_book_drop_total("orphan_update");
#endif
        std::fprintf(stderr,
                     "[OrderBook][error] on_order_update: find_slot(%llu) failed and "
                     "!final_status — reverse sync should have filled this slot. "
                     "Possible bug: reverse sync missed / foreign order / userTag decode failure.\n",
                     static_cast<unsigned long long>(upd.id));
        return;
    }

    const XtInstrumentId upd_iid = resolve_instrument_id_on_update(upd);
    if (upd_iid != kInvalidInstrumentId) tracked_codes_.insert(upd_iid);

    if (final_status && upd.id != 0) {
        XtOrderRuntimeBook::OrderLifecycleSlot* slot = find_slot(upd.id);
        if (slot) {
            remove_slot_close_contribution(*slot);
            erase_slot(upd.id);
        }
        publish_snapshot_();
        return;
    }

    OrderLifecycleSlot before;
    bool has_before = false;
    if (upd.id != 0) {
        XtOrderRuntimeBook::OrderLifecycleSlot* slot = find_slot(upd.id);
        if (slot) {
            before = *slot;
            has_before = true;
        }
    }
    upsert_order_atom_on_update(upd, upd_iid, effective_remaining_fp);

    if (upd.id != 0) {
        XtOrderRuntimeBook::OrderLifecycleSlot* slot = find_slot(upd.id);
        if (slot) {
            if (has_before) remove_slot_close_contribution(before);
            add_slot_close_contribution(*slot);
        }
    }
    publish_snapshot_();
}

void XtOrderRuntimeBook::accumulate_close_outstanding_fp(const char* code,
                                                          bool is_buy,
                                                          int64_t& out_today_fp,
                                                          int64_t& out_yesterday_fp) const {
    out_today_fp = 0;
    out_yesterday_fp = 0;

    auto guard = hp_domain_.protect(current_);
    if (!guard) return;

    for (std::size_t i = 0; i < guard->close_outstanding_count; ++i) {
        const auto& entry = guard->close_outstanding[i];
        if (entry.is_buy != is_buy || !snapshot_code_equals(entry.code, code)) continue;
        out_today_fp = entry.today_fp;
        out_yesterday_fp = entry.yesterday_fp;
        break;
    }
    out_today_fp = xt_fp::clamp_nonneg(out_today_fp);
    out_yesterday_fp = xt_fp::clamp_nonneg(out_yesterday_fp);
}

void XtOrderRuntimeBook::pre_warm_instrument(const char* code) {
    if (!code || code[0] == '\0') return;

    instrument_registry_.register_or_get(code);
    publish_snapshot_();
}

void XtOrderRuntimeBook::track_code(const char* code) {
    if (!code || code[0] == '\0') return;

    track_code_unlocked(code);
    publish_snapshot_();
}

void XtOrderRuntimeBook::track_code_unlocked(const char* code) {
    if (!code || code[0] == '\0') return;
    const XtInstrumentId iid = instrument_registry_.register_or_get(code);
    if (iid != kInvalidInstrumentId) tracked_codes_.insert(iid);
}

void XtOrderRuntimeBook::reset_for_recovery(const std::vector<std::string>& position_codes,
                                             bool is_cold_start) {
    if (is_cold_start) {

        close_outstanding_.clear();
        open_order_seq_ = 0;
        order_final_before_submit_.clear();
        order_terminal_seen_.clear();
        order_terminal_fifo_.clear();
        tracked_codes_.clear();

        init_slot_pool();
        for (const auto& code : position_codes) {
            if (code.empty()) continue;
            const XtInstrumentId iid = instrument_registry_.register_or_get(code.c_str());
            if (iid != kInvalidInstrumentId) tracked_codes_.insert(iid);
        }
        publish_snapshot_();
        return;
    }

    for (const auto& code : position_codes) {
        if (code.empty()) continue;
        const XtInstrumentId iid = instrument_registry_.register_or_get(code.c_str());
        if (iid != kInvalidInstrumentId) tracked_codes_.insert(iid);
    }
    publish_snapshot_();
}

void XtOrderRuntimeBook::collect_tracked_codes(std::vector<std::string>& out) const {
    out.clear();
    auto guard = hp_domain_.protect(current_);
    if (!guard) return;
    out.reserve(guard->tracked_code_count);
    for (std::size_t i = 0; i < guard->tracked_code_count; ++i) {
        const auto& entry = guard->tracked_codes[i];
        if (entry.code[0] != '\0') out.emplace_back(entry.code);
    }
}

std::vector<std::string> XtOrderRuntimeBook::sync_position_snapshot(
    const std::vector<std::string>& position_codes,
    bool is_recovery,
    bool is_cold_start) {
    std::vector<std::string> stale_codes;
    if (is_recovery) {

        {
            std::unordered_set<std::string> seen;
            seen.reserve(position_codes.size());
            for (const auto& c : position_codes) {
                if (!c.empty()) seen.insert(c);
            }
            std::vector<std::string> tracked;
            collect_tracked_codes(tracked);
            for (auto& code : tracked) {
                if (!code.empty() && seen.count(code) == 0) {
                    stale_codes.push_back(std::move(code));
                }
            }
        }

        reset_for_recovery(position_codes, is_cold_start);
        return stale_codes;
    }

    std::unordered_set<std::string> seen;
    seen.reserve(position_codes.size());
    for (const auto& c : position_codes) {
        if (!c.empty()) seen.insert(c);
    }
    std::vector<std::string> tracked;
    collect_tracked_codes(tracked);
    for (auto& code : tracked) {
        if (!code.empty() && seen.count(code) == 0) {
            stale_codes.push_back(std::move(code));
        }
    }

    for (const auto& c : position_codes) {
        if (!c.empty()) track_code_unlocked(c.c_str());
    }
    publish_snapshot_();
    return stale_codes;
}

void XtOrderRuntimeBook::on_position_snapshot(
    const std::vector<std::string>& position_codes,
    bool is_recovery,
    bool is_cold_start) {

    (void)sync_position_snapshot(position_codes, is_recovery, is_cold_start);
}

int64_t XtOrderRuntimeBook::open_no_trade_qty_fp(const char* code, bool is_buy) const {
    if (!code || code[0] == '\0') return 0;
    auto guard = hp_domain_.protect(current_);
    if (!guard) return 0;

    int64_t out_fp = 0;
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        const auto& snap = guard->orders[i];
        if (!snapshot_code_equals(snap.code, code)) continue;
        if (snap.is_close || snap.is_buy != is_buy) continue;
        if (snap.leaves_qty_fp > 0) out_fp += snap.leaves_qty_fp;
    }
    return out_fp;
}

int64_t XtOrderRuntimeBook::undone_qty_fp(const char* code) const {
    XT_ALLOC_GUARD();
    const bool filter_by_code = (code && code[0] != '\0');
    auto guard = hp_domain_.protect(current_);
    if (!guard) return 0;
    int64_t sum_fp = 0;
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        const auto& snap = guard->orders[i];
        if (filter_by_code && !snapshot_code_equals(snap.code, code)) continue;
        if (snap.id == 0) continue;
        if (snap.leaves_qty_fp > 0) sum_fp += snap.leaves_qty_fp;
    }
    return xt_fp::clamp_nonneg(sum_fp);
}

int64_t XtOrderRuntimeBook::signed_inflight_fp(StrategySid sid, const char* code) const {
    XT_ALLOC_GUARD();
    const bool filter_by_code = (code && code[0] != '\0');
    const bool filter_by_sid  = sid.has_value();
    const uint32_t want_sid   = sid.raw();
    auto guard = hp_domain_.protect(current_);
    if (!guard) return 0;
    int64_t sum_fp = 0;
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        const auto& snap = guard->orders[i];
        if (snap.id == 0) continue;
        if (snap.leaves_qty_fp <= 0) continue;
        if (filter_by_code && !snapshot_code_equals(snap.code, code)) continue;
        if (filter_by_sid && snap.sid_id != want_sid) continue;

        sum_fp += snap.is_buy ? snap.leaves_qty_fp : -snap.leaves_qty_fp;
    }
    return sum_fp;
}

void XtOrderRuntimeBook::collect_closeout_requests(std::vector<uint64_t>& cancel_ids) const {
    cancel_ids.clear();

    auto guard = hp_domain_.protect(current_);
    if (!guard) return;

    cancel_ids.reserve(guard->order_count);
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        const auto& snap = guard->orders[i];
        if (snap.leaves_qty_fp > 0) cancel_ids.push_back(snap.id);
    }
}

void XtOrderRuntimeBook::record_force_flat_order(uint64_t oid,
                                                  const std::string& code,
                                                  bool is_buy,
                                                  int64_t qty_fp,
                                                  XtOffset close_offset,
                                                  StrategySid sid) {
    assert(oid != 0);
    assert(!code.empty());
    if (oid == 0 || code.empty()) std::abort();

    if (!is_bound_worker_thread()) {
        if (post_to_worker([this, oid, code, is_buy, qty_fp, close_offset, sid] {
                record_force_flat_order(oid, code, is_buy, qty_fp, close_offset, sid);
            })) {
            return;
        }
    }

    const XtInstrumentId iid = instrument_registry_.register_or_get(code.c_str());

    bool inserted = false;
    XtOrderRuntimeBook::OrderLifecycleSlot* atom = upsert_slot(oid, inserted);
    if (!atom) return;

    if (!inserted) {
        remove_slot_close_contribution(*atom);
    }
    if (inserted) {
        atom->id = oid;
        atom->seq = ++open_order_seq_;
    }
    atom->instrument_id = iid;
    atom->is_buy = is_buy;
    atom->is_close = true;
    atom->is_force_flat = true;
    atom->close_offset = close_offset;
    atom->leaves_qty_fp = std::max<int64_t>(0, qty_fp);
    atom->qty_fp = std::max<int64_t>(0, qty_fp);
    atom->filled_upd_fp = 0;
    atom->filled_trd_fp = 0;
    atom->locked_today_fp = 0;
    atom->locked_yesterday_fp = 0;
    atom->filled_today_fp = 0;
    atom->filled_yesterday_fp = 0;
    atom->sid_id = sid.raw();

    atom->status = XtOrderStatus::Submitted;
    atom->recovered_unowned = false;
    atom->orphan_reap_attempted = false;
    xt_identity::OrderAttributionStore::remember_order_strategy_sid(oid, sid);

    if (atom->leaves_qty_fp > 0) {
        if (close_offset == XtOffset::CloseToday) {
            atom->locked_today_fp = atom->leaves_qty_fp;
        } else {
            atom->locked_yesterday_fp = atom->leaves_qty_fp;
        }
    }
    add_slot_close_contribution(*atom);

    if (iid != kInvalidInstrumentId) {
        tracked_codes_.insert(iid);
    }
    publish_snapshot_();
}

void XtOrderRuntimeBook::record_global_force_flat_order(uint64_t oid,
                                                         const std::string& code,
                                                         bool is_buy,
                                                         int64_t qty_fp,
                                                         XtOffset close_offset) {
    record_force_flat_order(oid, code, is_buy, qty_fp, close_offset, StrategySid::legacy());
}

void XtOrderRuntimeBook::fillup_populate_slot_(OrderLifecycleSlot& atom,
                                               const AliveInflightOrder& ord,
                                               XtInstrumentId iid,
                                               bool is_close,
                                               bool inserted_real,
                                               int64_t leaves_fp) {
    atom.instrument_id = iid;
    atom.is_buy = ord.is_buy;
    atom.is_close = is_close;
    atom.close_offset = ord.close_offset;
    atom.price_fp = ord.price_fp;
    atom.leaves_qty_fp = leaves_fp;

    {
        const int64_t seed_filled =
            ord.filled_qty_fp > 0 ? ord.filled_qty_fp : 0;
        if (ord.total_qty_fp > 0) atom.qty_fp = ord.total_qty_fp;
        if (inserted_real) {
            atom.filled_upd_fp = seed_filled;
            atom.filled_trd_fp = seed_filled;
        } else {
            atom.filled_upd_fp = std::max(atom.filled_upd_fp, seed_filled);
            atom.filled_trd_fp = std::max(atom.filled_trd_fp, seed_filled);
        }
        if (atom.qty_fp > 0) {
            atom.leaves_qty_fp = slot_leaves_from_filled(atom);
        }
    }
    atom.status = XtOrderStatus::Recovered;
    atom.locked_today_fp = 0;
    atom.locked_yesterday_fp = 0;
    atom.filled_today_fp = 0;
    atom.filled_yesterday_fp = 0;
}

void XtOrderRuntimeBook::fillup_resolve_sid_(OrderLifecycleSlot& atom,
                                             const AliveInflightOrder& ord,
                                             bool inserted_real) {
    const StrategySid resolved_sid =
        ord.strategy_sid.has_value()
            ? ord.strategy_sid
            : xt_identity::OrderAttributionStore::lookup_order_strategy_sid(ord.local_id);
    const uint32_t incoming_sid_id = resolved_sid.raw();
    if (inserted_real || incoming_sid_id != 0) {
        atom.sid_id = incoming_sid_id;
    }
    if (atom.sid_id != 0) {
        xt_identity::OrderAttributionStore::remember_order_strategy_sid(
            ord.local_id, StrategySid::from_raw(atom.sid_id));
    } else if (inserted_real) {
        xt_identity::OrderAttributionStore::forget_order_strategy_sid(ord.local_id);
    }

    if (inserted_real) {
        atom.recovered_unowned = (incoming_sid_id == 0);
    } else if (incoming_sid_id != 0) {
        atom.recovered_unowned = false;
    }
}

void XtOrderRuntimeBook::fillup_alloc_close_locks_(OrderLifecycleSlot& atom,
                                                   XtOffset close_offset,
                                                   int64_t leaves_fp,
                                                   xt_account::CloseableView& rem) {

    int64_t left_fp = leaves_fp;
    if (close_offset == XtOffset::CloseToday) {
        const int64_t take = std::min(left_fp, rem.today_fp);
        atom.locked_today_fp = take;
        rem.today_fp -= take;
        left_fp -= take;
        if (left_fp > 0) {

            atom.locked_today_fp += left_fp;
            left_fp = 0;
        }
    } else if (close_offset == XtOffset::CloseYesterday) {
        const int64_t take = std::min(left_fp, rem.yesterday_fp);
        atom.locked_yesterday_fp = take;
        rem.yesterday_fp -= take;
        left_fp -= take;
        if (left_fp > 0) {
            atom.locked_yesterday_fp += left_fp;
            left_fp = 0;
        }
    } else {

        const int64_t take_y = std::min(left_fp, rem.yesterday_fp);
        atom.locked_yesterday_fp = take_y;
        rem.yesterday_fp -= take_y;
        left_fp -= take_y;

        const int64_t take_t = std::min(left_fp, rem.today_fp);
        atom.locked_today_fp = take_t;
        rem.today_fp -= take_t;
        left_fp -= take_t;

        if (left_fp > 0) {

            atom.locked_yesterday_fp += left_fp;
            left_fp = 0;
        }
    }
}

void XtOrderRuntimeBook::fillup_alloc_filled_(OrderLifecycleSlot& atom, int64_t filled_fp) {

    if (filled_fp <= 0) return;

    if (atom.locked_today_fp == 0) {
        atom.filled_yesterday_fp = std::min(filled_fp, atom.locked_yesterday_fp);
    } else if (atom.locked_yesterday_fp == 0) {
        atom.filled_today_fp = std::min(filled_fp, atom.locked_today_fp);
    } else {

        const int64_t total_locked = atom.locked_today_fp + atom.locked_yesterday_fp;
        if (total_locked > 0) {
            const int64_t t = (filled_fp * atom.locked_today_fp) / total_locked;
            const int64_t y = filled_fp - t;
            atom.filled_today_fp = std::min(t, atom.locked_today_fp);
            atom.filled_yesterday_fp = std::min(y, atom.locked_yesterday_fp);
        }
    }
}

namespace {
// fillup batch 内顺序扣减 closeable quota 的临时余量 map key。
// key = (code, is_buy_to_close)，value = 剩余可平量。
struct QuotaKey {
    std::string code;
    bool is_buy_to_close;
    bool operator==(const QuotaKey& rhs) const noexcept {
        return is_buy_to_close == rhs.is_buy_to_close && code == rhs.code;
    }
};
struct QuotaKeyHash {
    size_t operator()(const QuotaKey& k) const noexcept {
        const auto h1 = std::hash<std::string>{}(k.code);
        const auto h2 = std::hash<bool>{}(k.is_buy_to_close);
        return h1 ^ (h2 << 1);
    }
};
}

void XtOrderRuntimeBook::fillup_alive_inflight_orders_batch(
    const std::vector<AliveInflightOrder>& orders,
    std::function<xt_account::CloseableView(const std::string& code,
                                             bool is_buy_to_close)> view_for) {

    std::unordered_map<QuotaKey, xt_account::CloseableView, QuotaKeyHash> remaining;
    remaining.reserve(orders.size() * 2);

    for (const auto& ord : orders) {
        if (ord.local_id == 0 || ord.code.empty()) continue;

        const XtInstrumentId iid = instrument_registry_.register_or_get(ord.code.c_str());
        if (iid == kInvalidInstrumentId) continue;

        const bool is_close = (ord.close_offset != XtOffset::Open);

        const bool is_buy_to_close = ord.is_buy;

        bool inserted_real = false;
        OrderLifecycleSlot* atom = upsert_slot(ord.local_id, inserted_real);
        if (!atom) {

            continue;
        }

        if (!inserted_real) {
            remove_slot_close_contribution(*atom);
        } else {
            atom->id = ord.local_id;
            atom->seq = ++open_order_seq_;
        }

        const int64_t total_fp = ord.total_qty_fp > 0 ? ord.total_qty_fp : 0;
        const int64_t filled_fp = ord.filled_qty_fp > 0 ? ord.filled_qty_fp : 0;
        const int64_t leaves_fp = total_fp > filled_fp ? (total_fp - filled_fp) : 0;

        fillup_populate_slot_(*atom, ord, iid, is_close, inserted_real, leaves_fp);
        fillup_resolve_sid_(*atom, ord, inserted_real);

        if (is_close && leaves_fp > 0) {
            const QuotaKey key{ord.code, is_buy_to_close};
            auto it = remaining.find(key);
            if (it == remaining.end()) {

                it = remaining.emplace(key, view_for ? view_for(ord.code, is_buy_to_close)
                                                     : xt_account::CloseableView{}).first;
            }

            fillup_alloc_close_locks_(*atom, ord.close_offset, leaves_fp, it->second);
            fillup_alloc_filled_(*atom, filled_fp);
        }

        add_slot_close_contribution(*atom);

        tracked_codes_.insert(iid);
    }

    publish_snapshot_();
}

void XtOrderRuntimeBook::collect_active_order_prices(const char* code,
                                                      std::vector<int64_t>& bid_prices,
                                                      std::vector<int64_t>& ask_prices) const {
    bid_prices.clear();
    ask_prices.clear();
    if (!code || code[0] == '\0') return;
    auto guard = hp_domain_.protect(current_);
    if (!guard) return;
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        const auto& s = guard->orders[i];
        if (!snapshot_code_equals(s.code, code)) continue;
        if (s.id == 0 || s.leaves_qty_fp <= 0) continue;
        if (s.is_buy) bid_prices.push_back(s.price_fp);
        else          ask_prices.push_back(s.price_fp);
    }
}

void XtOrderRuntimeBook::collect_working_ids_for_code(const char* code,
                                                       std::vector<uint64_t>& ids) const {
    ids.clear();
    if (!code || code[0] == '\0') return;
    auto guard = hp_domain_.protect(current_);
    if (!guard) return;
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        const auto& s = guard->orders[i];
        if (!snapshot_code_equals(s.code, code)) continue;
        if (s.id == 0 || s.leaves_qty_fp <= 0) continue;
        ids.push_back(s.id);
    }
}

void XtOrderRuntimeBook::collect_working_ids_for_code_and_sid(const char* code,
                                                               StrategySid sid,
                                                               std::vector<uint64_t>& ids) const {
    ids.clear();
    if (!code || code[0] == '\0' || !sid.has_value()) return;
    const uint32_t sid_id = sid.raw();
    auto guard = hp_domain_.protect(current_);
    if (!guard) return;
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        const auto& s = guard->orders[i];
        if (!snapshot_code_equals(s.code, code)) continue;
        if (s.id == 0 || s.leaves_qty_fp <= 0) continue;
        if (s.sid_id != sid_id) continue;
        ids.push_back(s.id);
    }
}

bool XtOrderRuntimeBook::is_force_flat_pending(const char* code) const {
    if (!code || code[0] == '\0') return false;
    auto guard = hp_domain_.protect(current_);
    if (!guard) return false;
    for (std::size_t i = 0; i < guard->force_flat_pending_count; ++i) {
        if (snapshot_code_equals(guard->force_flat_pending_codes[i].code, code)) return true;
    }
    return false;
}

uint32_t XtOrderRuntimeBook::force_flat_pending_sid_for(const char* code) const {
    if (!code || code[0] == '\0') return 0;
    auto guard = hp_domain_.protect(current_);
    if (!guard) return 0;
    for (std::size_t i = 0; i < guard->force_flat_pending_count; ++i) {
        if (snapshot_code_equals(guard->force_flat_pending_codes[i].code, code)) {
            return guard->force_flat_pending_codes[i].force_flat_sid_id;
        }
    }
    return 0;
}

bool XtOrderRuntimeBook::is_genuine_force_flat_pending(const char* code) const {
    if (!code || code[0] == '\0') return false;
    auto guard = hp_domain_.protect(current_);
    if (!guard) return false;
    for (std::size_t i = 0; i < guard->force_flat_pending_count; ++i) {
        if (guard->force_flat_pending_codes[i].is_genuine_force_flat
            && snapshot_code_equals(guard->force_flat_pending_codes[i].code, code)) {
            return true;
        }
    }
    return false;
}

bool XtOrderRuntimeBook::is_genuine_force_flat_pending_for_sid(const char* code, StrategySid sid) const {
    if (!code || code[0] == '\0') return false;
    auto guard = hp_domain_.protect(current_);
    if (!guard) return false;
    for (std::size_t i = 0; i < guard->force_flat_pending_count; ++i) {
        if (guard->force_flat_pending_codes[i].is_genuine_force_flat
            && snapshot_code_equals(guard->force_flat_pending_codes[i].code, code)
            && guard->force_flat_pending_codes[i].force_flat_sid_id == sid.raw()) {
            return true;
        }
    }
    return false;
}

bool XtOrderRuntimeBook::has_genuine_global_force_flat_pending(const char* code) const {
    return is_genuine_force_flat_pending_for_sid(code, StrategySid::legacy());
}

bool XtOrderRuntimeBook::is_collect_thread_allowed_() const noexcept {

    if (marshal_bound_.load(std::memory_order_acquire)) {
        return std::this_thread::get_id() == marshal_worker_tid_;
    }

    std::thread::id expected{};
    const std::thread::id self = std::this_thread::get_id();
    if (collect_first_touch_tid_.compare_exchange_strong(
            expected, self, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return true;
    }
    return expected == self;
}

void XtOrderRuntimeBook::on_cross_thread_direct_read_blocked_() const noexcept {

    xt_runtime_book_drops::cross_thread_direct_read_blocked().fetch_add(
        1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
    ct::metrics::XtMetricRegistry::instance().inc_runtime_book_drop_total(
        "cross_thread_direct_read");
#endif
    static std::atomic<bool> warned{false};
    if (!warned.exchange(true)) {
        std::fprintf(stderr,
                     "[OrderBook][error] cross-thread direct read blocked — "
                     "collect_* must run on the book worker thread\n");
    }
}

size_t XtOrderRuntimeBook::collect_open_orders(XtOrderExecUpdate* out, size_t max,
                                               const char* code_filter,
                                               uint32_t sid_filter) const {
    if (!out || max == 0) return 0;
    if (!is_collect_thread_allowed_()) {
        on_cross_thread_direct_read_blocked_();
        return 0;
    }

    XtInstrumentId filter_iid = kInvalidInstrumentId;
    if (code_filter && code_filter[0] != '\0') {
        filter_iid = instrument_registry_.id_of(code_filter);
        if (filter_iid == kInvalidInstrumentId) return 0;
    }

    size_t n = 0;
    for (const auto& kv : id_to_slot_idx_) {
        if (n >= max) break;
        const uint16_t idx = kv.second;
        if (idx >= kMaxInflightOrders) continue;
        const OrderLifecycleSlot& slot = slot_pool_[idx];
        if (slot.id == 0) continue;
        if (slot.leaves_qty_fp <= 0) continue;
        if (sid_filter != 0 && slot.sid_id != sid_filter) continue;
        if (filter_iid != kInvalidInstrumentId && slot.instrument_id != filter_iid) continue;

        XtOrderExecUpdate u{};
        u.id           = slot.id;
        u.is_buy       = slot.is_buy;
        u.close_offset = slot.close_offset;
        u.status       = slot.status;
        u.price_fp     = slot.price_fp;
        u.filled_fp    = slot.filled_today_fp + slot.filled_yesterday_fp;
        u.leaves_fp    = slot.leaves_qty_fp;
        u.qty_fp       = u.filled_fp + u.leaves_fp;
        u.sid_id       = slot.sid_id;

        out[n++] = u;
    }
    return n;
}

uint32_t XtOrderRuntimeBook::order_sid_id(uint64_t id) const {
    if (id == 0) return 0;
    if (!is_collect_thread_allowed_()) {
        on_cross_thread_direct_read_blocked_();
        return 0;
    }
    const auto it = id_to_slot_idx_.find(id);
    if (it == id_to_slot_idx_.end()) return 0;
    const uint16_t idx = it->second;
    if (idx >= kMaxInflightOrders) return 0;
    const OrderLifecycleSlot& slot = slot_pool_[idx];
    if (slot.id != id) return 0;
    return slot.sid_id;
}

size_t XtOrderRuntimeBook::collect_recovered_unowned_ids(uint64_t* out, size_t max) const {
    if (!out || max == 0) return 0;
    if (!is_collect_thread_allowed_()) {
        on_cross_thread_direct_read_blocked_();
        return 0;
    }

    size_t n = 0;
    for (const auto& kv : id_to_slot_idx_) {
        if (n >= max) break;
        const uint16_t idx = kv.second;
        if (idx >= kMaxInflightOrders) continue;
        const OrderLifecycleSlot& slot = slot_pool_[idx];
        if (slot.id == 0 || !slot.recovered_unowned) continue;
        if (slot.leaves_qty_fp <= 0) continue;
        out[n++] = slot.id;
    }
    return n;
}

size_t XtOrderRuntimeBook::reap_recovered_unowned(uint64_t* out, size_t max) {
    if (!out || max == 0) return 0;
    if (!is_collect_thread_allowed_()) {
        on_cross_thread_direct_read_blocked_();
        return 0;
    }

    size_t n = 0;
    std::array<uint64_t, 256> dead{};
    size_t dead_n = 0;
    for (const auto& kv : id_to_slot_idx_) {
        const uint16_t idx = kv.second;
        if (idx >= kMaxInflightOrders) continue;
        OrderLifecycleSlot& slot = slot_pool_[idx];
        if (slot.id == 0 || !slot.recovered_unowned) continue;
        if (slot.leaves_qty_fp <= 0) continue;
        if (!slot.orphan_reap_attempted) {

            slot.orphan_reap_attempted = true;
            if (n < max) out[n++] = slot.id;
        } else if (dead_n < dead.size()) {

            dead[dead_n++] = slot.id;
        }
    }
    for (size_t i = 0; i < dead_n; ++i) {
        erase_slot(dead[i]);
        xt_runtime_book_drops::orphan_reap_dropped().fetch_add(1, std::memory_order_relaxed);
#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance().inc_runtime_book_drop_total("orphan_reap");
#endif
    }
    if (dead_n > 0) publish_snapshot_();
    return n;
}

size_t XtOrderRuntimeBook::open_order_count() const {
    auto guard = hp_domain_.protect(current_);
    if (!guard) return 0;
    size_t count = 0;
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        if (guard->orders[i].leaves_qty_fp > 0) ++count;
    }
    return count;
}

size_t XtOrderRuntimeBook::tracked_code_count() const {
    auto guard = hp_domain_.protect(current_);
    return guard ? guard->tracked_code_count : 0;
}

OrderSnapshot XtOrderRuntimeBook::snapshot_order_by_id(uint64_t id) const noexcept {
    auto guard = hp_domain_.protect(current_);
    if (!guard) return {};
    for (std::size_t i = 0; i < guard->order_count; ++i) {
        if (guard->orders[i].id == id) return guard->orders[i];
    }
    return {};
}

xt_util::sync::HazardGuard<const OrderBookSnapshot>
XtOrderRuntimeBook::snapshot_all_orders() const noexcept {
    return hp_domain_.protect(current_);
}

}
}
