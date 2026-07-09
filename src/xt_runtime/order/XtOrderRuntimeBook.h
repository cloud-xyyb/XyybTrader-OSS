#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "api/StrategySid.h"
#include "xt_core/order/XtInstrumentRegistry.h"
#include "order/XtOrderSnapshot.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtOffset.h"
#include "xt_core/order/XtOrder.h"
#include "xt_util/sync/XtHazardDomain.h"
#include "xt_util/sync/XtSnapshotPool.h"

struct AliveInflightOrder;
struct XtTradeRecord;

namespace xt_account {
struct CloseableView;
}

namespace xt_runtime {
namespace order {

using xt_core::XtInstrumentId;
using xt_core::XtInstrumentRegistry;
using xt_core::kInvalidInstrumentId;
using xt_core::kMaxInstruments;

/// @brief 进程内在途订单的权威账本。
///
/// - 每笔订单从提交到终态的本地状态记录
/// - submit、回报和 recovery 以 XtOrderRuntimeBook 为准更新和查询。
/// - engine worker 单线程写；策略在同一 worker 线程可直读重建 open-order 列表，
/// - 其他线程只能读 Hazard Pointer 保护的 immutable OrderBookSnapshot，不碰内部 slot。
class XtOrderRuntimeBook {
public:
    XtOrderRuntimeBook();

    XtOrderRuntimeBook(XtOrderRuntimeBook&& other) noexcept;
    XtOrderRuntimeBook& operator=(XtOrderRuntimeBook&& other) noexcept;
    XtOrderRuntimeBook(const XtOrderRuntimeBook&) = delete;
    XtOrderRuntimeBook& operator=(const XtOrderRuntimeBook&) = delete;

    using WorkerPostFn = std::function<bool(std::function<void()>)>;

    void bind_worker_marshal(std::thread::id worker_tid, WorkerPostFn post);

    void unbind_worker_marshal();

    [[nodiscard]] bool is_bound_worker_thread() const noexcept;

    [[nodiscard]] bool post_to_worker(std::function<void()> fn) const;

    [[nodiscard]] bool should_skip_submit(uint64_t id);

    void on_order_submitted(uint64_t id,
                            const char* code,
                            bool is_buy,
                            int64_t qty_fp,
                            int64_t price_fp,
                            XtOffset close_offset,
                            const xt_account::CloseableView& closeable_view,
                            StrategySid sid);

    void on_order_submitted(uint64_t id,
                            const char* code,
                            bool is_buy,
                            int64_t qty_fp,
                            int64_t price_fp,
                            XtOffset close_offset,
                            const xt_account::CloseableView& closeable_view);

    void on_trade(const XtTradeRecord& tr);

    [[nodiscard]] std::string resolve_order_code_on_update(const XtOrderExecUpdate& upd) const;

    [[nodiscard]] uint32_t order_sid_id(uint64_t id) const;

    void on_order_update(const XtOrderExecUpdate& upd,
                         bool final_status,
                         int64_t effective_remaining_fp);

    void pre_warm_instrument(const char* code);

    void track_code(const char* code);

    void reset_for_recovery(const std::vector<std::string>& position_codes,
                            bool is_cold_start);

    void collect_tracked_codes(std::vector<std::string>& out) const;

    void fillup_alive_inflight_orders_batch(
        const std::vector<AliveInflightOrder>& orders,
        std::function<xt_account::CloseableView(const std::string& code,
                                                 bool is_buy_to_close)> view_for);

    [[nodiscard]] std::vector<std::string> sync_position_snapshot(
        const std::vector<std::string>& position_codes,
        bool is_recovery,
        bool is_cold_start);

    void on_position_snapshot(const std::vector<std::string>& position_codes,
                              bool is_recovery,
                              bool is_cold_start);

    void accumulate_close_outstanding_fp(const char* code,
                                         bool is_buy,
                                         int64_t& out_today_fp,
                                         int64_t& out_yesterday_fp) const;

    [[nodiscard]] int64_t open_no_trade_qty_fp(const char* code, bool is_buy) const;

    [[nodiscard]] int64_t undone_qty_fp(const char* code) const;

    [[nodiscard]] int64_t signed_inflight_fp(StrategySid sid, const char* code) const;

    void collect_closeout_requests(std::vector<uint64_t>& cancel_ids) const;

    void record_force_flat_order(uint64_t oid,
                                 const std::string& code,
                                 bool is_buy,
                                 int64_t qty_fp,
                                 XtOffset close_offset,
                                 StrategySid sid);

    void record_global_force_flat_order(uint64_t oid,
                                        const std::string& code,
                                        bool is_buy,
                                        int64_t qty_fp,
                                        XtOffset close_offset);

    void collect_active_order_prices(const char* code,
                                     std::vector<int64_t>& bid_prices,
                                     std::vector<int64_t>& ask_prices) const;

    void collect_working_ids_for_code(const char* code,
                                      std::vector<uint64_t>& ids) const;

    void collect_working_ids_for_code_and_sid(const char* code,
                                              StrategySid sid,
                                              std::vector<uint64_t>& ids) const;

    [[nodiscard]] bool is_force_flat_pending(const char* code) const;

    [[nodiscard]] uint32_t force_flat_pending_sid_for(const char* code) const;

    [[nodiscard]] bool is_genuine_force_flat_pending(const char* code) const;

    [[nodiscard]] bool is_genuine_force_flat_pending_for_sid(const char* code, StrategySid sid) const;

    [[nodiscard]] bool has_genuine_global_force_flat_pending(const char* code) const;

    [[nodiscard]] size_t open_order_count() const;

    [[nodiscard]] size_t tracked_code_count() const;

    [[nodiscard]] OrderSnapshot snapshot_order_by_id(uint64_t id) const noexcept;

    [[nodiscard]] xt_util::sync::HazardGuard<const OrderBookSnapshot>
    snapshot_all_orders() const noexcept;

    [[nodiscard]] size_t collect_open_orders(XtOrderExecUpdate* out, size_t max,
                                             const char* code_filter = nullptr,
                                             uint32_t sid_filter = 0) const;

    [[nodiscard]] size_t collect_recovered_unowned_ids(uint64_t* out, size_t max) const;

    [[nodiscard]] size_t reap_recovered_unowned(uint64_t* out, size_t max);

private:
    struct CloseOutstandingKey {
        XtInstrumentId instrument_id = kInvalidInstrumentId;
        bool is_buy = true;

        bool operator==(const CloseOutstandingKey& rhs) const noexcept {
            return instrument_id == rhs.instrument_id && is_buy == rhs.is_buy;
        }
    };

    struct CloseOutstandingKeyHash {
        size_t operator()(const CloseOutstandingKey& key) const noexcept {
            const uint64_t packed =
                (static_cast<uint64_t>(key.instrument_id) << 1) |
                static_cast<uint64_t>(key.is_buy ? 1 : 0);
            return std::hash<uint64_t>{}(packed);
        }
    };

    struct CloseOutstandingAgg {
        int64_t today_fp = 0;
        int64_t yesterday_fp = 0;
    };

    struct OrderLifecycleSlot {
        uint64_t id = 0;
        XtInstrumentId instrument_id = kInvalidInstrumentId;
        bool is_buy = true;
        bool is_close = false;

        bool is_force_flat = false;
        XtOffset close_offset = XtOffset::Close;
        int64_t price_fp = 0;
        int64_t leaves_qty_fp = 0;

        int64_t qty_fp = 0;

        int64_t filled_upd_fp = 0;
        int64_t filled_trd_fp = 0;
        int64_t locked_today_fp = 0;
        int64_t locked_yesterday_fp = 0;
        int64_t filled_today_fp = 0;
        int64_t filled_yesterday_fp = 0;
        uint64_t seq = 0;

        uint32_t sid_id = 0;

        XtOrderStatus status = XtOrderStatus::Submitted;

        bool recovered_unowned = false;

        bool orphan_reap_attempted = false;
    };

    static constexpr uint16_t kMaxInflightOrders =
        static_cast<uint16_t>(kOrderSnapshotMaxOrders);
    static constexpr uint16_t kInvalidSlotIdx = 0xFFFF;
    static constexpr std::size_t kTerminalOrderCacheCap = 8192;

    static int64_t slot_leaves_from_filled(const OrderLifecycleSlot& s);
    void init_slot_pool();
    uint16_t alloc_slot();
    void free_slot_by_idx(uint16_t idx);
    OrderLifecycleSlot* find_slot(uint64_t id);
    const OrderLifecycleSlot* find_slot(uint64_t id) const;
    OrderLifecycleSlot* upsert_slot(uint64_t id, bool& inserted);
    void erase_slot(uint64_t id);
    [[nodiscard]] bool terminal_order_seen_(uint64_t id) const noexcept;
    void remember_terminal_order_(uint64_t id);

    static int64_t unfilled_today_fp(const OrderLifecycleSlot& slot);
    static int64_t unfilled_yesterday_fp(const OrderLifecycleSlot& slot);
    static bool has_close_contribution(const OrderLifecycleSlot& slot);

    void add_close_outstanding_delta(XtInstrumentId id,
                                     bool is_buy,
                                     int64_t delta_today_fp,
                                     int64_t delta_yesterday_fp);
    void add_slot_close_contribution(const OrderLifecycleSlot& slot);
    void remove_slot_close_contribution(const OrderLifecycleSlot& slot);

    void track_code_unlocked(const char* code);
    void init_close_locks_on_submit(XtInstrumentId instrument_id,
                                    bool is_buy,
                                    int64_t qty_fp,
                                    XtOffset close_offset,
                                    const xt_account::CloseableView& closeable_view,
                                    OrderLifecycleSlot& slot) const;
    void accumulate_close_fills_on_trade(OrderLifecycleSlot& slot,
                                         int64_t fill_qty_fp,
                                         bool fill_today);
    [[nodiscard]] XtInstrumentId resolve_instrument_id_on_update(const XtOrderExecUpdate& upd) const;
    void upsert_order_atom_on_update(const XtOrderExecUpdate& upd,
                                     XtInstrumentId upd_instrument_id,
                                     int64_t effective_remaining_fp);
    void populate_submit_slot_(OrderLifecycleSlot& atom,
                               uint64_t id,
                               XtInstrumentId iid,
                               bool is_buy,
                               int64_t qty_fp,
                               int64_t price_fp,
                               XtOffset close_offset,
                               bool is_close,
                               StrategySid sid);

    void fillup_populate_slot_(OrderLifecycleSlot& atom,
                               const AliveInflightOrder& ord,
                               XtInstrumentId iid,
                               bool is_close,
                               bool inserted_real,
                               int64_t leaves_fp);
    void fillup_resolve_sid_(OrderLifecycleSlot& atom,
                             const AliveInflightOrder& ord,
                             bool inserted_real);
    void fillup_alloc_close_locks_(OrderLifecycleSlot& atom,
                                   XtOffset close_offset,
                                   int64_t leaves_fp,
                                   xt_account::CloseableView& rem);
    static void fillup_alloc_filled_(OrderLifecycleSlot& atom, int64_t filled_fp);

    [[nodiscard]] OrderSnapshot build_order_snapshot_(const OrderLifecycleSlot& slot) const noexcept;
    void publish_snapshot_() noexcept;

    [[nodiscard]] bool is_collect_thread_allowed_() const noexcept;

    void on_cross_thread_direct_read_blocked_() const noexcept;

    std::array<OrderLifecycleSlot, kMaxInflightOrders> slot_pool_;
    std::array<uint16_t, kMaxInflightOrders> free_stack_;
    uint16_t free_top_ = 0;
    std::unordered_map<uint64_t, uint16_t> id_to_slot_idx_;
    std::unordered_map<CloseOutstandingKey, CloseOutstandingAgg, CloseOutstandingKeyHash> close_outstanding_;
    uint64_t open_order_seq_ = 0;
    std::unordered_set<uint64_t> order_final_before_submit_;
    std::unordered_set<uint64_t> order_terminal_seen_;
    std::deque<uint64_t> order_terminal_fifo_;
    std::unordered_set<XtInstrumentId> tracked_codes_;

    std::unordered_map<uint64_t, std::size_t> publish_force_flat_keys_;

    XtInstrumentRegistry instrument_registry_;

    xt_util::sync::XtSnapshotPool<OrderBookSnapshot, 8> snapshot_pool_;
    xt_util::sync::HazardDomain hp_domain_;
    std::atomic<const OrderBookSnapshot*> current_{nullptr};
    int64_t next_generation_ = 1;

    mutable std::atomic<std::thread::id> collect_first_touch_tid_{std::thread::id{}};

    WorkerPostFn marshal_post_;
    std::thread::id marshal_worker_tid_{};
    std::atomic<bool> marshal_bound_{false};
};

}
}

namespace xt_runtime_book_drops {
std::atomic<uint64_t>& slot_pool_full_drops();

std::atomic<uint64_t>& orphan_update_dropped();

std::atomic<uint64_t>& orphan_reap_dropped();

std::atomic<uint64_t>& snapshot_publish_skipped();

std::atomic<uint64_t>& cross_thread_direct_read_blocked();

std::atomic<uint64_t>& tracked_codes_truncated();

#ifdef XT_BUILD_TESTS

inline void reset_for_test() {
    slot_pool_full_drops().store(0, std::memory_order_relaxed);
    orphan_update_dropped().store(0, std::memory_order_relaxed);
    orphan_reap_dropped().store(0, std::memory_order_relaxed);
    snapshot_publish_skipped().store(0, std::memory_order_relaxed);
    cross_thread_direct_read_blocked().store(0, std::memory_order_relaxed);
    tracked_codes_truncated().store(0, std::memory_order_relaxed);
}
#endif
}
