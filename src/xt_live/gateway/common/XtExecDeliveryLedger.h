#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "xt_core/order/XtOrderTypes.h"

namespace xt_live::gateway {

enum class FillSource : uint8_t {
    kLivePush = 0,
    kQueryReplay = 1,
    kGapFill = 2,
};

enum class TerminalProvenance : uint8_t {
    kVenuePush = 0,
    kVenueQuery,
    kSynthCancelNoTarget,
    kSynthCumFilled,
    kSynthReconcile,
    kSynthSuppressedResend,
};

enum class CancelOutcome : uint8_t {
    kSentToVenue = 0,
    kLocalGateFalse,
    kTransportFail,
    kVenueNoTarget,
    kVenueAcked,
};

/// @brief per-order 交付投影（delivery projection），venue-neutral。
///
/// 记录三类交付事实：fill 记账（exactly-once 判据：`fill_key ∈ fills` ⇔ 已记账）、
/// 终态 payload + provenance、撤单尝试结局证据。venue 键以 opaque string 存储，
/// 路由语义留在各 venue 适配层。
///
/// 刻意不做：不存钱、不算 PnL、不持久化（in-memory，崩溃后由 venue 重建）；
/// 不复制 book/account 的账本职责——本类是投影不是第二账本。
///
/// retention：未终态记录不淘汰（上界 = venue 侧最大挂单量）；终态记录与
/// orderless fill key 归 FIFO 淘汰域（cap 见 ctor），retention 由 cap 兜底。
/// `clear_session()` 为同进程交易日轮转的可选整体清理入口，不是 retention 前提。
/// 前提：local_id 进程内单调不复用；同 id 重复注册按整体替换处理。
///
/// 线程契约：所有公开方法线程安全（内部单 mutex，不在锁内回调）；计数为
/// 进程内 atomic；查询一律按值出参/返回副本，不暴露内部引用。
class XtExecDeliveryLedger {
public:
    /// @brief 单笔 fill 的记账条目；per-order 有界（单笔订单 fill 数天然小）。
    struct ExecFillEntry {
        std::string fill_key;
        int64_t qty_fp = 0;
        FillSource source = FillSource::kLivePush;
        bool absorbed = false;
        uint32_t query_gen = 0;
    };

    /// @brief 终态 payload；按值出参供合成重发使用。
    struct TerminalRecord {
        XtOrderStatus status = XtOrderStatus::Canceled;
        int64_t filled_fp = 0;
        TerminalProvenance prov = TerminalProvenance::kVenuePush;
        uint64_t seen_us = 0;
    };

    /// @brief 单次撤单尝试的结局证据。
    struct CancelAttempt {
        uint64_t ts_ms = 0;
        CancelOutcome outcome = CancelOutcome::kSentToVenue;
    };

    static constexpr std::size_t kCancelAttemptRing = 8;
    static constexpr std::size_t kTerminalRecordCap = 200000;

    /// @brief per-order 投影记录。fills 是 exactly-once 判据集合；
    ///        cum_filled_fp = Σ fills.qty_fp（含 absorbed 行）。
    struct ExecOrderRecord {

        uint64_t local_id = 0;
        uint32_t sid_id = 0;
        std::string code;
        std::string venue_key;
        int64_t qty_fp = 0;
        bool is_buy = true;
        XtOffset close_offset = XtOffset::Open;

        std::vector<ExecFillEntry> fills;
        int64_t cum_filled_fp = 0;
        int64_t absorbed_watermark_fp = 0;

        bool has_terminal = false;
        TerminalRecord terminal{};

        std::array<CancelAttempt, kCancelAttemptRing> cancel_attempts{};
        uint32_t cancel_attempt_total = 0;
        uint32_t session_gen = 0;
    };

    /// @brief 注册参数包（submit 预注册 / reverse-sync rebuild 共用）。
    struct OrderIdentity {
        uint32_t sid_id = 0;
        std::string code;
        std::string venue_key;
        int64_t qty_fp = 0;
        bool is_buy = true;
        XtOffset close_offset = XtOffset::Open;
        int64_t absorbed_watermark_fp = 0;
        uint32_t session_gen = 0;
    };

    /// @brief fill 记账参数包。
    struct FillArgs {
        std::string fill_key;
        int64_t qty_fp = 0;
        FillSource source = FillSource::kLivePush;
        bool absorbed = false;
        uint32_t query_gen = 0;
    };

    /// @brief 观测快照（进程内计数；不接 metrics 面）。
    struct Stats {

        std::size_t open_records = 0;
        std::size_t terminal_records = 0;
        std::size_t accounted_fill_keys = 0;
        std::size_t orderless_fill_keys = 0;

        uint64_t duplicate_fill_suppressed = 0;
        uint64_t duplicate_terminal_ignored = 0;
        uint64_t fill_record_miss = 0;
        uint64_t terminal_record_miss = 0;
        uint64_t cancel_attempt_miss = 0;
        uint64_t venue_key_miss = 0;
        uint64_t venue_key_collision = 0;
        uint64_t evicted_terminal_records = 0;
        uint64_t evicted_orderless_keys = 0;
        uint64_t session_clear_total = 0;
        uint64_t session_cleared_entries = 0;
        uint64_t invalid_input_dropped = 0;
    };

    explicit XtExecDeliveryLedger(std::size_t terminal_cap = kTerminalRecordCap);

    void register_order(uint64_t local_id, OrderIdentity identity);

    void rollback_register(uint64_t local_id);

    bool record_fill(uint64_t local_id, FillArgs fill);

    bool fill_accounted(const std::string& fill_key) const;

    int64_t cum_filled_fp(uint64_t local_id) const;

    bool record_terminal(uint64_t local_id, const TerminalRecord& terminal);

    bool terminal_record(uint64_t local_id, TerminalRecord& out) const;

    void record_cancel_attempt(uint64_t local_id, uint64_t ts_ms, CancelOutcome outcome);

    void set_venue_key(uint64_t local_id, const std::string& venue_key);

    std::optional<uint64_t> local_for_venue_key(const std::string& venue_key) const;

    std::vector<uint64_t> open_ids() const;

    bool get_record(uint64_t local_id, ExecOrderRecord& out) const;

    void clear_session();

    Stats stats() const;

private:
    using RecordMap = std::unordered_map<uint64_t, ExecOrderRecord>;

    void bind_venue_key_unlocked_(uint64_t local_id, const std::string& venue_key);

    void erase_record_unlocked_(RecordMap::iterator it);

    void enforce_terminal_cap_unlocked_();

    void enforce_orderless_cap_unlocked_();

    const std::size_t terminal_cap_;

    mutable std::mutex mu_;
    RecordMap records_;
    std::unordered_map<std::string, uint64_t> venue_key_to_local_;
    std::unordered_set<std::string> fill_key_index_;
    std::deque<uint64_t> terminal_fifo_;
    std::deque<std::string> orderless_fifo_;
    std::size_t terminal_records_ = 0;

    std::atomic<uint64_t> duplicate_fill_suppressed_{0};
    std::atomic<uint64_t> duplicate_terminal_ignored_{0};
    std::atomic<uint64_t> fill_record_miss_{0};
    std::atomic<uint64_t> terminal_record_miss_{0};
    std::atomic<uint64_t> cancel_attempt_miss_{0};
    std::atomic<uint64_t> venue_key_miss_{0};
    std::atomic<uint64_t> venue_key_collision_{0};
    std::atomic<uint64_t> evicted_terminal_records_{0};
    std::atomic<uint64_t> evicted_orderless_keys_{0};
    std::atomic<uint64_t> session_clear_total_{0};
    std::atomic<uint64_t> session_cleared_entries_{0};
    std::atomic<uint64_t> invalid_input_dropped_{0};
};

std::atomic<uint64_t>& exec_delivery_terminal_evicted_total();

}
