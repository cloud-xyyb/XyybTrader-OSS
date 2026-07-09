#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "xt_core/order/XtOrderTypes.h"

namespace binance_u {

struct TransportAck;

/// @brief Binance TD 的本地订单账本
///
/// - BinanceOrderLedger 是 Binance 交易网关里的订单身份和状态表，
///   给 ACK、UDS、撤单、对账提供统一的对账记录
/// - 所有方法线程安全。
class BinanceOrderLedger {
public:
    /// @brief 单笔订单的合并记录：身份路由、submit 原始字段和 in-flight 状态。
    ///
    /// 三部分的写入/清理时机互相独立，见各字段注释。
    struct OrderRecord {
        std::string client_order_id;
        std::string symbol;
        uint64_t    trace_id = 0;
        std::string sys_id;
        XtOrderStatus status = XtOrderStatus::Submitted;
        bool     is_buy = true;
        int64_t  price_fp = 0;
        int64_t  qty_fp = 0;
        XtOffset close_offset = XtOffset::Open;
        uint64_t submit_ts_ms = 0;
        bool has_pending = false;
        bool has_inflight = false;
    };

    /// @brief in-flight 记录的只读快照行（ghost sweep / gap-fill 归因用）。
    struct InflightBrief {
        uint64_t    local_id = 0;
        std::string sys_id;
        std::string symbol;
        uint64_t    submit_ts_ms = 0;
        XtOffset    close_offset = XtOffset::Open;
    };

    /// @brief 终态判定结果；账本变更已在锁内完成，caller 按 action 在锁外 emit。
    struct TerminalDecision {
        enum class Action {
            kEmit,
            kSkip,
            kKeepInflight,
        };
        Action action = Action::kSkip;
        XtOrderExecUpdate update{};
        bool terminal_seen_before = false;
    };

    enum class ReconcileTerminal {
        kNotExistCanceled,
        kBlankSysIdReject,
        kVenueTerminal,
    };

    static constexpr std::size_t kTerminalOrderCacheCap = 8192;

    void register_submit(uint64_t local_id, OrderRecord rec);

    void upsert_recovered(uint64_t local_id, OrderRecord venue_rec);

    void bind_sys_id_if_blank(uint64_t local_id, const std::string& sys_id);

    TerminalDecision on_ack(const TransportAck& ack, uint64_t local_cb_us);

    TerminalDecision on_uds_terminal(const XtOrderExecUpdate& upd);

    TerminalDecision on_reconcile_terminal(uint64_t local_id, ReconcileTerminal kind,
                                           XtOrderExecUpdate& upd);

    bool terminal_seen(uint64_t local_id) const;

    void remember_terminal(uint64_t local_id);

    bool is_inflight(uint64_t local_id) const;

    std::string sys_id_for(uint64_t local_id) const;

    std::string client_order_id_for(uint64_t local_id) const;

    std::optional<uint64_t> local_for_client(std::string_view client_order_id) const;

    uint64_t trace_id_for(uint64_t local_id) const;

    std::string symbol_for(uint64_t local_id) const;

    std::vector<InflightBrief> inflight_snapshot() const;

    std::optional<InflightBrief> find_inflight_by_sys_id(std::string_view sys_id) const;

private:

    static void apply_record_fields_(const OrderRecord& rec, XtOrderExecUpdate& upd);

    void remember_terminal_unlocked_(uint64_t local_id);

    static void erase_inflight_unlocked_(OrderRecord& rec);

    void erase_client_route_unlocked_(OrderRecord& rec);

    mutable std::mutex ledger_mu_;
    std::unordered_map<uint64_t, OrderRecord> records_;
    std::unordered_map<std::string, uint64_t> client_to_local_;
    std::unordered_set<uint64_t> terminal_orders_;
    std::deque<uint64_t> terminal_order_fifo_;
};

}
