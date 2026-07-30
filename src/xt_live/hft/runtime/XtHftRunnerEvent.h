#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "xt_core/market/XtL2MarketStructs.h"
#include "api/AliveInflightOrder.h"
#include "api/ITradeGateway.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"

/// @file XtHftRunnerEvent.h
/// @brief HFT live runner 的跨线程队列事件模型。
///
/// `XtHftChannels` 在 MD/TD SDK 回调线程构造这些事件，`XtHftEventLoop`
/// 在 worker 线程消费并分发到 `XtHftEngine`。事件按 Exec / Account /
/// Control 三域收口；`type` 与 `payload` variant 分支必须保持一致，建议
/// 调用 `XtHftRunnerEvent::make_*` factory 构造。

enum class XtRunnerExecEventType : uint8_t {
    OrderQueue = 0,
    OrderDetail,
    Transaction,
    XtOrderExecUpdate,
    Trade,
};

/// @brief L2 委托队列事件。
struct XtRunnerOrderQueueEvent {
    XtOrderQueue value;
};

/// @brief L2 逐笔委托事件。
struct XtRunnerOrderDetailEvent {
    XtOrderDetail value;
};

/// @brief L2 逐笔成交事件。
struct XtRunnerTransactionEvent {
    XtTransaction value;
};

/// @brief TD 订单状态回报事件。
struct XtRunnerOrderUpdateEvent {
    XtOrderExecUpdate value;

    bool latency_ineligible = false;
};

/// @brief TD 成交回报事件。
struct XtRunnerTradeEvent {
    XtTradeRecord value;
    ITradeGateway::TdState td_state = ITradeGateway::TdState::Unknown;
    std::string route_status;
    bool latency_ineligible = false;
};

using XtRunnerExecPayload = std::variant<XtRunnerOrderQueueEvent,
                                          XtRunnerOrderDetailEvent,
                                          XtRunnerTransactionEvent,
                                          XtRunnerOrderUpdateEvent,
                                          XtRunnerTradeEvent>;

/// @brief Exec 域队列事件。
struct XtRunnerExecEvent {
    XtRunnerExecEventType type = XtRunnerExecEventType::OrderQueue;
    XtRunnerExecPayload payload;

    template <typename T>
    const T* get_if() const {
        return std::get_if<T>(&payload);
    }

    template <typename T>
    T* get_if() {
        return std::get_if<T>(&payload);
    }
};

enum class XtRunnerAccountEventType : uint8_t {
    DailySettlement = 0,
    CashTransfer,
    PositionSnapshot,
    AccountSnapshot,
};

/// @brief 每日结算增量事件；直载 gateway delta（单次映射，字段不拆包重拼）。
struct XtRunnerDailySettlementEvent {
    ITradeGateway::DailySettlementDelta delta;
};

/// @brief 出入金增量事件；直载 gateway delta。
struct XtRunnerCashTransferEvent {
    ITradeGateway::CashTransferDelta delta;
};

/// @brief 全量持仓快照事件。
struct XtRunnerPositionSnapshotEvent {

    std::vector<ITradeGateway::PositionSnapshot> value;
    ITradeGateway::TdState td_state = ITradeGateway::TdState::Unknown;

    bool is_cold_start = false;

    uint32_t session_gen = 0;
};

/// @brief 账户资金快照事件。
struct XtRunnerAccountSnapshotEvent {
    ITradeGateway::AccountSnapshot value;
};

using XtRunnerAccountPayload = std::variant<XtRunnerDailySettlementEvent,
                                            XtRunnerCashTransferEvent,
                                            XtRunnerPositionSnapshotEvent,
                                            XtRunnerAccountSnapshotEvent>;

/// @brief Account 域队列事件。
struct XtRunnerAccountEvent {
    XtRunnerAccountEventType type = XtRunnerAccountEventType::DailySettlement;
    XtRunnerAccountPayload payload;

    template <typename T>
    const T* get_if() const {
        return std::get_if<T>(&payload);
    }

    template <typename T>
    T* get_if() {
        return std::get_if<T>(&payload);
    }
};

enum class XtRunnerControlEventType : uint8_t {
    ChannelReady = 0,
    ChannelLost,
    ParamsUpdated,
    ReverseSyncBatch,
    ReapRecoveredOrphans,
    WorkerTask,
};

/// @brief 通道状态边沿事件。
struct XtRunnerChannelEvent {
    std::string channel;
    int reason = 0;
};

/// @brief 参数版本更新事件。
struct XtRunnerParamsUpdatedEvent {
    uint64_t param_version = 0;
};

/// @brief 孤儿回收 / lost-terminal 对账 sweep 参数；HftRunnerControl 按节拍产生，worker 线程执行。
/// @note reap_orphans 与 reconcile_lost_terminals 两个能力各自独立门控（HFT-D6 解耦）：
///       只开 reconcile 时事件仍推送，worker 只跑 reconcile 不跑 orphan reap。
struct XtRunnerReapOrphansEvent {
    int64_t now_ms = 0;
    int64_t grace_ms = 0;
    uint32_t max_cancels = 0;
    bool reconcile_lost_terminals = false;
    bool reap_orphans = true;
};

/// @brief Worker 闭包任务；构造方保证 fn 非空，worker 线程直接执行。
/// @note 与 td order rtn 同队列 FIFO：off-worker submit 的注册闭包必须早于
///       该单首条回报被消费（XtOrderRuntimeBook::bind_worker_marshal 契约）。
struct XtRunnerWorkerTaskEvent {
    std::function<void()> fn;
};

using XtRunnerControlPayload = std::variant<XtRunnerChannelEvent,
                                            XtRunnerParamsUpdatedEvent,
                                            ReverseSyncBatchEventPayload,
                                            XtRunnerReapOrphansEvent,
                                            XtRunnerWorkerTaskEvent>;

/// @brief Control 域队列事件。
struct XtRunnerControlEvent {
    XtRunnerControlEventType type = XtRunnerControlEventType::ChannelReady;
    XtRunnerControlPayload payload;

    template <typename T>
    const T* get_if() const {
        return std::get_if<T>(&payload);
    }

    template <typename T>
    T* get_if() {
        return std::get_if<T>(&payload);
    }
};

/**
 * @brief HFT live runner 的跨线程队列事件。
 *
 * 由 MD/TD SDK 回调线程或 control loop 构造，经 XtHftEventLoop worker 消费。
 * payload 按 Exec / Account / Control 三域收口，`type` 与 `payload` variant 分支
 * 必须保持一致；调用方应通过 `make_*` factory 构造。
 *
 * producer 可在入队前填 `enqueue_ts_us`，供 EventLoop 统计 TD/flow 队列延迟。
 */
struct XtHftRunnerEvent {

    std::variant<XtRunnerExecEvent, XtRunnerAccountEvent, XtRunnerControlEvent> payload;
    uint64_t enqueue_ts_us = 0;

    template <typename T>
    const T* get_if() const {
        return std::get_if<T>(&payload);
    }

    template <typename T>
    T* get_if() {
        return std::get_if<T>(&payload);
    }

    bool is_critical() const {
        return !std::holds_alternative<XtRunnerAccountEvent>(payload);
    }

    bool must_not_drop() const {
        if (const auto* exec = std::get_if<XtRunnerExecEvent>(&payload)) {
            if (exec->type == XtRunnerExecEventType::Trade) return true;
            if (exec->type == XtRunnerExecEventType::XtOrderExecUpdate) {
                const auto* ou = std::get_if<XtRunnerOrderUpdateEvent>(&exec->payload);
                return ou && order_status_is_final(ou->value.status);
            }
            return false;
        }
        if (const auto* acc = std::get_if<XtRunnerAccountEvent>(&payload)) {

            return acc->type == XtRunnerAccountEventType::PositionSnapshot;
        }
        if (const auto* ctrl = std::get_if<XtRunnerControlEvent>(&payload)) {
            return ctrl->type == XtRunnerControlEventType::ChannelLost
                || ctrl->type == XtRunnerControlEventType::ChannelReady
                || ctrl->type == XtRunnerControlEventType::ReverseSyncBatch
                || ctrl->type == XtRunnerControlEventType::WorkerTask;
        }
        return false;
    }

    static XtHftRunnerEvent make_channel_ready(std::string channel) {
        return XtHftRunnerEvent{XtRunnerControlEvent{
            XtRunnerControlEventType::ChannelReady,
            XtRunnerChannelEvent{std::move(channel), 0}}};
    }

    static XtHftRunnerEvent make_channel_lost(std::string channel, int reason) {
        return XtHftRunnerEvent{XtRunnerControlEvent{
            XtRunnerControlEventType::ChannelLost,
            XtRunnerChannelEvent{std::move(channel), reason}}};
    }

    static XtHftRunnerEvent make_order_queue(const XtOrderQueue& value) {
        return XtHftRunnerEvent{XtRunnerExecEvent{
            XtRunnerExecEventType::OrderQueue, XtRunnerOrderQueueEvent{value}}};
    }

    static XtHftRunnerEvent make_order_detail(const XtOrderDetail& value) {
        return XtHftRunnerEvent{XtRunnerExecEvent{
            XtRunnerExecEventType::OrderDetail, XtRunnerOrderDetailEvent{value}}};
    }

    static XtHftRunnerEvent make_transaction(const XtTransaction& value) {
        return XtHftRunnerEvent{XtRunnerExecEvent{
            XtRunnerExecEventType::Transaction, XtRunnerTransactionEvent{value}}};
    }

    static XtHftRunnerEvent make_order_update(const XtOrderExecUpdate& value,
                                              bool latency_ineligible = false) {
        return XtHftRunnerEvent{XtRunnerExecEvent{
            XtRunnerExecEventType::XtOrderExecUpdate,
            XtRunnerOrderUpdateEvent{value, latency_ineligible}}};
    }

    static XtHftRunnerEvent make_trade(const XtTradeRecord& value,
                                     ITradeGateway::TdState td_state,
                                     std::string route_status = {},
                                     bool latency_ineligible = false) {
        return XtHftRunnerEvent{XtRunnerExecEvent{
            XtRunnerExecEventType::Trade,
            XtRunnerTradeEvent{value, td_state, std::move(route_status), latency_ineligible}}};
    }

    static XtHftRunnerEvent make_daily_settlement(const ITradeGateway::DailySettlementDelta& delta) {
        return XtHftRunnerEvent{XtRunnerAccountEvent{
            XtRunnerAccountEventType::DailySettlement,
            XtRunnerDailySettlementEvent{delta}}};
    }

    static XtHftRunnerEvent make_cash_transfer(const ITradeGateway::CashTransferDelta& delta) {
        return XtHftRunnerEvent{XtRunnerAccountEvent{
            XtRunnerAccountEventType::CashTransfer,
            XtRunnerCashTransferEvent{delta}}};
    }

    static XtHftRunnerEvent make_position_snapshot(const std::vector<ITradeGateway::PositionSnapshot>& value,
                                                 ITradeGateway::TdState td_state,
                                                 bool is_cold_start = false,
                                                 uint32_t session_gen = 0) {
        return XtHftRunnerEvent{XtRunnerAccountEvent{
            XtRunnerAccountEventType::PositionSnapshot,
            XtRunnerPositionSnapshotEvent{value, td_state, is_cold_start, session_gen}}};
    }

    static XtHftRunnerEvent make_account_snapshot(const ITradeGateway::AccountSnapshot& value) {
        return XtHftRunnerEvent{XtRunnerAccountEvent{
            XtRunnerAccountEventType::AccountSnapshot,
            XtRunnerAccountSnapshotEvent{value}}};
    }

    static XtHftRunnerEvent make_params_updated(uint64_t param_version) {
        return XtHftRunnerEvent{XtRunnerControlEvent{
            XtRunnerControlEventType::ParamsUpdated,
            XtRunnerParamsUpdatedEvent{param_version}}};
    }

    static XtHftRunnerEvent make_reverse_sync_batch(std::vector<AliveInflightOrder> orders,
                                                  bool is_cold_start) {
        return XtHftRunnerEvent{XtRunnerControlEvent{
            XtRunnerControlEventType::ReverseSyncBatch,
            ReverseSyncBatchEventPayload{std::move(orders), is_cold_start}}};
    }

    static XtHftRunnerEvent make_reap_recovered_orphans(int64_t now_ms,
                                                        int64_t grace_ms,
                                                        uint32_t max_cancels,
                                                        bool reconcile_lost_terminals = false,
                                                        bool reap_orphans = true) {
        return XtHftRunnerEvent{XtRunnerControlEvent{
            XtRunnerControlEventType::ReapRecoveredOrphans,
            XtRunnerReapOrphansEvent{now_ms, grace_ms, max_cancels,
                                     reconcile_lost_terminals, reap_orphans}}};
    }

    static XtHftRunnerEvent make_worker_task(std::function<void()> fn) {
        return XtHftRunnerEvent{XtRunnerControlEvent{
            XtRunnerControlEventType::WorkerTask,
            XtRunnerWorkerTaskEvent{std::move(fn)}}};
    }
};
