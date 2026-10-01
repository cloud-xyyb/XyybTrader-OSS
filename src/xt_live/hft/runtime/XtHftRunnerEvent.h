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

// ── Exec 域 ──────────────────────────────────────────────
/// @brief 执行域事件类型；覆盖 L2 行情、订单状态和成交回报。
enum class XtRunnerExecEventType : uint8_t {
    OrderQueue = 0,      ///< L2 委托队列快照。
    OrderDetail,         ///< L2 逐笔委托明细。
    Transaction,         ///< L2 逐笔成交明细。
    XtOrderExecUpdate,   ///< TD 订单状态回报。
    Trade,               ///< TD 成交回报。
};

/// @brief L2 委托队列事件。
struct XtRunnerOrderQueueEvent {
    XtOrderQueue value;  ///< L2 委托队列快照
};

/// @brief L2 逐笔委托事件。
struct XtRunnerOrderDetailEvent {
    XtOrderDetail value; ///< L2 逐笔委托明细
};

/// @brief L2 逐笔成交事件。
struct XtRunnerTransactionEvent {
    XtTransaction value; ///< L2 逐笔成交明细
};

/// @brief TD 订单状态回报事件。
struct XtRunnerOrderUpdateEvent {
    XtOrderExecUpdate value; ///< 订单状态更新（报入/部成/全成/撤单等）
    /// gateway 边界盖章的 latency ineligibility（true=合成/延迟重放，不喂 latency
    /// tracker；仅观测面 gate，业务处理不受影响）。默认 false（eligible）。
    bool latency_ineligible = false;
};

/// @brief TD 成交回报事件。
struct XtRunnerTradeEvent {
    XtTradeRecord value;  ///< 成交回报记录；strategy_id 由 venue gateway 在回报边界 stamp。
    ITradeGateway::TdState td_state = ITradeGateway::TdState::Unknown; ///< TD 会话状态；Recovering 期 engine 跳过业务 realized。
    std::string route_status; ///< recorder 路由状态 sidecar；空值表示由 strategy_id 推导。
    bool latency_ineligible = false;  ///< gateway 边界盖章的 latency ineligibility（延迟重放不喂 tracker）
};

/// @brief Exec 域 variant 载荷；分支必须与 `XtRunnerExecEvent::type` 对齐。
using XtRunnerExecPayload = std::variant<XtRunnerOrderQueueEvent,
                                          XtRunnerOrderDetailEvent,
                                          XtRunnerTransactionEvent,
                                          XtRunnerOrderUpdateEvent,
                                          XtRunnerTradeEvent>;

/// @brief Exec 域队列事件。
struct XtRunnerExecEvent {
    XtRunnerExecEventType type = XtRunnerExecEventType::OrderQueue; ///< 事件子类型标识；必须与 payload 分支一致。
    XtRunnerExecPayload payload; ///< 事件载荷（variant，按 type 解读）。

    /// @brief 按 variant 分支读取载荷；类型不匹配返回 nullptr。
    template <typename T>
    const T* get_if() const {
        return std::get_if<T>(&payload);
    }

    /// @brief 按 variant 分支读取可变载荷；类型不匹配返回 nullptr。
    template <typename T>
    T* get_if() {
        return std::get_if<T>(&payload);
    }
};

// ── Account 域 ──────────────────────────────────────────────
/// @brief 账户域事件类型；覆盖低频账户、持仓与结算快照。
enum class XtRunnerAccountEventType : uint8_t {
    DailySettlement = 0, ///< 每日结算增量。
    CashTransfer,        ///< 出入金增量。
    PositionSnapshot,    ///< 全量持仓快照。
    AccountSnapshot,     ///< 账户资金快照。
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
    // position snapshot 为低频（登录/定期刷新），允许 vector 堆分配。
    std::vector<ITradeGateway::PositionSnapshot> value; ///< 全量持仓快照
    ITradeGateway::TdState td_state = ITradeGateway::TdState::Unknown; ///< TD 会话状态（区分实时/恢复）
    /// @brief cold-start vs warm-reconnect 区分。
    /// true  = 进程冷启 / 首次 recovery → XtOrderRuntimeBook 全量 wipe slot_pool_。
    /// false = warm reconnect mid-session → 不 wipe，保留本进程已 submit 的 alive
    ///         in-flight slot 视角，仅去重合并 tracked_codes_。
    /// 默认 false；普通 position snapshot 事件按 warm 处理，cold-start 语义主要由
    /// reverse-sync batch 携带。
    bool is_cold_start = false;
    /// @brief TD session generation（gen fence），enqueue 时（SDK 线程）从
    /// XtRunnerSignals::td_session_gen 捕获。observer 仅对 gen == 当前 gen 置 readiness latch，
    /// 旧 gen 的 stale snapshot 跳过（防断连 ChannelLost 物理满丢导致旧 baseline 带入新 session）。
    uint32_t session_gen = 0;
};

/// @brief 账户资金快照事件。
struct XtRunnerAccountSnapshotEvent {
    ITradeGateway::AccountSnapshot value; ///< 账户资金快照
};

/// @brief Account 域 variant 载荷；分支必须与 `XtRunnerAccountEvent::type` 对齐。
using XtRunnerAccountPayload = std::variant<XtRunnerDailySettlementEvent,
                                            XtRunnerCashTransferEvent,
                                            XtRunnerPositionSnapshotEvent,
                                            XtRunnerAccountSnapshotEvent>;

/// @brief Account 域队列事件。
struct XtRunnerAccountEvent {
    XtRunnerAccountEventType type = XtRunnerAccountEventType::DailySettlement; ///< 事件子类型标识；必须与 payload 分支一致。
    XtRunnerAccountPayload payload; ///< 事件载荷（variant，按 type 解读）。

    /// @brief 按 variant 分支读取载荷；类型不匹配返回 nullptr。
    template <typename T>
    const T* get_if() const {
        return std::get_if<T>(&payload);
    }

    /// @brief 按 variant 分支读取可变载荷；类型不匹配返回 nullptr。
    template <typename T>
    T* get_if() {
        return std::get_if<T>(&payload);
    }
};

// ── Control 域 ──────────────────────────────────────────────
/// @brief 控制域事件类型；用于通道状态、参数版本和 reverse-sync。
enum class XtRunnerControlEventType : uint8_t {
    ChannelReady = 0, ///< MD/TD 通道 ready 边沿。
    ChannelLost,      ///< MD/TD 通道断开边沿。
    ParamsUpdated,    ///< 参数域版本更新。
    ReverseSyncBatch, ///< gateway 反向同步的 alive in-flight orders。
    ReapRecoveredOrphans, ///< 周期回收 recovered-unowned（cross-restart 孤儿）在途单。
    WorkerTask,           ///< worker 闭包任务；off-worker submit 的 book/account 注册经此与 rtn 同队列序列化。
};

/// @brief 通道状态边沿事件。
struct XtRunnerChannelEvent {
    std::string channel; ///< 通道标识（如 "md"、"td"）
    int reason = 0;      ///< 断开原因码（ChannelReady 时为 0）
};

/// @brief 参数版本更新事件。
struct XtRunnerParamsUpdatedEvent {
    uint64_t param_version = 0; ///< 参数版本号（单调递增）
};

/// @brief 孤儿回收 / lost-terminal 对账 sweep 参数；HftRunnerControl 按节拍产生，worker 线程执行。
/// @note reap_orphans 与 reconcile_lost_terminals 两个能力各自独立门控（HFT-D6 解耦）：
///       只开 reconcile 时事件仍推送，worker 只跑 reconcile 不跑 orphan reap。
struct XtRunnerReapOrphansEvent {
    int64_t now_ms = 0;        ///< 控制线程 steady ms（与 engine reverse-sync 戳同源）
    int64_t grace_ms = 0;      ///< reverse-sync 后静默窗口 ms
    uint32_t max_cancels = 0;  ///< 单次 sweep 撤单上限
    bool reconcile_lost_terminals = false;  ///< B4：本节拍跑 lost-terminal reconcile（book-open vs venue-terminal）
    bool reap_orphans = true;               ///< HFT-D6：本节拍跑 orphan reap（撤 recovered-unowned）；与 reconcile 各自独立
};

/// @brief Worker 闭包任务；构造方保证 fn 非空，worker 线程直接执行。
/// @note 与 td order rtn 同队列 FIFO：off-worker submit 的注册闭包必须早于
///       该单首条回报被消费（XtOrderRuntimeBook::bind_worker_marshal 契约）。
struct XtRunnerWorkerTaskEvent {
    std::function<void()> fn;  ///< worker 线程执行的闭包。
};

/// @brief Control 域 variant 载荷；分支必须与 `XtRunnerControlEvent::type` 对齐。
using XtRunnerControlPayload = std::variant<XtRunnerChannelEvent,
                                            XtRunnerParamsUpdatedEvent,
                                            ReverseSyncBatchEventPayload,
                                            XtRunnerReapOrphansEvent,
                                            XtRunnerWorkerTaskEvent>;

/// @brief Control 域队列事件。
struct XtRunnerControlEvent {
    XtRunnerControlEventType type = XtRunnerControlEventType::ChannelReady; ///< 事件子类型标识；必须与 payload 分支一致。
    XtRunnerControlPayload payload; ///< 事件载荷（variant，按 type 解读）。

    /// @brief 按 variant 分支读取载荷；类型不匹配返回 nullptr。
    template <typename T>
    const T* get_if() const {
        return std::get_if<T>(&payload);
    }

    /// @brief 按 variant 分支读取可变载荷；类型不匹配返回 nullptr。
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
    /// 三域载荷（Exec/Account/Control）。
    std::variant<XtRunnerExecEvent, XtRunnerAccountEvent, XtRunnerControlEvent> payload;
    uint64_t enqueue_ts_us = 0; ///< 入队时间戳 µs（steady clock，延迟统计用）

    /// @brief 按三域 variant 分支读取载荷；类型不匹配返回 nullptr。
    template <typename T>
    const T* get_if() const {
        return std::get_if<T>(&payload);
    }

    /// @brief 按三域 variant 分支读取可变载荷；类型不匹配返回 nullptr。
    template <typename T>
    T* get_if() {
        return std::get_if<T>(&payload);
    }

    /// @brief 返回 drop accounting 使用的关键事件标记。
    /// @note Exec/Control 域为 critical；Account 域为非 critical。
    bool is_critical() const {
        return !std::holds_alternative<XtRunnerAccountEvent>(payload);
    }

    /// @brief 高水位反压时绝不可丢的事件（critical bypass）。
    /// @note 成交 + 终态订单回报 + 通道 ready/lost 边沿 + 全量持仓快照 + reverse-sync 批次—
    ///       丢失即漏成交/僵尸挂单/连接误判/readiness 永久 wedge；L2 行情与非终态订单回报可丢
    ///       （book 由后续回报自愈）。物理队列满仍丢但大声告警。通道 ready 纳入：readiness latch
    ///       收成 worker 单写后依赖 ChannelReady 置 td 通道 up，软水位丢它=td_position_ready
    ///       永不 re-arm。ReverseSyncBatch 纳入：gateway recovery 后单发反向灌 alive in-flight
    ///       单（无 ack/无重发）随即 satisfy readiness，软水位丢它=book 缺 venue-alive 单而
    ///       readiness 已放行（缺单直到后续回报或下次 reconcile 才自愈）。WorkerTask 纳入：
    ///       off-worker submit 的 book/account 注册闭包，软水位丢它=跨线程注册漏账（book/account
    ///       缺已提交订单）；绕软水位后入队仅停机/物理满失败，两者经 post_to_worker 溢出
    ///       侧道接手（侧道 cap 打满这一理论极限仍 loud drop），故 WorkerTask 的「物理满
    ///       仍丢」口径收窄为「侧道打满才丢」。
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
            // 全量持仓快照 gateway 单发不重发；td_position_ready 诚实化后，软水位静默丢它
            // 会让 readiness 永久 false → fail-closed 拒掉整段 submit。绕过软水位；物理满
            // 仍丢但走 loud CRITICAL log（非静默）。其余 Account 事件仍可丢。
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

    // ── Channel factories ──────────────────────────────────────────────
    /// @brief 构造通道 ready 边沿事件。
    static XtHftRunnerEvent make_channel_ready(std::string channel) {
        return XtHftRunnerEvent{XtRunnerControlEvent{
            XtRunnerControlEventType::ChannelReady,
            XtRunnerChannelEvent{std::move(channel), 0}}};
    }

    /// @brief 构造通道 lost 边沿事件。
    static XtHftRunnerEvent make_channel_lost(std::string channel, int reason) {
        return XtHftRunnerEvent{XtRunnerControlEvent{
            XtRunnerControlEventType::ChannelLost,
            XtRunnerChannelEvent{std::move(channel), reason}}};
    }

    // ── Exec factories ─────────────────────────────────────────────────
    /// @brief 构造 L2 委托队列事件；通常进入 flow queue。
    static XtHftRunnerEvent make_order_queue(const XtOrderQueue& value) {
        return XtHftRunnerEvent{XtRunnerExecEvent{
            XtRunnerExecEventType::OrderQueue, XtRunnerOrderQueueEvent{value}}};
    }

    /// @brief 构造 L2 逐笔委托事件；通常进入 flow queue。
    static XtHftRunnerEvent make_order_detail(const XtOrderDetail& value) {
        return XtHftRunnerEvent{XtRunnerExecEvent{
            XtRunnerExecEventType::OrderDetail, XtRunnerOrderDetailEvent{value}}};
    }

    /// @brief 构造 L2 逐笔成交事件；通常进入 flow queue。
    static XtHftRunnerEvent make_transaction(const XtTransaction& value) {
        return XtHftRunnerEvent{XtRunnerExecEvent{
            XtRunnerExecEventType::Transaction, XtRunnerTransactionEvent{value}}};
    }

    /// @brief 构造 TD 订单状态更新事件；通常进入 TD queue。
    /// @param latency_ineligible gateway 边界盖章：true=合成/延迟重放不喂 latency tracker。
    static XtHftRunnerEvent make_order_update(const XtOrderExecUpdate& value,
                                              bool latency_ineligible = false) {
        return XtHftRunnerEvent{XtRunnerExecEvent{
            XtRunnerExecEventType::XtOrderExecUpdate,
            XtRunnerOrderUpdateEvent{value, latency_ineligible}}};
    }

    /// @brief 构造 TD 成交回报事件；通常进入 TD queue。
    /// @param td_state TD 会话状态；Recovering 期 engine 跳过业务 realized。
    /// @param route_status recorder 路由状态 sidecar；空值表示由 strategy_id 推导。
    /// @param latency_ineligible gateway 边界盖章：true=延迟重放不喂 latency tracker。
    static XtHftRunnerEvent make_trade(const XtTradeRecord& value,
                                     ITradeGateway::TdState td_state,
                                     std::string route_status = {},
                                     bool latency_ineligible = false) {
        return XtHftRunnerEvent{XtRunnerExecEvent{
            XtRunnerExecEventType::Trade,
            XtRunnerTradeEvent{value, td_state, std::move(route_status), latency_ineligible}}};
    }

    // ── Account factories ──────────────────────────────────────────────
    /// @brief 构造每日结算增量事件（直载 gateway delta）。
    static XtHftRunnerEvent make_daily_settlement(const ITradeGateway::DailySettlementDelta& delta) {
        return XtHftRunnerEvent{XtRunnerAccountEvent{
            XtRunnerAccountEventType::DailySettlement,
            XtRunnerDailySettlementEvent{delta}}};
    }

    /// @brief 构造出入金增量事件（直载 gateway delta）。
    static XtHftRunnerEvent make_cash_transfer(const ITradeGateway::CashTransferDelta& delta) {
        return XtHftRunnerEvent{XtRunnerAccountEvent{
            XtRunnerAccountEventType::CashTransfer,
            XtRunnerCashTransferEvent{delta}}};
    }

    /// @brief 构造全量持仓快照事件。
    /// @param td_state TD 会话状态；Recovering 表示 recovery 快照。
    /// @param is_cold_start true 表示 cold-start recovery，可触发 runtime book 全量重建。
    static XtHftRunnerEvent make_position_snapshot(const std::vector<ITradeGateway::PositionSnapshot>& value,
                                                 ITradeGateway::TdState td_state,
                                                 bool is_cold_start = false,
                                                 uint32_t session_gen = 0) {
        return XtHftRunnerEvent{XtRunnerAccountEvent{
            XtRunnerAccountEventType::PositionSnapshot,
            XtRunnerPositionSnapshotEvent{value, td_state, is_cold_start, session_gen}}};
    }

    /// @brief 构造账户资金快照事件。
    static XtHftRunnerEvent make_account_snapshot(const ITradeGateway::AccountSnapshot& value) {
        return XtHftRunnerEvent{XtRunnerAccountEvent{
            XtRunnerAccountEventType::AccountSnapshot,
            XtRunnerAccountSnapshotEvent{value}}};
    }

    // ── Control/recovery factories ─────────────────────────────────────
    /// @brief 构造参数版本更新事件。
    static XtHftRunnerEvent make_params_updated(uint64_t param_version) {
        return XtHftRunnerEvent{XtRunnerControlEvent{
            XtRunnerControlEventType::ParamsUpdated,
            XtRunnerParamsUpdatedEvent{param_version}}};
    }

    /// @brief 构造 reverse-sync 批次事件。
    /// @param orders gateway 反向同步得到的 alive in-flight orders。
    /// @param is_cold_start true 表示 cold-start recovery，可触发 runtime book 全量重建。
    static XtHftRunnerEvent make_reverse_sync_batch(std::vector<AliveInflightOrder> orders,
                                                  bool is_cold_start) {
        return XtHftRunnerEvent{XtRunnerControlEvent{
            XtRunnerControlEventType::ReverseSyncBatch,
            ReverseSyncBatchEventPayload{std::move(orders), is_cold_start}}};
    }

    /// @brief 构造孤儿回收 / lost-terminal 对账 sweep 事件（HftRunnerControl 节拍产生）。
    /// @param reconcile_lost_terminals 本节拍是否跑 lost-terminal reconcile（book-open vs venue-terminal）。
    /// @param reap_orphans            本节拍是否跑 orphan reap（撤 recovered-unowned）；两 flag 各自独立（HFT-D6）。
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

    /// @brief 构造 worker 闭包任务事件（off-worker submit 注册入队用）。
    static XtHftRunnerEvent make_worker_task(std::function<void()> fn) {
        return XtHftRunnerEvent{XtRunnerControlEvent{
            XtRunnerControlEventType::WorkerTask,
            XtRunnerWorkerTaskEvent{std::move(fn)}}};
    }
};
