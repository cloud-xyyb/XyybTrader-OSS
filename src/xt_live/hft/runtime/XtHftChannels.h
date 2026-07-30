#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "xt_core/market/XtL2MarketStructs.h"
#include "api/AliveInflightOrder.h"
#include "api/IMdGateway.h"
#include "api/ITradeGateway.h"
#include "api/XtRunnerSignals.h"
#include "xt_live/common/TdHandoffRetryBuffer.h"
#include "xt_live/hft/runtime/XtHftEventLoop.h"

class IAccountTruthSink;

/// @brief MD/TD gateway 回调到事件循环的桥接层。
///
/// XtHftChannels 在启动期把自己绑定为行情和交易网关的回调：SDK 线程推来的
/// 行情、订单、成交、账户和通道状态，在这里转成队列事件写入 XtHftEventLoop。
/// 入队失败按事件性质分两档：must_not_drop 事件（成交/终态回报/持仓快照/
/// 通道边沿/reverse-sync 批次）持 payload 进有界重投缓冲（store-and-forward，
/// 缓冲溢出才落既有 drop 计数与 fail-closed 停机语义）；其余事件维持既有
/// 语义——计数后丢弃，不在 SDK 线程重试。
///
/// @note 构造参数均为 non-owning；生命周期必须覆盖 wire() 到 stop() 之间的回调窗口。
/// @warning SDK 回调路径必须保持非阻塞，不得引入同步 I/O 或等待 worker。
class XtHftChannels {
public:

    XtHftChannels(IMdGateway* md_channel,
                   ITradeGateway* td_gateway,
                   XtHftEventLoop* event_loop,
                   XtRunnerSignals* shared_state,
                   class XtLiveLatencyTracker* latency_tracker,
                   std::function<const char*(uint64_t, ITradeGateway::TdState)> route_status_resolver);

    void wire();

    bool start();

    void stop();

    void on_md_tick(const XtRawTick& tick, uint64_t capture_ns);

    void on_md_order_queue(const XtOrderQueue& oq);

    void on_md_order_detail(const XtOrderDetail& od);

    void on_md_transaction(const XtTransaction& tr);

    void on_md_front(bool connected, int reason);

    void on_md_ready();

    void on_td_order_update(const XtOrderExecUpdate& upd, XtLatencyEligibility elig);

    void set_account_truth_sink(IAccountTruthSink* sink) noexcept {
        truth_sink_.store(sink, std::memory_order_release);
    }

    void set_stp_rearm_sink(std::function<void(const std::vector<AliveInflightOrder>&, bool)> sink) {
        stp_rearm_sink_ = std::move(sink);
    }

    void on_td_broker_account_truth(const BrokerVenueTruth& bt);

    void on_td_account_snapshot(const ITradeGateway::AccountSnapshot& snap);

    void on_td_position_snapshot(const std::vector<ITradeGateway::PositionSnapshot>& positions,
                                 ITradeGateway::TdState td_state,
                                 bool is_cold_start = false);

    void on_td_trade(const XtTradeRecord& tr, ITradeGateway::TdState td_state,
                     XtLatencyEligibility elig);

    void on_td_reverse_sync_batch(const std::vector<AliveInflightOrder>& orders,
                                  bool is_cold_start);

    void on_td_daily_settlement(const ITradeGateway::DailySettlementDelta& ev);

    void on_td_cash_transfer(const ITradeGateway::CashTransferDelta& ev);

    void on_td_ready();

    void on_td_front(bool connected, int reason);

    using TdHandoffStats = xt_live::TdHandoffRetryBuffer<XtHftRunnerEvent>::Stats;

    size_t drain_td_handoff_buffer();

    [[nodiscard]] TdHandoffStats td_handoff_stats() const { return td_handoff_.stats(); }

    void set_td_handoff_capacity(size_t cap) { td_handoff_.set_capacity(cap); }

private:

    bool enqueue_td_event(XtHftRunnerEvent ev);

    size_t drain_td_handoff_();

    bool enqueue_flow_event(XtHftRunnerEvent ev);

    void publish_channel_event(std::atomic<bool>* ready_flag,
                               bool ready,
                               const char* channel,
                               int reason);

    XtHftEventLoop* event_loop_ = nullptr;
    XtRunnerSignals* shared_state_ = nullptr;
    IMdGateway* md_ = nullptr;
    ITradeGateway* td_ = nullptr;

    class XtLiveLatencyTracker* latency_tracker_ = nullptr;

    std::function<const char*(uint64_t, ITradeGateway::TdState)> route_status_resolver_;

    std::atomic<IAccountTruthSink*> truth_sink_{nullptr};

    std::function<void(const std::vector<AliveInflightOrder>&, bool)> stp_rearm_sink_;

    xt_live::TdHandoffRetryBuffer<XtHftRunnerEvent> td_handoff_;

    int64_t td_drop_last_warn_ms_ = 0;
    int64_t flow_drop_last_warn_ms_ = 0;
    uint64_t td_drop_burst_total_ = 0;
    uint64_t flow_drop_burst_total_ = 0;
    int64_t td_handoff_last_warn_ms_ = 0;
    uint64_t td_handoff_burst_total_ = 0;
};
