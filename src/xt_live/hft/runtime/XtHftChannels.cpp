#include "xt_live/hft/runtime/XtHftChannels.h"

#include <cassert>

#include "XtLogger.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_live/telemetry/latency/XtLiveLatencyTracker.h"
#include "xt_util/time/XtSteadyClock.h"
#include "xt_account/api/IAccountTruthSink.h"  // update_broker_overlay / note_fill_fee_fp
#include "XtFixedPoint.h"                     // to_fp (note_fill_fee_fp)

XtHftChannels::XtHftChannels(IMdGateway* md_channel,
                               ITradeGateway* td_gateway,
                               XtHftEventLoop* event_loop,
                               XtRunnerSignals* shared_state,
                               class XtLiveLatencyTracker* latency_tracker,
                               std::function<const char*(uint64_t, ITradeGateway::TdState)> route_status_resolver)
    : md_(md_channel),
      td_(td_gateway),
      event_loop_(event_loop),
      shared_state_(shared_state),
      latency_tracker_(latency_tracker),
      route_status_resolver_(std::move(route_status_resolver)) {}

void XtHftChannels::wire() {
    IMdGateway::Callbacks md_cb;
    md_cb.on_tick         = [this](const XtRawTick& tick, uint64_t capture_ns) { on_md_tick(tick, capture_ns); };
    md_cb.on_order_queue  = [this](const XtOrderQueue& oq)    { on_md_order_queue(oq); };
    md_cb.on_order_detail = [this](const XtOrderDetail& od)   { on_md_order_detail(od); };
    md_cb.on_transaction  = [this](const XtTransaction& tr)   { on_md_transaction(tr); };
    md_cb.on_front        = [this](bool c, int r)                   { on_md_front(c, r); };
    md_cb.on_ready        = [this]()                                { on_md_ready(); };
    md_->bind_callbacks(std::move(md_cb));

    ITradeGateway::Callbacks td_cb;
    td_cb.on_order_update     = [this](const XtOrderExecUpdate& upd, XtLatencyEligibility elig) { on_td_order_update(upd, elig); };
    td_cb.on_account          = [this](const ITradeGateway::AccountSnapshot& snap)             { on_td_account_snapshot(snap); };
    td_cb.on_position         = [this](const std::vector<ITradeGateway::PositionSnapshot>& ps) {
        on_td_position_snapshot(ps, td_->state(), /*is_cold_start=*/false);
    };
    td_cb.on_recovery_position = [this](
        const std::vector<ITradeGateway::PositionSnapshot>& ps, bool is_cold_start) {
        on_td_position_snapshot(ps, ITradeGateway::TdState::Recovering, is_cold_start);
    };
    td_cb.on_broker_account_truth = [this](const BrokerVenueTruth& bt)                          { on_td_broker_account_truth(bt); };
    td_cb.on_trade            = [this](const XtTradeRecord& tr, XtLatencyEligibility elig)     { on_td_trade(tr, td_->state(), elig); };
    td_cb.on_daily_settlement = [this](const ITradeGateway::DailySettlementDelta& ev)          { on_td_daily_settlement(ev); };
    td_cb.on_cash_transfer    = [this](const ITradeGateway::CashTransferDelta& ev)             { on_td_cash_transfer(ev); };
    td_cb.on_ready            = [this]()                                                       { on_td_ready(); };
    td_cb.on_front            = [this](bool c, int r)                                          { on_td_front(c, r); };
    // Raw ACK/UDS 延迟采样：不走 SPSC，直连 latency tracker（自带 mutex，可被 SDK 回调线程
    // 并发调用）。raw_uds 与 raw_ack 联合决定 submit_to_ack 的 effective first arrival；
    // ack_unavailable 标记 raw ACK 不会到达，防 timeout/reconcile fallback 被当真实样本。
    td_cb.on_transport_ack = [this](uint64_t local_id, uint64_t ack_us) {
        if (latency_tracker_) latency_tracker_->record_raw_trade_ack(local_id, ack_us);
    };
    td_cb.on_uds_arrival = [this](uint64_t local_id, uint64_t uds_ts_us) {
        if (latency_tracker_) latency_tracker_->record_raw_uds_arrival(local_id, uds_ts_us);
    };
    td_cb.on_transport_ack_unavailable = [this](uint64_t local_id, uint64_t mark_us) {
        if (latency_tracker_) latency_tracker_->record_raw_trade_ack_unavailable(local_id, mark_us);
    };
    td_cb.on_reverse_sync_batch = [this](const std::vector<AliveInflightOrder>& orders, bool is_cold_start) {
        on_td_reverse_sync_batch(orders, is_cold_start);
    };
    td_->bind_callbacks(std::move(td_cb));
}

void XtHftChannels::on_td_broker_account_truth(const BrokerVenueTruth& bt) {
    // broker venue-truth overlay：直接转发到 truth sink，不入 td 队列
    // （仅 report/control 1Hz 消费，与策略热路径正交）。truth_sink_ 由 runner 晚绑。
    if (auto* sink = truth_sink_.load(std::memory_order_acquire)) {
        sink->update_broker_overlay(bt);
    }
}

bool XtHftChannels::start() {
    if (!md_ || !td_ || !event_loop_ || !shared_state_) {
        XtLogger::error_m("RT", "live channels start failed: invalid dependencies");
        return false;
    }

    if (!md_->start()) {
        XtLogger::error_m("RT", "md start failed");
        return false;
    }

    // TD 启动失败时须反向清理已启动的 MD，避免资源泄漏
    if (!td_->start()) {
        XtLogger::error_m("RT", "td start failed");
        td_->stop();
        md_->stop();
        return false;
    }
    XtLogger::info_m("RT", "td started");
    return true;
}

void XtHftChannels::stop() {
    shared_state_->md_ready.store(false, std::memory_order_release);
    shared_state_->td_ready.store(false, std::memory_order_release);
    shared_state_->td_position_ready.store(false, std::memory_order_release);
    shared_state_->position_snapshot_applied.store(false, std::memory_order_release);
    shared_state_->refresh_trading_ready();
    md_->stop();
    td_->stop();
}

// drop site 只做计数和 60s 节流告警：首次 drop 立即告警，随后按窗口输出 burst summary。
namespace {
constexpr int64_t kDropWarnThrottleMs = 60'000;
int64_t steady_now_ms() {
    return static_cast<int64_t>(XtSteadyClock::steady_now_ms());
}

// TD drop reason 必须在 push 前分类；push_td_event 按值接收，失败后 caller 侧
// `ev` 已是 moved-from 状态，不能再读取 payload variant。
enum class TdDropReason : uint8_t {
    Position = 0,  ///< 全量持仓快照（must_not_drop，仅物理满会到这）
    Control,       ///< 仅 ChannelReady/ChannelLost 两个通道边沿事件（must_not_drop 同上）
    Other,         ///< 其余 exec/account 事件；critical 维度由 critical_td_event_drop_cnt 聚合
};

TdDropReason classify_td_drop(const XtHftRunnerEvent& ev) {
    if (auto* acc = ev.get_if<XtRunnerAccountEvent>()) {
        if (std::holds_alternative<XtRunnerPositionSnapshotEvent>(acc->payload)) return TdDropReason::Position;
        return TdDropReason::Other;
    }
    if (auto* ctrl = ev.get_if<XtRunnerControlEvent>()) {
        // ChannelReady/Lost 与 ReverseSyncBatch 是 must_not_drop 的通道边沿 / 反向灌批次事件，
        // 计入 control 桶（喂 xt_td_control_event_drop_total + critical alert，语义 = 丢了不可丢
        // 事件，仅物理满才会到这）。其余 control 事件（ReapRecoveredOrphans/WorkerTask 等可被软
        // 水位丢）归 other，避免软丢误触发 readiness-wedge 告警。
        if (ctrl->type == XtRunnerControlEventType::ChannelReady ||
            ctrl->type == XtRunnerControlEventType::ChannelLost ||
            ctrl->type == XtRunnerControlEventType::ReverseSyncBatch) {
            return TdDropReason::Control;
        }
        return TdDropReason::Other;
    }
    return TdDropReason::Other;
}

void increment_td_drop_counter(TdDropReason reason, XtRunnerSignals& state) {
    switch (reason) {
        case TdDropReason::Position: state.td_drop_position_cnt.fetch_add(1, std::memory_order_relaxed); break;
        case TdDropReason::Control:  state.td_drop_control_cnt.fetch_add(1, std::memory_order_relaxed);  break;
        case TdDropReason::Other:
        default:                     state.td_drop_other_cnt.fetch_add(1, std::memory_order_relaxed);    break;
    }
}
}  // namespace

bool XtHftChannels::enqueue_td_event(XtHftRunnerEvent ev) {
    assert(event_loop_);
    const bool critical = ev.is_critical();
    // 先保留 payload 分类；push_td_event 失败后 `ev` 已不可再读取。
    const TdDropReason drop_reason = classify_td_drop(ev);
    // 永久丢弃计数 + 节流告警：非关键事件入队失败，或 must_not_drop 缓冲溢出。
    auto record_drop = [&]() {
        shared_state_->td_event_drop_cnt.fetch_add(1, std::memory_order_relaxed);
        if (critical) {
            shared_state_->critical_td_event_drop_cnt.fetch_add(1, std::memory_order_relaxed);
        }
        // 按 payload type 分桶，便于定位丢弃集中在哪类 TD event。
        increment_td_drop_counter(drop_reason, *shared_state_);
        // 首次告警后按 60s 窗口汇总，避免队列拥塞时刷日志。
        ++td_drop_burst_total_;
        const int64_t now_ms = steady_now_ms();
        if (td_drop_last_warn_ms_ == 0 || now_ms - td_drop_last_warn_ms_ >= kDropWarnThrottleMs) {
            const char* mark = (td_drop_last_warn_ms_ == 0) ? "FIRST" : "BURST";
            XtLogger::warn_m("RT", "td_event drop {} burst_total={} critical={} (60s throttle)",
                              mark, td_drop_burst_total_, critical ? 1 : 0);
            td_drop_last_warn_ms_ = now_ms;
            td_drop_burst_total_ = 0;
        }
    };

    if (!ev.must_not_drop()) {
        // 非关键事件维持既有语义：直发、失败即丢（允许超车缓冲中的关键事件，
        // gateway 侧 deduper/rank 吸收倒序）。S&F 期间顺带尝试排空缓冲——
        // hint 为 lock-free relaxed 读，模式关闭时本分支零额外成本。
        if (td_handoff_.in_store_and_forward()) {
            drain_td_handoff_();
        }
        if (event_loop_->push_td_event(std::move(ev))) {
            return true;
        }
        record_drop();
        return false;
    }

    // must_not_drop：经重投缓冲投递（enqueue 失败持 payload 进 S&F，模式内保序
    // 追加队尾）。push 适配 copy-probe：push_td_event 按值签名失败即消费 payload，
    // 拷贝后投递保原件可重投（must_not_drop 为低频路径，拷贝成本可接受）。
    auto push = [this](XtHftRunnerEvent& e) {
        XtHftRunnerEvent probe = e;
        return event_loop_->push_td_event(std::move(probe));
    };
    using HandoffOffer = xt_live::TdHandoffRetryBuffer<XtHftRunnerEvent>::Offer;
    switch (td_handoff_.offer(std::move(ev), push)) {
        case HandoffOffer::kEnqueued:
            return true;
        case HandoffOffer::kBuffered: {
            // payload 未丢（进程内必达承诺），不计 drop；节流告警可见 S&F 生效。
            ++td_handoff_burst_total_;
            const int64_t now_ms = steady_now_ms();
            if (td_handoff_last_warn_ms_ == 0 ||
                now_ms - td_handoff_last_warn_ms_ >= kDropWarnThrottleMs) {
                const auto st = td_handoff_.stats();
                XtLogger::warn_m("RT",
                    "td_event handoff buffered (store-and-forward) burst={} depth={} "
                    "redelivered_total={} (60s throttle)",
                    td_handoff_burst_total_, st.depth, st.redelivered_total);
                td_handoff_last_warn_ms_ = now_ms;
                td_handoff_burst_total_ = 0;
            }
            return true;
        }
        case HandoffOffer::kOverflow:
        default:
            // 缓冲溢出 = 真丢弃，落入既有 critical drop 计数与 fail-closed 停机语义。
            record_drop();
            return false;
    }
}

size_t XtHftChannels::drain_td_handoff_buffer() {
    return drain_td_handoff_();
}

size_t XtHftChannels::drain_td_handoff_() {
    const size_t n = td_handoff_.try_drain([this](XtHftRunnerEvent& e) {
        // copy-probe 同 enqueue 路径：失败保原件在缓冲队首。
        XtHftRunnerEvent probe = e;
        return event_loop_->push_td_event(std::move(probe));
    });
    if (n > 0 && !td_handoff_.in_store_and_forward()) {
        XtLogger::info_m("RT", "td_event handoff drained {} event(s), store-and-forward exit", n);
    }
    return n;
}

bool XtHftChannels::enqueue_flow_event(XtHftRunnerEvent ev) {
    assert(event_loop_);
    if (event_loop_->push_flow_event(std::move(ev))) {
        return true;
    }
    shared_state_->flow_event_drop_cnt.fetch_add(1, std::memory_order_relaxed);
    // 首次告警后按 60s 窗口汇总，避免队列拥塞时刷日志。
    ++flow_drop_burst_total_;
    const int64_t now_ms = steady_now_ms();
    if (flow_drop_last_warn_ms_ == 0 || now_ms - flow_drop_last_warn_ms_ >= kDropWarnThrottleMs) {
        const char* mark = (flow_drop_last_warn_ms_ == 0) ? "FIRST" : "BURST";
        XtLogger::warn_m("RT", "flow_event drop {} burst_total={} (60s throttle)",
                          mark, flow_drop_burst_total_);
        flow_drop_last_warn_ms_ = now_ms;
        flow_drop_burst_total_ = 0;
    }
    return false;
}

void XtHftChannels::publish_channel_event(std::atomic<bool>* ready_flag,
                                         bool ready,
                                         const char* channel,
                                         int reason) {
    // edge-triggered：仅在 ready 状态真正翻转时发布事件，避免重复回调导致事件风暴
    const bool prev = ready_flag->exchange(ready, std::memory_order_acq_rel);
    if (prev == ready) {
        return;
    }
    shared_state_->refresh_trading_ready();
    XtHftRunnerEvent ev = ready
                           ? XtHftRunnerEvent::make_channel_ready(channel)
                           : XtHftRunnerEvent::make_channel_lost(channel, reason);
    (void)enqueue_td_event(std::move(ev));
    if (ready) {
        XtLogger::info_m("RT", "{} ready", channel);
    }
}

// hot path：非阻塞入队，丢弃优于阻塞——策略宁可跳 tick 也不能因队列满而挂起 MD 线程。
// capture_ns：SHM connector publish 前 stamp 的跨进程 steady 时刻（直连 venue 路径为 0），
// 转发到 try_push_tick 供 trigger 起点选择器（capture 有效且不晚于 enqueue 则用 capture）。
void XtHftChannels::on_md_tick(const XtRawTick& tick, uint64_t capture_ns) {
    assert(shared_state_ && event_loop_);
    shared_state_->md_in_cnt.fetch_add(1, std::memory_order_relaxed);
    // action_date/time 为 0 表示行情源未填充，不覆写（防止时间回退）
    if (tick.action_date) shared_state_->last_tick_date.store(tick.action_date, std::memory_order_release);
    if (tick.action_time) shared_state_->last_tick_time.store(tick.action_time, std::memory_order_release);
    if (!event_loop_->try_push_tick(tick, capture_ns)) {
        shared_state_->drop_cnt.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    shared_state_->enq_cnt.fetch_add(1, std::memory_order_relaxed);
}

void XtHftChannels::on_md_order_queue(const XtOrderQueue& oq) {
    (void)enqueue_flow_event(XtHftRunnerEvent::make_order_queue(oq));
}

void XtHftChannels::on_md_order_detail(const XtOrderDetail& od) {
    (void)enqueue_flow_event(XtHftRunnerEvent::make_order_detail(od));
}

void XtHftChannels::on_md_transaction(const XtTransaction& tr) {
    (void)enqueue_flow_event(XtHftRunnerEvent::make_transaction(tr));
}

void XtHftChannels::on_md_front(bool connected, int reason) {
    if (!connected) {
        publish_channel_event(&shared_state_->md_ready, false, "MD", reason);
    }
}

void XtHftChannels::on_md_ready() {
    publish_channel_event(&shared_state_->md_ready, true, "MD", 0);
}

void XtHftChannels::on_td_order_update(const XtOrderExecUpdate& upd, XtLatencyEligibility elig) {
    // sid_id 由 venue gateway 在回报边界 stamp，engine.on_order_update 据 upd.sid_id 路由到对应 ctx。
    // elig 仅 gate latency tracker feed；业务处理（book/account/策略）不受影响。
    (void)enqueue_td_event(XtHftRunnerEvent::make_order_update(
        upd, elig == XtLatencyEligibility::kIneligible));
}

void XtHftChannels::on_td_account_snapshot(const ITradeGateway::AccountSnapshot& snap) {
    (void)enqueue_td_event(XtHftRunnerEvent::make_account_snapshot(snap));
}

void XtHftChannels::on_td_position_snapshot(const std::vector<ITradeGateway::PositionSnapshot>& positions,
                                             ITradeGateway::TdState td_state,
                                             bool is_cold_start) {
    // 不 eager 置 td_position_ready：仅入队，snapshot 经 worker → engine.on_position_snapshot
    // 真正灌入 account_store 后由 observer 置 applied 并配对 td_ready 放行（诚实契约）。
    // enqueue 时（SDK 回调线程，与 on_td_front 的 gen++ 同一 gateway SPI 线程）捕获当前 gen；
    // 断连前入队的 snapshot 拿旧 gen，断连后（新 session）拿新 gen —— observer 据此跳过 stale。
    (void)enqueue_td_event(XtHftRunnerEvent::make_position_snapshot(
        positions, td_state, is_cold_start,
        shared_state_->td_session_gen.load(std::memory_order_acquire)));
}

void XtHftChannels::on_td_trade(const XtTradeRecord& tr, ITradeGateway::TdState td_state,
                                XtLatencyEligibility elig) {
    // venue commission 真值累加（fees_today）：转发到 truth sink。venue-neutral —
    // CTP sink note_fill_fee_fp 为 no-op（CTP fees 走 provider）。
    if (auto* sink = truth_sink_.load(std::memory_order_acquire); sink && tr.fee != 0.0) {
        sink->note_fill_fee_fp(xt_fp::to_fp(tr.fee), tr.fill_ts_us * 1000ULL);
    }
    // tr.strategy_id 由 venue gateway 在回报边界 stamp；route_status 是 recorder/report sidecar，
    // 不参与 engine 成交处理；返回指针只需在调用期间有效，此处立即拷贝。
    std::string route_status;
    if (route_status_resolver_) {
        if (const char* status = route_status_resolver_(tr.order_id, td_state)) {
            route_status = status;
        }
    }
    (void)enqueue_td_event(XtHftRunnerEvent::make_trade(
        tr, td_state, std::move(route_status), elig == XtLatencyEligibility::kIneligible));
}

void XtHftChannels::on_td_reverse_sync_batch(const std::vector<AliveInflightOrder>& orders,
                                              bool is_cold_start) {
    // STP re-arm 在本回调点（gateway 调用线程）同步完成，早于本函数返回 → gateway 标
    // ReverseSync satisfied → td_ready → trading_ready 放行。cold（首次 recovery）clear+seed；
    // warm（后续 resync，trading_ready 常驻 true）**no-op**（R4 设计裁决：STP 跟随 book 裁决——
    // union-seed 会复活「本进程已终态、venue 快照仍含」的争议单；warm re-arm 唯一来源改为下方
    // worker 的 book fillup 接受点经 rearm_active_order 驱动，并发 stamp 由 no-op 天然保留）。
    // STP 全 API 走 active_mu_/map_mu_ 线程安全，无需 worker 编组。
    if (stp_rearm_sink_) stp_rearm_sink_(orders, is_cold_start);
    // book fillup 仍走 worker：按值复制入队（reconcile 冷路径，小批量），
    // XtHftEventLoop dispatch 到 engine.on_reverse_sync_batch → book fillup。单向流不破
    // （gateway 不直接调 book），跟 reverse-sync emit batch 契约对齐。
    (void)enqueue_td_event(XtHftRunnerEvent::make_reverse_sync_batch(
        std::vector<AliveInflightOrder>(orders), is_cold_start));
}

void XtHftChannels::on_td_daily_settlement(const ITradeGateway::DailySettlementDelta& ev) {
    (void)enqueue_td_event(XtHftRunnerEvent::make_daily_settlement(ev));
}

void XtHftChannels::on_td_cash_transfer(const ITradeGateway::CashTransferDelta& ev) {
    (void)enqueue_td_event(XtHftRunnerEvent::make_cash_transfer(ev));
}

void XtHftChannels::on_td_ready() {
    // 仅设 td_ready（供 trading_ready=md&&td）并入队 ChannelReady 事件。
    // td_position_ready 的配对置位收到 worker：observer.on_channel_ready("TD") 处理，
    // readiness latch 全程 worker 单写（无 SDK×worker rendezvous）。
    publish_channel_event(&shared_state_->td_ready, true, "TD", 0);
}

// TD 断连（gen fence）：
//  1) `td_session_gen++`(release) —— 断连前入队的 stale snapshot 拿旧 gen，之后（新 session）拿
//     新 gen；observer 只对当前 gen 置 readiness → stale snapshot 永不 re-latch（即便 ChannelLost
//     物理满被丢也不 fail-open）。
//  2) SDK-即时 clear td_position_ready + position_snapshot_applied（release）—— 清掉断连前已
//     legitimately 置 true 的 readiness（ChannelLost 若丢, worker 侧清不到）。**increment 先于
//     clear**：observer 若在 gen 检查通过后、store 落地前撞上本 clear，由 observer 侧
//     validate-after-set（XtHftLiveObserverCombo::revoke_if_gen_moved_）撤销，不依赖本 clear
//     与 store 的先后。gen fence 保证此 SDK clear 不被 stale snapshot re-latch 推翻（observer
//     按 gen 跳过旧 snapshot）。直写 signals_ 不经 observer（其
//     assert_worker_thread_ 只守 observer 方法）。
//  3) td_ready=false（trading_ready 立即拒 submit）并入队 ChannelLost（worker 侧正常路径清
//     td_channel_up_，非唯一权威 clear）。
// **不变量（承重）**：td_session_gen++ happens-before 任何「断连后」的 position snapshot 入队 —— 靠
// equality-compare（gen 单调递增，任一 increment 即令旧-gen snapshot 失配），**不要求严格同线程**：
//  · 正常路径：OnFrontDisconnected(gen++) 与 OnRspQryInvestorPosition(on_td_position_snapshot 读 gen)
//    都是 CTP SPI 回调 → SDK 串行单 SPI 线程 → 程序序 happens-before。
//  · off-SPI stale_rebuild（TraderCTP check_health 的 stale-rebuild 路径从 health/watchdog 线程调
//    on_front(false,-99)）：gen++ 仍单调，但 on_front 先于 api_->Release()，Release 前旧 SPI net 线程未 join ——
//    存在旧 api 恰在此窗口 publish 一帧 snapshot 且捕获到已 bump 新 gen 的可能。危害有界：该帧是
//    旧 api 刚查回的真实 venue 持仓（非 stale-empty），且新 session 的 publish_positions → emit_ready
//    顺序保证新基线 snapshot 先于 ChannelReady 重灌；要成真 fail-open 还需叠加新基线 snapshot 物理满
//    被丢。observer 侧 validate-after-set 进一步收窄该窗口。
// binance 不发 on_front → gen 恒 0（inert）。未来若有 venue 在 SPI 之外并发 publish position，需重新论证
// gen 捕获序（equality-compare 下只需保证「断连后 publish 见到已 bump 的 gen」）。
void XtHftChannels::on_td_front(bool connected, int reason) {
    if (!connected) {
        shared_state_->td_session_gen.fetch_add(1, std::memory_order_release);
        shared_state_->td_position_ready.store(false, std::memory_order_release);
        shared_state_->position_snapshot_applied.store(false, std::memory_order_release);
        publish_channel_event(&shared_state_->td_ready, false, "TD", reason);
    }
}
