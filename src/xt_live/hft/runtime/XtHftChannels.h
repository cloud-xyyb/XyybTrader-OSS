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
    /// @brief 构造 HFT 回调桥接器。
    /// @note 参数均为 non-owning；调用方必须保证依赖对象覆盖 `wire()` 到
    ///       `stop()` 完成之间的全部 SDK 回调窗口。
    XtHftChannels(IMdGateway* md_channel,
                   ITradeGateway* td_gateway,
                   XtHftEventLoop* event_loop,
                   XtRunnerSignals* shared_state,
                   class XtLiveLatencyTracker* latency_tracker,
                   std::function<const char*(uint64_t, ITradeGateway::TdState)> route_status_resolver);

    /// @brief 将自身 `on_xxx` 方法绑定为 MD/TD gateway 回调。
    /// @pre 必须在 `start()` 前调用；重复绑定语义由具体 gateway 决定。
    void wire();
    /// @brief 启动 MD/TD gateway。
    /// @return true 表示两条通道均启动成功；TD 启动失败时会停止已启动的 MD。
    bool start();
    /// @brief 停止 MD/TD gateway，并清除共享 ready gate。
    /// @note 调用方仍需随后停止 EventLoop，确保已入队事件被 worker 按自身 stop 语义处理。
    void stop();

    // ── MD 回调桥接 ──────────────────────────────────────────────

    /**
     * @brief MD tick 回调热路径：更新快照计数 + try_push_tick 入队（hot-path tick bridge）。
     *
     * @param tick       MD 回调解析后的原始行情。
     * @param capture_ns SHM connector publish 前 stamp 的跨进程 steady ns（0=直连 venue 无 capture）；
     *                   转发到 try_push_tick 作为 trigger 起点选择器输入。
     * @alloc none — 本回调不做堆分配
     * @complexity O(1) worst-case
     */
    void on_md_tick(const XtRawTick& tick, uint64_t capture_ns);
    /**
     * @brief MD L2 委托队列回调：转发为 flow 事件入队（order queue bridge）。
     *
     * @alloc none — XtHftRunnerEvent 值类型 move 入队
     * @complexity O(1) worst-case
     */
    void on_md_order_queue(const XtOrderQueue& oq);
    /**
     * @brief MD L2 逐笔委托回调：转发为 flow 事件入队（order detail bridge）。
     *
     * @alloc none
     * @complexity O(1) worst-case
     */
    void on_md_order_detail(const XtOrderDetail& od);
    /**
     * @brief MD L2 逐笔成交回调：转发为 flow 事件入队（transaction bridge）。
     *
     * @alloc none
     * @complexity O(1) worst-case
     */
    void on_md_transaction(const XtTransaction& tr);
    /// @brief MD front 状态回调；只在断连方向发布 ChannelLost。
    /// @note 连接方向等待 on_md_ready 确认数据就绪后再发布 ChannelReady。
    void on_md_front(bool connected, int reason);
    /// @brief MD 数据就绪回调；发布一次 MD ChannelReady 边沿事件。
    void on_md_ready();

    // ── TD 回调桥接 ──────────────────────────────────────────────
    /// @brief TD 订单状态回报桥接；入队失败时按关键 TD event 丢弃统计处理。
    /// @param elig latency eligibility（gateway 边界盖章；仅 gate tracker feed，业务不受影响）。
    void on_td_order_update(const XtOrderExecUpdate& upd, XtLatencyEligibility elig);
    // Raw transport ACK / UDS 到达 / ack-unavailable 三路延迟采样在 wire() 内 lambda 直连
    // latency tracker（不走 EventLoop 队列；tracker 自带 mutex，可被 SDK 回调线程并发调用）。
    /// @brief 注入 account truth 写侧 sink。
    /// @pre Runner 应在 `start()` 前注入，避免启动窗口内的 broker overlay 被丢弃。
    /// @note release/acquire 连接 runner 装配线程与 SDK 回调线程；nullptr 表示丢弃 overlay 和 fee truth。
    void set_account_truth_sink(IAccountTruthSink* sink) noexcept {
        truth_sink_.store(sink, std::memory_order_release);
    }
    /// @brief 注入 STP reverse-sync re-arm 回调（recovered alive 单 → re-arm STP 边界）。
    /// @note H1/K1：runner 装配处 wire 到 order gateway（stp_rearm_reverse_sync，cold 清场重播
    ///       / warm union-seed）。在 SDK/gateway 回调线程被同步调用（STP 全 API 走
    ///       active_mu_/map_mu_ 线程安全，无需 worker 编组）；null 时跳过。is_cold_start 传入
    ///       决定 cold（clear+seed）/ warm（不 clear，保留并发 stamp）分叉。
    void set_stp_rearm_sink(std::function<void(const std::vector<AliveInflightOrder>&, bool)> sink) {
        stp_rearm_sink_ = std::move(sink);
    }
    /// @brief broker venue-truth overlay 桥接 → truth source（read-side，不入 td 队列）。
    void on_td_broker_account_truth(const BrokerVenueTruth& bt);
    /// @brief TD 资金快照桥接；写入 td event queue，worker 线程再更新账户状态。
    void on_td_account_snapshot(const ITradeGateway::AccountSnapshot& snap);
    /// @brief TD 持仓快照回调；仅入队，不 eager 置 td_position_ready。
    /// @note td_position_ready 由 observer 在快照真正灌入 account_store 后置位（诚实契约），
    ///       并与 worker 侧 td 通道 up 配对——整条 readiness latch 是 worker 单写状态机；
    ///       trading_ready 仍由 md_ready && td_ready 单独 gate（不含 position baseline）。
    void on_td_position_snapshot(const std::vector<ITradeGateway::PositionSnapshot>& positions,
                                 ITradeGateway::TdState td_state,
                                 bool is_cold_start = false);
    /// @brief TD 成交回报主链路桥接；成交事件入队后由 worker 更新 engine/order/account。
    /// @note `route_status` 仅是 recorder/report sidecar，不参与 engine 路由或账户记账。
    /// @param elig latency eligibility（gateway 边界盖章；仅 gate tracker feed）。
    void on_td_trade(const XtTradeRecord& tr, ITradeGateway::TdState td_state,
                     XtLatencyEligibility elig);
    /// @brief gateway 反向灌 alive in-flight orders 桥接到 td event queue。
    /// @note `orders` 在发布时按值复制/移动；reconcile 冷路径允许该成本。
    void on_td_reverse_sync_batch(const std::vector<AliveInflightOrder>& orders,
                                  bool is_cold_start);
    /// @brief TD 日终/结算增量桥接；作为账户事件入队。
    void on_td_daily_settlement(const ITradeGateway::DailySettlementDelta& ev);
    /// @brief TD 出入金增量桥接；作为账户事件入队。
    void on_td_cash_transfer(const ITradeGateway::CashTransferDelta& ev);
    /// @brief TD ready 回调；仅设 td_ready 并入队 ChannelReady。
    /// @note td_position_ready 的配对置位在 worker（observer.on_channel_ready("TD")）完成，
    ///       readiness latch 全程 worker 单写；此处不再触碰 td_position_ready。
    void on_td_ready();
    /// @brief TD front 状态回调；断连时设 td_ready=false 并入队 ChannelLost。
    /// @note readiness latch 的权威 clear 交 worker（observer.on_channel_lost("TD")），
    ///       排在 stale snapshot 之后清，确保旧快照不能重新放行 readiness；
    ///       trading_ready 立即拒 submit 遮窗口。
    void on_td_front(bool connected, int reason);

    // ── TD handoff 重投缓冲（must_not_drop store-and-forward）────────

    using TdHandoffStats = xt_live::TdHandoffRetryBuffer<XtHftRunnerEvent>::Stats;

    /// @brief 尝试排空 must_not_drop 重投缓冲（周期 hook 入口）。
    /// @return 本次成功重投的事件数。
    /// @note 线程安全（组件自带 mutex），任意线程可调；缓冲为空时 O(1) 早退。
    ///       预期由 1Hz frame-tail 周期调用兜底；enqueue 调用点自身也会顺带
    ///       尝试排空，故本 hook 只覆盖「S&F 后无后续 TD 事件到达」的静默尾巴。
    size_t drain_td_handoff_buffer();

    /// @brief 重投缓冲观测快照（buffered/redelivered/overflow 计数 + 深度 + 模式）。
    [[nodiscard]] TdHandoffStats td_handoff_stats() const { return td_handoff_.stats(); }

    /// @brief 调整重投缓冲容量；0 = 关闭（回退失败即丢旧语义，回滚开关/测试 seam）。
    void set_td_handoff_capacity(size_t cap) { td_handoff_.set_capacity(cap); }

private:

    /**
     * @brief try-push 入队 TD/control 事件；must_not_drop 事件失败时持 payload
     *        进重投缓冲（store-and-forward），其余事件失败即丢并更新统计。
     *
     * @return true 表示事件已发布给 worker 或已进入重投缓冲（进程内必达承诺）；
     *         false 表示事件永久丢弃（非关键事件入队失败，或缓冲溢出）。
     * @alloc must_not_drop 路径一次事件拷贝（copy-probe：push_td_event 按值签名
     *        失败即消费 payload，须留原件供重投）；非关键路径 none。
     * @complexity O(1) worst-case（S&F 排空批量摊销在低频 must_not_drop 路径）
     */
    bool enqueue_td_event(XtHftRunnerEvent ev);
    /** @brief drain_td_handoff_buffer 内核：重投 + 模式退出日志。 */
    size_t drain_td_handoff_();
    /**
     * @brief try-push 入队 L2 flow 事件；队列满时丢弃并更新统计。
     *
     * @return true 表示事件已发布给 worker；false 表示事件永久丢弃且不重试。
     * @alloc none
     * @complexity O(1) worst-case
     */
    bool enqueue_flow_event(XtHftRunnerEvent ev);
    /** @brief 发布通道 ready/lost 事件并维护就绪标志。 */
    void publish_channel_event(std::atomic<bool>* ready_flag,
                               bool ready,
                               const char* channel,
                               int reason);

    // ── Runtime dependencies (non-owning) ────────────────────────────
    // Runner 拥有这些对象；Channels 只在 `wire()` 后到 `stop()` 完成前解引用。
    XtHftEventLoop* event_loop_ = nullptr;    ///< tick / TD / flow 三队列入口；回调窗口内必需非空。
    XtRunnerSignals* shared_state_ = nullptr; ///< 跨线程 ready gate、drop counter 与 tick 观测信号板；回调窗口内必需非空。
    IMdGateway* md_ = nullptr;                ///< MD gateway 回调绑定与 start/stop 入口；`wire()` 前必需非空。
    ITradeGateway* td_ = nullptr;             ///< TD gateway 回调绑定与 start/stop 入口；`wire()` 前必需非空。

    // ── Optional enrichment hooks ────────────────────────────────────
    class XtLiveLatencyTracker* latency_tracker_ = nullptr; ///< Raw ACK/UDS 延迟采样 sink；nullptr 时跳过延迟增强。
    /// order_id → recorder route_status 解析器；空函数或返回 nullptr 时成交事件保留空 route_status。
    /// 该 sidecar 只影响 recorder/report triage，不参与 engine 成交处理。
    /// 返回指针只需在调用期间有效，`on_td_trade()` 会立即拷贝为 std::string。
    std::function<const char*(uint64_t, ITradeGateway::TdState)> route_status_resolver_;
    /// account truth 写侧 sink；runner `start()` 前 release 写，SDK 回调线程 acquire 读。
    /// nullptr 表示丢弃 broker overlay 和 venue fee truth，不进入 td event queue。
    std::atomic<IAccountTruthSink*> truth_sink_{nullptr};
    /// STP reverse-sync re-arm sink（H1/K1）：reverse-sync 回调点同步 re-arm STP 边界（cold
    /// 清场重播 / warm union-seed），早于 gateway 标 ReverseSync satisfied → td_ready →
    /// trading_ready 放行。bool 形参 = is_cold_start（cold/warm 分叉）。null 时跳过。
    std::function<void(const std::vector<AliveInflightOrder>&, bool)> stp_rearm_sink_;

    // ── TD handoff 重投缓冲 ──────────────────────────────────────────
    /// must_not_drop 事件的 store-and-forward 缓冲；容量默认 kDefaultCapacity，
    /// set_td_handoff_capacity(0) 关闭（回退失败即丢旧语义）。
    xt_live::TdHandoffRetryBuffer<XtHftRunnerEvent> td_handoff_;

    // ── Drop warning throttle state ──────────────────────────────────
    // 日志节流只影响告警聚合，不参与业务同步；首次 drop 立即告警，之后按窗口汇总。
    int64_t td_drop_last_warn_ms_ = 0;    ///< TD/control drop 上次告警的 steady-clock ms；0 表示尚未告警。
    int64_t flow_drop_last_warn_ms_ = 0;  ///< L2 flow drop 上次告警的 steady-clock ms；0 表示尚未告警。
    uint64_t td_drop_burst_total_ = 0;    ///< 当前 TD/control 节流窗口内累计 drop 数；告警后清零。
    uint64_t flow_drop_burst_total_ = 0;  ///< 当前 L2 flow 节流窗口内累计 drop 数；告警后清零。
    int64_t td_handoff_last_warn_ms_ = 0;   ///< S&F 持入告警上次输出 ms；同上仅告警聚合。
    uint64_t td_handoff_burst_total_ = 0;   ///< 当前节流窗口内累计持入缓冲数；告警后清零。
};
