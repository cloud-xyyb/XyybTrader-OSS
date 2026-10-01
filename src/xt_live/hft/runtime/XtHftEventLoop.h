#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "xt_core/container/XtLockFreeQueue.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/market/XtRawTick.h"
#include "xt_live/common/XtLagHistogram.h"
#include "xt_live/hft/runtime/XtHftRunnerEvent.h"
#include "xt_live/hft/runtime/XtHftEventLoopCfg.h"
#include "xt_core/session/XtSessionWindow.h"
#include "xt_metrics/XtStrategyMetricsAggregator.h"
#include "xt_engine/hft/XtHftEngineStats.h"  // XtHftUnmatchedStats（engine 仍 forward-declare）

class XtCommodityCatalog;
class XtFeeRuleBook;
class XtTradeCostConfig;

class XtHftEngine;
class XtLiveLatencyTracker;
namespace xt_risk { class SelfMatchDetector; }

/// @brief HFT 实盘的单消费者事件循环。
///
/// 交易所 SDK 的回调来自各自的线程，而策略逻辑要求单线程串行。
/// XtHftEventLoop 用 tick / TD 回报 / 逐笔 flow 三条无锁队列把两边解耦：
/// SDK 线程只管快速入队，worker 线程按批取出、串行调用 XtHftEngine。
///
/// @warning 入队接口是 SDK 回调热路径；队列满或超过高水位时返回 false，
/// 调用方必须按丢弃/背压处理，不得等待 worker。
/// @note EventLoop 本身不做业务决策；撮合、账户、风控和策略回调都在 XtHftEngine。
class XtHftEventLoop {
public:
    // ── 批量容量上限（batch caps；也是 WorkerRuntime 数组容量）──────
    // 默认值 SSOT 在 XtHftEventLoopCfg；这里是内部 batch array 与 parser/validator 共用上限。
    static constexpr size_t kTickBatchMax = 256;
    static constexpr size_t kTdEventBatchMax = 256;
    static constexpr size_t kFlowEventBatchMax = 256;

    // ── 类型别名 ──────────────────────────────────────────────
    using Config    = XtHftEventLoopCfg;
    using QueueMode = XtHftEventLoopCfg::QueueMode;

    /// @brief EventLoop 运行依赖；全部为 non-owning，由 Runner 保证生命周期。
    ///
    /// 业务组件依赖通过 `XtHftEngine::Config` 注入 engine；EventLoop 只接收
    /// worker 控制、观测原子和成交成本归一化所需的只读数据源。
    struct Deps {
        XtHftEngine*            engine                = nullptr; ///< 统一 HFT 引擎（non-owning；持有全部业务逻辑）
        std::atomic<bool>*      shutdown_flag         = nullptr; ///< 全局关停标志（外部写，worker 读）
        std::atomic<uint32_t>*  last_tick_action_time = nullptr; ///< 最近 tick action_time（控制面心跳）
        std::atomic<uint32_t>*  last_tick_steady_ms   = nullptr; ///< 最近 tick monotonic ms（risk_monitor stale_quote）
        std::atomic<uint64_t>*  tick_proc_cnt         = nullptr; ///< tick 消费累计数（外部观测进度）

        const XtTradeCostConfig* live_cost = nullptr; ///< 成交成本配置；nullptr 表示跳过归一化。
        const XtFeeRuleBook*     live_fees = nullptr; ///< fee rule book；仅在 live_cost 非空时读取。
        const XtCommodityCatalog* catalog  = nullptr; ///< 合约目录；仅在成交成本归一化时读取。
        uint64_t                 initial_param_version = 0; ///< worker 启动后同步给 engine 的初始参数版本。

        std::atomic<bool>*                      in_trading_section = nullptr; ///< session gate 输出 flag；nullptr 禁用。
        const std::vector<std::pair<int,int>>*  session_sections   = nullptr; ///< 半开交易时段 `[start_hhmm, end_hhmm)`。

        std::atomic<bool>*                  closeout_no_open = nullptr; ///< closeout no-open 输出 flag；nullptr 禁用。
        const std::vector<XtSessionWindow>* closeout_windows = nullptr; ///< no-open/force closeout 时段配置。

        xt_risk::SelfMatchDetector* self_match_detector = nullptr; ///< post-trade 自成交检测；nullptr 禁用。
        std::function<void(const char*, const char*)> blacklist_sink; ///< 自成交命中后写黑名单；空 = 跳过。

        /// off-worker 注册 marshal 溢出侧道的 worker 侧 drain hook；worker 每轮循环调用一次
        /// （XtOrderRuntimeBook::drain_marshal_side_channel，侧道空时 O(1) 早退）。
        /// 空函数 = 无 book marshal（bench/单测），不 drain。
        std::function<void()> marshal_overflow_drain;

        /// 延迟采样器（non-owning，可 null）；worker run() 首部 bind_producer_slot
        /// 让 worker 上的 on_submit_result/on_order_update/on_trade tap 走确定性 SPSC slot。
        XtLiveLatencyTracker* latency_tracker = nullptr;
    };

    /// @brief 构造事件循环并绑定引擎与共享运行时状态。
    XtHftEventLoop(const Config& cfg, const Deps& deps);
    /// @brief 析构时请求停止并 join worker 线程。
    ~XtHftEventLoop();

    /// @brief 启动 worker 线程；重复启动返回 true。
    /// @pre 首次启动时 `running_ == false`；已运行时重复调用安全返回 true。
    /// @post 返回 true 时 worker 线程已进入事件循环。
    [[nodiscard]] bool start();

    /// @brief 请求停止并 join worker 线程。
    /// @post `running_ == false` 且 worker 线程已完全退出。
    void stop();

    /// @brief 非阻塞 tick 入队（non-blocking enqueue）。
    ///
    /// @param tick MD 回调解析后的原始行情。
    /// @return true 表示已发布给 worker；false 表示 worker 停止、高水位拦截
    ///         或 SPSC ring buffer 写满，tick 永久丢弃且不重试。
    /// @note O(1) 快速路径；ring buffer 预分配，不等待 worker。
    /// @note 单参 overload = capture_ns 未知（直连 venue / 测试路径）；转发二参 overload 传 0。
    [[nodiscard]] bool try_push_tick(const XtRawTick& tick);

    /// @brief 非阻塞 tick 入队（携带 SHM connector capture 时戳）。
    /// @param tick       MD 回调解析后的原始行情。
    /// @param capture_ns connector publish 前 stamp 的跨进程 steady ns（0=未 stamp）；
    ///                   截断为 µs 存入 `TickEvent::capture_ts_us`，dispatch 时经
    ///                   `xt_live::select_trigger_anchor_us` 决定 trigger 起点。
    /// @return 同单参 overload。
    [[nodiscard]] bool try_push_tick(const XtRawTick& tick, uint64_t capture_ns);

    /// @brief 非阻塞 TD 事件入队。
    /// @return false 表示 worker 停止、高水位拦截或 MPSC 队列写满；事件不重试。
    /// @note `XtHftRunnerEvent` move 入队；正常路径不分配、不等待。
    [[nodiscard]] bool push_td_event(XtHftRunnerEvent ev);

    /// @brief 非阻塞逐笔 flow 事件入队。
    /// @return false 表示 worker 停止、高水位拦截或 MPSC 队列写满；事件不重试。
    /// @note `XtHftRunnerEvent` move 入队；正常路径不分配、不等待。
    [[nodiscard]] bool push_flow_event(XtHftRunnerEvent ev);

    /// @brief worker 线程 id；start() 成功后有效（stop/join 后失效）。
    /// @note 供 runner 绑定 XtOrderRuntimeBook::bind_worker_marshal（off-worker
    ///       submit 注册入队的线程判定基准）。
    [[nodiscard]] std::thread::id worker_thread_id() const { return worker_.get_id(); }

    // ── Telemetry getters ─────────────────────────────────
    // 冷路径观测 getter；除 drain_* 接口外均不清零。
    // tick_backlog 是测试观测 seam（背压/排空契约断言）；生产 tick lag 走 drain_tick_lag_*。
    // td_event_backlog 是 TD 事件在途 instant，兼作 xt_queue_depth gauge 的生产源
    // （instant 口径，与 CTA/SEL 统一）；td_event_peak_backlog_val 是 lifetime peak，仅日志诊断。
    [[nodiscard]] uint64_t tick_backlog() const;
    [[nodiscard]] uint64_t td_event_backlog() const;
    /// @brief 读取并清零 tick 队列滞留（enqueue→consume）报告窗口 observed peak。
    [[nodiscard]] uint64_t drain_tick_lag_peak_us();
    /// @brief 读取并清零 tick 队列滞留窗口直方图（真 p50/p99，桶上界保守值）。
    [[nodiscard]] xt_live::XtLagHistogram::Stats drain_tick_lag_hist_stats();
    /// @brief 读取并清零 TD+flow 队列滞留窗口直方图（两队列共用单口径）。
    [[nodiscard]] xt_live::XtLagHistogram::Stats drain_td_flow_lag_hist_stats();
    [[nodiscard]] uint64_t td_event_peak_backlog_val() const;
    [[nodiscard]] uint64_t td_flow_event_lag_us_max_val() const;
    /// @brief 逐 context 读取并清空 strategy metrics 聚合快照。
    /// @return (strategy_id, snapshot) 按 context 注入序；无 context 返回空。
    /// @pre 跨线程调用要求每个 context 已 enable_concurrent_metrics()
    ///      （runner boot 期 inject_signals_into_all_contexts_ 保证）。
    [[nodiscard]] std::vector<std::pair<std::string, ct::metrics::XtStrategyMetricsSnapshot>>
    drain_all_strategy_metrics();
    [[nodiscard]] uint64_t engine_dropped_fill_count() const noexcept;
    [[nodiscard]] uint64_t engine_dropped_order_update_count() const noexcept;
    [[nodiscard]] uint64_t engine_orphan_reap_cancel_count() const noexcept;
    /// @brief journal 降级下 orphan reap 被抑制（告警-only）的累计 sweep 数。
    [[nodiscard]] uint64_t engine_orphan_reap_suppressed_count() const noexcept;
    /// @brief 策略 on_init 失败被 fail-closed disable 的 context 累计数。
    [[nodiscard]] uint64_t engine_strategy_init_failure_count() const noexcept;
    [[nodiscard]] uint32_t engine_open_order_count() const noexcept;
    /// @brief 一次性读出 engine 路由/恢复诊断计数（reason 拆分 + dispatch 分母 + 恢复计数）。
    [[nodiscard]] XtHftUnmatchedStats engine_unmatched_stats() const noexcept;

    // ── Exception-guard counters（仅异常路径自增，冷路径 getter）──────────
    /// @brief worker-body backstop 捕获的逃逸异常累计数(cumulative)。
    [[nodiscard]] uint64_t worker_body_exception_count() const noexcept;
    /// @brief normalize 抛异常 → passthrough 原始成交的累计数(cumulative)。
    [[nodiscard]] uint64_t normalize_fail_count() const noexcept;
    /// @brief recording emit 抛异常 → swallow 的累计数(cumulative)。
    [[nodiscard]] uint64_t record_fail_count() const noexcept;

private:
    /// @brief tick 队列内部事件；保存 core tick 和入队时间戳。
    struct TickEvent {
        XtTick core_tick;           ///< 已转换为 core 口径的 tick。
        uint64_t enqueue_ts_us = 0; ///< steady clock µs；本进程入队时刻，队列延迟/tick-lag 口径不变。
        uint64_t capture_ts_us = 0; ///< SHM connector capture 时刻 µs（0=未 stamp）；仅 trigger 起点上溯用。
    };

    /// @brief worker 调度预算；构造期按 cfg clamp 一次，之后只读。
    struct WorkerBudgets {
        size_t tick_batch_size = 0;       ///< 每轮最多消费的 tick 数；范围 `[1, kTickBatchMax]`。
        size_t td_batch_size = 0;         ///< 单次 `pop_bulk` 的 TD/control 事件上限。
        size_t flow_batch_size = 0;       ///< 单次 `pop_bulk` 的 L2 flow 事件上限。
        size_t td_budget_per_loop = 0;    ///< 每轮 TD/control 总预算；0 配置会回退到 td_batch_size。
        size_t flow_budget_per_loop = 0;  ///< 每轮 L2 flow 总预算；0 配置会回退到 flow_batch_size。
        size_t td_backlog_warn_threshold = 0; ///< TD backlog 告警阈值；0 表示禁用。
    };

    /// @brief worker 线程局部可变运行态；批次缓冲区放堆上，避免线程栈膨胀。
    struct WorkerRuntime {
        std::array<TickEvent, kTickBatchMax> tick_batch{}; ///< 本轮 tick `pop_bulk` 复用缓冲。
        std::array<XtHftRunnerEvent, kTdEventBatchMax> td_event_batch{}; ///< TD/control 复用缓冲。
        std::array<XtHftRunnerEvent, kFlowEventBatchMax> flow_event_batch{}; ///< L2 flow 复用缓冲。
        uint64_t tick_pending_since_us = 0;     ///< 0 表示当前无 tick backlog 等待。
        uint64_t last_tick_starve_warn_us = 0;  ///< tick starvation 上次告警 steady-clock us。
        uint64_t last_td_backlog_warn_us = 0;   ///< TD backlog 上次告警 steady-clock us。
        uint32_t tick_wait_loops = 0;           ///< tick 有 backlog 但 pop 为空的连续轮次。
    };

    // ── Enqueue and backlog accounting ────────────────────────────────
    /// @brief 判断 producer 当前是否应拒绝入队。
    ///
    /// @param high_watermark 软高水位；0 表示禁用软背压。
    /// @param backlog 对应队列当前 reserve 数；只用于 relaxed 背压观测。
    /// @param bypass_watermark true 时本 guard 不检查软高水位；后续 `queue.push()` 仍可能因物理容量失败。
    /// @return true 表示调用方不得继续入队；false 表示可尝试 `queue.push()`。
    [[nodiscard]] bool reject_enqueue_guard(uint64_t high_watermark,
                                            const std::atomic<uint64_t>& backlog,
                                            bool bypass_watermark = false) const;
    /// @brief 将 TD/flow 事件按 reserve-before-publish 协议入队并更新 backlog。
    ///
    /// - 当前状态：producer 已持有完整 Runner event，backlog_counter 表示已 reserve
    ///   但 worker 尚未消费的事件数。
    /// - 事件：先递增 backlog 再发布到无锁队列；物理 push 失败时回滚 reserve。
    /// - 下一步：worker pop 成功后调用 `safe_decrement_backlog()` 配对递减。
    /// @return true 表示事件已发布给 worker；false 表示事件未进入队列且调用方应按 drop 处理。
    [[nodiscard]] bool enqueue_runner_event(XtHftRunnerEvent ev,
                                            XtLockFreeQueue<XtHftRunnerEvent>& queue,
                                            uint64_t high_watermark,
                                            std::atomic<uint64_t>& backlog_counter,
                                            std::atomic<uint64_t>& peak_backlog_counter);

    /// @brief Consumer 端递减 backlog；异常下溢时 clamp 回 0，避免 uint64 wraparound。
    static void safe_decrement_backlog(std::atomic<uint64_t>& backlog,
                                       const char* counter_name);
    /// @brief 请求 worker 停止，并同步置位外部 shutdown flag（若已注入）。
    /// @note 只发出停止信号，不 join；可在 worker 错误路径内调用。
    void request_worker_stop() noexcept;

    // ── Event dispatch ────────────────────────────────────────────────
    /// @brief 在 worker 线程按 Runner event variant 分派到 exec/account/control handler。
    /// @return false 表示 handler 已请求停机；run loop 应退出。
    [[nodiscard]] bool dispatch_strategy_event(XtHftRunnerEvent& ev);
    /// @brief 分派行情 flow、订单回报和成交事件到 engine 的执行侧入口。
    /// @return false 表示 engine 或 guarded strategy callback 失败，worker 应停机。
    [[nodiscard]] bool dispatch_exec_event(const XtRunnerExecEvent& exec);
    /// @brief 分派账户、持仓、结算和出入金事件到 engine 账户侧入口。
    /// @note 这些事件已在 worker 线程串行化，不直接触碰 SDK 回调线程状态。
    [[nodiscard]] bool dispatch_account_event(const XtRunnerAccountEvent& account);
    /// @brief 分派通道、参数、recovery 和 worker task 控制事件。
    /// @return false 仅用于 guarded strategy callback 失败；内部 book 同步任务不走 strategy guard。
    [[nodiscard]] bool dispatch_control_event(const XtRunnerControlEvent& control);

    // ── Exec event handling ───────────────────────────────────────────
    /// @brief 先写 live recorder，再把订单状态回报交给 engine。
    /// @return false 表示 engine 拒绝继续运行，调用方应请求 worker stop。
    [[nodiscard]] bool handle_exec_order_update_(const XtRunnerOrderUpdateEvent& evt);
    /// @brief 归一化成交、驱动 engine 记账，并补发 self-match / recorder side effect。
    /// @return false 表示 engine 成交处理失败，调用方应请求 worker stop。
    [[nodiscard]] bool handle_exec_trade_(const XtRunnerTradeEvent& evt);

    // ── Trade adaptation ──────────────────────────────────────────────
    /// @brief 按 live 成本配置归一化成交记录；未配置时透传原始成交。
    /// @note fee book / catalog 可为空；normalize helper 会按缺失数据保守 fallback。
    [[nodiscard]] XtTradeRecord normalize_trade_event(const XtRunnerTradeEvent& evt) const;
    /// @brief post-trade 自成交兜底检测；命中时把合约加入 risk blacklist。
    /// @note 使用原始 TD 成交回报，不依赖 engine 内部持仓或订单状态。
    void check_post_trade_self_match(const XtTradeRecord& trade);

    // ── Live recording ────────────────────────────────────────────────
    /// @brief 将订单回报写入全局 live recorder；recorder 未启用时 no-op。
    /// @note 只消费 recorder sidecar（cancel initiator / reject err），不影响 engine 处理。
    void emit_order_update_record(const XtRunnerOrderUpdateEvent& evt) const;
    /// @brief 将成交回报写入全局 live recorder；recorder 未启用时 no-op。
    /// @note `effective_sid` 在调用期内转为 c-string；recorder 必须同步拷贝字段。
    void emit_trade_record(const XtRunnerTradeEvent& evt,
                           const XtTradeRecord& normalized,
                           int64_t realized_fp,
                           const std::string& effective_sid) const;

    // ── Worker processing ─────────────────────────────────────────────
    /// @brief 校验注入的 engine 非空并转引用；null 抛 std::invalid_argument。
    [[nodiscard]] static XtHftEngine& require_engine_(XtHftEngine* engine);
    /// @brief 按 cfg clamp 出 worker 调度预算（构造期算一次）。
    [[nodiscard]] static WorkerBudgets compute_worker_budgets_(const Config& cfg);
    /// @brief budgets 因 cfg 越界被 clamp 时输出一次告警（构造期调用）。
    void warn_if_budgets_clamped_() const;
    /// @brief 分配 worker 线程局部运行态；batch array 不占用 worker 栈。
    [[nodiscard]] std::unique_ptr<WorkerRuntime> make_worker_runtime_() const;
    /// @brief 根据 tick backlog 维护 tick 等待起点和 starvation 计数状态。
    void update_tick_pending_state(WorkerRuntime& st);
    /// @brief 参数化 runner-event 队列 drain（td/flow 共用）。kCheckTickWait 仅 TD 实例
    /// 开启 tick_max_wait 让位（flow 优先级本在 tick 之后，无让位）；lag peak 硬编码
    /// 共享成员 td_flow_event_lag_us_max_（两队列单口径，不随 backlog 参数化）。
    template <bool kCheckTickWait>
    [[nodiscard]] bool drain_runner_queue_(WorkerRuntime& st,
                                           XtLockFreeQueue<XtHftRunnerEvent>& queue,
                                           size_t budget,
                                           XtHftRunnerEvent* batch_buf,
                                           size_t batch_size_cfg,
                                           std::atomic<uint64_t>& backlog_counter,
                                           const char* backlog_label,
                                           bool& progressed);
    [[nodiscard]] bool process_td_events(WorkerRuntime& st, bool& progressed);
    /// @brief TD backlog 超过阈值时按节流窗口输出告警。
    void warn_td_backlog_if_needed(WorkerRuntime& st);
    /// @brief tick backlog 长时间无法 pop 时按节流窗口输出 starvation 告警。
    void warn_tick_starvation_if_needed(WorkerRuntime& st);
    /// @brief 计算单笔 tick 从入队到消费的观测延迟；返回 0 表示时间戳不可用。
    [[nodiscard]] static uint64_t observe_tick_lag_us(const TickEvent& ev);
    /// @brief 写入 tick 侧观测原子，供 control loop / stale quote monitor 读取。
    void update_tick_observability(const XtTick& tick, uint32_t batch_steady_ms) const;
    /// @brief 在策略 dispatch 前刷新 session 与 closeout gate。
    void update_tick_session_flags(const XtTick& tick);
    /// @brief 在 ORDER_SUBMIT tap TLS 窗口内调用 `engine_->on_tick()`。
    [[nodiscard]] bool dispatch_tick_to_engine(const TickEvent& ev);
    /// @brief 消费 tick 队列，更新观测/gate，并把 tick 串行投递给 engine。
    /// @param progressed 输出参数；本轮至少消费一个 tick 时置 true。
    /// @return false 表示 engine tick callback 失败并已触发停机语义。
    [[nodiscard]] bool process_ticks(WorkerRuntime& st, bool& progressed);
    /// @brief 按 flow 预算消费 L2 order queue/detail/transaction 队列。
    /// @param progressed 输出参数；本轮至少消费一个 flow 事件时置 true。
    /// @return false 表示 dispatch 失败并已触发停机语义。
    [[nodiscard]] bool process_flow_events(WorkerRuntime& st, bool& progressed);
    /// @brief stop 后清空残留队列并归零 backlog 计数。
    /// @note td 队列里的 WorkerTask 注册闭包执行而非丢弃（post-join 无并发写者；
    ///       book 侧注册经溢出侧道落地），其余事件维持既有丢弃语义。
    void drain_pending_queues();
    /// @brief worker 主循环；完成 init/参数同步后按 TD、tick、flow 顺序循环调度。
    void run();

    // ── Dependencies and configuration ────────────────────────────────
    // 除 engine_ 外均从 Deps 复制 non-owning 指针；nullptr 按对应功能禁用处理。
    Config cfg_;                                ///< 构造期复制的事件循环配置；运行期只读。
    const WorkerBudgets     budgets_;          ///< cfg clamp 后的 worker 调度预算；运行期只读。
    XtHftEngine&            engine_;             ///< 必需依赖，构造期校验非空。
    std::atomic<bool>*      shutdown_flag_         = nullptr; ///< 外部全局停机旗标；nullptr 表示只停本 loop。
    std::atomic<uint32_t>*  last_tick_action_time_ = nullptr; ///< tick action_time 输出；nullptr 禁用心跳更新。
    std::atomic<uint32_t>*  last_tick_steady_ms_   = nullptr; ///< tick 消费 steady ms 输出；nullptr 禁用 stale quote 观测。
    std::atomic<uint64_t>*  tick_proc_cnt_         = nullptr; ///< worker 已消费 tick 计数；nullptr 禁用进度观测。

    // ── Optional risk hooks ───────────────────────────────────────────
    xt_risk::SelfMatchDetector* self_match_detector_ = nullptr; ///< 成交后自成交检测器；nullptr 禁用。
    std::function<void(const char*, const char*)> blacklist_sink_; ///< 自成交命中后的黑名单写入 sink；空函数跳过。
    std::function<void()> marshal_overflow_drain_; ///< off-worker 注册溢出侧道 drain hook；worker 每轮调用，空函数跳过。
    XtLiveLatencyTracker* latency_tracker_ = nullptr; ///< non-owning；worker run() 首部 bind_producer_slot。

    // ── Trade normalization inputs ────────────────────────────────────
    const XtTradeCostConfig* live_cost_ = nullptr; ///< live 成本配置；nullptr 时成交记录不做成本归一化。
    const XtFeeRuleBook*     live_fees_ = nullptr; ///< fee 规则源；可为空，由成本模型按缺省规则兜底。
    const XtCommodityCatalog* catalog_  = nullptr; ///< 合约元数据源；可为空，缺失时跳过品种规则补全。
    uint64_t                 initial_param_version_ = 0; ///< worker init 后首次参数同步版本；0 表示不补发。

    // ── Session and closeout gates ────────────────────────────────────
    std::atomic<bool>*                     in_trading_section_ = nullptr; ///< tick 前更新的可交易时段 gate；nullptr 禁用。
    const std::vector<std::pair<int,int>>* session_sections_   = nullptr; ///< 半开 `[start_hhmm,end_hhmm)` session 列表。
    std::atomic<bool>*                     closeout_no_open_   = nullptr; ///< closeout no-open 输出 gate；nullptr 禁用。
    const std::vector<XtSessionWindow>*    closeout_windows_   = nullptr; ///< closeout 闭区间窗口；nullptr 表示无窗口。

    // ── Queues ────────────────────────────────────────────────────────
    XtLockFreeQueue<TickEvent> tick_q_;              ///< tick SPSC 无锁队列；MD producer -> worker consumer。
    XtLockFreeQueue<XtHftRunnerEvent> td_event_q_;   ///< TD/control MPSC 无锁队列；gateway/control -> worker。
    XtLockFreeQueue<XtHftRunnerEvent> flow_event_q_; ///< L2 flow MPSC 无锁队列；MD L2 callbacks -> worker。

    // ── Worker lifecycle ─────────────────────────────────────────────
    std::thread worker_;                      ///< worker 线程句柄；start/stop 管理，worker_thread_id() 冷路径读取。
    std::atomic<bool> running_{false};        ///< start/stop 生命周期 gate；producer 入队前 acquire 读取。
    std::atomic<bool> stop_requested_{false}; ///< worker 停机请求；guard 失败或 stop() 会 release 置位。

    // ── Tick backlog ────────────────────────────────────────────────
    // try_push_tick（MD）fetch_add，process_ticks（Worker）fetch_sub，每 tick 均触发。
    // 两端写频率对等，独占 cache line 消除跨核 invalidation。
    alignas(64)
    std::atomic<uint64_t> tick_backlog_{0};            ///< tick 在途数（MD fetch_add / Worker fetch_sub）

    // ── Tick latency KPI ────────────────────────────────────────────
    alignas(64)
    std::atomic<uint64_t> tick_lag_us_peak_{0};        ///< tick 队列滞留报告窗口 observed peak（µs）

    // ── Lag histograms（worker 单写，1Hz control loop drain）────────
    xt_live::XtLagHistogram tick_lag_hist_;     ///< tick 队列滞留窗口分布（真 p50/p99 源）
    xt_live::XtLagHistogram td_flow_lag_hist_;  ///< TD+flow 队列滞留窗口分布（两队列单口径）

    // ── TD/Flow backlog and lag ─────────────────────────────────────
    std::atomic<uint64_t> td_event_backlog_{0};        ///< TD 事件当前在途数（背压控制）
    std::atomic<uint64_t> td_event_peak_backlog_{0};   ///< TD 事件在途峰值（日志用）
    std::atomic<uint64_t> flow_event_backlog_{0};      ///< Flow 事件当前在途数（背压控制）
    std::atomic<uint64_t> td_flow_event_lag_us_max_{0};  ///< TD+Flow 延迟最大值 µs（日志用）

    // ── Exception-guard counters ────────────────────────────────────
    // run_guarded counter 重载载体；仅异常路径 relaxed +1，正常路径不触碰(hot path 零原子写)。
    // worker-body escape 另会置 stop（runner 停机 = 最响信号）；swallow 路径靠这些 counter + 日志观测。
    std::atomic<uint64_t> worker_body_ex_ct_{0};   ///< worker-body backstop 捕获逃逸异常数
    std::atomic<uint64_t> normalize_fail_ct_{0};   ///< normalize 抛→passthrough raw 次数
    std::atomic<uint64_t> record_fail_ct_{0};      ///< recording emit 抛→swallow 次数
};
