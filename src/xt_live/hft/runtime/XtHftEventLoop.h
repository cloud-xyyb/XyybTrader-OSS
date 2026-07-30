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
#include "xt_engine/hft/XtHftEngineStats.h"

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

    static constexpr size_t kTickBatchMax = 256;
    static constexpr size_t kTdEventBatchMax = 256;
    static constexpr size_t kFlowEventBatchMax = 256;

    using Config    = XtHftEventLoopCfg;
    using QueueMode = XtHftEventLoopCfg::QueueMode;

    /// @brief EventLoop 运行依赖；全部为 non-owning，由 Runner 保证生命周期。
    ///
    /// 业务组件依赖通过 `XtHftEngine::Config` 注入 engine；EventLoop 只接收
    /// worker 控制、观测原子和成交成本归一化所需的只读数据源。
    struct Deps {
        XtHftEngine*            engine                = nullptr;
        std::atomic<bool>*      shutdown_flag         = nullptr;
        std::atomic<uint32_t>*  last_tick_action_time = nullptr;
        std::atomic<uint32_t>*  last_tick_steady_ms   = nullptr;
        std::atomic<uint64_t>*  tick_proc_cnt         = nullptr;

        const XtTradeCostConfig* live_cost = nullptr;
        const XtFeeRuleBook*     live_fees = nullptr;
        const XtCommodityCatalog* catalog  = nullptr;
        uint64_t                 initial_param_version = 0;

        std::atomic<bool>*                      in_trading_section = nullptr;
        const std::vector<std::pair<int,int>>*  session_sections   = nullptr;

        std::atomic<bool>*                  closeout_no_open = nullptr;
        const std::vector<XtSessionWindow>* closeout_windows = nullptr;

        xt_risk::SelfMatchDetector* self_match_detector = nullptr;
        std::function<void(const char*, const char*)> blacklist_sink;

        std::function<void()> marshal_overflow_drain;

        XtLiveLatencyTracker* latency_tracker = nullptr;
    };

    XtHftEventLoop(const Config& cfg, const Deps& deps);

    ~XtHftEventLoop();

    [[nodiscard]] bool start();

    void stop();

    [[nodiscard]] bool try_push_tick(const XtRawTick& tick);

    [[nodiscard]] bool try_push_tick(const XtRawTick& tick, uint64_t capture_ns);

    [[nodiscard]] bool push_td_event(XtHftRunnerEvent ev);

    [[nodiscard]] bool push_flow_event(XtHftRunnerEvent ev);

    [[nodiscard]] std::thread::id worker_thread_id() const { return worker_.get_id(); }

    [[nodiscard]] uint64_t tick_backlog() const;
    [[nodiscard]] uint64_t td_event_backlog() const;

    [[nodiscard]] uint64_t drain_tick_lag_peak_us();

    [[nodiscard]] xt_live::XtLagHistogram::Stats drain_tick_lag_hist_stats();

    [[nodiscard]] xt_live::XtLagHistogram::Stats drain_td_flow_lag_hist_stats();
    [[nodiscard]] uint64_t td_event_peak_backlog_val() const;
    [[nodiscard]] uint64_t td_flow_event_lag_us_max_val() const;

    [[nodiscard]] std::vector<std::pair<std::string, ct::metrics::XtStrategyMetricsSnapshot>>
    drain_all_strategy_metrics();
    [[nodiscard]] uint64_t engine_dropped_fill_count() const noexcept;
    [[nodiscard]] uint64_t engine_dropped_order_update_count() const noexcept;
    [[nodiscard]] uint64_t engine_orphan_reap_cancel_count() const noexcept;

    [[nodiscard]] uint64_t engine_orphan_reap_suppressed_count() const noexcept;

    [[nodiscard]] uint64_t engine_strategy_init_failure_count() const noexcept;
    [[nodiscard]] uint32_t engine_open_order_count() const noexcept;

    [[nodiscard]] XtHftUnmatchedStats engine_unmatched_stats() const noexcept;

    [[nodiscard]] uint64_t worker_body_exception_count() const noexcept;

    [[nodiscard]] uint64_t normalize_fail_count() const noexcept;

    [[nodiscard]] uint64_t record_fail_count() const noexcept;

private:
    /// @brief tick 队列内部事件；保存 core tick 和入队时间戳。
    struct TickEvent {
        XtTick core_tick;
        uint64_t enqueue_ts_us = 0;
        uint64_t capture_ts_us = 0;
    };

    /// @brief worker 调度预算；构造期按 cfg clamp 一次，之后只读。
    struct WorkerBudgets {
        size_t tick_batch_size = 0;
        size_t td_batch_size = 0;
        size_t flow_batch_size = 0;
        size_t td_budget_per_loop = 0;
        size_t flow_budget_per_loop = 0;
        size_t td_backlog_warn_threshold = 0;
    };

    /// @brief worker 线程局部可变运行态；批次缓冲区放堆上，避免线程栈膨胀。
    struct WorkerRuntime {
        std::array<TickEvent, kTickBatchMax> tick_batch{};
        std::array<XtHftRunnerEvent, kTdEventBatchMax> td_event_batch{};
        std::array<XtHftRunnerEvent, kFlowEventBatchMax> flow_event_batch{};
        uint64_t tick_pending_since_us = 0;
        uint64_t last_tick_starve_warn_us = 0;
        uint64_t last_td_backlog_warn_us = 0;
        uint32_t tick_wait_loops = 0;
    };

    [[nodiscard]] bool reject_enqueue_guard(uint64_t high_watermark,
                                            const std::atomic<uint64_t>& backlog,
                                            bool bypass_watermark = false) const;

    [[nodiscard]] bool enqueue_runner_event(XtHftRunnerEvent ev,
                                            XtLockFreeQueue<XtHftRunnerEvent>& queue,
                                            uint64_t high_watermark,
                                            std::atomic<uint64_t>& backlog_counter,
                                            std::atomic<uint64_t>& peak_backlog_counter);

    static void safe_decrement_backlog(std::atomic<uint64_t>& backlog,
                                       const char* counter_name);

    void request_worker_stop() noexcept;

    [[nodiscard]] bool dispatch_strategy_event(XtHftRunnerEvent& ev);

    [[nodiscard]] bool dispatch_exec_event(const XtRunnerExecEvent& exec);

    [[nodiscard]] bool dispatch_account_event(const XtRunnerAccountEvent& account);

    [[nodiscard]] bool dispatch_control_event(const XtRunnerControlEvent& control);

    [[nodiscard]] bool handle_exec_order_update_(const XtRunnerOrderUpdateEvent& evt);

    [[nodiscard]] bool handle_exec_trade_(const XtRunnerTradeEvent& evt);

    [[nodiscard]] XtTradeRecord normalize_trade_event(const XtRunnerTradeEvent& evt) const;

    void check_post_trade_self_match(const XtTradeRecord& trade);

    void emit_order_update_record(const XtRunnerOrderUpdateEvent& evt) const;

    void emit_trade_record(const XtRunnerTradeEvent& evt,
                           const XtTradeRecord& normalized,
                           int64_t realized_fp,
                           const std::string& effective_sid) const;

    [[nodiscard]] static XtHftEngine& require_engine_(XtHftEngine* engine);

    [[nodiscard]] static WorkerBudgets compute_worker_budgets_(const Config& cfg);

    void warn_if_budgets_clamped_() const;

    [[nodiscard]] std::unique_ptr<WorkerRuntime> make_worker_runtime_() const;

    void update_tick_pending_state(WorkerRuntime& st);

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

    void warn_td_backlog_if_needed(WorkerRuntime& st);

    void warn_tick_starvation_if_needed(WorkerRuntime& st);

    [[nodiscard]] static uint64_t observe_tick_lag_us(const TickEvent& ev);

    void update_tick_observability(const XtTick& tick, uint32_t batch_steady_ms) const;

    void update_tick_session_flags(const XtTick& tick);

    [[nodiscard]] bool dispatch_tick_to_engine(const TickEvent& ev);

    [[nodiscard]] bool process_ticks(WorkerRuntime& st, bool& progressed);

    [[nodiscard]] bool process_flow_events(WorkerRuntime& st, bool& progressed);

    void drain_pending_queues();

    void run();

    Config cfg_;
    const WorkerBudgets     budgets_;
    XtHftEngine&            engine_;
    std::atomic<bool>*      shutdown_flag_         = nullptr;
    std::atomic<uint32_t>*  last_tick_action_time_ = nullptr;
    std::atomic<uint32_t>*  last_tick_steady_ms_   = nullptr;
    std::atomic<uint64_t>*  tick_proc_cnt_         = nullptr;

    xt_risk::SelfMatchDetector* self_match_detector_ = nullptr;
    std::function<void(const char*, const char*)> blacklist_sink_;
    std::function<void()> marshal_overflow_drain_;
    XtLiveLatencyTracker* latency_tracker_ = nullptr;

    const XtTradeCostConfig* live_cost_ = nullptr;
    const XtFeeRuleBook*     live_fees_ = nullptr;
    const XtCommodityCatalog* catalog_  = nullptr;
    uint64_t                 initial_param_version_ = 0;

    std::atomic<bool>*                     in_trading_section_ = nullptr;
    const std::vector<std::pair<int,int>>* session_sections_   = nullptr;
    std::atomic<bool>*                     closeout_no_open_   = nullptr;
    const std::vector<XtSessionWindow>*    closeout_windows_   = nullptr;

    XtLockFreeQueue<TickEvent> tick_q_;
    XtLockFreeQueue<XtHftRunnerEvent> td_event_q_;
    XtLockFreeQueue<XtHftRunnerEvent> flow_event_q_;

    std::thread worker_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_requested_{false};

    alignas(64)
    std::atomic<uint64_t> tick_backlog_{0};

    alignas(64)
    std::atomic<uint64_t> tick_lag_us_peak_{0};

    xt_live::XtLagHistogram tick_lag_hist_;
    xt_live::XtLagHistogram td_flow_lag_hist_;

    std::atomic<uint64_t> td_event_backlog_{0};
    std::atomic<uint64_t> td_event_peak_backlog_{0};
    std::atomic<uint64_t> flow_event_backlog_{0};
    std::atomic<uint64_t> td_flow_event_lag_us_max_{0};

    std::atomic<uint64_t> worker_body_ex_ct_{0};
    std::atomic<uint64_t> normalize_fail_ct_{0};
    std::atomic<uint64_t> record_fail_ct_{0};
};
