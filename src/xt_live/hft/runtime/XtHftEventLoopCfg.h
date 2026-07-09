#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
/// @brief HFT event loop 队列、批处理和 worker 调优配置。
struct XtHftEventLoopCfg {
    static constexpr std::size_t   kTickBatchDefault              = 64;
    static constexpr std::size_t   kTdEventBatchDefault           = 128;
    static constexpr std::size_t   kFlowEventBatchDefault         = 128;
    static constexpr std::size_t   kTdEventBudgetPerLoopDefault   = 128;
    static constexpr std::size_t   kFlowEventBudgetPerLoopDefault = 64;
    static constexpr std::uint64_t kTickMaxWaitUsDefault          = 2000;
    static constexpr std::uint32_t kTickStarvationWarnLoopsDefault = 128;

    static constexpr std::size_t   kTickHighWatermarkPctDefault      = 90;
    static constexpr std::size_t   kTdEventHighWatermarkPctDefault   = 90;
    static constexpr std::size_t   kFlowEventHighWatermarkPctDefault = 80;

    enum class QueueMode { SPSC, MPSC };

    QueueMode     tick_queue_mode              = QueueMode::SPSC;
    std::size_t   tick_queue_capacity          = 65536;
    std::size_t   tick_high_watermark          = 0;
    QueueMode     td_event_queue_mode          = QueueMode::MPSC;
    QueueMode     flow_event_queue_mode        = QueueMode::MPSC;
    std::size_t   td_event_queue_capacity      = 65536;
    std::size_t   flow_event_queue_capacity    = 65536;
    std::size_t   td_event_high_watermark      = 0;
    std::size_t   flow_event_high_watermark    = 0;
    std::size_t   td_event_budget_per_loop     = kTdEventBudgetPerLoopDefault;
    std::size_t   flow_event_budget_per_loop   = kFlowEventBudgetPerLoopDefault;
    std::uint64_t tick_max_wait_us             = kTickMaxWaitUsDefault;
    std::uint32_t tick_starvation_warn_loops   = kTickStarvationWarnLoopsDefault;
    std::size_t   td_backlog_warn_threshold    = 0;
    std::size_t   tick_batch_size              = kTickBatchDefault;
    std::size_t   td_event_batch_size          = kTdEventBatchDefault;
    std::size_t   flow_event_batch_size        = kFlowEventBatchDefault;
    int           worker_affinity              = -1;
    std::string   worker_priority;
    bool          enable_pure_spin             = true;
};
