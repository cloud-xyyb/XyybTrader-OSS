#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
/// @brief HFT event loop 队列、批处理和 worker 调优配置。
struct XtHftEventLoopCfg {
    static constexpr std::size_t   kTickBatchDefault              = 64;
    static constexpr std::size_t   kTdEventBatchDefault           = 128;
    static constexpr std::size_t   kFlowEventBatchDefault         = 128;
    static constexpr std::size_t   kTdEventBudgetPerLoopDefault   = 128;  ///< 每轮最多处理的 TD 事件数，避免挤占 tick。
    static constexpr std::size_t   kFlowEventBudgetPerLoopDefault = 64;   ///< 每轮最多处理的逐笔 flow 事件数。
    static constexpr std::uint64_t kTickMaxWaitUsDefault          = 2000; ///< tick 已积压时，TD 事件最多继续处理的等待时间。
    static constexpr std::uint32_t kTickStarvationWarnLoopsDefault = 128;  ///< tick 积压但连续未消费的告警轮次阈值。
    // parser 默认高水位百分比；字段 in-class 默认 0=禁用，仅用于裸构造兜底。
    static constexpr std::size_t   kTickHighWatermarkPctDefault      = 90;
    static constexpr std::size_t   kTdEventHighWatermarkPctDefault   = 90;
    static constexpr std::size_t   kFlowEventHighWatermarkPctDefault = 80;

    enum class QueueMode { SPSC, MPSC };

    QueueMode     tick_queue_mode              = QueueMode::SPSC; ///< MD 单生产者默认 SPSC
    std::size_t   tick_queue_capacity          = 65536;
    std::size_t   tick_high_watermark          = 0; ///< 0=禁用背压；生产经 parser 默认 capacity×90%（裸构造保持 0）
    QueueMode     td_event_queue_mode          = QueueMode::MPSC;
    QueueMode     flow_event_queue_mode        = QueueMode::MPSC;
    std::size_t   td_event_queue_capacity      = 65536;
    std::size_t   flow_event_queue_capacity    = 65536;
    std::size_t   td_event_high_watermark      = 0; ///< 0=禁用背压；生产经 parser 默认 capacity×90%（裸构造保持 0）
    std::size_t   flow_event_high_watermark    = 0; ///< 0=禁用背压；生产经 parser 默认 capacity×80%（裸构造保持 0）
    std::size_t   td_event_budget_per_loop     = kTdEventBudgetPerLoopDefault;
    std::size_t   flow_event_budget_per_loop   = kFlowEventBudgetPerLoopDefault;
    std::uint64_t tick_max_wait_us             = kTickMaxWaitUsDefault;
    std::uint32_t tick_starvation_warn_loops   = kTickStarvationWarnLoopsDefault;
    std::size_t   td_backlog_warn_threshold    = 0; ///< 0 表示不按 backlog 告警
    std::size_t   tick_batch_size              = kTickBatchDefault;
    std::size_t   td_event_batch_size          = kTdEventBatchDefault;
    std::size_t   flow_event_batch_size        = kFlowEventBatchDefault;
    int           worker_affinity              = -1; ///< -1 表示不绑核
    std::string   worker_priority;                   ///< 空表示不调整调度优先级
    bool          enable_pure_spin             = true; ///< worker 绑核时纯自旋；经 parser md.enable_pure_spin 可配
};
