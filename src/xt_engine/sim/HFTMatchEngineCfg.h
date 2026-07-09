#pragma once
#include <cstdint>

/// @brief 配置 XtHftMatchEngine 的延迟、撮合和拆分行为。
///
/// 控制 ACK/TTL 延迟、拒单/误差注入、OFI erosion、market impact carry 和
/// split fill。延迟字段中 `*_us` 优先于 `*_ticks`；0 表示对应机制关闭。
struct XtHftMatchEngineCfg {

    double        queue_cancel_ratio  = 0.0;
    std::uint64_t ack_delay_ticks     = 0;
    std::uint64_t ttl_ticks           = 0;
    std::uint64_t ack_delay_us        = 0;
    std::uint64_t ttl_us              = 0;
    std::uint64_t ack_delay_stddev_us = 0;

    double        error_rate            = 0.0;
    double        reject_rate           = 0.0;
    double        ofi_queue_scale       = 0.0;
    bool          market_impact_enabled = false;
    double        market_impact_decay   = 0.5;

    bool          cancel_latency_enabled = false;

    bool          price_limit_enabled = false;

    bool          split_fills    = false;
    double        split_min_qty  = 1.0;
    double        split_max_qty  = 100.0;
    double        split_qty_tick = 1.0;

    std::int64_t  random_seed = 1;
    bool          volume_limit = true;
};
