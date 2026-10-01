#pragma once
#include <cstdint>

/// @brief 配置 XtHftMatchEngine 的延迟、撮合和拆分行为。
///
/// 控制 ACK/TTL 延迟、拒单/误差注入、OFI erosion、market impact carry 和
/// split fill。延迟字段中 `*_us` 优先于 `*_ticks`；0 表示对应机制关闭。
struct XtHftMatchEngineCfg {
    // Queue / clock.
    double        queue_cancel_ratio  = 0.0;  ///< 价格不变且盘口缩量时的 queue erosion ratio
    std::uint64_t ack_delay_ticks     = 0;    ///< ACK 延迟 tick 数；ack_delay_us 非零时不生效
    std::uint64_t ttl_ticks           = 0;    ///< TTL tick 数；yaml 字段为 order_timeout_ticks
    std::uint64_t ack_delay_us        = 0;    ///< ACK 延迟均值 us；由 engine 逐单采样后传给 order manager
    std::uint64_t ttl_us              = 0;    ///< TTL us；0 关闭
    std::uint64_t ack_delay_stddev_us = 0;    ///< ACK 延迟标准差 us；0 表示固定延迟

    // Simulation knobs.
    double        error_rate            = 0.0;  ///< 随机撤单概率 [0,1]
    double        reject_rate           = 0.0;  ///< submit 后本地拒单概率 [0,1]
    double        ofi_queue_scale       = 0.0;  ///< 最优档 OFI erosion scale；0 关闭
    bool          market_impact_enabled = false; ///< true 时成交量会扣减下一 tick 同价位可见深度
    double        market_impact_decay   = 0.5;  ///< market impact carry 每 tick 衰减 [0,1]
    /// true 时撤单请求经 latency model 的 entry 延迟到达撮合（cancel-fill race）；
    /// 未配 latency_model 时启动校验拒绝。默认 false 表示撤单即时生效。
    bool          cancel_latency_enabled = false;
    /// true 时启用涨跌停 limit-lock：触板整侧冻结 + 板价被动单 erosion 免疫；
    /// tick limit 字段为 0 时 fail-open。默认 false 表示撮合不读 limit。
    bool          price_limit_enabled = false;

    // Split fill；字段来自 yaml `backtest.fill.*`，数量单位为 lot。
    bool          split_fills    = false; ///< true 时把单笔 fill 拆成多个 trade record
    double        split_min_qty  = 1.0;   ///< 单个拆分片段最小数量
    double        split_max_qty  = 100.0; ///< 单个拆分片段最大数量
    double        split_qty_tick = 1.0;   ///< 拆分数量对齐步长

    std::int64_t  random_seed = 1;     ///< 0 使用 random_device，结果不可复现
    bool          volume_limit = true; ///< false 时每单独立判断盘口深度，不共享 tick 成交容量
};
