#pragma once
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

/// @brief 延迟模型配置（latency model config）
///
/// Yaml 形态:
/// @code
/// backtest:
///   latency_model:
///     type: constant            # constant | interpolated | cdf
///     entry_us: 1500            # constant only
///     response_us: 1500         # constant only
///     source: var/data/lat.csv  # interpolated csv / cdf yaml
/// @endcode
///
/// 三种 type 与对应 C++ 实现：
///   - `constant`         → `ConstantLatencyModel`：常量延迟（默认零延迟）
///   - `cdf` (推荐新写法) → `CdfLatencyModel`：percentile 索引，按 golden-ratio
///                          LDS 抽样在 CDF 上插值。`source` 必须为 `.yaml/.yml`。
///                          适合把 live 延迟分布注入任意 backtest 时段（与时钟无关）。
///   - `interpolated`     → `InterpolatedLatencyModel`：`source` 必须为 `.csv`，
///                          按 `ts_local_ns` 在 `(ts, entry, response)` 序列上插值。
///                          适合 live 录制窗口内的重放；窗口外 clamp 到首/末值。
///
/// 装配：Runner 据此构造 ILatencyModel 并注入 match engine；未配时撮合走 0 延迟
///       路径（等价老 `fill_latency_us=0`）。
struct LatencyModelCfg {
    std::string type;
    int64_t     entry_us = 0;
    int64_t     response_us = 0;
    std::string source;
};

/// @brief 队列模型配置（queue model config）
///
/// Yaml 形态:
/// @code
/// backtest:
///   queue_model:
///     type: ratio                # ratio | prob_power1 | prob_power2
///     ratio: 0.0                 # ratio only
///     cancel_resubmit_ratio: 1.0 # ratio only
///     k: 1.5                     # prob_power1/2 only
/// @endcode
///
/// 装配：Runner 据此构造 IQueueModel 并注入 match engine；未配时 queue_ahead
///       计算视为 0（等价老 `queue_ahead_ratio=0.0`）。
struct QueueModelCfg {
    std::string type;
    double      ratio = 0.0;
    double      cancel_resubmit_ratio = 1.0;
    double      k = 1.0;
};

/// @brief HFT backtest engine 的运行配置快照。
///
/// Runner 从 YAML 解析本结构后注入 engine/gateway/replayer 装配链；字段只描述
/// backtest-only 行为，例如 EOF grace、爆仓模拟、latency/queue model 与 T+1 模式。
struct XtHftBacktestEngineCfg {
    enum class T1Mode : std::uint8_t { Unset = 0, Disabled = 1, Enabled = 2 };

    std::uint32_t eof_grace_ms = 0;

    bool margin_call_enabled = false;

    double maintenance_ratio = 0.9;

    std::optional<LatencyModelCfg> latency_model;

    std::optional<QueueModelCfg> queue_model;

    std::uint32_t fill_return_latency_ms = 0;
    T1Mode t1_mode = T1Mode::Unset;
    double initial_cash = 1000000.0;
    std::size_t history_ticks = 2048;
    std::size_t history_bars = 512;
    double max_order_qty = 0.0;
    bool debug_signal_log_enabled = false;
    std::uint32_t debug_signal_log_every_n_ticks = 1;
    std::string config_path;
    std::string stra_id;

    uint32_t match_delay_ticks = 2;
    int session_offset_minutes = 0;

    bool conditional_orders_enabled = false;

    void validate(std::vector<std::string>& errors) const;
};
