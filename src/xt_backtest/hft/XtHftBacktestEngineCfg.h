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
    std::string type;            ///< "constant" | "interpolated" | "cdf"
    int64_t     entry_us = 0;    ///< 去程延迟 us（constant 用）
    int64_t     response_us = 0; ///< 回程延迟 us（constant 用）
    std::string source;          ///< 数据源路径（interpolated .csv / cdf .yaml/.yml）
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
    std::string type;                       ///< "ratio" | "prob_power1" | "prob_power2"
    double      ratio = 0.0;                ///< 普通 queue_ahead 比例（ratio 用）
    double      cancel_resubmit_ratio = 1.0;///< 撤单重入比例（ratio 用）
    double      k = 1.0;                    ///< prob functor 参数（prob_power1/2 用）
};

/// @brief HFT backtest engine 的运行配置快照。
///
/// Runner 从 YAML 解析本结构后注入 engine/gateway/replayer 装配链；字段只描述
/// backtest-only 行为，例如 EOF grace、爆仓模拟、latency/queue model 与 T+1 模式。
struct XtHftBacktestEngineCfg {
    enum class T1Mode : std::uint8_t { Unset = 0, Disabled = 1, Enabled = 2 };

    /// @brief Strict 模式下 EOF 时刻的容忍窗口 (ms)。
    /// @details 推 EOF clock 到 `last_tick_ns + eof_grace_ms * 1e6`,fill visible
    ///   在此窗口内的 drain。默认 0 = 最严格 (= last_tick_ns)。
    ///   非零值用于"模拟 live 进程多活 N ms 收正常回程 fill"折中场景。
    std::uint32_t eof_grace_ms = 0;

    /// @brief true 时逐 tick 检查维持保证金（equity < maint → 撤全部挂单 + 对手价
    ///        IOC 强平）；默认 false = 无爆仓模拟（既有行为，账面可穿仓）。
    bool margin_call_enabled = false;
    /// @brief 维持保证金系数：maint = Σ |pos|×per_unit_margin(mark)×本值；∈(0,1]。
    double maintenance_ratio = 0.9;

    /// @brief 可选 latency_model 配置。Runner 据此装配 ILatencyModel 注入 match engine；
    ///        nullopt 时撮合走 0 延迟路径。
    std::optional<LatencyModelCfg> latency_model;

    /// @brief 可选 queue_model 配置。Runner 据此装配 IQueueModel 注入 match engine；
    ///        nullopt 时 queue_ahead 计算视为 0。
    std::optional<QueueModelCfg> queue_model;

    /// @brief Fill 回程延迟（ms）— match 到 strategy.on_fill 可见的等待时间。
    ///
    /// 对应真实网络回程：交易所撮合完成 → fill msg 经网络回到策略的 RTT。
    /// Gateway 的 pending_fills_ 队列为每笔 fill 打上 `visible_at_ns =
    /// match_ns + fill_return_latency_ms * 1e6`，drain_fills(now_ns) 只弹出
    /// visible_at_ns <= now_ns 的 fill。
    ///
    /// - `0`（默认）：最近一个 on_clock 事件就 drain（~ 250 ms 后，即半个 tick）
    /// - `50`：match 后 50 ms 才可见
    /// - `600`：跨过下一个 tick 才可见（慢链路场景）
    ///
    /// @note 与 `latency_model.entry_us` 正交。entry_us 是撮合引擎内部
    ///       submit→match 的延迟（对应网络去程 + exchange 撮合时间），
    ///       `fill_return_latency_ms` 是 match→strategy 可见的延迟（对应回程）。
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
    /// @brief 撮合延迟 tick 数。0=same tick 内撮合；1=下一 tick 撮合；>=2=N-tick 延迟（含 order wheel）。
    ///        默认 2（tick N 提交 → tick N+1 盘口撮合 + order wheel 一跳）。
    uint32_t match_delay_ticks = 2;
    int session_offset_minutes = 0;           ///< 夜盘偏移，300=中国期货
    /// 本地条件单开关（`backtest.conditional_orders_enabled`）；默认关。
    /// runner 把此值同时喂给 engine Config 与每个 XtStrategyContext::Config。
    bool conditional_orders_enabled = false;

    void validate(std::vector<std::string>& errors) const;
};
