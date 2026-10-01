#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "api/IReplayRuntime.h"
#include "xt_backtest/core/IReplayReporter.h"

class XtMarketDataSource;

/// @brief 回测 market-data 事件调度器。
///
/// 单线程读取 `XtMarketDataSource` 并驱动 `IReplayRuntime`；
/// 启用 clock grid 时，在相邻 source event 之间注入
/// `runtime.on_clock(action_date, action_time_ms, trading_date)`（event-axis 网格）。
///
class XtReplayer {
public:
    // ── 配置模型 ──────────────────────────────────────────────
    /// @brief replay pacing 与 clock grid 注入配置。
    struct Config {
        /// 每个 source event 派发后的 sleep 毫秒数；0 表示全速回放。
        uint32_t sleep_ms = 0;

        /// @brief Clock grid 分辨率（ns）；0 表示不注入 clock。
        ///
        /// 大于 0 时，按绝对 ns 网格在相邻 source event 时间戳之间注入
        /// `runtime.on_clock(action_date, action_time_ms, trading_date)`；首个 source event 前不注入，
        /// 且注入点必须早于下一个 source event。
        int64_t clock_resolution_ns = 0;

        /// @brief 校验字段取值范围，错误信息追加到 `errors`。
        void validate(std::vector<std::string>& errors) const;
    };

    // ── 回放主流程 ──────────────────────────────────────────────
    /// @brief 执行一次完整 replay，返回派发给 runtime 的 tick 数。
    ///
    /// 进入时重置 source。tick 模式只派发 tick；event_stream 模式按 source 已归并的
    /// 时间戳顺序派发 tick/orderflow 事件。clock grid 注入发生在下一个 source event
    /// 派发前；orderflow 事件不计入返回值。
    [[nodiscard]] size_t run(XtMarketDataSource& source,
                             IReplayRuntime& runtime,
                             IReplayReporter& reporter);

    // ── 回放节流控制 ──────────────────────────────────────────────
    /// @brief 设置 replay 配置。
    void set_config(const Config& cfg);

    /// @brief 覆盖每个 source event 后的 sleep 毫秒数；0 表示全速回放。
    void set_sleep_ms(uint32_t sleep_ms);

private:
    // ── 运行时状态 ──────────────────────────────────────────────
    uint32_t sleep_ms_             = 0;  ///< 每个 source event 后 sleep；0 表示关闭 pacing
    int64_t  clock_resolution_ns_  = 0;  ///< Clock grid 分辨率；0 表示不注入 clock
};

namespace YAML { class Node; }

/// @brief 从 YAML `backtest` 节点解析 replayer 配置。
XtReplayer::Config parse_replayer_config(const YAML::Node& backtest_node);
