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

    struct Config {

        uint32_t sleep_ms = 0;

        int64_t clock_resolution_ns = 0;

        void validate(std::vector<std::string>& errors) const;
    };

    [[nodiscard]] size_t run(XtMarketDataSource& source,
                             IReplayRuntime& runtime,
                             IReplayReporter& reporter);

    void set_config(const Config& cfg);

    void set_sleep_ms(uint32_t sleep_ms);

private:

    uint32_t sleep_ms_             = 0;
    int64_t  clock_resolution_ns_  = 0;
};

namespace YAML { class Node; }

XtReplayer::Config parse_replayer_config(const YAML::Node& backtest_node);
