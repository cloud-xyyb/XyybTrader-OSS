#pragma once

/// @file
/// @brief 把每个策略的当日盈亏写成 Prometheus 指标。
///
/// 控制环每轮把 drain 到的 realized+fee 行喂进来；emit_strategy_pnl_metrics()
/// 负责对齐日初 baseline、给没有成交的策略补零心跳、写 gauge。
/// 只做输出，不读策略或 account 状态。

#include <cstddef>
#include <vector>

#include "XtRunnerControlCfg.h"
#include "xt_metrics/XtStrategyPnlAggregator.h"

namespace ct::metrics {
class XtStrategyPnlBaseline;
}
namespace xt_strategy_engine {
class XtDeclaredSymbolRegistry;
}

namespace xt_live::control {

struct StrategyPnlEmitResult {
    std::size_t row_count = 0;         ///< 实际 emit 的非零/有 row 记录数。
    std::size_t zero_target_count = 0; ///< declared target 中需要补零 heartbeat 的数量。
    bool baseline_ready = false;       ///< false 表示 baseline 尚未初始化，本轮不 emit today PnL。
};

/// @brief 写入 `xt_today_pnl_net_v2{strategy,book,venue,symbol}` 矩阵。
/// @param last_baseline_day_id in/out；检测到 trading day rollover 时更新。
StrategyPnlEmitResult emit_strategy_pnl_metrics(
    const XtRunnerControlConfig& cfg,
    ct::metrics::XtStrategyPnlBaseline* baseline,
    const xt_strategy_engine::XtDeclaredSymbolRegistry* declared_registry,
    const std::vector<ct::metrics::XtStrategyPnlAggregator::Row>& rows,
    int& last_baseline_day_id);

}  // namespace xt_live::control
