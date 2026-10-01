#pragma once

/// @file
/// @brief HFT PnL 统计组件的装配与停机保存。
///
/// wire_hft_telemetry() 在启动期载入 PnL aggregator / 日初 baseline，并把 fee
/// observer 与 lifecycle / realized / cash-reanchor 回调填进 engine Hooks（engine
/// 构造期一次性接管）；save_hft_telemetry() 在停机时把累计盈亏写盘，与启动时的
/// load 用同一路径，保证跨重启 PnL 连续。组件所有权归调用方传入的 slot。

#include <memory>

#include "api/IHftEngineObserver.h"
#include "xt_engine/hft/XtHftEngine.h"
#include "xt_metrics/XtStrategyPnlAggregator.h"
#include "xt_metrics/XtStrategyPnlBaseline.h"

struct XtLiveConfig;

namespace xt_live::hft {

/// @brief 装配 HFT live telemetry 依赖并产出 engine hook。
///
/// 仅填充调用方传入的三个 unique_ptr slot，不接管 ownership；调用方必须保证
/// observer lifetime 覆盖 engine callback 窗口。
///
/// @param aggr_slot      caller-owned aggregator slot（在此构造 + load_state）
/// @param baseline_slot  caller-owned baseline slot（在此构造 + load）
/// @param fee_observer_slot caller-owned fee observer slot（在此构造）
/// @param hooks          输出：追加 fee observer 并填 lifecycle / fill_realized /
///                       cash_reanchor 回调；observer 追加序即 engine fan-out 序
/// @param cfg            提供 md_provider / control account_label / risk 的配置
/// @return false 表示 PnL state 损坏且 per-strategy daily-loss 风控已启用，
///         caller 必须 fail-closed 中止启动；true 表示装配完成（含冷启动）。
bool wire_hft_telemetry(
    std::unique_ptr<ct::metrics::XtStrategyPnlAggregator>& aggr_slot,
    std::unique_ptr<ct::metrics::XtStrategyPnlBaseline>& baseline_slot,
    std::unique_ptr<xt_runtime::IHftEngineObserver>& fee_observer_slot,
    XtHftEngine::Hooks& hooks,
    const XtLiveConfig& cfg);

/// @brief shutdown 期保存 aggregator 累计 realized/fees。
///
/// 与 `wire_hft_telemetry()` 的 load_state 使用同一路径，保证跨重启 PnL 连续。
/// @param aggr  aggregator raw ptr（可为 null，则 no-op）
/// @param cfg   提供 md_provider 的配置
void save_hft_telemetry(const ct::metrics::XtStrategyPnlAggregator* aggr,
                        const XtLiveConfig& cfg);

}  // namespace xt_live::hft
