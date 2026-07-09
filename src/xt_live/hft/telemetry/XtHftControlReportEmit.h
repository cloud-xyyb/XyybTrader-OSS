#pragma once

/// @file
/// @brief 控制环每轮的观测输出：live 报表、KPI 和 Prometheus 指标。
///
/// HftRunnerControl 每帧把采样好的 LoopFrame 交给本文件的函数输出。
/// 只做输出，不采样、不驱动策略链、不处理 shutdown。

#include <string>
#include <string_view>
#include <vector>

#include "xt_live/hft/control/HftRunnerControlState.h"
#include "api/BrokerVenueTruth.h"

struct XtRunnerSignals;

namespace xt_live::control {

void emit_live_reports(const XtRunnerControlConfig& cfg,
                       XtHftRunnerControlContext& ctx,
                       xt_report::XtLiveReporter& live_tracker,
                       const detail::LoopFrame& frame,
                       detail::ControlLoopState& st);

void emit_td_drop_metrics(std::string_view venue,
                          const XtRunnerSignals& signals,
                          detail::ControlLoopState& st);

void emit_unmatched_drop_metrics(const XtRunnerSignals& signals,
                                 detail::ControlLoopState& st);

void flush_strategy_metrics_if_needed(const XtRunnerControlConfig& cfg,
                                      XtHftRunnerControlContext& ctx,
                                      xt_report::XtLiveReporter& live_tracker,
                                      const detail::LoopFrame& frame,
                                      detail::ControlLoopState& st);

void emit_per_symbol_truth_gauges(std::string_view venue,
                                  std::string_view account,
                                  const std::vector<PerSymbolTruth>& per_symbol,
                                  std::vector<std::string>& last_emitted_symbols);

}
