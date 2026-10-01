#pragma once

/// @file
/// @brief 控制环每轮的观测输出：live 报表、KPI 和 Prometheus 指标。
///
/// HftRunnerControl 每帧把采样好的 LoopFrame 交给本文件的函数输出。
/// 只做输出，不采样、不驱动策略链、不处理 shutdown。

#include <string>
#include <string_view>
#include <vector>

#include "xt_live/hft/control/HftRunnerControlState.h"  // detail::LoopFrame / detail::ControlLoopState + XtHftRunnerControlContext / XtRunnerControlConfig / xt_report::XtLiveReporter (经 HftRunnerControl.h 传递)
#include "api/BrokerVenueTruth.h"   // PerSymbolTruth

struct XtRunnerSignals;  // api/XtRunnerSignals.h

namespace xt_live::control {

/// @brief 发本轮全部可观测报告（live contract report + KPI + per-strategy PnL）。
void emit_live_reports(const XtRunnerControlConfig& cfg,
                       XtHftRunnerControlContext& ctx,
                       xt_report::XtLiveReporter& live_tracker,
                       const detail::LoopFrame& frame,
                       detail::ControlLoopState& st);

/// @brief TD 队列丢弃三桶（position/control/other）cumulative→delta emit。
///
/// XtRunnerSignals 持有单调累计 counter，本函数按 ControlLoopState 中的上次基线
/// 计算 Prometheus counter delta。首帧 emit 全量，之后只 emit 增量；
/// XT_HAVE_METRICS 未定义时为 no-op。
void emit_td_drop_metrics(std::string_view venue,
                          const XtRunnerSignals& signals,
                          detail::ControlLoopState& st);

/// @brief unmatched 丢弃（xt_dropped_total{kind,reason}）+ engine 自愈
///        （xt_engine_recovery_total{kind}）cumulative→delta emit。
///
/// 读取 1Hz wire 写入 XtRunnerSignals 的 reason 拆分和恢复累计值，并按
/// ControlLoopState 中的上次基线转换为 Prometheus delta。XT_HAVE_METRICS 未定义时为 no-op。
void emit_unmatched_drop_metrics(const XtRunnerSignals& signals,
                                 detail::ControlLoopState& st);

/// @brief 按窗口周期 drain 策略指标，写摘要日志并桥接到 Prom custom gauge。
void flush_strategy_metrics_if_needed(const XtRunnerControlConfig& cfg,
                                      XtHftRunnerControlContext& ctx,
                                      xt_report::XtLiveReporter& live_tracker,
                                      const detail::LoopFrame& frame,
                                      detail::ControlLoopState& st);

/// @brief 把 venue per-symbol truth 写入 {venue,symbol} + {venue,account,symbol} 持仓 gauge。
///
/// 持仓归零后 truth source 会过滤全 0 行，symbol 从 per_symbol 消失；对上周期
/// emit 过、本周期已消失的 symbol 显式写 0，防 gauge 残留旧仓位值误导诊断。
/// 写 0 而非 Remove series：panel 直查该 series 拿到明确的 0，且 symbol 基数
/// 有界（runner 只交易配置内 symbol）无 series 泄漏。
/// @param last_emitted_symbols 跨周期已 emit symbol 集（in/out，调用后刷新为当前列表）。
void emit_per_symbol_truth_gauges(std::string_view venue,
                                  std::string_view account,
                                  const std::vector<PerSymbolTruth>& per_symbol,
                                  std::vector<std::string>& last_emitted_symbols);

}  // namespace xt_live::control
