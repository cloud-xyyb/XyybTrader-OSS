#pragma once

#include "xt_strategy_engine/IStrategyTelemetrySink.h"

namespace xt_live::common {

/// @brief live strategy telemetry 到 XtLiveRecorder 的转发 sink。
///
/// XtLiveStrategyTelemetrySink 不持有 recorder；每次调用读取 `XtLiveRecorder::global()`，
/// recorder 已关闭时返回 noop/false。对象无状态，可被多个 strategy context 共享。
class XtLiveStrategyTelemetrySink final : public xt_strategy_engine::IStrategyTelemetrySink {
public:
    void on_metric(uint64_t ts_local_ns, const char* name, double val,
                   const char* strategy_id, const char* book_id) override;
    [[nodiscard]] bool intent_trace_enabled() const override;
    [[nodiscard]] bool should_sample_intent_trace(uint64_t trace_id) const override;
    void emit_intent_trace(const xt_strategy_engine::StrategyIntentTrace& t) override;
};

/// @brief 进程级共享 sink 实例（function-local static，immortal）。
///
/// 无状态 + 生命周期到进程退出 → ctx 持有的指针永不悬空，runner 状态结构
/// 无需关心声明/析构顺序。
XtLiveStrategyTelemetrySink& live_strategy_telemetry_sink();

}  // namespace xt_live::common
