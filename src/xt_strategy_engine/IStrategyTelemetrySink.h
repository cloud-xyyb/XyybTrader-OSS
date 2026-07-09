#pragma once

#include <cstdint>

namespace xt_strategy_engine {

/// @brief 策略侧 intent trace 参数（SDK 侧 POD，仅 ctx 生产的字段）。
///
/// @note 所有 `const char*` 字段仅在 emit_intent_trace 调用期内有效（指向
///       caller 栈上/成员缓冲），sink 实现必须同步拷贝，不得持有指针。
struct StrategyIntentTrace {
    uint64_t    trace_id           = 0;
    const char* stage              = nullptr;
    const char* strategy_id        = nullptr;
    const char* book_id            = nullptr;
    const char* code_raw           = nullptr;
    const char* code_effective     = nullptr;
    const char* dir                = nullptr;
    int64_t     price_raw_fp       = 0;
    int64_t     price_effective_fp = 0;
    int64_t     qty_raw_fp         = 0;
    int64_t     qty_effective_fp   = 0;
    const char* tif_raw            = nullptr;
    const char* tif_effective      = nullptr;
    const char* offset_raw         = nullptr;
    const char* offset_effective   = nullptr;
    bool        reduce_only        = false;
    const char* exec_flags         = nullptr;
    uint64_t    trigger_tick_ts    = 0;
    uint64_t    decision_mono_ns   = 0;
    uint64_t    event_mono_ns      = 0;
    int64_t     best_bid_fp        = 0;
    int64_t     best_ask_fp        = 0;
    const char* reject_scope       = nullptr;
    const char* reject_code        = nullptr;
    const char* reject_reason      = nullptr;
    uint64_t    user_tag_hash      = 0;
};

/// @brief 策略上下文的 telemetry 输出接口。
///
/// live runner 向 live 注入 IStrategyTelemetrySink
/// IStrategyTelemetrySink 接口让 xt_strategy_engine 不直接依赖 xt_live / XtLiveRecorder。避免反向依赖
class IStrategyTelemetrySink {
public:
    virtual ~IStrategyTelemetrySink() = default;

    virtual void on_metric(uint64_t ts_local_ns, const char* name, double val,
                           const char* strategy_id, const char* book_id) = 0;

    [[nodiscard]] virtual bool intent_trace_enabled() const = 0;

    [[nodiscard]] virtual bool should_sample_intent_trace(uint64_t trace_id) const = 0;

    virtual void emit_intent_trace(const StrategyIntentTrace& t) = 0;
};

}
