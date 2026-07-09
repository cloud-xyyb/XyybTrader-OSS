#pragma once

#include <cstddef>
#include <cstdint>

/// @file XtLiveTapContext.h
/// @brief 实盘录制 tap 点的 thread-local context。
///
/// 策略侧在 submit 前写入触发 tick 与决策原因，gateway 侧录制 tap 读取。
/// 上下文仅在当前线程、当前同步调用链内有效，跨线程调度不继承；
/// 未设置时读取返回 0/空串，回测与单元测试路径可按 noop 使用。

namespace xt_core::tap {

void     set_current_tick_ts_ns(uint64_t ts) noexcept;

uint64_t current_tick_ts_ns() noexcept;

void set_next_order_reason(const char* reason) noexcept;

void peek_next_order_reason(char* out, size_t cap) noexcept;

void clear_next_order_reason() noexcept;

uint64_t current_intent_trace_id() noexcept;

/// @brief 在当前同步调用链内临时设置 intent trace id，析构时恢复旧值。
/// @note 只维护 trace id TLS，不读写 tick ts / reason TLS；支持嵌套 scope。
class ScopedIntentTraceId {
public:
    explicit ScopedIntentTraceId(uint64_t trace_id) noexcept;
    ~ScopedIntentTraceId();

    ScopedIntentTraceId(const ScopedIntentTraceId&) = delete;
    ScopedIntentTraceId& operator=(const ScopedIntentTraceId&) = delete;

private:
    uint64_t prior_ = 0;
};

}
