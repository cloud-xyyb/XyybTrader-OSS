#pragma once

#include <cstddef>
#include <cstdint>

/// @file XtLiveTapContext.h
/// @brief 下单归因与 event-clock 上下文，供录制 tap 与延迟观测读取。
///
/// 实盘录制想在订单记录里标上「哪条 tick 触发了这一单、当时决策原因是什么」，但这些信息
/// 产生在策略层、实际发单在下游 gateway 层，中间隔着好几层不便改签名。本文件用 thread-local
/// 让策略在 submit 前把触发 tick 时间戳、决策原因、trace id 挂到当前线程，gateway 的录制
/// tap 顺同一条同步调用链读回；TLS 只对当前线程当前调用链可见，不跨线程继承，未设置时读
/// 0/空串（回测与单测按 noop）。
///
/// event clock anchor 是进程共享的（非 TLS，单 atomic offset）：worker 用行情 event 时刻
/// re-anchor 后，panel/watchdog 等非 worker 线程也能投影出最近的 event 时刻。纯观测，
/// 任何状态都不参与下单准入。

namespace xt_core::tap {

void     set_current_tick_ts_ns(uint64_t ts) noexcept;

uint64_t current_tick_ts_ns() noexcept;

void set_next_order_reason(const char* reason) noexcept;

void peek_next_order_reason(char* out, size_t cap) noexcept;

void clear_next_order_reason() noexcept;

uint64_t current_intent_trace_id() noexcept;

void publish_event_clock_anchor(int64_t event_ns, int64_t steady_now_ns) noexcept;

bool project_event_ts_ns(int64_t steady_now_ns, int64_t& out_event_ts_ns) noexcept;

void clear_event_clock_anchor() noexcept;

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
