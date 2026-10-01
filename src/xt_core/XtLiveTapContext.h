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

/// @brief 记录当前正在处理的 tick 时间戳（供录制 tap 标注触发 tick）。
/// @note  EventLoop::process_ticks 每 tick 前调用；其他路径可显式置 0 避免误伤。
void     set_current_tick_ts_ns(uint64_t ts) noexcept;

/// @brief 读取当前 tick 时间戳；未设置时返回 0。
uint64_t current_tick_ts_ns() noexcept;

/// @brief 设置下一次 order 的决策原因字符串。
/// @param reason  null 或空串等价于清除。超过 47 字节（内部缓冲 48 字节）将被截断。
void set_next_order_reason(const char* reason) noexcept;

/// @brief 读取 reason 到调用方缓冲区，**不清除**（供多切片共用同一 reason）。
/// @note  out/cap 非法时 no-op；未设置时写入空串。
void peek_next_order_reason(char* out, size_t cap) noexcept;

/// @brief 清除 reason TLS，避免下一次 submit 误用上一次设置的值。
/// @note  buy_fp/sell_fp 返回前自动调用，策略层通常不需要手动调用。
void clear_next_order_reason() noexcept;

/// @brief 读取当前 intent trace id；未进入 traced submit 时返回 0。
uint64_t current_intent_trace_id() noexcept;

// ── event clock anchor（进程共享，非 TLS）───────────────────────
// worker 在 tick 派发前用行情 event 时刻 re-anchor；panel/watchdog 等非
// worker 线程的 submit 也能拿最近 anchor 投影（TLS 无法服务跨线程，故用
// 单 atomic offset，release/acquire）。BestEffort observability：任何状态
// 不得参与 order admission。

/// @brief 发布 anchor：offset = event_ns − steady_now_ns（release-store）。
/// @note offset 不可表示（__int128 越 int64）时保持旧 anchor 不动。
void publish_event_clock_anchor(int64_t event_ns, int64_t steady_now_ns) noexcept;

/// @brief 投影当前 event 时刻：anchor 有效写 out 并返 true；无效返 false。
bool project_event_ts_ns(int64_t steady_now_ns, int64_t& out_event_ts_ns) noexcept;

/// @brief 清除 anchor（回 invalid sentinel）；测试 seam / 显式断连场景用。
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

}  // namespace xt_core::tap
