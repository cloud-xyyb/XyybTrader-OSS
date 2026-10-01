#include "xt_core/XtLiveTapContext.h"

#include "xt_util/string/XtCStringUtil.h"

#include <atomic>
#include <climits>

namespace xt_core::tap {

namespace {

constexpr size_t kReasonCap = 48;

thread_local uint64_t tls_current_tick_ts_ns = 0;
thread_local char     tls_reason[kReasonCap] = {};
thread_local uint64_t tls_intent_trace_id = 0;

// 进程共享 event clock anchor；INT64_MIN = invalid sentinel（真实 offset 碰巧
// 等于 sentinel 视为 invalid 而非错误，影响面仅 observability validity）。
std::atomic<int64_t> g_event_minus_steady_ns{INT64_MIN};

}  // namespace

void set_current_tick_ts_ns(uint64_t ts) noexcept {
    tls_current_tick_ts_ns = ts;
}

uint64_t current_tick_ts_ns() noexcept {
    return tls_current_tick_ts_ns;
}

void set_next_order_reason(const char* reason) noexcept {
    XtCStringUtil::copy(tls_reason, kReasonCap, reason);
}

void peek_next_order_reason(char* out, size_t cap) noexcept {
    XtCStringUtil::copy(out, cap, tls_reason);
}

void clear_next_order_reason() noexcept {
    tls_reason[0] = '\0';
}

uint64_t current_intent_trace_id() noexcept {
    return tls_intent_trace_id;
}

void publish_event_clock_anchor(int64_t event_ns, int64_t steady_now_ns) noexcept {
    const __int128 off = static_cast<__int128>(event_ns) - steady_now_ns;
    if (off > INT64_MAX || off <= INT64_MIN) return;  // 不可表示 → 保持旧 anchor
    g_event_minus_steady_ns.store(static_cast<int64_t>(off), std::memory_order_release);
}

bool project_event_ts_ns(int64_t steady_now_ns, int64_t& out_event_ts_ns) noexcept {
    const int64_t off = g_event_minus_steady_ns.load(std::memory_order_acquire);
    if (off == INT64_MIN) return false;
    const __int128 v = static_cast<__int128>(steady_now_ns) + off;
    if (v > INT64_MAX || v < 0) return false;  // 投影越域 → 本次 invalid
    out_event_ts_ns = static_cast<int64_t>(v);
    return true;
}

void clear_event_clock_anchor() noexcept {
    g_event_minus_steady_ns.store(INT64_MIN, std::memory_order_release);
}

ScopedIntentTraceId::ScopedIntentTraceId(uint64_t trace_id) noexcept
    : prior_(tls_intent_trace_id) {
    tls_intent_trace_id = trace_id;
}

ScopedIntentTraceId::~ScopedIntentTraceId() {
    tls_intent_trace_id = prior_;
}

}  // namespace xt_core::tap
