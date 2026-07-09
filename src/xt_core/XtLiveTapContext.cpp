#include "xt_core/XtLiveTapContext.h"

#include "xt_util/string/XtCStringUtil.h"

namespace xt_core::tap {

namespace {

constexpr size_t kReasonCap = 48;

thread_local uint64_t tls_current_tick_ts_ns = 0;
thread_local char     tls_reason[kReasonCap] = {};
thread_local uint64_t tls_intent_trace_id = 0;

}

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

ScopedIntentTraceId::ScopedIntentTraceId(uint64_t trace_id) noexcept
    : prior_(tls_intent_trace_id) {
    tls_intent_trace_id = trace_id;
}

ScopedIntentTraceId::~ScopedIntentTraceId() {
    tls_intent_trace_id = prior_;
}

}
