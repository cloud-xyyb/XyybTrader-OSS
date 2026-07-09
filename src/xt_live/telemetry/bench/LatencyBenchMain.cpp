#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "latency/XtLiveLatencyTracker.h"
#include "xt_util/time/XtSteadyClock.h"

namespace {

uint64_t arg_u64(int argc, char** argv, int idx, uint64_t fallback) {
    if (idx >= argc) return fallback;
    char* end = nullptr;
    const unsigned long long v = std::strtoull(argv[idx], &end, 10);
    if (end == argv[idx] || (end && *end != '\0')) return fallback;
    return static_cast<uint64_t>(v);
}

uint64_t steady_now_us() {
    return XtSteadyClock::steady_now_us();
}

}

int main(int argc, char** argv) {
    const uint64_t samples = arg_u64(argc, argv, 1, 200000);
    const uint64_t submit_wire_us = arg_u64(argc, argv, 2, 20);
    const uint64_t ack_after_submit_us = arg_u64(argc, argv, 3, 80);
    const uint64_t fill_after_submit_us = arg_u64(argc, argv, 4, 180);
    const uint64_t stale_ttl_us = arg_u64(argc, argv, 5, 86400000000ULL);
    const uint64_t sample_step_us = arg_u64(argc, argv, 6, 10);

    XtLiveLatencyTracker tracker(XtLiveLatencyTracker::Config{true, stale_ttl_us});
    const uint64_t base_us = steady_now_us();
    for (uint64_t i = 0; i < samples; ++i) {
        const uint64_t order_id = i + 1;
        const uint64_t t0 = base_us + i * sample_step_us;
        const uint64_t t1 = t0 + submit_wire_us;
        const uint64_t t_ack = t1 + ack_after_submit_us;
        const uint64_t t_fill = t1 + fill_after_submit_us;
        const std::string trace = std::string("bench_") + std::to_string(order_id);

        tracker.on_submit_result(order_id, trace, t0, t1);

        XtOrderExecUpdate ack;
        ack.id = order_id;
        ack.status = XtOrderStatus::Accepted;
        ack.local_cb_us = t_ack;
        ack.local_strategy_us = t_ack + 3;
        tracker.on_order_update(ack);

        XtTradeRecord tr;
        tr.order_id = order_id;
        tr.trace_id = trace;
        tr.local_submit_begin_us = t0;
        tr.local_submit_end_us = t1;
        tr.local_cb_us = t_fill;
        tr.local_strategy_us = t_fill + 4;
        tracker.on_trade(tr);

        XtOrderExecUpdate done;
        done.id = order_id;
        done.status = XtOrderStatus::Filled;
        done.filled_fp = order_exec_to_fp(1.0);
        done.last_fill_fp = order_exec_to_fp(1.0);
        done.local_cb_us = t_fill;
        done.local_strategy_us = t_fill + 4;
        tracker.on_order_update(done);
    }

    const XtLiveLatencyTracker::Snapshot snap = tracker.snapshot();
    std::printf("samples=%llu\n", static_cast<unsigned long long>(samples));
    std::printf("stale_ttl_us=%llu\n", static_cast<unsigned long long>(stale_ttl_us));
    std::printf("sample_step_us=%llu\n", static_cast<unsigned long long>(sample_step_us));
    std::printf("submit_wire_p99_us=%llu\n", static_cast<unsigned long long>(snap.submit_wire.p99_us));
    std::printf("submit_wire_p999_us=%llu\n", static_cast<unsigned long long>(snap.submit_wire.p999_us));
    std::printf("submit_to_ack_p99_us=%llu\n", static_cast<unsigned long long>(snap.submit_to_ack.p99_us));
    std::printf("submit_to_ack_p999_us=%llu\n", static_cast<unsigned long long>(snap.submit_to_ack.p999_us));
    std::printf("submit_to_fill_p99_us=%llu\n", static_cast<unsigned long long>(snap.submit_to_fill.p99_us));
    std::printf("submit_to_fill_p999_us=%llu\n", static_cast<unsigned long long>(snap.submit_to_fill.p999_us));
    std::printf("ack_to_fill_p99_us=%llu\n", static_cast<unsigned long long>(snap.ack_to_fill.p99_us));
    std::printf("ack_to_fill_p999_us=%llu\n", static_cast<unsigned long long>(snap.ack_to_fill.p999_us));
    std::printf("cb_to_strategy_p99_us=%llu\n", static_cast<unsigned long long>(snap.cb_to_strategy.p99_us));
    const uint64_t ext_ack_p99_us = snap.submit_to_ack.p99_us > snap.submit_wire.p99_us
                                        ? (snap.submit_to_ack.p99_us - snap.submit_wire.p99_us)
                                        : 0;
    const uint64_t ext_fill_p99_us = snap.submit_to_fill.p99_us > snap.submit_wire.p99_us
                                         ? (snap.submit_to_fill.p99_us - snap.submit_wire.p99_us)
                                         : 0;
    std::printf("external_ack_p99_us=%llu\n", static_cast<unsigned long long>(ext_ack_p99_us));
    std::printf("external_fill_p99_us=%llu\n", static_cast<unsigned long long>(ext_fill_p99_us));
    std::printf("inflight_orders=%llu\n", static_cast<unsigned long long>(snap.inflight_orders));
    return 0;
}
