#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "recording/XtLiveRecorder.h"

namespace {

uint64_t arg_u64(int argc, char** argv, int idx, uint64_t fallback) {
    if (idx >= argc) return fallback;
    char* end = nullptr;
    const unsigned long long v = std::strtoull(argv[idx], &end, 10);
    if (end == argv[idx] || (end && *end != '\0')) return fallback;
    return static_cast<uint64_t>(v);
}

uint64_t now_ns() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
}

uint64_t percentile_ns(const std::vector<uint64_t>& samples, double pct) {
    if (samples.empty()) return 0;
    const double rank = pct * static_cast<double>(samples.size() - 1);
    return samples[static_cast<size_t>(rank)];
}

}

int main(int argc, char** argv) {
    const uint64_t samples = arg_u64(argc, argv, 1, 100000);
    const uint64_t warmup = arg_u64(argc, argv, 2, 10000);

    XtLiveRecorder::Config cfg;
    cfg.enabled = true;
    cfg.file_path = "/dev/null";
    cfg.queue_capacity = static_cast<size_t>(samples + warmup + 1024);

    XtLiveRecorder rec(cfg);
    rec.start();

    XtLiveRecorder::TradeRtnParams p;
    p.ts_local_ns = now_ns();
    p.ts_exch_ns = p.ts_local_ns;
    p.trade_action_date = 20260521;
    p.trade_action_time = 93000000;
    p.trade_id = 1;
    p.logical_id = 100;
    p.code = "BTCUSDT";
    p.exchange_id = "BINANCE";
    p.side = "buy";
    p.price_fp = 70000LL * 1000000LL;
    p.qty = 1000;
    p.strat_id = "bench_hft";
    p.book_id = "bench_hft";
    p.account_id = "_default";
    p.route_status = "routed";
    p.trading_day = 20260521;

    for (uint64_t i = 0; i < warmup; ++i) {
        p.trade_id = i + 1;
        rec.emit_trade_rtn(p);
    }

    std::vector<uint64_t> lat_ns;
    lat_ns.reserve(static_cast<size_t>(samples));
    for (uint64_t i = 0; i < samples; ++i) {
        p.trade_id = warmup + i + 1;
        p.ts_local_ns = now_ns();
        const uint64_t t0 = now_ns();
        rec.emit_trade_rtn(p);
        const uint64_t t1 = now_ns();
        lat_ns.push_back(t1 - t0);
    }

    const uint64_t expected = samples + warmup;
    for (int i = 0; i < 200 && rec.written_count() < expected && rec.drop_count() == 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    rec.stop();

    std::sort(lat_ns.begin(), lat_ns.end());
    const uint64_t p50 = percentile_ns(lat_ns, 0.50);
    const uint64_t p99 = percentile_ns(lat_ns, 0.99);
    const uint64_t p999 = percentile_ns(lat_ns, 0.999);
    const uint64_t max = lat_ns.empty() ? 0 : lat_ns.back();

    std::printf("samples=%llu\n", static_cast<unsigned long long>(samples));
    std::printf("warmup=%llu\n", static_cast<unsigned long long>(warmup));
    std::printf("event_size_bytes=%zu\n", sizeof(XtLiveRecorder::Event));
    std::printf("emit_count=%llu\n", static_cast<unsigned long long>(rec.emit_count()));
    std::printf("drop_count=%llu\n", static_cast<unsigned long long>(rec.drop_count()));
    std::printf("written_count=%llu\n", static_cast<unsigned long long>(rec.written_count()));
    std::printf("emit_trade_rtn_enqueue_p50_ns=%llu\n", static_cast<unsigned long long>(p50));
    std::printf("emit_trade_rtn_enqueue_p99_ns=%llu\n", static_cast<unsigned long long>(p99));
    std::printf("emit_trade_rtn_enqueue_p999_ns=%llu\n", static_cast<unsigned long long>(p999));
    std::printf("emit_trade_rtn_enqueue_max_ns=%llu\n", static_cast<unsigned long long>(max));
    std::printf("emit_trade_rtn_enqueue_p99_us=%.3f\n", static_cast<double>(p99) / 1000.0);
    return rec.drop_count() == 0 ? 0 : 2;
}
