#include "XtReplayer.h"
#include "xt_core/market/XtRawTick.h"
#include "xt_core/market/XtTickConvert.h"
#include "XtLogger.h"
#include "xt_backtest/data/source/XtMarketDataSource.h"
#include "xt_backtest/util/TickCsvUtil.h"
#include "xt_util/time/XtTime.h"

#include <yaml-cpp/yaml.h>

#include <cstdint>
#include <thread>
#include <chrono>
#include <limits>

XtReplayer::Config parse_replayer_config(const YAML::Node& backtest_node) {
    XtReplayer::Config cfg;
    if (backtest_node && backtest_node["replay_sleep_ms"]) {
        cfg.sleep_ms = backtest_node["replay_sleep_ms"].as<uint32_t>();
    }
    if (backtest_node && backtest_node["clock_resolution_ns"]) {
        cfg.clock_resolution_ns = backtest_node["clock_resolution_ns"].as<int64_t>();
    }
    return cfg;
}

void XtReplayer::Config::validate(std::vector<std::string>& errors) const {
    if (sleep_ms > 600000U) {
        errors.push_back("backtest.replay_sleep_ms 过大（建议 <= 600000ms）");
    }
    if (clock_resolution_ns < 0) {
        errors.push_back("backtest.clock_resolution_ns 不能为负数");
    }
}

void XtReplayer::set_config(const Config& cfg) {
    sleep_ms_ = cfg.sleep_ms;
    clock_resolution_ns_ = cfg.clock_resolution_ns;
}

void XtReplayer::set_sleep_ms(uint32_t sleep_ms) {
    sleep_ms_ = sleep_ms;
}

namespace {
inline int64_t align_up(int64_t v, int64_t grid) noexcept {
    return ((v + grid - 1) / grid) * grid;
}

int64_t sort_key_to_ns(uint64_t key) noexcept {
    if (key == 0) return 0;
    const uint32_t trading_day = static_cast<uint32_t>(key / kActionTimeDaySpan);
    const uint64_t shifted     = key % kActionTimeDaySpan;
    const uint32_t action_time = static_cast<uint32_t>(
        (shifted + kActionTimeDaySpan - kDefaultNightShiftMs) % kActionTimeDaySpan);
    return xt_time::to_ns(trading_day, action_time);
}
}

size_t XtReplayer::run(XtMarketDataSource& source,
                       IReplayRuntime& runtime,
                       IReplayReporter& reporter) {
    source.reset();
    XtLogger::info_m("BT", "replayer start, io={} mode={} clock_res_ns={}",
                     source.streaming() ? "stream" : "memory",
                     source.mode() == XtReplayMode::EventStream ? "event_stream" : "tick",
                     clock_resolution_ns_);
    size_t  tick_count    = 0;
    int64_t last_clock_ns = std::numeric_limits<int64_t>::min();

    while (true) {

        uint64_t next_key = 0;
        if (!source.peek_next_key(next_key)) break;

        if (clock_resolution_ns_ > 0
            && last_clock_ns != std::numeric_limits<int64_t>::min()) {
            const int64_t next_ns = sort_key_to_ns(next_key);
            int64_t next_clock_ns = align_up(last_clock_ns + 1, clock_resolution_ns_);
            while (next_clock_ns < next_ns) {
                uint32_t d = 0, t = 0;
                xt_time::from_ns(next_clock_ns, d, t);
                runtime.on_clock(d, t);
                last_clock_ns = next_clock_ns;
                next_clock_ns += clock_resolution_ns_;
            }
        }

        XtReplayEvent ev;
        if (!source.next_event(ev)) break;
        if (ev.type == XtReplayEventType::Tick) {

            const int64_t ts = xt_time::packed_ts(ev.tick.action_date, ev.tick.action_time);
            reporter.on_tick_pre(ts, ev.tick);
            runtime.on_tick(xt_tick_convert::to_core(ev.tick));
            reporter.on_tick_post(ts, runtime.active_orders());
            ++tick_count;
        } else if (ev.type == XtReplayEventType::OrderQueue) {
            runtime.on_order_queue(ev.order_queue);
        } else if (ev.type == XtReplayEventType::OrderDetail) {
            runtime.on_order_detail(ev.order_detail);
        } else if (ev.type == XtReplayEventType::Transaction) {
            runtime.on_transaction(ev.transaction);
        }

        if (clock_resolution_ns_ > 0) {
            last_clock_ns = sort_key_to_ns(next_key);
        }

        if (sleep_ms_ > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms_));
        }
    }

    XtLogger::debug_m("BT", "replayer done, ticks={}", tick_count);
    return tick_count;
}
