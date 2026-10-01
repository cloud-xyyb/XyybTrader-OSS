#include "XtReplayer.h"
#include "xt_core/market/XtRawTick.h"
#include "xt_core/market/XtTickConvert.h"
#include "XtLogger.h"
#include "xt_backtest/data/source/XtMarketDataSource.h"
#include "xt_util/time/XtTime.h"             // xt_time::to_ns / from_ns / packed_ts

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

// event-axis 真实时刻：sort key 只管序（夜盘旋转编码不可反解），时间一律
// 取自事件 payload 头；action_date 缺失时 fallback trading_date（preopen/合成源）。
inline int64_t header_event_ns(const XtReplayEventHeader& h) noexcept {
    const uint32_t d = (h.action_date != 0) ? h.action_date : h.trading_date;
    if (d == 0 && h.action_time == 0) return 0;
    return xt_time::to_ns(d, h.action_time);
}
}  // namespace

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
        // ── 阶段 1: peek 下一个事件的时间头（non-consuming） ────────────
        XtReplayEventHeader next_hdr;
        if (!source.peek_next_event_header(next_hdr)) break;
        const int64_t next_ns = header_event_ns(next_hdr);

        // ── 阶段 2: 在 next_ns 之前按绝对网格注入 on_clock（event-axis） ───
        //   - clock_resolution_ns_ == 0 时整段跳过（向后兼容）
        //   - 严格 next_clock_ns < next_ns（不允许 ==）
        //   - align_up(last_clock_ns + 1, res) 保证绝对网格对齐
        //   - last_clock_ns == INT64_MIN 时跳过（第一个事件前不注入）
        //   - trading_date 取即将到来事件的交易日（仅供 report/session 戳）
        if (clock_resolution_ns_ > 0
            && last_clock_ns != std::numeric_limits<int64_t>::min()) {
            int64_t next_clock_ns = align_up(last_clock_ns + 1, clock_resolution_ns_);
            while (next_clock_ns < next_ns) {
                uint32_t d = 0, t = 0;
                xt_time::from_ns(next_clock_ns, d, t);
                runtime.on_clock(d, t, next_hdr.trading_date);
                last_clock_ns = next_clock_ns;
                next_clock_ns += clock_resolution_ns_;
            }
        }

        // ── 阶段 3: advance 并分发事件 ────────────────────────────
        XtReplayEvent ev;
        if (!source.next_event(ev)) break;  // 理论上 peek 成功后不应发生
        if (ev.type == XtReplayEventType::Tick) {
            // 这里要用 reporter/event stream 约定的 packed timestamp，因此需要进行转换。
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

        // 更新 last_clock_ns 到当前事件的 event ns（仅 clock 模式启用时需要）
        if (clock_resolution_ns_ > 0) {
            last_clock_ns = next_ns;
        }

        if (sleep_ms_ > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms_));
        }
    }

    XtLogger::debug_m("BT", "replayer done, ticks={}", tick_count);
    return tick_count;
}
