#pragma once

#include <atomic>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ct::metrics {

/// @brief 单个策略自定义指标在一个 drain 区间内的聚合结果。
struct XtStrategyMetricAggregate {
    std::string metric;
    uint64_t count = 0;
    double sum = 0.0;
    double min = 0.0;
    double max = 0.0;
    double last = 0.0;
};

/// @brief drain_snapshot() 返回的策略指标区间快照。
struct XtStrategyMetricsSnapshot {
    uint64_t tick_count = 0;
    std::vector<XtStrategyMetricAggregate> metrics;
};

/// @brief 策略自定义指标的区间聚合器。
///
/// 策略在热路径上高频调用 report_metric 打点，逐条输出既昂贵也没有消费价值；
/// XtStrategyMetricsAggregator 把每个指标在区间内聚合成 count/sum/min/max/last，
/// 由 reporter/control 线程低频取走并清零，进入下一区间。
///
/// 默认模式单线程（回测）不加锁；live 模式是策略 worker 单写 + 低频 reporter
/// 取走的双缓冲。metric key 以 string_view 存储，调用方必须传字面量等长生命周期字符串。
class XtStrategyMetricsAggregator {
public:

    void reserve_metrics(std::initializer_list<const char*> names) {

        const size_t extra = names.size();
        metrics_.reserve(metrics_.size() + extra);
        for (auto& buf : live_bufs_)
            buf.metrics.reserve(buf.metrics.size() + extra);
        for (const char* name : names) {
            if (!name || !*name) continue;
            const std::string_view sv(name);
            metrics_.emplace(sv, MetricState{});
            for (auto& buf : live_bufs_) buf.metrics.emplace(sv, MetricState{});
        }
    }

    void enable_live_mode() noexcept {
        live_mode_ = true;
    }

    void on_tick() {
        tick_count_.fetch_add(1, std::memory_order_relaxed);
    }

    void observe(const char* metric, double value) {
        if (!metric || metric[0] == '\0') return;
        const std::string_view sv(metric);
        if (live_mode_) {
            observe_live(sv, value);
        } else {

            observe_impl(metrics_, sv, value);
        }
    }

    XtStrategyMetricsSnapshot drain_snapshot() {
        if (live_mode_) return drain_snapshot_live();

        const uint64_t ticks = tick_count_.exchange(0, std::memory_order_relaxed);
        return build_and_reset_snapshot(metrics_, ticks);
    }

private:
    /// @brief 单个指标的运行时累计状态。
    struct MetricState {
        uint64_t count = 0;
        double sum     = 0.0;
        double min     = std::numeric_limits<double>::infinity();
        double max     = -std::numeric_limits<double>::infinity();
        double last    = 0.0;
    };

    using MetricMap = std::unordered_map<std::string_view, MetricState>;

    /// @brief live 双缓冲中的单个缓冲区。
    struct LiveBuffer {
        MetricMap metrics;
    };

    void observe_live(std::string_view metric, double value) {

        wseq_.fetch_add(1u, std::memory_order_seq_cst);
        const int idx = active_buf_.load(std::memory_order_acquire);
        observe_impl(live_bufs_[idx].metrics, metric, value);
        wseq_.fetch_add(1u, std::memory_order_release);
    }

    XtStrategyMetricsSnapshot drain_snapshot_live() {

        const int old_idx = active_buf_.fetch_xor(1, std::memory_order_seq_cst);
        while (wseq_.load(std::memory_order_acquire) & 1u) {}
        const uint64_t ticks = tick_count_.exchange(0, std::memory_order_relaxed);
        return build_and_reset_snapshot(live_bufs_[old_idx].metrics, ticks);
    }

    static void observe_impl(MetricMap& m, std::string_view metric, double value) {

        auto it = m.find(metric);
        if (it == m.end()) {

            it = m.emplace(metric, MetricState{}).first;
        }
        MetricState& st = it->second;
        if (st.count == 0) {
            st.min = value;
            st.max = value;
        } else {
            if (value < st.min) st.min = value;
            if (value > st.max) st.max = value;
        }
        ++st.count;
        st.sum  += value;
        st.last  = value;
    }

    static XtStrategyMetricsSnapshot build_and_reset_snapshot(MetricMap& m, uint64_t ticks) {
        XtStrategyMetricsSnapshot out;
        out.tick_count = ticks;
        out.metrics.reserve(m.size());
        for (const auto& kv : m) {
            const MetricState& st = kv.second;
            if (st.count == 0) continue;
            XtStrategyMetricAggregate row;
            row.metric = std::string(kv.first);
            row.count = st.count;
            row.sum   = st.sum;
            row.min   = st.min;
            row.max   = st.max;
            row.last  = st.last;
            out.metrics.push_back(std::move(row));
        }
        reset_map(m);
        return out;
    }

    static void reset_map(MetricMap& m) {
        for (auto& kv : m) kv.second = MetricState{};
    }

    std::atomic<uint64_t> tick_count_{0};

    MetricMap metrics_;

    bool                 live_mode_ = false;
    LiveBuffer           live_bufs_[2];
    std::atomic<int>     active_buf_{0};
    std::atomic<uint32_t> wseq_{0};
};

}
