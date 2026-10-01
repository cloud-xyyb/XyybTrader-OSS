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
    std::string metric;   ///< 指标名；由 string_view key 转成稳定输出字符串。
    uint64_t count = 0;   ///< 本区间 observe 次数。
    double sum = 0.0;     ///< 本区间样本和，供均值或累计值计算。
    double min = 0.0;     ///< 本区间最小样本值。
    double max = 0.0;     ///< 本区间最大样本值。
    double last = 0.0;    ///< 本区间最后一次 observe 的值。
};

/// @brief drain_snapshot() 返回的策略指标区间快照。
struct XtStrategyMetricsSnapshot {
    uint64_t tick_count = 0; ///< 上次 drain 以来的策略 tick 数。
    std::vector<XtStrategyMetricAggregate> metrics; ///< count > 0 的输出行。
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
    /// @brief 预注册策略自定义指标名。
    ///
    /// 为 default map 和两个 live buffer 同时预留 bucket 并插入空状态，
    /// 避免 live 热路径首次 observe 时触发 unordered_map insert / rehash。
    ///
    /// @param names 指标名字面量集合；nullptr 和空字符串会被忽略。
    /// @pre 应在策略初始化阶段调用，且不能与 observe()/drain_snapshot() 并发。
    /// @warning name 指针生命周期必须覆盖聚合器；不要传临时 std::string.c_str()。
    void reserve_metrics(std::initializer_list<const char*> names) {
        // 初始化阶段由调用方保证单线程，无需 mutex。
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

    /// @brief 启用 live 单生产者双缓冲模式。
    ///
    /// 调用后 observe() 写入 active live buffer，drain_snapshot() 翻转 buffer 后
    /// 等待在途 writer 完成，再读取并清零旧 buffer。
    ///
    /// @pre 必须在任何 observe()/drain_snapshot() 前、初始化阶段调用。
    /// @pre reserve_metrics() 如需使用，应在本方法之前完成。
    /// @warning live_mode_ 不是 atomic；本接口不支持运行期热切换。
    void enable_live_mode() noexcept {
        live_mode_ = true;
    }

    /// @brief 记录一个策略 tick。
    ///
    /// 只更新区间 tick_count；不访问指标 map，也不为其他字段建立同步关系。
    /// drain_snapshot() 会把计数 exchange 回 0。
    void on_tick() {
        tick_count_.fetch_add(1, std::memory_order_relaxed);
    }

    /// @brief 记录一次策略自定义指标观测值。
    ///
    /// default 模式由调用方保证单线程；live 模式只支持一个 strategy worker 写入。
    /// 未预注册指标仍会插入 map，但 live 热路径应通过 reserve_metrics() 预热。
    ///
    /// @param metric 指标名，必须指向字面量或其他长生命周期存储。
    /// @param value  本次观测样本值，聚合为 count/sum/min/max/last。
    /// @note metric 为 nullptr 或空字符串时直接忽略。
    /// @pre metric 指向的内存必须至少活到聚合器销毁。
    void observe(const char* metric, double value) {
        if (!metric || metric[0] == '\0') return;
        const std::string_view sv(metric);
        if (live_mode_) {
            observe_live(sv, value);
        } else {
            // 默认路径用于回测 / CTA 等单线程场景；并发约束由调用方保证。
            observe_impl(metrics_, sv, value);
        }
    }

    /// @brief 取出并重置当前区间指标。
    ///
    /// 返回值只包含本次 drain 前 count > 0 的指标行；已预注册但本区间未 observe
    /// 的指标留在内部 map 中，下一轮仍可复用 bucket。
    ///
    /// @return 上一轮 drain 以来的 tick_count 与聚合指标。
    /// @note live 模式下 observe() 调用方不等待 drain；
    ///       drain 线程只在冷路径短暂等待在途 writer。
    /// @pre default 模式不得与 observe() 并发；live 模式只支持单生产者。
    XtStrategyMetricsSnapshot drain_snapshot() {
        if (live_mode_) return drain_snapshot_live();
        // 单线程 drain 由调用方保证不与 observe() 并发。
        const uint64_t ticks = tick_count_.exchange(0, std::memory_order_relaxed);
        return build_and_reset_snapshot(metrics_, ticks);
    }

private:
    /// @brief 单个指标的运行时累计状态。
    struct MetricState {
        uint64_t count = 0;                                       ///< 当前 drain 区间样本数。
        double sum     = 0.0;                                     ///< 当前 drain 区间样本和。
        double min     = std::numeric_limits<double>::infinity();  ///< count==0 时的空状态哨兵。
        double max     = -std::numeric_limits<double>::infinity(); ///< count==0 时的空状态哨兵。
        double last    = 0.0;                                     ///< 最近一次 observe 值。
    };

    using MetricMap = std::unordered_map<std::string_view, MetricState>;

    // live 模式是单生产者 + reporter drain 的双缓冲协议。
    // writer 在读取 active_buf_ 前先进入全局 wseq_ critical section；drain
    // 先切换 active buffer，再等待已进入 observe_live() 的 writer 退出，之后
    // 才 reset old buffer。不要改成“读 active_buf_ 后再标记 per-buffer seq”：
    // 读 idx 与 drain 翻转之间会留下未标记旧 buffer 的窗口。

    /// @brief live 双缓冲中的单个缓冲区。
    struct LiveBuffer {
        MetricMap metrics; ///< 本 buffer 持有一轮 live observe 聚合状态。
    };

    // ── live 热路径：strategy worker observe ─────────────────────────────
    /// @brief live 模式下的单生产者写入路径。
    ///
    /// wseq_ 奇偶位只保护 buffer flip 与 reset，不保护多生产者同时写同一 map。
    /// 调用方必须保证同一聚合器只有一个 strategy worker 调 observe()。
    void observe_live(std::string_view metric, double value) {
        // seq_cst 标记先于 active_buf_ 读取，保证 drain 能看到在途 writer。
        wseq_.fetch_add(1u, std::memory_order_seq_cst);
        const int idx = active_buf_.load(std::memory_order_acquire);
        observe_impl(live_bufs_[idx].metrics, metric, value);
        wseq_.fetch_add(1u, std::memory_order_release);
    }

    // ── live 冷路径：reporter drain ───────────────────────────────────────
    /// @brief live 模式下翻转 active buffer 并 drain 旧 buffer。
    ///
    /// 该函数只等待已经进入 observe_live() 的 writer；翻转之后进入的新 writer
    /// 会写入新的 active buffer，不阻塞在 reporter drain 上。
    XtStrategyMetricsSnapshot drain_snapshot_live() {
        // seq_cst 翻转与 writer 的 seq_cst 标记形成全序，避免 drain 到错误缓冲区。
        const int old_idx = active_buf_.fetch_xor(1, std::memory_order_seq_cst);
        while (wseq_.load(std::memory_order_acquire) & 1u) {}
        const uint64_t ticks = tick_count_.exchange(0, std::memory_order_relaxed);
        return build_and_reset_snapshot(live_bufs_[old_idx].metrics, ticks);
    }

    // ── 共享工具函数 ─────────────────────────────────────────────────────────
    /// @brief 向指定 map 累加一条样本。
    ///
    /// 不做同步，也不复制 metric key；调用方负责选择正确 buffer 并保证 key 生命周期。
    static void observe_impl(MetricMap& m, std::string_view metric, double value) {
        // string_view key 不复制 metric 名称；生命周期契约由 public observe() 限定。
        auto it = m.find(metric);
        if (it == m.end()) {
            // 未预注册指标允许插入；live 热路径应通过 reserve_metrics 避免 rehash。
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

    /// @brief 将 count > 0 的指标转换为输出快照，并清零 map 中所有状态。
    ///
    /// 保留 map bucket 与已注册 key，避免下一轮 observe 重新分配。
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

    /// @brief 清零每个已注册指标的区间状态，保留 key 与 bucket 分配。
    static void reset_map(MetricMap& m) {
        for (auto& kv : m) kv.second = MetricState{};
    }

    // ── 数据成员 ─────────────────────────────────────────────────────────────
    std::atomic<uint64_t> tick_count_{0}; ///< observe/drain 共享的区间 tick 计数。

    // 默认路径：非 live 模式专用，调用方保证 observe/drain 单线程。
    MetricMap metrics_;

    // live 路径：enable_live_mode() 后使用；live_mode_ 初始化后不再跨线程修改。
    bool                 live_mode_ = false; ///< 初始化期模式开关；不支持运行期并发写。
    LiveBuffer           live_bufs_[2];      ///< 双缓冲：一个 active 给 writer，一个 old 给 drain。
    std::atomic<int>     active_buf_{0};     ///< 当前 writer 应写入的 live_bufs_ 下标。
    std::atomic<uint32_t> wseq_{0};          ///< 全局 writer epoch；奇数表示 observe_live() 在途。
};

}  // namespace ct::metrics
