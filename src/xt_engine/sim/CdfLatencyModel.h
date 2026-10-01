#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "ILatencyModel.h"

/// @brief 从 CDF 分位点抽样 HFT 回测 entry/response 延迟。
///
/// YAML 构造函数读取 `cdf: [{p, entry_ns, response_ns}, ...]`；
/// 点集构造函数供测试和程序化装配使用。
/// 模型不依赖 `ts_local_ns`，可用于任意历史 tick 窗口。
///
/// 默认 paired 模式让 entry/response 复用同一 percentile，以保留双向延迟相关性。
/// 仅在 HFT 回测主线程内调用。
class CdfLatencyModel final : public ILatencyModel {
public:
    /// @brief CDF 数据点；所在集合构造后按 percentile 升序保存。
    struct Point {
        double  percentile;  ///< 分位点，[0, 100]。
        int64_t entry_ns;    ///< submit -> 可撮合延迟，单位 ns。
        int64_t response_ns; ///< 成交 -> 策略可见延迟，单位 ns；YAML 缺省为 0。
    };

    /// @param yaml_path CDF YAML 路径；缺失、格式错误或 cdf 为空时抛 std::runtime_error。
    explicit CdfLatencyModel(const std::string& yaml_path);

    /// @brief 直接用 CDF 点构造，供测试和程序化装配使用。
    /// @param points CDF 点；可无序，空集合或 percentile 越界时抛 std::runtime_error。
    /// @param paired entry/response 是否复用同一 percentile；构造期固定。
    explicit CdfLatencyModel(std::vector<Point> points, bool paired = true);

    /// @brief 抽样 submit -> 可撮合延迟；忽略 `ts_local_ns`。
    int64_t entry_latency_ns(int64_t ts_local_ns) const override;

    /// @brief 抽样成交 -> 策略可见延迟；忽略 `ts_local_ns`。
    /// @note paired 模式优先复用上一笔 entry percentile。
    int64_t response_latency_ns(int64_t ts_local_ns) const override;

    /// @brief CDF 点数。
    size_t size() const noexcept { return cdf_.size(); }

    /// @brief entry/response API 调用次数；不等同于 LDS counter 推进次数。
    int64_t sample_count() const noexcept { return entry_calls_ + response_calls_; }

    /// @brief entry/response 是否复用同一 percentile（构造期固定）。
    bool paired() const noexcept { return paired_; }

private:
    /// 在 cdf_ 上按 percentile p ∈ [0, 100] 线性插值。
    template <typename Sel>
    int64_t lookup_percentile_(double p, Sel sel) const;

    /// 下一个抽样 percentile；只依赖 sample_counter_，不依赖 RNG 状态。
    double next_percentile_(int64_t& counter) const noexcept;

    std::vector<Point> cdf_;  ///< 按 percentile 升序保存，查询时边界 clamp。

    // API 调用计数仅供测试观测；不参与 LDS 抽样。
    mutable int64_t entry_calls_ = 0;
    mutable int64_t response_calls_ = 0;

    // paired 抽样状态；const 查询函数在 HFT 回测主线程内更新。
    bool   paired_ = true;
    mutable int64_t sample_counter_  = 1;    ///< LDS counter；1 避免首样本固定落到最小 percentile
    mutable double  last_percentile_ = 0.0;  ///< 上一次 entry 缓存的 percentile
    mutable bool    has_pending_     = false;  ///< true 表示 response 可复用 last_percentile_
};
