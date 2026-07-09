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
        double  percentile;
        int64_t entry_ns;
        int64_t response_ns;
    };

    explicit CdfLatencyModel(const std::string& yaml_path);

    explicit CdfLatencyModel(std::vector<Point> points, bool paired = true);

    int64_t entry_latency_ns(int64_t ts_local_ns) const override;

    int64_t response_latency_ns(int64_t ts_local_ns) const override;

    size_t size() const noexcept { return cdf_.size(); }

    int64_t sample_count() const noexcept { return entry_calls_ + response_calls_; }

    bool paired() const noexcept { return paired_; }

private:

    template <typename Sel>
    int64_t lookup_percentile_(double p, Sel sel) const;

    double next_percentile_(int64_t& counter) const noexcept;

    std::vector<Point> cdf_;

    mutable int64_t entry_calls_ = 0;
    mutable int64_t response_calls_ = 0;

    bool   paired_ = true;
    mutable int64_t sample_counter_  = 1;
    mutable double  last_percentile_ = 0.0;
    mutable bool    has_pending_     = false;
};
