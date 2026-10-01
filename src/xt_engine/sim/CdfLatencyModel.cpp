#include "CdfLatencyModel.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cmath>
#include <stdexcept>
#include <utility>

#include <yaml-cpp/yaml.h>

namespace {

// golden-ratio LDS 常量；固定值保证不同平台不依赖 RNG 实现。
constexpr double kGoldenRatio = 0.6180339887498949;  // (sqrt(5) - 1) / 2

void validate_and_sort_cdf(std::vector<CdfLatencyModel::Point>& cdf) {
    if (cdf.empty()) {
        throw std::runtime_error("CdfLatencyModel: cdf 为空");
    }
    for (const auto& pt : cdf) {
        if (pt.percentile < 0.0 || pt.percentile > 100.0) {
            throw std::runtime_error(
                "CdfLatencyModel: percentile 越界 [0,100]: " +
                std::to_string(pt.percentile));
        }
    }
    std::sort(cdf.begin(), cdf.end(),
              [](const auto& a, const auto& b) { return a.percentile < b.percentile; });
    // 插值只要求 percentile 有序；latency 值保留输入分布中的局部波动。
}

} // namespace

CdfLatencyModel::CdfLatencyModel(const std::string& yaml_path) {
    YAML::Node root;
    try {
        root = YAML::LoadFile(yaml_path);
    } catch (const YAML::Exception& e) {
        throw std::runtime_error(
            "CdfLatencyModel: 无法加载 YAML: " + yaml_path + " — " + e.what());
    }
    if (!root || !root.IsMap()) {
        throw std::runtime_error("CdfLatencyModel: YAML root 必须为 map: " + yaml_path);
    }
    const YAML::Node cdf_node = root["cdf"];
    if (!cdf_node || !cdf_node.IsSequence()) {
        throw std::runtime_error(
            "CdfLatencyModel: YAML 缺 'cdf' 列表节点: " + yaml_path);
    }
    cdf_.reserve(cdf_node.size());
    for (size_t i = 0; i < cdf_node.size(); ++i) {
        const YAML::Node row = cdf_node[i];
        if (!row || !row.IsMap()) {
            throw std::runtime_error(
                "CdfLatencyModel: cdf[" + std::to_string(i) + "] 必须为 map: " + yaml_path);
        }
        Point pt{};
        try {
            pt.percentile  = row["p"].as<double>();
            pt.entry_ns    = row["entry_ns"].as<int64_t>();
            pt.response_ns = row["response_ns"] ? row["response_ns"].as<int64_t>() : 0;
        } catch (const YAML::Exception& e) {
            throw std::runtime_error(
                "CdfLatencyModel: cdf[" + std::to_string(i) +
                "] 解析失败 (期望 {p, entry_ns, response_ns}): " + e.what());
        }
        cdf_.push_back(pt);
    }
    validate_and_sort_cdf(cdf_);
}

CdfLatencyModel::CdfLatencyModel(std::vector<Point> points, bool paired)
    : cdf_(std::move(points)), paired_(paired) {
    validate_and_sort_cdf(cdf_);
}

double CdfLatencyModel::next_percentile_(int64_t& counter) const noexcept {
    // golden-ratio LDS: counter * phi mod 1。counter 从 1 起步，避免首样本
    // 固定落到最小 percentile；序列无 RNG 状态，回测可复现。
    const double frac = std::fmod(static_cast<double>(counter) * kGoldenRatio, 1.0);
    ++counter;
    return frac * 100.0;
}

template <typename Sel>
int64_t CdfLatencyModel::lookup_percentile_(double p, Sel sel) const {
    // CDF 外侧 clamp 到首/末 percentile，不做外推。
    if (p <= cdf_.front().percentile) return sel(cdf_.front());
    if (p >= cdf_.back().percentile)  return sel(cdf_.back());

    auto it = std::lower_bound(
        cdf_.begin(), cdf_.end(), p,
        [](const Point& pt, double q) { return pt.percentile < q; });
    const auto hi_it = it;
    const auto lo_it = it - 1;
    const double t0 = lo_it->percentile;
    const double t1 = hi_it->percentile;
    const int64_t v0 = sel(*lo_it);
    const int64_t v1 = sel(*hi_it);
    if (t1 == t0) return v0;  // 重复 percentile 时避免除零。
    const double w = (p - t0) / (t1 - t0);
    return static_cast<int64_t>(v0 + (v1 - v0) * w);
}

int64_t CdfLatencyModel::entry_latency_ns(int64_t /*ts_local_ns*/) const {
    double p;
    if (paired_) {
        // paired 模式下 entry 推进抽样序列，并把 percentile 留给对应 response。
        p = next_percentile_(sample_counter_);
        last_percentile_ = p;
        has_pending_     = true;
    } else {
        // 非 paired 模式下 entry/response 各自独立推进抽样序列。
        p = next_percentile_(sample_counter_);
    }
    ++entry_calls_;
    return lookup_percentile_(p, [](const Point& pt) { return pt.entry_ns; });
}

int64_t CdfLatencyModel::response_latency_ns(int64_t /*ts_local_ns*/) const {
    double p;
    if (paired_ && has_pending_) {
        // 复用上一笔 entry 的 percentile，不推进抽样序列。
        p = last_percentile_;
        has_pending_ = false;
    } else {
        // 没有待复用 entry 时独立抽样，保持 response 查询可单独使用。
        p = next_percentile_(sample_counter_);
    }
    ++response_calls_;
    return lookup_percentile_(p, [](const Point& pt) { return pt.response_ns; });
}
