#include "CdfLatencyModel.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cmath>
#include <stdexcept>
#include <utility>

#include <yaml-cpp/yaml.h>

namespace {

constexpr double kGoldenRatio = 0.6180339887498949;

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

}

}

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

    const double frac = std::fmod(static_cast<double>(counter) * kGoldenRatio, 1.0);
    ++counter;
    return frac * 100.0;
}

template <typename Sel>
int64_t CdfLatencyModel::lookup_percentile_(double p, Sel sel) const {

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
    if (t1 == t0) return v0;
    const double w = (p - t0) / (t1 - t0);
    return static_cast<int64_t>(v0 + (v1 - v0) * w);
}

int64_t CdfLatencyModel::entry_latency_ns(int64_t  ) const {
    double p;
    if (paired_) {

        p = next_percentile_(sample_counter_);
        last_percentile_ = p;
        has_pending_     = true;
    } else {

        p = next_percentile_(sample_counter_);
    }
    ++entry_calls_;
    return lookup_percentile_(p, [](const Point& pt) { return pt.entry_ns; });
}

int64_t CdfLatencyModel::response_latency_ns(int64_t  ) const {
    double p;
    if (paired_ && has_pending_) {

        p = last_percentile_;
        has_pending_ = false;
    } else {

        p = next_percentile_(sample_counter_);
    }
    ++response_calls_;
    return lookup_percentile_(p, [](const Point& pt) { return pt.response_ns; });
}
