#include "InterpolatedLatencyModel.h"

#include <algorithm>
#include <cstddef>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace {

bool parse_csv_row(const std::string& line, int64_t& ts, int64_t& entry, int64_t& resp) {
    std::stringstream ss(line);
    std::string tok;
    if (!std::getline(ss, tok, ',')) return false;
    try { ts = std::stoll(tok); } catch (...) { return false; }
    if (!std::getline(ss, tok, ',')) return false;
    try { entry = std::stoll(tok); } catch (...) { return false; }
    if (!std::getline(ss, tok, ',')) return false;
    try { resp = std::stoll(tok); } catch (...) { return false; }
    return true;
}

}

InterpolatedLatencyModel::InterpolatedLatencyModel(const std::string& csv_path) {
    std::ifstream f(csv_path);
    if (!f.good()) {
        throw std::runtime_error("InterpolatedLatencyModel: 无法打开 csv: " + csv_path);
    }
    std::string line;
    bool first = true;
    while (std::getline(f, line)) {
        if (line.empty()) continue;

        if (first) {
            first = false;
            if (!line.empty() && (line[0] < '0' || line[0] > '9') && line[0] != '-') {
                continue;
            }
        }
        int64_t ts = 0, entry = 0, resp = 0;
        if (!parse_csv_row(line, ts, entry, resp)) {
            throw std::runtime_error(
                "InterpolatedLatencyModel: csv 行解析失败 (期望 3 列 int64): " + line);
        }
        ts_.push_back(ts);
        entry_.push_back(entry);
        resp_.push_back(resp);
    }
    if (ts_.empty()) {
        throw std::runtime_error("InterpolatedLatencyModel: csv 无有效数据: " + csv_path);
    }

    if (!std::is_sorted(ts_.begin(), ts_.end())) {
        std::vector<size_t> idx(ts_.size());
        for (size_t i = 0; i < idx.size(); ++i) idx[i] = i;
        std::sort(idx.begin(), idx.end(),
                  [&](size_t a, size_t b) { return ts_[a] < ts_[b]; });
        std::vector<int64_t> ts2(ts_.size()), e2(entry_.size()), r2(resp_.size());
        for (size_t i = 0; i < idx.size(); ++i) {
            ts2[i] = ts_[idx[i]];
            e2[i]  = entry_[idx[i]];
            r2[i]  = resp_[idx[i]];
        }
        ts_.swap(ts2);
        entry_.swap(e2);
        resp_.swap(r2);
    }
}

int64_t InterpolatedLatencyModel::entry_latency_ns(int64_t ts) const {
    return lookup_(entry_, ts);
}

int64_t InterpolatedLatencyModel::response_latency_ns(int64_t ts) const {
    return lookup_(resp_, ts);
}

int64_t InterpolatedLatencyModel::lookup_(const std::vector<int64_t>& v, int64_t ts) const {
    if (ts <= ts_.front()) return v.front();
    if (ts >= ts_.back())  return v.back();

    auto it = std::lower_bound(ts_.begin(), ts_.end(), ts);
    const size_t hi = static_cast<size_t>(std::distance(ts_.begin(), it));
    const size_t lo = hi - 1;
    const int64_t t0 = ts_[lo],  t1 = ts_[hi];
    const int64_t v0 = v[lo],   v1 = v[hi];
    if (t1 == t0) return v0;

    return v0 + (v1 - v0) * (ts - t0) / (t1 - t0);
}
