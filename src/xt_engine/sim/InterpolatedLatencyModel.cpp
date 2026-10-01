#include "InterpolatedLatencyModel.h"
#include "XtLatencyArith.h"

#include <algorithm>
#include <cstddef>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace {

constexpr const char* kV2Header = "event_ts_local_ns,entry_ns,response_ns";

// CSV parser：前三个逗号分隔字段必须能转 int64，后续字段忽略。
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

} // namespace

InterpolatedLatencyModel::InterpolatedLatencyModel(const std::string& csv_path) {
    std::ifstream f(csv_path);
    if (!f.good()) {
        throw std::runtime_error("InterpolatedLatencyModel: 无法打开 csv: " + csv_path);
    }
    std::string line;
    // v2 header 强制：索引列语义 = event-axis（live 录制 event_ts_local_ns）。
    // 旧 v1 steady 轴 CSV（header ts_local_ns 或无 header）与回测 query 轴不同域，
    // 静默复用 = 时变维度永远 clamp 首/尾样本——fail-fast 指路。
    if (!std::getline(f, line) || line.empty()) {
        throw std::runtime_error("InterpolatedLatencyModel: csv 空文件: " + csv_path);
    }
    if (!line.empty() && line.back() == '\r') line.pop_back();
    if (line != kV2Header) {
        throw std::runtime_error(
            "InterpolatedLatencyModel: 需要 v2 header '" + std::string(kV2Header) +
            "'（event-axis 索引），实际首行: '" + line +
            "'。旧 steady 轴 CSV 请用新 extract_latency.py 重出 v2，或改用 type:cdf");
    }
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        int64_t ts = 0, entry = 0, resp = 0;
        if (!parse_csv_row(line, ts, entry, resp)) {
            throw std::runtime_error(
                "InterpolatedLatencyModel: csv 行解析失败 (期望 3 列 int64): " + line);
        }
        // 构造期拒绝：负延迟/超界（消费点同样二次校验）、重复 ts（手写 CSV
        // 常见错误；插值在重复 ts 上无定义）。
        xt_latency::validate_latency_ns(entry, "csv entry_ns");
        xt_latency::validate_latency_ns(resp, "csv response_ns");
        if (!ts_.empty() && ts == ts_.back()) {
            throw std::runtime_error(
                "InterpolatedLatencyModel: 重复 event_ts_local_ns: " + std::to_string(ts));
        }
        ts_.push_back(ts);
        entry_.push_back(entry);
        resp_.push_back(resp);
    }
    if (ts_.empty()) {
        throw std::runtime_error("InterpolatedLatencyModel: csv 无有效数据: " + csv_path);
    }
    // 输入文件允许乱序；排序时必须保持 ts/entry/response 三列同行关系。
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
        // 排序后再查重复（乱序输入的重复只有排序后相邻）。
        for (size_t i = 1; i < ts_.size(); ++i) {
            if (ts_[i] == ts_[i - 1]) {
                throw std::runtime_error(
                    "InterpolatedLatencyModel: 重复 event_ts_local_ns: " +
                    std::to_string(ts_[i]));
            }
        }
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
    // 边界已提前处理；lower_bound 返回区间右端点。
    auto it = std::lower_bound(ts_.begin(), ts_.end(), ts);
    const size_t hi = static_cast<size_t>(std::distance(ts_.begin(), it));
    const size_t lo = hi - 1;
    const int64_t t0 = ts_[lo],  t1 = ts_[hi];
    const int64_t v0 = v[lo],   v1 = v[hi];
    // 构造期已拒重复 ts，t1 > t0 成立。全程 __int128 防中间量溢出，
    // 向零截断保确定性；结果再过一道域校验。
    const __int128 num = static_cast<__int128>(v1 - v0) * (ts - t0);
    const __int128 den = t1 - t0;
    const int64_t out = v0 + static_cast<int64_t>(num / den);
    return xt_latency::validate_latency_ns(out, "interpolated");
}
