#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "ILatencyModel.h"

/// @brief 从时间序列 CSV 查询 entry/response 延迟。
///
/// 构造时加载 `ts_local_ns,entry_ns,response_ns` CSV，可带 header。
/// 查询时按 `ts_local_ns` 线性插值；样本区间外返回首/末样本值，不外推。
/// entry/response 与同一原始行绑定。
class InterpolatedLatencyModel final : public ILatencyModel {
public:

    explicit InterpolatedLatencyModel(const std::string& csv_path);

    int64_t entry_latency_ns(int64_t ts_local_ns) const override;
    int64_t response_latency_ns(int64_t ts_local_ns) const override;

    size_t size() const noexcept { return ts_.size(); }

private:

    int64_t lookup_(const std::vector<int64_t>& v, int64_t ts) const;

    std::vector<int64_t> ts_;
    std::vector<int64_t> entry_;
    std::vector<int64_t> resp_;
};
