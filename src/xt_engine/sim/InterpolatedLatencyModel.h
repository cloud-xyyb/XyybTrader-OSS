#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "ILatencyModel.h"

/// @brief 从时间序列 CSV 查询 entry/response 延迟（时变模型）。
///
/// 构造时加载 v2 CSV（header 必须为 `event_ts_local_ns,entry_ns,response_ns`，
/// 索引列 = event-axis，与 live 录制侧同轴）；旧 v1 steady 轴 CSV fail-fast。
/// 查询按 event ts 线性插值（__int128 中间量，向零截断）；样本区间外返回
/// 首/末样本值，不外推。entry/response 与同一原始行绑定。
/// 负延迟/超界/重复 ts 构造期拒绝。
class InterpolatedLatencyModel final : public ILatencyModel {
public:
    /// @param csv_path CSV 文件路径；缺失、非 v2 header、格式错误、非法值
    ///        或无数据时抛 std::runtime_error。
    explicit InterpolatedLatencyModel(const std::string& csv_path);

    int64_t entry_latency_ns(int64_t event_ts_ns) const override;
    int64_t response_latency_ns(int64_t event_ts_ns) const override;

    /// @brief 时变模型：首个 event 前的 submit/cancel 必须 fail-fast（静默取
    /// 首样本正是 T2.SIM-1 的病）。
    bool requires_event_timestamp() const override { return true; }

    /// @brief 已加载样本点数。
    size_t size() const noexcept { return ts_.size(); }

private:
    /// @brief 在已排序 ts_ 上查询对应 latency 序列；边界外 clamp 到首/末样本。
    int64_t lookup_(const std::vector<int64_t>& v, int64_t ts) const;

    std::vector<int64_t> ts_;     ///< sorted event-axis timestamp ns
    std::vector<int64_t> entry_;  ///< 与 ts_ 同下标的 submit -> entry latency ns
    std::vector<int64_t> resp_;   ///< 与 ts_ 同下标的 entry/fill -> response latency ns
};
