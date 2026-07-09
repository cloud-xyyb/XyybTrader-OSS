#pragma once

#include <algorithm>
#include <type_traits>
#include <utility>

#include "IQueueModel.h"

/// @brief 用概率函数估算被动单 queue_ahead。
///
/// ProbFunc 接收 level_qty 并返回 [0, 1] 概率；结果按
/// `level_qty * (1 - clamp(ProbFunc(level_qty)))` 计算。
/// 函数对象按值持有，调用方应保证其 `operator()` 无外部生命周期依赖。
/// `is_resubmit` 不参与该模型，重入队惩罚应由传入的 ProbFunc 自行编码。
template <typename ProbFunc>
class ProbQueueModel final : public IQueueModel {
public:

    explicit ProbQueueModel(ProbFunc func) noexcept(std::is_nothrow_move_constructible_v<ProbFunc>)
        : func_(std::move(func)) {}

    double estimate_queue_ahead(double level_qty, bool  ) const override {
        if (level_qty <= 0.0) return 0.0;
        const double f = std::clamp(func_(level_qty), 0.0, 1.0);
        return level_qty * (1.0 - f);
    }

private:
    ProbFunc func_;
};
