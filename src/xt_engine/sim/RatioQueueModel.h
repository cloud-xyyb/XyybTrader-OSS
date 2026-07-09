#pragma once

#include <algorithm>

#include "IQueueModel.h"

/// @brief 用固定比例估算被动单 queue_ahead。
///
/// 普通入队返回 `level_qty * ratio`；撤单重入队返回
/// 撤单重入队返回 `level_qty * cancel_resubmit_ratio`。构造参数会 clamp 到非负数。
/// 调用方负责传入非负可见档位数量。
class RatioQueueModel final : public IQueueModel {
public:

    RatioQueueModel(double ratio, double cancel_resubmit_ratio = 1.0) noexcept
        : ratio_(std::max(0.0, ratio)),
          cancel_resubmit_ratio_(std::max(0.0, cancel_resubmit_ratio)) {}

    double estimate_queue_ahead(double level_qty, bool is_resubmit) const noexcept override {
        const double r = is_resubmit ? cancel_resubmit_ratio_ : ratio_;
        return level_qty * r;
    }

    double ratio() const noexcept { return ratio_; }
    double cancel_resubmit_ratio() const noexcept { return cancel_resubmit_ratio_; }

private:
    double ratio_;
    double cancel_resubmit_ratio_;
};
