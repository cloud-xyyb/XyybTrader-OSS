#pragma once

#include <cmath>

/// @brief 为 ProbQueueModel 提供幂次衰减概率。
///
/// q <= 0 时返回 1；k 越大，queue_ahead 估算越保守。
/// 本函数不 clamp k，通常应传非负值；输出概率由 ProbQueueModel 统一 clamp。
struct PowerProbQueueFunc1 {
    double k;  ///< 衰减强度；0 表示所有非空档位返回 1。
    double operator()(double q) const noexcept {
        if (q <= 0.0) return 1.0;
        return 1.0 / std::pow(1.0 + q, k);
    }
};

/// @brief 为 ProbQueueModel 提供指数衰减概率。
///
/// q <= 0 时返回 1；k 越大，queue_ahead 估算越保守。
/// 本函数不 clamp k，通常应传非负值；输出概率由 ProbQueueModel 统一 clamp。
struct PowerProbQueueFunc2 {
    double k;  ///< 衰减强度；0 表示所有非空档位返回 1。
    double operator()(double q) const noexcept {
        if (q <= 0.0) return 1.0;
        return std::exp(-k * q);
    }
};
