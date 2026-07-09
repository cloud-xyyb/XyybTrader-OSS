#pragma once
/// @file CtZScore.h
/// @brief header-only 滚动 Z-Score 指标。

#include "XtIndicators.h"

#include <cmath>

namespace xt_indicator {

/// @brief 滚动 Z-Score 指标（rolling Z-Score indicator）。
///
/// 窗口满之前 mean/std_dev/z_score 按未就绪语义返回保护值；latest() 要求至少已有一个样本。
/// 标准差按总体标准差计算，即分母为 window 而不是 window - 1。
///
/// @note 构造参数 window 应为正数；类型本身不做运行时校验。
/// @note 本指标不做内部同步，调用方需保证单线程访问和输入节奏。
class CtZScore {
public:
    explicit CtZScore(int window)
        : sma_(window), buf_(window), window_(window) {}

    void push(double val) {
        sma_.push(val);
        buf_.push(val);
    }

    bool ready() const { return static_cast<int>(buf_.size()) >= window_; }

    double latest() const { return buf_.newest(); }

    double mean() const { return sma_.value(); }

    double std_dev() const {
        if (!ready()) return 0.0;
        const double m = sma_.value();
        double sum2 = 0.0;
        for (size_t i = 0; i < buf_.size(); ++i) {
            const double diff = buf_[i] - m;
            sum2 += diff * diff;
        }
        return std::sqrt(sum2 / static_cast<double>(window_));
    }

    double z_score() const {
        if (!ready()) return 0.0;
        const double sd = std_dev();
        if (sd < 1e-15) return 0.0;
        return (latest() - mean()) / sd;
    }

private:
    CtSMA sma_;
    XtIndicatorRingBuf buf_;
    int window_;
};

}
