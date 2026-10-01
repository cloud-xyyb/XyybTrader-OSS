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

    /// @return true 表示窗口已累计 window 个样本，统计值可用于交易信号。
    bool ready() const { return static_cast<int>(buf_.size()) >= window_; }

    /// @pre 至少已经 push 过一个样本；空窗口下会触发底层 assert。
    double latest() const { return buf_.newest(); }

    /// @return ready() 前返回 0.0，ready() 后返回当前滑动窗口均值。
    double mean() const { return sma_.value(); }

    /// @return ready() 前返回 0.0；ready() 后返回 sqrt(sum((x_i - mean)^2) / window)。
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

    /// @return Z-Score = (latest - mean) / std_dev；未就绪或标准差接近 0 时返回 0.0。
    double z_score() const {
        if (!ready()) return 0.0;
        const double sd = std_dev();
        if (sd < 1e-15) return 0.0;
        return (latest() - mean()) / sd;
    }

private:
    CtSMA sma_;
    XtIndicatorRingBuf buf_;  ///< 存储最近 window 个样本，索引顺序为 oldest -> newest。
    int window_;              ///< 滑动窗口大小，要求与 sma_/buf_ 容量一致。
};

}  // namespace xt_indicator
