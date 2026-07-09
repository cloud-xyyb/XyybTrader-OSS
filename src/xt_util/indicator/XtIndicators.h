#pragma once
/// @file XtIndicators.h
/// @brief CTA 常用 header-only 技术指标 helper。

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <limits>
#include <vector>

namespace xt_indicator {

// 固定容量环形缓冲
/// @brief 固定容量环形缓冲（fixed-capacity ring buffer）。
///
/// 用于滑动窗口指标保存最近 N 个样本。cap 为 0 时 push() 静默丢弃输入，
/// newest()/oldest()/operator[] 要求调用方先确认 size() 足够。
struct XtIndicatorRingBuf {
    explicit XtIndicatorRingBuf(size_t cap = 0) { if (cap) init(cap); }
    void init(size_t cap) {
        data_.assign(cap, 0.0);
        head_ = sz_ = 0;
    }
    size_t capacity() const { return data_.size(); }
    size_t size()     const { return sz_; }
    bool   full()     const { return sz_ == data_.size(); }

    void push(double v) {
        if (data_.empty()) return;
        if (sz_ < data_.size()) {
            data_[(head_ + sz_) % data_.size()] = v;
            ++sz_;
        } else {
            data_[head_] = v;
            head_ = (head_ + 1) % data_.size();
        }
    }

    double operator[](size_t i) const { return data_[(head_ + i) % data_.size()]; }
    double newest() const { assert(sz_ > 0); return (*this)[sz_ - 1]; }
    double oldest() const { assert(sz_ > 0); return (*this)[0]; }

private:
    std::vector<double> data_;
    size_t head_ = 0, sz_ = 0;
};

// SMA（简单移动平均）
/// @brief 简单移动平均（Simple Moving Average）。
///
/// push() 为 O(1)，内部用 XtIndicatorRingBuf 保存最近 period 个样本。
/// ready() 前 value() 返回 0.0。
class CtSMA {
public:
    explicit CtSMA(int period) : period_(period), buf_(period) {}

    void push(double v) {
        sum_ += v;
        if (buf_.full()) sum_ -= buf_.oldest();
        buf_.push(v);
    }
    bool   ready() const { return static_cast<int>(buf_.size()) >= period_; }
    double value() const { return ready() ? sum_ / period_ : 0.0; }
    int    period() const { return period_; }

private:
    int period_;
    XtIndicatorRingBuf buf_;
    double sum_ = 0.0;
};

// EMA（指数移动平均）
/// @brief 指数移动平均（Exponential Moving Average）。
///
/// 使用 span 法平滑因子 k = 2 / (period + 1)。第一个样本作为 EMA seed；
/// ready() 前 value() 仍返回当前 seed/EMA 观测值，策略信号应以 ready() 为准。
class CtEMA {
public:
    explicit CtEMA(int period) : period_(period), k_(2.0 / (period + 1)) {}

    void push(double v) {
        if (cnt_ == 0) {
            ema_ = v;
        } else {
            ema_ = v * k_ + ema_ * (1.0 - k_);
        }
        ++cnt_;
    }
    bool   ready() const { return cnt_ >= period_; }
    double value() const { return ema_; }
    int    period() const { return period_; }

private:
    int period_;
    double k_;
    double ema_ = 0.0;
    int cnt_ = 0;
};

// ATR（真实波幅均值）
/// @brief 真实波幅均值（Average True Range）。
///
/// 前 period 根 bar 的 TR 算术均值作为 seed，之后使用 Wilder 平滑：
/// atr = (atr_prev * (period - 1) + tr) / period。
class CtATR {
public:
    explicit CtATR(int period = 14) : period_(period) {}

    void push(double high, double low, double prev_close) {
        const double tr = std::max({high - low,
                                    std::abs(high - prev_close),
                                    std::abs(low  - prev_close)});
        if (cnt_ < period_) {
            sum_tr_ += tr;
            ++cnt_;
            if (cnt_ == period_) atr_ = sum_tr_ / period_;
        } else {
            atr_ = (atr_ * (period_ - 1) + tr) / period_;
            ++cnt_;
        }
    }
    bool   ready() const { return cnt_ >= period_; }
    double value() const { return atr_; }
    int    period() const { return period_; }

private:
    int period_;
    int cnt_ = 0;
    double sum_tr_ = 0.0;
    double atr_ = 0.0;
};

// ADX（平均趋向指数）
/// @brief 平均趋向指数（Average Directional Index）。
///
/// 输出 ADX、+DI 和 -DI，供策略自行定义趋势/震荡阈值。
/// 计算流程为 Wilder 平滑的 +DM/-DM/TR -> +DI/-DI -> DX -> ADX；
/// ready() 前 value()/plus_di()/minus_di() 仅是 warm-up 观测值。
class CtADX {
public:
    explicit CtADX(int period = 14) : period_(period) {}

    void push(double high, double low, double close) {
        if (count_ > 0) {
            const double up_move   = high - prev_high_;
            const double down_move = prev_low_ - low;
            const double plus_dm   = (up_move > down_move && up_move > 0) ? up_move : 0.0;
            const double minus_dm  = (down_move > up_move && down_move > 0) ? down_move : 0.0;
            const double tr = std::max({high - low,
                                        std::abs(high - prev_close_),
                                        std::abs(low  - prev_close_)});

            if (count_ <= period_) {

                sum_plus_dm_  += plus_dm;
                sum_minus_dm_ += minus_dm;
                sum_tr_       += tr;
                if (count_ == period_) {
                    smooth_plus_dm_  = sum_plus_dm_;
                    smooth_minus_dm_ = sum_minus_dm_;
                    smooth_tr_       = sum_tr_;
                    update_di_dx();
                    sum_dx_ = dx_;
                    dx_count_ = 1;
                }
            } else {

                smooth_plus_dm_  = smooth_plus_dm_  - smooth_plus_dm_  / period_ + plus_dm;
                smooth_minus_dm_ = smooth_minus_dm_ - smooth_minus_dm_ / period_ + minus_dm;
                smooth_tr_       = smooth_tr_       - smooth_tr_       / period_ + tr;
                update_di_dx();
                if (dx_count_ < period_) {
                    sum_dx_ += dx_;
                    ++dx_count_;
                    if (dx_count_ == period_) adx_ = sum_dx_ / period_;
                } else {
                    adx_ = (adx_ * (period_ - 1) + dx_) / period_;
                }
            }
        }
        prev_high_  = high;
        prev_low_   = low;
        prev_close_ = close;
        ++count_;
    }

    bool   ready()    const { return dx_count_ >= period_; }
    double value()    const { return adx_; }
    double plus_di()  const { return plus_di_; }
    double minus_di() const { return minus_di_; }

private:
    void update_di_dx() {
        plus_di_  = smooth_tr_ > 0 ? 100.0 * smooth_plus_dm_  / smooth_tr_ : 0.0;
        minus_di_ = smooth_tr_ > 0 ? 100.0 * smooth_minus_dm_ / smooth_tr_ : 0.0;
        const double di_sum = plus_di_ + minus_di_;
        dx_ = di_sum > 0 ? 100.0 * std::abs(plus_di_ - minus_di_) / di_sum : 0.0;
    }

    int period_;
    int count_ = 0;
    double prev_high_ = 0, prev_low_ = 0, prev_close_ = 0;
    double sum_plus_dm_ = 0, sum_minus_dm_ = 0, sum_tr_ = 0;
    double smooth_plus_dm_ = 0, smooth_minus_dm_ = 0, smooth_tr_ = 0;
    double plus_di_ = 0, minus_di_ = 0, dx_ = 0;
    double sum_dx_ = 0;
    int dx_count_ = 0;
    double adx_ = 0;
};

// ATR 百分位排名
/// @brief ATR 百分位排名（ATR percentile over N bars）。
///
/// 先用内部 CtATR 得到 ATR，再在最近 rank_window 个 ATR 样本中计算当前值排名。
/// percentile() 返回窗口内严格小于当前 ATR 的样本占比。
class CtATRPercentile {
public:
    explicit CtATRPercentile(int atr_period = 14, int rank_window = 100)
        : atr_(atr_period), buf_(rank_window) {}

    void push(double high, double low, double prev_close) {
        atr_.push(high, low, prev_close);
        if (atr_.ready()) buf_.push(atr_.value());
    }

    bool   ready() const { return buf_.full(); }
    double atr()   const { return atr_.value(); }

    double percentile() const {
        if (!buf_.full()) return 0.5;
        const double cur = atr_.value();
        int below = 0;
        for (size_t i = 0; i < buf_.size(); ++i)
            if (buf_[i] < cur) ++below;
        return static_cast<double>(below) / static_cast<double>(buf_.size());
    }

private:
    CtATR atr_;
    XtIndicatorRingBuf buf_;
};

// 布林带
/// @brief 布林带（Bollinger Bands = SMA +/- n_std * StdDev）。
///
/// middle() 为 SMA，upper()/lower() 为 middle +/- n_std * population stddev。
/// std_dev() 采用 sum/sum2 差值法，适合 CTA bar 级窗口；高精度 tick 场景应换用 Welford。
class CtBBands {
public:
    explicit CtBBands(int period = 20, double n_std = 2.0)
        : period_(period), n_std_(n_std), buf_(period) {}

    void push(double v) {
        sum_  += v;
        sum2_ += v * v;
        if (buf_.full()) {
            const double old = buf_.oldest();
            sum_  -= old;
            sum2_ -= old * old;
        }
        buf_.push(v);
    }
    bool   ready()  const { return static_cast<int>(buf_.size()) >= period_; }
    double middle() const { return ready() ? sum_ / period_ : 0.0; }

    double std_dev() const {
        if (!ready()) return 0.0;
        const double mean = sum_ / period_;
        const double var  = sum2_ / period_ - mean * mean;
        return var > 0.0 ? std::sqrt(var) : 0.0;
    }
    double upper()  const { return middle() + n_std_ * std_dev(); }
    double lower()  const { return middle() - n_std_ * std_dev(); }
    double value()  const { return middle(); }
    int    period() const { return period_; }

private:
    int period_;
    double n_std_;
    XtIndicatorRingBuf buf_;
    double sum_ = 0.0;
    double sum2_ = 0.0;
};

// RSI（相对强弱指数）
/// @brief 相对强弱指数（Relative Strength Index）。
///
/// 使用 Wilder 平滑平均涨跌幅。ready() 前 value() 返回中性值 50.0；
/// 平均跌幅接近 0 时返回 100.0，避免除零并表达单边上涨窗口。
class CtRSI {
public:
    explicit CtRSI(int period = 14) : period_(period) {}

    void push(double price) {
        if (cnt_ > 0) {
            const double delta = price - prev_;
            const double gain  = delta > 0.0 ? delta : 0.0;
            const double loss  = delta < 0.0 ? -delta : 0.0;
            if (cnt_ < period_) {
                avg_gain_ += gain;
                avg_loss_ += loss;
                if (cnt_ == period_ - 1) {
                    avg_gain_ /= period_;
                    avg_loss_ /= period_;
                }
            } else {
                avg_gain_ = (avg_gain_ * (period_ - 1) + gain) / period_;
                avg_loss_ = (avg_loss_ * (period_ - 1) + loss) / period_;
            }
        }
        prev_ = price;
        ++cnt_;
    }
    bool   ready() const { return cnt_ > period_; }
    double value() const {
        if (!ready()) return 50.0;
        if (avg_loss_ < 1e-12) return 100.0;
        return 100.0 - 100.0 / (1.0 + avg_gain_ / avg_loss_);
    }
    int period() const { return period_; }

private:
    int period_;
    double prev_ = 0.0;
    double avg_gain_ = 0.0;
    double avg_loss_ = 0.0;
    int cnt_ = 0;
};

// MACD（DIF / DEA / Histogram）
/// @brief MACD 指标（Moving Average Convergence Divergence）。
///
/// 默认参数为 fast=12、slow=26、signal=9。histogram 按国内常用口径计算为 2 * (DIF - DEA)。
/// ready() 要求快慢 EMA 与 signal EMA 都完成 warm-up。
class CtMACD {
public:
    explicit CtMACD(int fast = 12, int slow = 26, int signal = 9)
        : ema_fast_(fast), ema_slow_(slow), ema_signal_(signal) {}

    void push(double price) {
        ema_fast_.push(price);
        ema_slow_.push(price);
        if (ema_fast_.ready() && ema_slow_.ready()) {
            const double dif = ema_fast_.value() - ema_slow_.value();
            ema_signal_.push(dif);
            dif_   = dif;
            dea_   = ema_signal_.value();
            hist_  = (dif - dea_) * 2.0;
        }
    }
    bool   ready()  const { return ema_fast_.ready() && ema_slow_.ready() && ema_signal_.ready(); }
    double dif()    const { return dif_; }
    double dea()    const { return dea_; }
    double hist()   const { return hist_; }
    double value()  const { return dif_; }

private:
    CtEMA ema_fast_, ema_slow_, ema_signal_;
    double dif_ = 0.0;
    double dea_ = 0.0;
    double hist_ = 0.0;
};

// Donchian Channel（唐奇安通道）
/// @brief 唐奇安通道（Donchian Channel = N 周期最高/最低价）。
///
/// upper()/lower() 每次按窗口线性扫描，适合 bar 级调用；tick hot path 使用前应评估开销。
class XtDonchian {
public:
    explicit XtDonchian(int period = 20) : period_(period), highs_(period), lows_(period) {}

    void push(double high, double low) {
        highs_.push(high);
        lows_.push(low);
    }
    bool ready() const { return static_cast<int>(highs_.size()) >= period_; }
    double upper() const {
        double v = std::numeric_limits<double>::lowest();
        for (size_t i = 0; i < highs_.size(); ++i) v = std::max(v, highs_[i]);
        return v;
    }
    double lower() const {
        double v = std::numeric_limits<double>::max();
        for (size_t i = 0; i < lows_.size(); ++i) v = std::min(v, lows_[i]);
        return v;
    }
    double middle() const { return (upper() + lower()) * 0.5; }
    double value()  const { return middle(); }
    int period()    const { return period_; }

private:
    int period_;
    XtIndicatorRingBuf highs_, lows_;
};

}
