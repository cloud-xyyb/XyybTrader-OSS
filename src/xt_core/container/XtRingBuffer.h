#pragma once

#include <cstddef>
#include <vector>

/// @brief 有界 FIFO 环形缓冲（bounded FIFO ring buffer）。
///
/// 固定容量内按时间顺序缓存最近 N 个元素；push() 在满容量时淘汰最旧条目。
/// `set_cap(0)` 会清空并禁用写入，后续 push() 直接丢弃。
///
/// @note 线程语义由调用方保证：单线程访问或外部同步。
template <typename T>
class XtRingBuffer {
public:

    void set_cap(size_t cap) {
        buf_.assign(cap, T{});
        cap_  = cap;
        head_ = 0;
        size_ = 0;
    }

    void push(const T& v) {
        if (cap_ == 0) return;
        buf_[(head_ + size_) % cap_] = v;
        if (size_ < cap_) {
            ++size_;
        } else {
            head_ = (head_ + 1) % cap_;
        }
    }

    size_t copy_tail(T* out, size_t max) const {
        if (!out || max == 0) return 0;
        const size_t n = (size_ < max) ? size_ : max;
        for (size_t i = 0; i < n; ++i)
            out[i] = buf_[(head_ + size_ - n + i) % cap_];
        return n;
    }

    size_t cap()   const noexcept { return cap_; }
    size_t size()  const noexcept { return size_; }
    bool   empty() const noexcept { return size_ == 0; }

private:
    std::vector<T> buf_;
    size_t cap_  = 0;
    size_t head_ = 0;
    size_t size_ = 0;
};
