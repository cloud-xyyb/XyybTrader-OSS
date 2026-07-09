#pragma once

/// @file secure_string.h
/// @brief move-only secret buffer，尽力 mlock 并在释放前显式清零。
///
/// 用于持有配置和运行期凭证。类本身避免隐式复制；调用 `unsafe_to_string()`
/// 会产生不受 mlock 保护的普通 `std::string` 副本，调用方必须立即消费。

#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string>
#include <string_view>
#include <utility>

#if defined(__unix__) || defined(__APPLE__)
#  include <sys/mman.h>
#  include <unistd.h>
#  define XT_SECURE_STRING_HAS_MLOCK 1
#else
#  define XT_SECURE_STRING_HAS_MLOCK 0
#endif

namespace XtUtil {

/// @brief 内存敏感凭证容器（mlock + volatile zero + move-only）。
///
/// POSIX 平台尽力用 `mlock` 防止 secret swap-out；非 POSIX 平台仍执行显式清零。
/// 类不可拷贝，避免隐式复制 secret。通过 `std::string&&` 构造时会清零 source
/// string 的内部缓冲；调用方不应继续保留或复用该 source。
class secure_string {
public:

    secure_string() noexcept = default;

    explicit secure_string(std::string&& source) {
        const size_t n = source.size();
        if (n == 0) {
            secure_zero_string(source);
            return;
        }
        allocate_(n);
        std::memcpy(buf_, source.data(), n);
        size_ = n;

        secure_zero_string(source);
    }

    secure_string(const char* data, size_t n) {
        if (data == nullptr || n == 0) return;
        allocate_(n);
        std::memcpy(buf_, data, n);
        size_ = n;
    }

    secure_string(const secure_string&) = delete;
    secure_string& operator=(const secure_string&) = delete;

    secure_string(secure_string&& other) noexcept
        : buf_(other.buf_), size_(other.size_), cap_(other.cap_) {
        other.buf_ = nullptr;
        other.size_ = 0;
        other.cap_ = 0;
    }

    secure_string& operator=(secure_string&& other) noexcept {
        if (this != &other) {
            release_();
            buf_ = other.buf_;
            size_ = other.size_;
            cap_ = other.cap_;
            other.buf_ = nullptr;
            other.size_ = 0;
            other.cap_ = 0;
        }
        return *this;
    }

    ~secure_string() { release_(); }

    const char* data() const noexcept { return buf_ ? buf_ : ""; }

    size_t size() const noexcept { return size_; }
    bool empty() const noexcept { return size_ == 0; }

    std::string_view view() const noexcept {
        return buf_ ? std::string_view{buf_, size_} : std::string_view{};
    }

    std::string unsafe_to_string() const {
        return buf_ ? std::string{buf_, size_} : std::string{};
    }

    void clear() noexcept { release_(); }

private:
    void allocate_(size_t n) {
        const size_t cap = n < 64 ? 64 : n;
#if XT_SECURE_STRING_HAS_MLOCK
        void* p = nullptr;
        const long page_sz = ::sysconf(_SC_PAGESIZE);
        const size_t align = (page_sz > 0) ? static_cast<size_t>(page_sz) : 4096u;
        if (::posix_memalign(&p, align, cap) != 0 || p == nullptr) {
            throw std::bad_alloc();
        }
        buf_ = static_cast<char*>(p);
        cap_ = cap;
        if (::mlock(buf_, cap_) != 0) {

            std::fprintf(stderr,
                         "[secure_string] mlock(%zuB) failed; secret may swap. "
                         "Check ulimit -l (RLIMIT_MEMLOCK).\n", cap_);
        }
#else
        buf_ = static_cast<char*>(std::malloc(cap));
        if (buf_ == nullptr) throw std::bad_alloc();
        cap_ = cap;
#endif
        std::memset(buf_, 0, cap_);
    }

    void release_() noexcept {
        if (buf_ == nullptr) return;

        secure_zero_buf(buf_, cap_);
#if XT_SECURE_STRING_HAS_MLOCK
        ::munlock(buf_, cap_);
#endif
        std::free(buf_);
        buf_ = nullptr;
        size_ = 0;
        cap_ = 0;
    }

    static void secure_zero_buf(void* p, size_t n) noexcept {
        if (p == nullptr || n == 0) return;
        volatile unsigned char* vp = static_cast<volatile unsigned char*>(p);
        while (n--) *vp++ = 0;
    }

    static void secure_zero_string(std::string& s) noexcept {
        if (!s.empty()) {
            secure_zero_buf(const_cast<char*>(s.data()), s.size());
        }
        s.clear();
        s.shrink_to_fit();
    }

    char* buf_ = nullptr;
    size_t size_ = 0;
    size_t cap_ = 0;
};

}
