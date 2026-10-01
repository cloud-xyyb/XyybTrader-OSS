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
    /// @brief 空 secure_string；不分配、不 mlock。
    secure_string() noexcept = default;

    /// @brief 从 rvalue string 接管 secret，并清零 source string 的内部缓冲。
    explicit secure_string(std::string&& source) {
        const size_t n = source.size();
        if (n == 0) {
            secure_zero_string(source);
            return;
        }
        allocate_(n);
        std::memcpy(buf_, source.data(), n);
        size_ = n;
        // 清掉 source string 内部缓冲，包括 SSO 与堆分配实现。
        secure_zero_string(source);
    }

    /// @brief 从 raw bytes 拷贝 secret；空指针或零长度得到 empty()。
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

    /// @brief 只读访问指针。返回的指针在 secure_string 生命周期内有效。
    /// @note 调用方 **不得** 拷贝出来再保存 std::string —— 失去 mlock 保护。
    /// @return  指向内部缓冲的常量指针；空 secure_string 返回 ""（C-string 安全）。
    const char* data() const noexcept { return buf_ ? buf_ : ""; }

    size_t size() const noexcept { return size_; }
    bool empty() const noexcept { return size_ == 0; }

    /// @brief 提供 string_view 接口供需要 length-bounded API 使用。
    std::string_view view() const noexcept {
        return buf_ ? std::string_view{buf_, size_} : std::string_view{};
    }

    /// @brief 为必须接收 owning string 的外部 API 提供转换；调用方必须立刻消费，不得长存。
    /// @note 名字带 unsafe_ 前缀强调反模式：每次调用产生一份未保护的
    ///       std::string 拷贝，**该拷贝不在 mlock 保护范围内**。
    std::string unsafe_to_string() const {
        return buf_ ? std::string{buf_, size_} : std::string{};
    }

    /// @brief 显式 zero + 释放（dtor 内部调用，public 仅供 reset 测试用）。
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
            // mlock 是 best-effort：失败不影响进程运行，但保留 stderr 诊断。
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
        // volatile-pointer zero 防止 free 前清零被优化为 dead store。
        secure_zero_buf(buf_, cap_);
#if XT_SECURE_STRING_HAS_MLOCK
        ::munlock(buf_, cap_);
#endif
        std::free(buf_);
        buf_ = nullptr;
        size_ = 0;
        cap_ = 0;
    }

    /// @brief Volatile-pointer zero —— 防 dead store 优化。
    static void secure_zero_buf(void* p, size_t n) noexcept {
        if (p == nullptr || n == 0) return;
        volatile unsigned char* vp = static_cast<volatile unsigned char*>(p);
        while (n--) *vp++ = 0;
    }

    /// @brief 清掉 std::string 内部缓冲。同时覆盖 SSO 与 heap 两种实现。
    /// @note 用 const_cast 让代码同时支持 C++14（C++17 才允许 string::data()
    ///       non-const 重载）。本项目要求 C++17 但保留兼容性。
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

}  // namespace XtUtil
