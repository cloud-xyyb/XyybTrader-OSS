#pragma once

/// @file XtAllocGuard.h
/// @brief debug-only hot-path 堆分配检测守卫。

#include <cassert>

namespace xt_util {
namespace sync {

#if !defined(NDEBUG) && defined(XT_ALLOC_GUARD_ENABLED)

namespace detail {

    inline thread_local int alloc_guard_depth_ = 0;

    inline bool is_alloc_guarded() noexcept {
        return alloc_guard_depth_ > 0;
    }
}

/// @brief Debug RAII 守卫：作用域内禁止堆分配。
///
/// 支持嵌套；真正的断言发生在调用方接入的 `operator new/new[]` hook 中。
class ScopedAllocGuard {
public:
    ScopedAllocGuard() noexcept { ++detail::alloc_guard_depth_; }
    ~ScopedAllocGuard() noexcept { --detail::alloc_guard_depth_; }
    ScopedAllocGuard(const ScopedAllocGuard&) = delete;
    ScopedAllocGuard& operator=(const ScopedAllocGuard&) = delete;
};

#define XT_ALLOC_GUARD() ::xt_util::sync::ScopedAllocGuard alloc_guard_##__LINE__

#else

/// @brief Release build 或未启用 XT_ALLOC_GUARD_ENABLED 时的 no-op RAII 类型。
class ScopedAllocGuard {
public:
    ScopedAllocGuard() noexcept = default;
    ~ScopedAllocGuard() noexcept = default;
    ScopedAllocGuard(const ScopedAllocGuard&) = delete;
    ScopedAllocGuard& operator=(const ScopedAllocGuard&) = delete;
};

#define XT_ALLOC_GUARD() (void)0

#endif

}
}
