#pragma once

/// @file
/// @brief 统一的异常保护 wrapper
///
/// 策略插件等外部代码抛异常不能带崩整个进程。run_guarded() 在指定 tag/stage
/// 下执行回调，捕获所有异常并写结构化错误日志，用返回值或可选计数器把失败
/// 交还调用方。
/// 它不决定吞掉、升级停机还是重试；失败策略由 callsite 显式表达。

#include "xt_logger/XtLogger.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <utility>

namespace xt_log {

template <typename Fn>
bool run_guarded(const char* tag, const char* stage, Fn&& fn) noexcept {
    try {
        std::forward<Fn>(fn)();
        return true;
    } catch (const std::exception& e) {
        XtLogger::error_m(tag, "guard stage={} err={}", stage, e.what());
    } catch (...) {
        XtLogger::error_m(tag, "guard stage={} err=unknown", stage);
    }
    return false;
}

template <typename Fn>
bool run_guarded(const char* tag, const char* stage,
                 std::atomic<uint64_t>& counter, Fn&& fn) noexcept {
    try {
        std::forward<Fn>(fn)();
        return true;
    } catch (const std::exception& e) {
        const auto n = counter.fetch_add(1, std::memory_order_relaxed) + 1;
        XtLogger::error_m(tag, "guard stage={} count={} err={}", stage, n, e.what());
    } catch (...) {
        const auto n = counter.fetch_add(1, std::memory_order_relaxed) + 1;
        XtLogger::error_m(tag, "guard stage={} count={} err=unknown", stage, n);
    }
    return false;
}

/// @brief run_guarded 的可选日志上下文; 字段仅在异常路径格式化 (success 路径零开销)。
/// @note 仅承载日志元数据 (序号 / 标识 / 计数器), 不承载失败策略 — 失败策略由 callsite 决定。
struct GuardCtx {
    std::size_t index = 0;
    bool has_index = false;
    const char* id = nullptr;
    std::atomic<uint64_t>* counter = nullptr;
};

inline GuardCtx guard_item(std::size_t index, const char* id,
                           std::atomic<uint64_t>* counter = nullptr) {
    GuardCtx c;
    c.index = index;
    c.has_index = true;
    c.id = id;
    c.counter = counter;
    return c;
}

inline GuardCtx guard_id(const char* id, std::atomic<uint64_t>* counter = nullptr) {
    GuardCtx c;
    c.id = id;
    c.counter = counter;
    return c;
}

namespace detail {

inline void log_guard_exc(const char* tag, const char* stage,
                          const GuardCtx& ctx, const char* what) noexcept {
    if (ctx.counter) ctx.counter->fetch_add(1, std::memory_order_relaxed);
    const char* w = what ? what : "unknown";
    if (ctx.has_index && ctx.id) {
        XtLogger::error_m(tag, "guard stage={} idx={} id={} err={}", stage, ctx.index, ctx.id, w);
    } else if (ctx.id) {
        XtLogger::error_m(tag, "guard stage={} id={} err={}", stage, ctx.id, w);
    } else if (ctx.has_index) {
        XtLogger::error_m(tag, "guard stage={} idx={} err={}", stage, ctx.index, w);
    } else {
        XtLogger::error_m(tag, "guard stage={} err={}", stage, w);
    }
}
}

template <typename Fn>
bool run_guarded(const char* tag, const char* stage,
                 const GuardCtx& ctx, Fn&& fn) noexcept {
    try {
        std::forward<Fn>(fn)();
        return true;
    } catch (const std::exception& e) {
        detail::log_guard_exc(tag, stage, ctx, e.what());
    } catch (...) {
        detail::log_guard_exc(tag, stage, ctx, nullptr);
    }
    return false;
}

}
