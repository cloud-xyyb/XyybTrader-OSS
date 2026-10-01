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

/// @brief 执行不可信回调; 捕获并记录异常, 决不外抛。
/// @param tag   logger module tag (如 "XtHftEngine" / "RT" / "TD")。
/// @param stage 阶段描述 (如 "strategy.on_tick"; callsite 可拼 sid/execid)。
/// @param fn    被保护的可调用对象 (返回值忽略)。
/// @return true = fn() 正常完成; false = 捕获到异常 (caller 据此决定后续动作)。
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

/// @brief run_guarded 带异常计数器重载; 每次异常 relaxed +1 后写入日志。
/// @param counter caller-owned 进程内异常计数器，可作为观测或导出载体。
/// @note 计数器仅在异常路径自增; 正常完成不触碰, 不引入 hot-path 原子写。
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
    std::size_t index = 0;                     ///< fan-out 序号 (如 ctx_idx)
    bool has_index = false;                    ///< index 是否有效
    const char* id = nullptr;                  ///< 标识 (如 sid / execid)
    std::atomic<uint64_t>* counter = nullptr;  ///< 可选异常计数器 (nullable)
};

/// @brief 构造 fan-out item 上下文 (序号 + 标识 + 可选计数器)。
inline GuardCtx guard_item(std::size_t index, const char* id,
                           std::atomic<uint64_t>* counter = nullptr) {
    GuardCtx c;
    c.index = index;
    c.has_index = true;
    c.id = id;
    c.counter = counter;
    return c;
}

/// @brief 构造仅含标识的上下文 (如 executer execid)。
inline GuardCtx guard_id(const char* id, std::atomic<uint64_t>* counter = nullptr) {
    GuardCtx c;
    c.id = id;
    c.counter = counter;
    return c;
}

namespace detail {
/// @brief 异常路径日志 + 计数 (冷路径; 按 ctx 字段选格式)。what=nullptr 表示非 std 异常。
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
} // namespace detail

/// @brief run_guarded 带日志上下文重载 (fan-out item / 带 id / 带计数器)。
/// @note ctx 字段仅在异常路径格式化, success 路径零开销; 适合 hot-path fan-out。
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

} // namespace xt_log
