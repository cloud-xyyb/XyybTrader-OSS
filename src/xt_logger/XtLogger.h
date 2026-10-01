#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>

#include <spdlog/spdlog.h>
#include "XtLoggerCfg.h"

/// @brief 进程级异步日志入口。
///
/// - 全部状态由 static API 管理，构造 XtLogger 对象不会创建独立 logger。
/// - 日志写入 spdlog async 队列、由后台线程落盘
/// - 队列满时丢弃最旧一条，不阻塞调用方。
/// - module filter 与 throttle 可按模块/key 控制输出量
/// - 未初始化时所有调用为 no-op
class XtLogger {
public:
    using Config = XtLoggerCfg;

    // ── 初始化与配置 ──────────────────────────────────────────────
    /// @brief 初始化进程级 async logger。
    ///
    /// 配置完成后原子发布 logger snapshot。`queue_size` 和 `thread_count` 传给 spdlog
    /// 全局 async thread pool；队列满时丢弃最旧排队日志。
    static void init_fast(const char* logfile,
                          LogLevel level = LogLevel::Info,
                          size_t max_size = 50 * 1024 * 1024,
                          size_t max_files = 10,
                          size_t queue_size = 8192,
                          size_t thread_count = 1,
                          bool console = false);
    /// @brief 使用配置对象初始化 logger，并随后应用 module filter 与 throttle 配置。
    static void init(const Config& cfg);

    /// @brief 调整进程级最低日志级别；logger 尚未初始化时为 no-op。
    static void set_level(LogLevel level);
    /// @brief 设置 module filter；未显式配置的 module 默认启用。
    static void set_module_enabled(const std::string& module, bool enabled);
    /// @brief 设置 throttle key 对应的窗口毫秒数；`ms <= 0` 表示不节流。
    static void set_throttle_ms(const std::string& key, int ms);
    /// @brief 读取 throttle key 的窗口毫秒数；未知 key 返回 0。
    static int throttle_ms(const std::string& key);

    // ── 通用日志接口 ──────────────────────────────────────────────
    template <typename... Args>
    static void trace(const char* fmt, const Args&... args) {
        log_impl(LogLevel::Trace, fmt, args...);
    }

    template <typename... Args>
    static void debug(const char* fmt, const Args&... args) {
        log_impl(LogLevel::Debug, fmt, args...);
    }

    template <typename... Args>
    static void info(const char* fmt, const Args&... args) {
        log_impl(LogLevel::Info, fmt, args...);
    }

    template <typename... Args>
    static void warn(const char* fmt, const Args&... args) {
        log_impl(LogLevel::Warn, fmt, args...);
    }

    template <typename... Args>
    static void error(const char* fmt, const Args&... args) {
        log_impl(LogLevel::Error, fmt, args...);
    }

    template <typename... Args>
    static void critical(const char* fmt, const Args&... args) {
        log_impl(LogLevel::Critical, fmt, args...);
    }

    // ── 分模块日志接口 ──────────────────────────────────────────────
    template <typename... Args>
    static void trace_m(const char* module, const char* fmt, const Args&... args) {
        log_impl_mod(LogLevel::Trace, module, fmt, args...);
    }

    template <typename... Args>
    static void debug_m(const char* module, const char* fmt, const Args&... args) {
        log_impl_mod(LogLevel::Debug, module, fmt, args...);
    }

    template <typename... Args>
    static void info_m(const char* module, const char* fmt, const Args&... args) {
        log_impl_mod(LogLevel::Info, module, fmt, args...);
    }

    template <typename... Args>
    static void warn_m(const char* module, const char* fmt, const Args&... args) {
        log_impl_mod(LogLevel::Warn, module, fmt, args...);
    }

    template <typename... Args>
    static void error_m(const char* module, const char* fmt, const Args&... args) {
        log_impl_mod(LogLevel::Error, module, fmt, args...);
    }

    // ── 节流日志接口 ──────────────────────────────────────────────
    template <typename... Args>
    static void info_mt(const char* module, const char* key, const char* fmt, const Args&... args) {
        log_impl_mod_throttle(LogLevel::Info, module, key, fmt, args...);
    }

    template <typename... Args>
    static void warn_mt(const char* module, const char* key, const char* fmt, const Args&... args) {
        log_impl_mod_throttle(LogLevel::Warn, module, key, fmt, args...);
    }

    template <typename... Args>
    static void error_mt(const char* module, const char* key, const char* fmt, const Args&... args) {
        log_impl_mod_throttle(LogLevel::Error, module, key, fmt, args...);
    }

    // ── 全局关闭 ──────────────────────────────────────────────
    /// @brief flush 当前 logger，关闭 spdlog，并重置进程级 logger snapshot。
    static void shutdown();

private:
    // ── 内部状态与工具函数 ──────────────────────────────────────────────
    /** @brief 获取/创建全局 logger 实例。 */
    static std::shared_ptr<spdlog::logger> logger();  // 返回原子快照副本（防与 shutdown reset 竞争）
    /** @brief 将项目日志级别映射到 spdlog 级别。 */
    static int to_spd_level(LogLevel level);
    /** @brief 判断模块日志是否启用。 */
    static bool module_enabled(const std::string& module);
    /** @brief 判断 key 是否应被节流。 */
    static bool should_throttle(const std::string& key, int ms);

    // ── 内部日志实现模板 ──────────────────────────────────────────────
    template <typename... Args>
    static void log_impl(LogLevel level, const char* fmt, const Args&... args) {
        auto lg = logger();  // 本地副本：!lg 检查与 lg->log 用同一快照，消除 TOCTOU
        if (!lg)
            return;
        // spdlog 1.12+ 的 logger::log() variadic 重载要求 format_string_t 在编译期校验;
        // XtLogger 的 fmt 是运行时 const char*, 必须用 SPDLOG_FMT_RUNTIME 旁路 consteval check.
        lg->log(static_cast<spdlog::level::level_enum>(to_spd_level(level)),
                SPDLOG_FMT_RUNTIME(fmt), args...);
    }

    template <typename... Args>
    static void log_impl_mod(LogLevel level, const char* module, const char* fmt, const Args&... args) {
        if (!module || !module[0]) {
            log_impl(level, fmt, args...);
            return;
        }
        if (!module_enabled(module))
            return;
        auto lg = logger();  // 本地副本：!lg 检查与 lg->log 用同一快照，消除 TOCTOU
        if (!lg)
            return;
        std::string f = std::string("[") + module + "] " + fmt;
        lg->log(static_cast<spdlog::level::level_enum>(to_spd_level(level)),
                SPDLOG_FMT_RUNTIME(f.c_str()), args...);
    }

    template <typename... Args>
    static void log_impl_mod_throttle(LogLevel level, const char* module, const char* key, const char* fmt, const Args&... args) {
        int ms = throttle_ms(key ? key : "");
        if (ms > 0 && should_throttle(std::string(module ? module : "") + ":" + (key ? key : ""), ms))
            return;
        log_impl_mod(level, module, fmt, args...);
    }
};
